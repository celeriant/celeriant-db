import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

/**
 * Celeriant kTLS test cluster on EC2.
 *
 * Mirrors the RPi cluster setup (see docs/pending/rpi-ktls-testbed.md):
 *   - 2 data nodes (leader + follower) with dual-CA mTLS, local NVMe storage
 *   - 1 client node for running benchmarks and CLI
 *   - Real S3 for cluster coordination (replaces MinIO)
 *   - Grafana Cloud for observability (replaces self-hosted Grafana/Prometheus/Loki)
 *
 * Default instance: c6id.2xlarge (8 vCPUs, 16GB RAM, 1x 474GB NVMe)
 * Data nodes use the local NVMe for /var/lib/celeriant (formatted XFS on boot).
 */
export class Ec2ClusterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // --- Context values (override with -c key=value) ---
    const instanceType = this.node.tryGetContext('instanceType') ?? 'c6id.2xlarge';
    const keyPairName = this.node.tryGetContext('keyPair');

    // Grafana Cloud (optional — set all three to enable)
    const grafanaPromUser = this.node.tryGetContext('grafanaPromUser') ?? '';
    const grafanaPromUrl = this.node.tryGetContext('grafanaPromUrl') ?? '';
    const grafanaLokiUser = this.node.tryGetContext('grafanaLokiUser') ?? '';
    const grafanaLokiUrl = this.node.tryGetContext('grafanaLokiUrl') ?? '';
    const grafanaApiKey = this.node.tryGetContext('grafanaApiKey') ?? '';
    const grafanaEnabled = grafanaApiKey && grafanaPromUrl && grafanaLokiUrl;

    // --- VPC (default VPC) ---
    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true });

    // --- S3 bucket for cluster coordination ---
    const bucket = new s3.Bucket(this, 'ClusterBucket', {
      bucketName: `celeriant-ktls-test-${cdk.Aws.ACCOUNT_ID}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // --- IAM role for data nodes (S3 access) ---
    const nodeRole = new iam.Role(this, 'NodeRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });
    bucket.grantReadWrite(nodeRole);

    // --- IAM role for client node (no S3 needed, just SSM) ---
    const clientRole = new iam.Role(this, 'ClientRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });

    // --- Security group ---
    const sg = new ec2.SecurityGroup(this, 'ClusterSg', {
      vpc,
      description: 'Celeriant kTLS test cluster',
      allowAllOutbound: true,
    });

    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(22), 'SSH');
    sg.addIngressRule(sg, ec2.Port.tcp(10000), 'Celeriant client port');
    sg.addIngressRule(sg, ec2.Port.tcp(10001), 'Celeriant replication port');
    sg.addIngressRule(sg, ec2.Port.tcp(9090), 'Prometheus metrics');

    // --- AMI: Amazon Linux 2023 (x86_64) ---
    const ami = ec2.MachineImage.latestAmazonLinux2023({
      cpuType: ec2.AmazonLinuxCpuType.X86_64,
    });

    // --- Key pair (optional — SSM works without it) ---
    const keyPair = keyPairName
      ? ec2.KeyPair.fromKeyPairName(this, 'KeyPair', keyPairName)
      : undefined;

    // --- User data ---
    const commonSetup = [
      '#!/bin/bash',
      'set -euo pipefail',
      '',
      '# kTLS kernel module',
      'modprobe tls',
      'echo tls > /etc/modules-load.d/tls.conf',
      '',
      '# File descriptor and memlock limits',
      "cat > /etc/security/limits.d/celeriant.conf <<'LIMITS'",
      '*  soft  nofile  1048576',
      '*  hard  nofile  1048576',
      '*  soft  memlock  unlimited',
      '*  hard  memlock  unlimited',
      'LIMITS',
      '',
      'sysctl -w fs.file-max=1048576',
      "echo 'fs.file-max = 1048576' > /etc/sysctl.d/99-celeriant.conf",
      'sysctl -p /etc/sysctl.d/99-celeriant.conf',
      '',
      'dnf install -y tar gzip',
    ];

    // NVMe detection, format, and mount for data nodes
    const nvmeSetup = [
      '',
      '# Mount local NVMe instance store for data',
      '# Find the first NVMe instance store device (skip root EBS which is also NVMe)',
      'DATA_DEV=""',
      'for dev in /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1; do',
      '  if [[ -b "$dev" ]]; then',
      '    DATA_DEV="$dev"',
      '    break',
      '  fi',
      'done',
      '',
      'if [[ -n "$DATA_DEV" ]]; then',
      '  mkfs.xfs -f "$DATA_DEV"',
      '  mkdir -p /var/lib/celeriant',
      '  mount -o noatime "$DATA_DEV" /var/lib/celeriant',
      '  echo "$DATA_DEV /var/lib/celeriant xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      '  echo "Mounted $DATA_DEV as /var/lib/celeriant"',
      'else',
      '  echo "WARNING: No NVMe instance store found, using root EBS"',
      '  mkdir -p /var/lib/celeriant',
      'fi',
      '',
      'mkdir -p /etc/celeriant/certs',
    ];

    // Grafana Alloy agent for shipping metrics + logs to Grafana Cloud
    const alloySetup = grafanaEnabled ? [
      '',
      '# Install Grafana Alloy for metrics + log shipping',
      'dnf install -y dnf-plugins-core',
      'cat > /etc/yum.repos.d/grafana.repo <<\'GRAFANAREPO\'',
      '[grafana]',
      'name=grafana',
      'baseurl=https://rpm.grafana.com',
      'repo_gpgcheck=1',
      'enabled=1',
      'gpgcheck=1',
      'gpgkey=https://rpm.grafana.com/gpg.key',
      'sslverify=1',
      'sslcacert=/etc/pki/tls/certs/ca-bundle.crt',
      'GRAFANAREPO',
      'dnf install -y alloy',
      '',
      'HOSTNAME=$(hostname)',
      `cat > /etc/alloy/config.alloy <<'ALLOYEOF'`,
      'prometheus.scrape "celeriant" {',
      '  targets = [{"__address__" = "localhost:9090"}]',
      '  scrape_interval = "5s"',
      '  forward_to = [prometheus.remote_write.grafana_cloud.receiver]',
      '}',
      '',
      'prometheus.remote_write "grafana_cloud" {',
      '  endpoint {',
      `    url = "${grafanaPromUrl}"`,
      '    basic_auth {',
      `      username = "${grafanaPromUser}"`,
      `      password = "${grafanaApiKey}"`,
      '    }',
      '  }',
      '}',
      '',
      'loki.source.journal "celeriant" {',
      '  relabel_rules = loki.relabel.journal.rules',
      '  forward_to = [loki.write.grafana_cloud.receiver]',
      '  matches = "_SYSTEMD_UNIT=celeriant.service"',
      '}',
      '',
      'loki.relabel "journal" {',
      '  forward_to = []',
      '  rule {',
      '    source_labels = ["__journal__systemd_unit"]',
      '    target_label = "unit"',
      '  }',
      '  rule {',
      '    source_labels = ["__journal__hostname"]',
      '    target_label = "node"',
      '  }',
      '}',
      '',
      'loki.write "grafana_cloud" {',
      '  endpoint {',
      `    url = "${grafanaLokiUrl}"`,
      '    basic_auth {',
      `      username = "${grafanaLokiUser}"`,
      `      password = "${grafanaApiKey}"`,
      '    }',
      '  }',
      '}',
      'ALLOYEOF',
      '',
      'systemctl enable --now alloy',
    ] : [];

    const nodeUserData = [...commonSetup, ...nvmeSetup, ...alloySetup].join('\n');
    const clientUserData = [...commonSetup, '', 'mkdir -p /etc/celeriant/certs'].join('\n');

    // --- Helper to create instances ---
    const createInstance = (
      name: string,
      role: iam.IRole,
      userData: string,
    ): ec2.Instance => {
      const ud = ec2.UserData.forLinux();
      ud.addCommands(userData);

      const instance = new ec2.Instance(this, name, {
        vpc,
        instanceType: new ec2.InstanceType(instanceType),
        machineImage: ami,
        securityGroup: sg,
        role,
        userData: ud,
        keyPair,
        blockDevices: [{
          deviceName: '/dev/xvda',
          volume: ec2.BlockDeviceVolume.ebs(20, {
            volumeType: ec2.EbsDeviceVolumeType.GP3,
          }),
        }],
        vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      });

      cdk.Tags.of(instance).add('Name', `celeriant-${name.toLowerCase()}`);
      cdk.Tags.of(instance).add('Project', 'celeriant-ktls-test');

      return instance;
    };

    // --- Instances ---
    const leader = createInstance('Leader', nodeRole, nodeUserData);
    const follower = createInstance('Follower', nodeRole, nodeUserData);
    const client = createInstance('Client', clientRole, clientUserData);

    // --- Outputs ---
    new cdk.CfnOutput(this, 'LeaderPrivateIp', { value: leader.instancePrivateIp });
    new cdk.CfnOutput(this, 'FollowerPrivateIp', { value: follower.instancePrivateIp });
    new cdk.CfnOutput(this, 'ClientPrivateIp', { value: client.instancePrivateIp });
    new cdk.CfnOutput(this, 'LeaderPublicIp', { value: leader.instancePublicIp });
    new cdk.CfnOutput(this, 'FollowerPublicIp', { value: follower.instancePublicIp });
    new cdk.CfnOutput(this, 'ClientPublicIp', { value: client.instancePublicIp });
    new cdk.CfnOutput(this, 'BucketName', { value: bucket.bucketName });
    new cdk.CfnOutput(this, 'Region', { value: cdk.Aws.REGION });
    new cdk.CfnOutput(this, 'LeaderInstanceId', { value: leader.instanceId });
    new cdk.CfnOutput(this, 'FollowerInstanceId', { value: follower.instanceId });
    new cdk.CfnOutput(this, 'ClientInstanceId', { value: client.instanceId });
  }
}
