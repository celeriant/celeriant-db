import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

/**
 * Celeriant performance test cluster on EC2.
 *
 * Mirrors the RPi cluster setup (deploy/rpi-cluster):
 *   - 2 data nodes (leader + follower) with dual-CA mTLS, systemd services
 *   - 1 client node for running benchmarks and CLI
 *   - Real S3 for cluster coordination (replaces MinIO)
 *   - Self-hosted Grafana/Prometheus/Loki on client #1 (see scripts/setup-infra.sh)
 *
 * Storage modes:
 *   - instance-store (default): local NVMe for /var/lib/celeriant (e.g. c6id, i4i, i4g)
 *   - ebs: dedicated gp3 volume for /var/lib/celeriant (e.g. t3, m5, c5)
 *
 * Architecture: auto-detected from instance type family. ARM families (i4g, c7g, etc.)
 * get an ARM AMI; all others get x86_64. Both data and client nodes must be the same
 * architecture (they share binaries built with `make build` or `make build-arm`).
 */
export class Ec2ClusterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // --- Context values (override with -c key=value) ---
    const instanceType = this.node.tryGetContext('instanceType') ?? 'c6id.2xlarge';
    const clientInstanceType = this.node.tryGetContext('clientInstanceType') ?? instanceType;
    const clientCount = Math.min(parseInt(this.node.tryGetContext('clientCount') ?? '1', 10), 4);
    const keyPairName = this.node.tryGetContext('keyPair');
    const storageType = this.node.tryGetContext('storageType') ?? 'instance-store';
    const ebsDataVolumeSize = parseInt(this.node.tryGetContext('ebsDataVolumeSize') ?? '100', 10);
    // gp3 ships at 3,000 IOPS / 125 MB/s, which makes any comparison against local NVMe a
    // measurement of the default rather than of EBS. IOPS is settable here; THROUGHPUT IS NOT
    // — CloudFormation's AWS::EC2::Instance block device mapping has no Throughput field, so
    // CDK drops it with a warning and the volume launches at 125 MB/s. Raise it after deploy:
    //   aws ec2 modify-volume --volume-id <id> --throughput 1000
    // See the EBS section of README.md.
    const ebsIops = parseInt(this.node.tryGetContext('ebsIops') ?? '16000', 10);
    // RAID0-stripe all instance-store NVMes into one volume. Default on: for an append-only
    // event store the main win is capacity (full aggregate of every drive — more events on
    // local NVMe before compaction/S3 offload), plus ~+32% throughput on the 16xlarge where a
    // single drive saturates. Harmless on single-NVMe instances (mounts the one drive),
    // throughput-neutral on the 8xlarge. -c raid0=false opts out.
    const raid0 = String(this.node.tryGetContext('raid0') ?? 'true') === 'true';
    // Spot pricing for every instance. A benchmark cluster lives for under an hour and is
    // rebuilt from scratch each time, so an interruption costs a re-run, not data — and spot
    // is ~70% cheaper (i4i.16xlarge in ap-southeast-2: ~$1.93/hr vs $6.58 on-demand).
    // No maxPrice: capping at anything below on-demand only adds capacity failures.
    // -c spot=false reverts to on-demand.
    const spot = String(this.node.tryGetContext('spot') ?? 'true') === 'true';

    // Detect ARM (Graviton) instance types for correct AMI selection.
    // ARM families end in 'g' or 'gn' before the dot (i4g, c7g, c7gn, im4gn, is4gen, etc.)
    const isArmFamily = (type: string): boolean => {
      const family = type.split('.')[0];
      return /g[dn]?$/.test(family);
    };
    const dataIsArm = isArmFamily(instanceType);
    const clientIsArm = isArmFamily(clientInstanceType);
    if (dataIsArm !== clientIsArm) {
      throw new Error(
        `Architecture mismatch: data nodes (${instanceType}) and client (${clientInstanceType}) ` +
        `must be the same architecture. Both must be ARM or both must be x86_64.`
      );
    }
    const cpuType = dataIsArm ? ec2.AmazonLinuxCpuType.ARM_64 : ec2.AmazonLinuxCpuType.X86_64;

    // Home IP (CIDR) allowed to reach Grafana on :3000 — e.g. -c homeIp=1.2.3.4/32
    const homeIp = this.node.tryGetContext('homeIp') ?? '';

    // Pin every instance to one AZ. Same-AZ placement is part of the benchmark methodology
    // (LAN-equivalent latency between the nodes), and spot capacity is per-AZ — when a family
    // runs dry in the default AZ the whole stack rolls back, so the AZ has to be movable.
    const az = this.node.tryGetContext('az');

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
    sg.addIngressRule(sg, ec2.Port.tcp(3100), 'Loki log ingest (intra-cluster)');
    if (homeIp) {
      sg.addIngressRule(ec2.Peer.ipv4(homeIp), ec2.Port.tcp(3000), 'Grafana (home IP)');
    }

    // --- AMI: Amazon Linux 2023 (auto-detect x86_64 or ARM64) ---
    const ami = ec2.MachineImage.latestAmazonLinux2023({ cpuType });

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
      'sysctl -w net.ipv4.ip_local_port_range="1024 65535"',
      'sysctl -w net.core.somaxconn=65535',
      "cat > /etc/sysctl.d/99-celeriant.conf <<'SYSCTL'",
      'fs.file-max = 1048576',
      'net.ipv4.ip_local_port_range = 1024 65535',
      'net.core.somaxconn = 65535',
      'SYSCTL',
      'sysctl -p /etc/sysctl.d/99-celeriant.conf',
      '',
      'dnf install -y tar gzip sysstat',
    ];

    // Storage setup for data nodes — depends on storageType
    const storageSetup = storageType === 'ebs' ? [
      '',
      '# Mount dedicated EBS data volume.',
      '# Pick it by device MODEL, not by name: on instances that also have instance store',
      '# (i4i and friends) the local NVMes occupy /dev/nvme1n1 upward too, so a name-based',
      '# guess silently benchmarks instance store while reporting EBS. The root volume is',
      '# excluded by having a mountpoint; the data volume is bare.',
      'DATA_DEV=""',
      'for i in $(seq 1 30); do',
      '  for dev in /dev/nvme*n1; do',
      '    [[ -b "$dev" ]] || continue',
      '    model=$(cat /sys/block/$(basename "$dev")/device/model 2>/dev/null || true)',
      '    [[ "$model" == *"Elastic Block Store"* ]] || continue',
      '    [[ -z "$(lsblk -no MOUNTPOINT "$dev" | tr -d " \\n")" ]] || continue',
      '    DATA_DEV="$dev"; break 2',
      '  done',
      '  sleep 2',
      'done',
      '',
      'if [[ -n "$DATA_DEV" ]]; then',
      '  mkfs.xfs -f "$DATA_DEV"',
      '  mkdir -p /var/lib/celeriant',
      '  mount -o noatime "$DATA_DEV" /var/lib/celeriant',
      '  echo "$DATA_DEV /var/lib/celeriant xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      '  echo "Mounted $DATA_DEV as /var/lib/celeriant"',
      'else',
      '  echo "ERROR: EBS data volume not found after 30s"',
      '  mkdir -p /var/lib/celeriant',
      'fi',
    ] : raid0 ? [
      '',
      '# RAID0-stripe all NVMe instance-store devices into /dev/md0',
      'dnf install -y mdadm',
      'DEVS=()',
      'for dev in /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1; do',
      '  [[ -b "$dev" ]] && DEVS+=("$dev")',
      'done',
      '',
      'if [[ ${#DEVS[@]} -ge 2 ]]; then',
      '  mdadm --create /dev/md0 --level=0 --raid-devices=${#DEVS[@]} "${DEVS[@]}" --run',
      '  mkfs.xfs -f /dev/md0',
      '  mkdir -p /var/lib/celeriant',
      '  mount -o noatime /dev/md0 /var/lib/celeriant',
      '  mdadm --detail --scan >> /etc/mdadm.conf',
      '  echo "/dev/md0 /var/lib/celeriant xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      '  echo "Mounted RAID0 (/dev/md0) over ${#DEVS[@]} devices as /var/lib/celeriant"',
      'elif [[ ${#DEVS[@]} -eq 1 ]]; then',
      '  echo "Only one NVMe found — RAID0 needs 2+, mounting it directly"',
      '  mkfs.xfs -f "${DEVS[0]}"',
      '  mkdir -p /var/lib/celeriant',
      '  mount -o noatime "${DEVS[0]}" /var/lib/celeriant',
      '  echo "${DEVS[0]} /var/lib/celeriant xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      'else',
      '  echo "WARNING: No NVMe instance store found, using root EBS"',
      '  mkdir -p /var/lib/celeriant',
      'fi',
    ] : [
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
    ];

    const directoryAndServiceSetup = [
      '',
      'mkdir -p /etc/celeriant/certs',
      '',
      '# Systemd service (mirrors deploy/rpi-cluster/setup-nodes.sh)',
      "cat > /etc/systemd/system/celeriant.service <<'SERVICE'",
      '[Unit]',
      'Description=Celeriant Database',
      'After=network-online.target',
      'Wants=network-online.target',
      '',
      '[Service]',
      'Type=simple',
      'EnvironmentFile=/etc/celeriant/celeriant.env',
      'ExecStart=/usr/local/bin/celeriant',
      'Restart=on-failure',
      'RestartSec=5',
      'LimitNOFILE=1048576',
      'LimitMEMLOCK=infinity',
      '',
      '[Install]',
      'WantedBy=multi-user.target',
      'SERVICE',
      'systemctl daemon-reload',
    ];

    const nodeUserData = [
      ...commonSetup, ...storageSetup, ...directoryAndServiceSetup,
    ].join('\n');
    const clientUserData = [...commonSetup, '', 'mkdir -p /etc/celeriant/certs'].join('\n');

    // --- Spot launch template (market options only) ---
    // One-time request that terminates on reclaim: the cluster is rebuilt from scratch for
    // every benchmark, so there is nothing to preserve across an interruption.
    const spotTemplate = spot
      ? new ec2.LaunchTemplate(this, 'SpotTemplate', {
        spotOptions: {
          requestType: ec2.SpotRequestType.ONE_TIME,
          interruptionBehavior: ec2.SpotInstanceInterruption.TERMINATE,
        },
      })
      : undefined;

    // --- Helper to create instances ---
    const createInstance = (
      name: string,
      role: iam.IRole,
      userData: string,
      instType: string,
      extraBlockDevices?: ec2.BlockDevice[],
    ): ec2.Instance => {
      const ud = ec2.UserData.forLinux();
      ud.addCommands(userData);

      const blockDevices: ec2.BlockDevice[] = [{
        deviceName: '/dev/xvda',
        volume: ec2.BlockDeviceVolume.ebs(20, {
          volumeType: ec2.EbsDeviceVolumeType.GP3,
        }),
      }];
      if (extraBlockDevices) blockDevices.push(...extraBlockDevices);

      const instance = new ec2.Instance(this, name, {
        vpc,
        instanceType: new ec2.InstanceType(instType),
        machineImage: ami,
        securityGroup: sg,
        role,
        userData: ud,
        keyPair,
        blockDevices,
        vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC, ...(az && { availabilityZones: [az] }) },
      });

      if (spotTemplate) {
        // `AWS::EC2::Instance` has no market options in CloudFormation, so spot has to
        // arrive via a launch template. Only the market options come from the template —
        // everything else is set on the instance itself, which takes precedence.
        (instance.node.defaultChild as ec2.CfnInstance).launchTemplate = {
          launchTemplateId: spotTemplate.launchTemplateId,
          version: spotTemplate.latestVersionNumber,
        };
      }

      cdk.Tags.of(instance).add('Name', `celeriant-${name.toLowerCase()}`);
      cdk.Tags.of(instance).add('Project', 'celeriant-ktls-test');

      return instance;
    };

    // --- Data volume for EBS storage mode ---
    const dataBlockDevices: ec2.BlockDevice[] | undefined = storageType === 'ebs'
      ? [{
        deviceName: '/dev/xvdb',
        volume: ec2.BlockDeviceVolume.ebs(ebsDataVolumeSize, {
          volumeType: ec2.EbsDeviceVolumeType.GP3,
          iops: ebsIops,
        }),
      }]
      : undefined;

    // --- Instances ---
    const leader = createInstance('Leader', nodeRole, nodeUserData, instanceType, dataBlockDevices);
    const follower = createInstance('Follower', nodeRole, nodeUserData, instanceType, dataBlockDevices);

    const clients: ec2.Instance[] = [];
    for (let i = 1; i <= clientCount; i++) {
      const name = clientCount === 1 ? 'Client' : `Client${i}`;
      clients.push(createInstance(name, clientRole, clientUserData, clientInstanceType));
    }

    // --- Outputs ---
    new cdk.CfnOutput(this, 'LeaderPrivateIp', { value: leader.instancePrivateIp });
    new cdk.CfnOutput(this, 'FollowerPrivateIp', { value: follower.instancePrivateIp });
    new cdk.CfnOutput(this, 'LeaderPublicIp', { value: leader.instancePublicIp });
    new cdk.CfnOutput(this, 'FollowerPublicIp', { value: follower.instancePublicIp });
    new cdk.CfnOutput(this, 'BucketName', { value: bucket.bucketName });
    new cdk.CfnOutput(this, 'Region', { value: cdk.Aws.REGION });
    new cdk.CfnOutput(this, 'LeaderInstanceId', { value: leader.instanceId });
    new cdk.CfnOutput(this, 'FollowerInstanceId', { value: follower.instanceId });
    new cdk.CfnOutput(this, 'InstanceType', { value: instanceType });
    new cdk.CfnOutput(this, 'ClientInstanceType', { value: clientInstanceType });
    new cdk.CfnOutput(this, 'ClientCount', { value: String(clientCount) });
    new cdk.CfnOutput(this, 'StorageType', { value: storageType });
    if (storageType === 'ebs') {
      new cdk.CfnOutput(this, 'EbsSpec', { value: `gp3 ${ebsDataVolumeSize}GB ${ebsIops}iops` });
    }
    new cdk.CfnOutput(this, 'Raid0', { value: String(raid0) });
    new cdk.CfnOutput(this, 'Spot', { value: String(spot) });
    new cdk.CfnOutput(this, 'Architecture', { value: dataIsArm ? 'arm64' : 'x86_64' });

    // Client outputs — backward compatible: first client uses 'ClientPublicIp'/'ClientPrivateIp'
    clients.forEach((c, idx) => {
      const suffix = idx === 0 ? '' : String(idx + 1);
      new cdk.CfnOutput(this, `Client${suffix}PublicIp`, { value: c.instancePublicIp });
      new cdk.CfnOutput(this, `Client${suffix}PrivateIp`, { value: c.instancePrivateIp });
      new cdk.CfnOutput(this, `Client${suffix}InstanceId`, { value: c.instanceId });
    });
  }
}
