import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

/**
 * Kafka KRaft benchmark cluster on EC2.
 *
 * For comparison benchmarking against the Celeriant ec2-cluster.
 *   - 3 Kafka KRaft brokers (combined controller+broker, no ZooKeeper)
 *   - 2 client nodes for running kafka-producer-perf-test / kafka-consumer-perf-test
 *   - TLS enabled by default (configurable via -c tls=false)
 *   - Same instance types as Celeriant cluster for hardware parity
 *
 * Storage: matches Celeriant setup — instance-store (NVMe) or EBS.
 *
 * Architecture: auto-detected from instance type family (ARM vs x86_64).
 */
export class Ec2KafkaClusterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // --- Context values (override with -c key=value) ---
    const instanceType = this.node.tryGetContext('instanceType') ?? 'i4i.8xlarge';
    const clientInstanceType = this.node.tryGetContext('clientInstanceType') ?? 'c7i.4xlarge';
    const clientCount = Math.min(parseInt(this.node.tryGetContext('clientCount') ?? '2', 10), 4);
    const keyPairName = this.node.tryGetContext('keyPair');
    const storageType = this.node.tryGetContext('storageType') ?? 'instance-store';
    const ebsDataVolumeSize = parseInt(this.node.tryGetContext('ebsDataVolumeSize') ?? '100', 10);
    const kafkaVersion = this.node.tryGetContext('kafkaVersion') ?? '4.0.2';

    // ARM detection (same logic as ec2-cluster)
    const isArmFamily = (type: string): boolean => {
      const family = type.split('.')[0];
      return /g[dn]?$/.test(family);
    };
    const dataIsArm = isArmFamily(instanceType);
    const clientIsArm = isArmFamily(clientInstanceType);
    if (dataIsArm !== clientIsArm) {
      throw new Error(
        `Architecture mismatch: brokers (${instanceType}) and clients (${clientInstanceType}) ` +
        `must be the same architecture.`
      );
    }
    const cpuType = dataIsArm ? ec2.AmazonLinuxCpuType.ARM_64 : ec2.AmazonLinuxCpuType.X86_64;

    // --- VPC (default VPC) ---
    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true });

    // --- IAM role (SSM access only — no S3 needed for Kafka) ---
    const nodeRole = new iam.Role(this, 'NodeRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });

    // --- Security group ---
    const sg = new ec2.SecurityGroup(this, 'ClusterSg', {
      vpc,
      description: 'Kafka KRaft benchmark cluster',
      allowAllOutbound: true,
    });

    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(22), 'SSH');
    sg.addIngressRule(sg, ec2.Port.tcp(9092), 'Kafka client port (PLAINTEXT)');
    sg.addIngressRule(sg, ec2.Port.tcp(9093), 'Kafka client port (SSL)');
    sg.addIngressRule(sg, ec2.Port.tcp(9094), 'Kafka controller port');
    sg.addIngressRule(sg, ec2.Port.tcp(9090), 'Prometheus/JMX metrics');

    // --- AMI ---
    const ami = ec2.MachineImage.latestAmazonLinux2023({ cpuType });

    // --- Key pair ---
    const keyPair = keyPairName
      ? ec2.KeyPair.fromKeyPairName(this, 'KeyPair', keyPairName)
      : undefined;

    // --- User data: common setup ---
    const commonSetup = [
      '#!/bin/bash',
      'set -euo pipefail',
      '',
      '# File descriptor limits (Kafka needs many open files)',
      "cat > /etc/security/limits.d/kafka.conf <<'LIMITS'",
      '*  soft  nofile  1048576',
      '*  hard  nofile  1048576',
      'LIMITS',
      '',
      'sysctl -w fs.file-max=1048576',
      'sysctl -w net.ipv4.ip_local_port_range="1024 65535"',
      'sysctl -w net.core.somaxconn=65535',
      "cat > /etc/sysctl.d/99-kafka.conf <<'SYSCTL'",
      'fs.file-max = 1048576',
      'net.ipv4.ip_local_port_range = 1024 65535',
      'net.core.somaxconn = 65535',
      'SYSCTL',
      'sysctl -p /etc/sysctl.d/99-kafka.conf',
      '',
      '# Install Java 21 (Kafka 3.7+ supports it)',
      'dnf install -y java-21-amazon-corretto-headless tar gzip',
      '',
      `# Download and install Kafka ${kafkaVersion}`,
      `KAFKA_URL="https://dlcdn.apache.org/kafka/${kafkaVersion}/kafka_2.13-${kafkaVersion}.tgz"`,
      'cd /opt',
      'curl -sL "$KAFKA_URL" -o kafka.tgz',
      'tar xzf kafka.tgz',
      `ln -s kafka_2.13-${kafkaVersion} kafka`,
      'rm kafka.tgz',
      '',
      '# Create kafka user and directories',
      'useradd -r -s /sbin/nologin kafka',
      'mkdir -p /etc/kafka/certs /opt/kafka/logs',
      'chown kafka:kafka /opt/kafka/logs',
    ];

    // Storage setup for broker nodes
    const storageSetup = storageType === 'ebs' ? [
      '',
      '# Mount dedicated EBS data volume',
      'DATA_DEV=""',
      'for i in $(seq 1 30); do',
      '  for dev in /dev/nvme1n1 /dev/xvdb; do',
      '    if [[ -b "$dev" ]]; then DATA_DEV="$dev"; break 2; fi',
      '  done',
      '  sleep 1',
      'done',
      '',
      'if [[ -n "$DATA_DEV" ]]; then',
      '  mkfs.xfs -f "$DATA_DEV"',
      '  mkdir -p /var/lib/kafka',
      '  mount -o noatime "$DATA_DEV" /var/lib/kafka',
      '  echo "$DATA_DEV /var/lib/kafka xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      'else',
      '  echo "ERROR: EBS data volume not found after 30s"',
      '  mkdir -p /var/lib/kafka',
      'fi',
    ] : [
      '',
      '# Mount local NVMe instance store for data',
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
      '  mkdir -p /var/lib/kafka',
      '  mount -o noatime "$DATA_DEV" /var/lib/kafka',
      '  echo "$DATA_DEV /var/lib/kafka xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      'else',
      '  echo "WARNING: No NVMe instance store found, using root EBS"',
      '  mkdir -p /var/lib/kafka',
      'fi',
    ];

    const serviceSetup = [
      '',
      'chown -R kafka:kafka /var/lib/kafka',
      '',
      '# Systemd service for Kafka',
      "cat > /etc/systemd/system/kafka.service <<'SERVICE'",
      '[Unit]',
      'Description=Apache Kafka (KRaft)',
      'After=network-online.target',
      'Wants=network-online.target',
      '',
      '[Service]',
      'Type=simple',
      'User=kafka',
      'Group=kafka',
      'EnvironmentFile=/etc/kafka/kafka.env',
      'ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties',
      'ExecStop=/opt/kafka/bin/kafka-server-stop.sh',
      'Restart=on-failure',
      'RestartSec=5',
      'LimitNOFILE=1048576',
      '',
      '[Install]',
      'WantedBy=multi-user.target',
      'SERVICE',
      'systemctl daemon-reload',
    ];

    const brokerUserData = [
      ...commonSetup, ...storageSetup, ...serviceSetup,
    ].join('\n');
    const clientUserData = [
      ...commonSetup, '', 'mkdir -p /etc/kafka/certs',
    ].join('\n');

    // --- Helper to create instances ---
    const createInstance = (
      name: string,
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
        role: nodeRole,
        userData: ud,
        keyPair,
        blockDevices,
        vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      });

      cdk.Tags.of(instance).add('Name', `kafka-bench-${name.toLowerCase()}`);
      cdk.Tags.of(instance).add('Project', 'kafka-benchmark');

      return instance;
    };

    // --- Data volume for EBS storage mode ---
    const dataBlockDevices: ec2.BlockDevice[] | undefined = storageType === 'ebs'
      ? [{
        deviceName: '/dev/xvdb',
        volume: ec2.BlockDeviceVolume.ebs(ebsDataVolumeSize, {
          volumeType: ec2.EbsDeviceVolumeType.GP3,
        }),
      }]
      : undefined;

    // --- Instances: 3 brokers + N clients ---
    const broker1 = createInstance('Broker1', brokerUserData, instanceType, dataBlockDevices);
    const broker2 = createInstance('Broker2', brokerUserData, instanceType, dataBlockDevices);
    const broker3 = createInstance('Broker3', brokerUserData, instanceType, dataBlockDevices);

    const clients: ec2.Instance[] = [];
    for (let i = 1; i <= clientCount; i++) {
      clients.push(createInstance(`Client${i}`, clientUserData, clientInstanceType));
    }

    // --- Outputs ---
    const brokers = [broker1, broker2, broker3];
    brokers.forEach((b, idx) => {
      const n = idx + 1;
      new cdk.CfnOutput(this, `Broker${n}PrivateIp`, { value: b.instancePrivateIp });
      new cdk.CfnOutput(this, `Broker${n}PublicIp`, { value: b.instancePublicIp });
      new cdk.CfnOutput(this, `Broker${n}InstanceId`, { value: b.instanceId });
    });

    clients.forEach((c, idx) => {
      const n = idx + 1;
      new cdk.CfnOutput(this, `Client${n}PublicIp`, { value: c.instancePublicIp });
      new cdk.CfnOutput(this, `Client${n}PrivateIp`, { value: c.instancePrivateIp });
      new cdk.CfnOutput(this, `Client${n}InstanceId`, { value: c.instanceId });
    });

    new cdk.CfnOutput(this, 'Region', { value: cdk.Aws.REGION });
    new cdk.CfnOutput(this, 'InstanceType', { value: instanceType });
    new cdk.CfnOutput(this, 'ClientInstanceType', { value: clientInstanceType });
    new cdk.CfnOutput(this, 'ClientCount', { value: String(clientCount) });
    new cdk.CfnOutput(this, 'StorageType', { value: storageType });
    new cdk.CfnOutput(this, 'Architecture', { value: dataIsArm ? 'arm64' : 'x86_64' });
    new cdk.CfnOutput(this, 'KafkaVersion', { value: kafkaVersion });
  }
}
