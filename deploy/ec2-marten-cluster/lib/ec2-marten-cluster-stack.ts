import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

/**
 * Marten (PostgreSQL) benchmark cluster on EC2.
 *
 * For comparison benchmarking against the Celeriant ec2-cluster.
 *   - 1 PostgreSQL node (single, no replica)
 *   - 1-4 client nodes for running marten-bench (.NET)
 *   - No TLS — plaintext connections
 *   - Same instance types as Celeriant cluster for hardware parity
 *
 * Storage: matches Celeriant setup — instance-store (NVMe) or EBS.
 *
 * Architecture: auto-detected from instance type family (ARM vs x86_64).
 */
export class Ec2MartenClusterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const instanceType = this.node.tryGetContext('instanceType') ?? 'i4i.8xlarge';
    const clientInstanceType = this.node.tryGetContext('clientInstanceType') ?? 'c7i.4xlarge';
    const clientCount = Math.min(parseInt(this.node.tryGetContext('clientCount') ?? '3', 10), 4);
    const keyPairName = this.node.tryGetContext('keyPair');
    const storageType = this.node.tryGetContext('storageType') ?? 'instance-store';
    const ebsDataVolumeSize = parseInt(this.node.tryGetContext('ebsDataVolumeSize') ?? '100', 10);
    const pgVersion = this.node.tryGetContext('pgVersion') ?? '17';

    // ARM detection (same logic as ec2-cluster)
    const isArmFamily = (type: string): boolean => {
      const family = type.split('.')[0];
      return /g[dn]?$/.test(family);
    };
    const dataIsArm = isArmFamily(instanceType);
    const clientIsArm = isArmFamily(clientInstanceType);
    if (dataIsArm !== clientIsArm) {
      throw new Error(
        `Architecture mismatch: PostgreSQL node (${instanceType}) and clients (${clientInstanceType}) ` +
        `must be the same architecture.`
      );
    }
    const cpuType = dataIsArm ? ec2.AmazonLinuxCpuType.ARM_64 : ec2.AmazonLinuxCpuType.X86_64;

    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true });

    const nodeRole = new iam.Role(this, 'NodeRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });

    const sg = new ec2.SecurityGroup(this, 'ClusterSg', {
      vpc,
      description: 'Marten/PostgreSQL benchmark cluster',
      allowAllOutbound: true,
    });

    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(22), 'SSH');
    sg.addIngressRule(sg, ec2.Port.tcp(5432), 'PostgreSQL');

    const ami = ec2.MachineImage.latestAmazonLinux2023({ cpuType });

    const keyPair = keyPairName
      ? ec2.KeyPair.fromKeyPairName(this, 'KeyPair', keyPairName)
      : undefined;

    // --- PostgreSQL node user data ---
    const pgSetup = [
      '#!/bin/bash',
      'set -euo pipefail',
      '',
      '# Kernel tuning',
      "cat > /etc/security/limits.d/postgres.conf <<'LIMITS'",
      '*  soft  nofile  1048576',
      '*  hard  nofile  1048576',
      'LIMITS',
      '',
      'sysctl -w fs.file-max=1048576',
      'sysctl -w net.ipv4.ip_local_port_range="1024 65535"',
      'sysctl -w net.core.somaxconn=65535',
      'sysctl -w vm.overcommit_memory=2',
      'sysctl -w vm.overcommit_ratio=90',
      "cat > /etc/sysctl.d/99-postgres.conf <<'SYSCTL'",
      'fs.file-max = 1048576',
      'net.ipv4.ip_local_port_range = 1024 65535',
      'net.core.somaxconn = 65535',
      'vm.overcommit_memory = 2',
      'vm.overcommit_ratio = 90',
      'vm.swappiness = 1',
      'SYSCTL',
      'sysctl -p /etc/sysctl.d/99-postgres.conf',
      '',
      '# Huge pages (optional, PostgreSQL will use if available)',
      'sysctl -w vm.nr_hugepages=8192',
      'echo "vm.nr_hugepages = 8192" >> /etc/sysctl.d/99-postgres.conf',
      '',
      `# Install PostgreSQL ${pgVersion} from AL2023 repo`,
      `dnf install -y postgresql${pgVersion}-server postgresql${pgVersion}-contrib`,
    ];

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
      '  mount -o noatime "$DATA_DEV" /var/lib/pgsql',
      '  echo "$DATA_DEV /var/lib/pgsql xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      'else',
      '  echo "ERROR: EBS data volume not found after 30s"',
      'fi',
    ] : [
      '',
      '# Mount local NVMe instance store for PostgreSQL data',
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
      '  mount -o noatime "$DATA_DEV" /var/lib/pgsql',
      '  echo "$DATA_DEV /var/lib/pgsql xfs defaults,noatime,nofail 0 2" >> /etc/fstab',
      'else',
      '  echo "WARNING: No NVMe instance store found, using root EBS"',
      'fi',
    ];

    const pgInitSetup = [
      '',
      'chown postgres:postgres /var/lib/pgsql',
      '',
      `# Initialize PostgreSQL data directory (AL2023 uses /var/lib/pgsql/data)`,
      `sudo -u postgres /usr/bin/initdb -D /var/lib/pgsql/data`,
      '',
      '# Systemd service is already installed by the RPM (postgresql)',
      'systemctl daemon-reload',
    ];

    const pgUserData = [...pgSetup, ...storageSetup, ...pgInitSetup].join('\n');

    // --- Client node user data ---
    const clientSetup = [
      '#!/bin/bash',
      'set -euo pipefail',
      '',
      '# Kernel tuning (same as PostgreSQL node)',
      "cat > /etc/security/limits.d/bench.conf <<'LIMITS'",
      '*  soft  nofile  1048576',
      '*  hard  nofile  1048576',
      'LIMITS',
      '',
      'sysctl -w fs.file-max=1048576',
      'sysctl -w net.ipv4.ip_local_port_range="1024 65535"',
      'sysctl -w net.core.somaxconn=65535',
      "cat > /etc/sysctl.d/99-bench.conf <<'SYSCTL'",
      'fs.file-max = 1048576',
      'net.ipv4.ip_local_port_range = 1024 65535',
      'net.core.somaxconn = 65535',
      'SYSCTL',
      'sysctl -p /etc/sysctl.d/99-bench.conf',
      '',
      '# Install libicu (required by .NET globalization) and .NET 10 SDK',
      'dnf install -y libicu',
      '',
      'rpm -Uvh https://packages.microsoft.com/config/centos/9/packages-microsoft-prod.rpm 2>/dev/null || true',
      'dnf install -y dotnet-sdk-10.0 || {',
      '  # Fallback: install via script if RPM repo does not have .NET 10 yet',
      '  curl -sSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 10.0 --install-dir /usr/share/dotnet',
      '  ln -sf /usr/share/dotnet/dotnet /usr/local/bin/dotnet',
      '}',
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

      cdk.Tags.of(instance).add('Name', `marten-bench-${name.toLowerCase()}`);
      cdk.Tags.of(instance).add('Project', 'marten-benchmark');

      return instance;
    };

    const dataBlockDevices: ec2.BlockDevice[] | undefined = storageType === 'ebs'
      ? [{
        deviceName: '/dev/xvdb',
        volume: ec2.BlockDeviceVolume.ebs(ebsDataVolumeSize, {
          volumeType: ec2.EbsDeviceVolumeType.GP3,
        }),
      }]
      : undefined;

    // --- Instances: 1 PostgreSQL node + N clients ---
    const pgNode = createInstance('PostgreSQL', pgUserData, instanceType, dataBlockDevices);

    const clients: ec2.Instance[] = [];
    for (let i = 1; i <= clientCount; i++) {
      clients.push(createInstance(`Client${i}`, clientSetup, clientInstanceType));
    }

    // --- Outputs ---
    new cdk.CfnOutput(this, 'PostgreSQLPrivateIp', { value: pgNode.instancePrivateIp });
    new cdk.CfnOutput(this, 'PostgreSQLPublicIp', { value: pgNode.instancePublicIp });
    new cdk.CfnOutput(this, 'PostgreSQLInstanceId', { value: pgNode.instanceId });

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
    new cdk.CfnOutput(this, 'PgVersion', { value: pgVersion });
  }
}
