#!/bin/bash

# Must run as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (sudo ./tune-host-network.sh)"
    exit 1
fi

echo "🔧 Applying network performance tuning to host..."

# Increase connection queue limits
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=8192

# Increase port range for outbound connections
sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Enable TCP fast open
sysctl -w net.ipv4.tcp_fastopen=3

# Reuse TIME_WAIT sockets faster
sysctl -w net.ipv4.tcp_tw_reuse=1

# Increase network buffer sizes
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 67108864"
sysctl -w net.ipv4.tcp_wmem="4096 65536 67108864"

# Increase backlog queues
sysctl -w net.core.netdev_max_backlog=16384 2>/dev/null || echo "  ⊘ netdev_max_backlog not available"

echo ""
echo "✅ Host network tuning applied!"
echo "These settings will persist until reboot."
echo ""
echo "To make permanent, add to /etc/sysctl.conf:"
echo "  sudo tee -a /etc/sysctl.conf <<EOF
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=8192
net.ipv4.ip_local_port_range=1024 65535
net.ipv4.tcp_fastopen=3
net.ipv4.tcp_tw_reuse=1
net.core.rmem_max=134217728
net.core.wmem_max=134217728
net.ipv4.tcp_rmem=4096 87380 67108864
net.ipv4.tcp_wmem=4096 65536 67108864
EOF"