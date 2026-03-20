#!/usr/bin/env bash
# Build and install a kTLS-enabled kernel on Raspberry Pi 5.
# This script runs ON the Pi (copied there by the Makefile).
# Usage: setup-ktls.sh [kernel_branch]
#   If omitted, derives branch from running kernel (e.g. 6.12.25-v8+ → rpi-6.12.y)
set -euo pipefail

if [ -n "${1:-}" ]; then
    KERNEL_BRANCH="$1"
else
    KERNEL_VERSION=$(uname -r)
    KERNEL_MAJOR_MINOR=$(echo "$KERNEL_VERSION" | grep -oP '^\d+\.\d+')
    if [ -z "$KERNEL_MAJOR_MINOR" ]; then
        echo "ERROR: Could not parse kernel version from uname -r: $KERNEL_VERSION"
        exit 1
    fi
    KERNEL_BRANCH="rpi-${KERNEL_MAJOR_MINOR}.y"
    echo "Auto-detected kernel branch: ${KERNEL_BRANCH} (from uname -r: ${KERNEL_VERSION})"
fi

echo "=== Building kTLS kernel (branch: ${KERNEL_BRANCH}) ==="
echo "This will take 30-60 minutes on a Pi 5."

# Check if kTLS is already available
if modprobe tls 2>/dev/null && lsmod | grep -q tls; then
    echo "kTLS module already loaded. Nothing to do."
    exit 0
fi

# Install build dependencies
echo ">>> Installing build dependencies..."
sudo apt install -y git bc bison flex libssl-dev make libncurses-dev

# Clone kernel source
if [ -d ~/linux ]; then
    echo ">>> ~/linux already exists, pulling latest..."
    cd ~/linux && git pull
else
    echo ">>> Cloning RPi kernel source..."
    git clone --depth=1 --branch "$KERNEL_BRANCH" https://github.com/raspberrypi/linux ~/linux
fi
cd ~/linux

# Configure and enable kTLS
echo ">>> Configuring kernel..."
make bcm2712_defconfig
scripts/config --module CONFIG_TLS

# Build
echo ">>> Building kernel (this takes a while)..."
make -j4 Image.gz modules dtbs

# Install
echo ">>> Installing kernel..."
sudo make modules_install
sudo make dtbs_install
sudo cp arch/arm64/boot/Image.gz /boot/firmware/kernel_2712.img

# Auto-load tls module on boot
echo tls | sudo tee /etc/modules-load.d/tls.conf > /dev/null

echo ""
echo "=== Kernel build complete ==="
echo "REBOOT REQUIRED: sudo reboot"
echo "After reboot, verify with: sudo modprobe tls && lsmod | grep tls"
