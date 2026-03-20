#!/usr/bin/env bash
# Format and mount NVMe storage on a data node.
# Usage: setup-nvme.sh <hostname>
#
# DESTRUCTIVE: Wipes the NVMe drive. Prompts for confirmation.
set -euo pipefail

source config.env

HOST="$1"

RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

# Check if NVMe device exists
printf ">>> Checking for NVMe device on %s...\n" "$HOST"
if ! ssh "$HOST" "test -b ${NVME_DEVICE}"; then
    printf "${RED}${BOLD}ERROR:${RESET} NVMe device ${NVME_DEVICE} not found on %s.\n" "$HOST"
    printf "Check that the NVMe hat and drive are properly connected.\n"
    printf "Run: ssh %s 'lsblk' to see available block devices.\n" "$HOST"
    exit 1
fi

# Show current state
printf "\n${BOLD}Current block devices on %s:${RESET}\n" "$HOST"
ssh "$HOST" "lsblk ${NVME_DEVICE}"

# Check if already mounted at correct location
MOUNT_POINT=$(ssh "$HOST" "findmnt -n -o TARGET ${NVME_PARTITION} 2>/dev/null || true")
if [ -n "$MOUNT_POINT" ]; then
    if [ "$MOUNT_POINT" = "$REMOTE_DATA_ROOT" ]; then
        printf "${GREEN}Already mounted at the correct location. Skipping.${RESET}\n"
        exit 0
    fi
    printf "\n${YELLOW}WARNING:${RESET} ${NVME_PARTITION} is currently mounted at ${MOUNT_POINT} on %s.\n" "$HOST"
    printf "Unmount it first or choose a different device.\n"
    exit 1
fi

# Check if the partition exists and has a filesystem (i.e. has data to lose)
HAS_DATA=$(ssh "$HOST" "test -b ${NVME_PARTITION} && sudo blkid -o value -s TYPE ${NVME_PARTITION} 2>/dev/null || true")
if [ -n "$HAS_DATA" ]; then
    printf "\n${RED}${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}\n"
    printf "${RED}${BOLD}║  WARNING: This will ERASE ALL DATA on ${NVME_DEVICE} on %-11s ║${RESET}\n" "$HOST"
    printf "${RED}${BOLD}║  Existing filesystem: %-38s ║${RESET}\n" "$HAS_DATA"
    printf "${RED}${BOLD}║  This operation is IRREVERSIBLE.                             ║${RESET}\n"
    printf "${RED}${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}\n\n"
    read -p "Type the hostname '${HOST}' to confirm: " confirm
    if [ "$confirm" != "$HOST" ]; then
        printf "Aborted.\n"
        exit 1
    fi
else
    printf ">>> Drive is empty/unformatted, proceeding without confirmation.\n"
fi

printf "\n>>> Formatting %s on %s as XFS...\n" "$NVME_DEVICE" "$HOST"
ssh "$HOST" bash -s <<REMOTE_FORMAT
set -euo pipefail
sudo wipefs -a ${NVME_DEVICE}
sudo parted ${NVME_DEVICE} --script mklabel gpt mkpart primary xfs 0% 100%
sleep 1
sudo mkfs.xfs -f ${NVME_PARTITION}
sudo mkdir -p ${REMOTE_DATA_ROOT}
sudo mount ${NVME_PARTITION} ${REMOTE_DATA_ROOT}

# Persist across reboots (idempotent)
if ! grep -q "${NVME_PARTITION}" /etc/fstab; then
    echo '${NVME_PARTITION} ${REMOTE_DATA_ROOT} xfs defaults,noatime 0 2' | sudo tee -a /etc/fstab
fi
REMOTE_FORMAT

printf "${GREEN}NVMe formatted and mounted at %s on %s.${RESET}\n" "$REMOTE_DATA_ROOT" "$HOST"

# Verify
printf "\n>>> Verifying mount:\n"
ssh "$HOST" "df -h ${REMOTE_DATA_ROOT}"
