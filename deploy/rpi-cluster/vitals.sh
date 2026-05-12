#!/usr/bin/env bash
# Snapshot RPi vitals: SoC temperature, throttle bitmask, current CPU frequency.
# Used to correlate thermal behaviour with chaos failures (RPi5 throttles at 80°C).
#
# Usage: vitals.sh <host> [<host> ...]
#
# Throttle bitmask (vcgencmd get_throttled):
#   0x0      clean
#   bit  0   under-voltage NOW
#   bit  1   arm frequency capped NOW
#   bit  2   currently throttled
#   bit  3   soft temperature limit active
#   bit 16   under-voltage has occurred since boot
#   bit 17   arm frequency capping has occurred since boot
#   bit 18   throttling has occurred since boot
#   bit 19   soft temperature limit has occurred since boot
set -euo pipefail

if [ "$#" -lt 1 ]; then
    printf "Usage: %s <host> [<host> ...]\n" "$0" >&2
    exit 1
fi

printf "%-18s %8s %12s %10s   %s\n" "host" "temp" "throttled" "freq_mhz" "notes"
for HOST in "$@"; do
    raw=$(ssh -o ConnectTimeout=3 "$HOST" 'vcgencmd measure_temp; vcgencmd get_throttled; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq' 2>/dev/null) || raw=""
    if [ -z "$raw" ]; then
        printf "%-18s %8s %12s %10s   %s\n" "$HOST" "?" "?" "?" "ssh failed"
        continue
    fi
    temp=$(printf "%s" "$raw" | awk -F"[='C]" '/^temp=/{print $2}')
    throttled=$(printf "%s" "$raw" | awk -F'=' '/^throttled=/{print $2}')
    freq_khz=$(printf "%s" "$raw" | awk '/^[0-9]+$/{print; exit}')
    freq_mhz=$(( freq_khz / 1000 ))

    notes=""
    if [ "$throttled" != "0x0" ]; then
        t_int=$(( throttled ))
        (( t_int & 0x4 )) && notes="${notes}THROTTLED_NOW "
        (( t_int & 0x1 )) && notes="${notes}UV_NOW "
        (( t_int & 0x8 )) && notes="${notes}SOFT_TEMP_NOW "
        (( t_int & 0x40000 )) && notes="${notes}throttled_since_boot "
        (( t_int & 0x10000 )) && notes="${notes}uv_since_boot "
    fi

    printf "%-18s %8s %12s %10s   %s\n" "$HOST" "$temp°C" "$throttled" "$freq_mhz" "$notes"
done
