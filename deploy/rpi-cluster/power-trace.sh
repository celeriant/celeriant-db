#!/usr/bin/env bash
# Sample the Pi 5 PMIC rails once a second so a power event leaves a trace.
# Installed as power-trace.service by setup-nodes.sh.
#
# Writes to the SD card, not /var/lib/nvme: that is celeriant's data device and
# fsyncing a line a second onto it would contend with the IO path under test.
# fsync per line is deliberate — a brownout kills the board with no warning, so
# a buffered tail would lose exactly the seconds worth having.
set -u

OUT=/var/log/power-trace.log

printf 'ts\text5v\tvdd_core_a\tthrottled\tsoc_temp\tnvme_temp\tload1\n' >> "$OUT"

while true; do
    adc=$(vcgencmd pmic_read_adc 2>/dev/null)
    # "     EXT5V_V volt(24)=5.12550000V" — the value is after '=', not in $3.
    ext5v=$(awk -F= '/EXT5V_V/ {gsub(/[^0-9.]/,"",$2); print $2}' <<<"$adc")
    core_a=$(awk -F= '/VDD_CORE_A/ {gsub(/[^0-9.]/,"",$2); print $2}' <<<"$adc")
    thr=$(vcgencmd get_throttled 2>/dev/null | cut -d= -f2)
    soc=$(vcgencmd measure_temp 2>/dev/null | tr -dc '0-9.')
    nvme=$(awk '{printf "%.1f", $1/1000}' /sys/class/hwmon/hwmon*/temp1_input 2>/dev/null | head -c 6)
    load=$(cut -d' ' -f1 /proc/loadavg)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(date -u +%H:%M:%S)" "${ext5v:-?}" "${core_a:-?}" "${thr:-?}" \
        "${soc:-?}" "${nvme:-?}" "$load" >> "$OUT"
    sync -d "$OUT" 2>/dev/null
    sleep 1
done
