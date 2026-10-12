#!/usr/bin/env bash
# Launch all four CC batches in independent, terminal-detached sessions.
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly monitor_dir="${ns3_dir}/run-logs/bidl-rdma-all-hadoop-attack-8x8-20260924"
readonly launcher_log="${monitor_dir}/persistent-launcher.log"

mkdir -p "${monitor_dir}"
printf '%s\n' "[$(date -Is)] launching four detached CC batches" | tee -a "${launcher_log}"

launch_batch() {
  local label=$1
  local script=$2
  local log="${monitor_dir}/${label}.launcher.log"
  local pid_file="${monitor_dir}/${label}.launcher.pid"
  setsid nohup "${script_dir}/${script}" >"${log}" 2>&1 < /dev/null &
  local pid=$!
  printf '%s\n' "${pid}" >"${pid_file}"
  printf '%s %s pid=%s\n' "[$(date -Is)]" "${label}" "${pid}" | tee -a "${launcher_log}"
}

launch_batch hpcc run-bidl-rdma-hpcc-hadoop-attack-8x8-20260924.sh
launch_batch dcqcn run-bidl-rdma-dcqcn-hadoop-attack-8x8-20260924.sh
launch_batch dctcp run-bidl-rdma-dctcp-hadoop-attack-8x8-20260924.sh
launch_batch timely run-bidl-rdma-timely-hadoop-attack-8x8-20260924.sh

printf '%s\n' "[$(date -Is)] all four detached batch launchers started" | tee -a "${launcher_log}"
