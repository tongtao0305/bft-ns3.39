#!/usr/bin/env bash
set -euo pipefail
readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly run_dir="${ns3_dir}/run-logs"
readonly source_topology="${ns3_dir}/examples/consensus/topologies/topology-leaf-spine-4x8x128-41os.txt"
readonly corrected_topology="${run_dir}/bidl-rdma-4x8x128-41os-corrected-20260928.txt"

mkdir -p "${run_dir}"
awk 'NR == 1 { $2 = 12 } { print }' "${source_topology}" >"${corrected_topology}"
echo "[$(date -Is)] generated run-local topology ${corrected_topology} from ${source_topology}; corrected switch count 8->12"

launch() {
  local unit=$1 script=$2
  systemd-run --user --unit="${unit}" --service-type=simple \
    --property=WorkingDirectory="${ns3_dir}" --property=KillMode=control-group \
    "${script_dir}/${script}"
}
launch bidl-hpcc-4x8-20260928 run-bidl-rdma-hpcc-hadoop-attack-4x8-20260928.sh
launch bidl-dcqcn-4x8-20260928 run-bidl-rdma-dcqcn-hadoop-attack-4x8-20260928.sh
launch bidl-dctcp-4x8-20260928 run-bidl-rdma-dctcp-hadoop-attack-4x8-20260928.sh
launch bidl-timely-4x8-20260928 run-bidl-rdma-timely-hadoop-attack-4x8-20260928.sh
