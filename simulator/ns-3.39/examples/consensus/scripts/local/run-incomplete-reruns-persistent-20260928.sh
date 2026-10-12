#!/usr/bin/env bash
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly runner="${script_dir}/run-bidl-rdma-incomplete-rerun-20260928.sh"

launch() {
  local unit=$1 label=$2 config=$3 mode=$4 loads=$5
  systemd-run --user --unit="${unit}" --service-type=simple \
    --property=WorkingDirectory="${ns3_dir}" --property=KillMode=control-group \
    env BIDL_CONFIG="${config}" BIDL_EXPERIMENT_NAME="${label}" \
      BIDL_CC_LABEL="${label}" BIDL_CC_MODE="${mode}" BIDL_LOADS="${loads}" \
      "${runner}"
}

launch bidl-hpcc-rerun-20260928 \
  bidl-rdma-hpcc-incomplete-rerun-20260928 \
  examples/consensus/configs/paper/2026-09/bidl-rdma-hpcc-hadoop-attack-8x8-20260924.conf 3 50
launch bidl-dcqcn-rerun-20260928 \
  bidl-rdma-dcqcn-incomplete-rerun-20260928 \
  examples/consensus/configs/paper/2026-09/bidl-rdma-dcqcn-hadoop-attack-8x8-20260924.conf 1 15,20,25,30,35,40,45,50
