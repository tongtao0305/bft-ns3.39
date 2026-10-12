#!/usr/bin/env bash
set -euo pipefail
readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export BIDL_CONFIG="examples/consensus/configs/paper/2026-09/bidl-rdma-dcqcn-hadoop-attack-4x8-20260928.conf"
export BIDL_EXPERIMENT_NAME="bidl-rdma-dcqcn-hadoop-attack-4x8-20260928"
export BIDL_CC_LABEL="DCQCN-4x8"
export BIDL_CC_MODE=1
export BIDL_TOPOLOGY_FILE="run-logs/bidl-rdma-4x8x128-41os-corrected-20260928.txt"
export BIDL_LAUNCH_SCRIPT="${script_dir}/$(basename -- "${BASH_SOURCE[0]}")"
exec "${script_dir}/run-bidl-rdma-cc-hadoop-attack-8x8-20260924.sh"
