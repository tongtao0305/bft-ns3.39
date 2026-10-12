#!/usr/bin/env bash
set -euo pipefail
readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export BIDL_CONFIG="examples/consensus/configs/paper/2026-09/bidl-rdma-hadoop-attack-8x8-20260921.conf"
export BIDL_EXPERIMENT_NAME="bidl-rdma-dcqcn-hadoop-attack-8x8-20260922-rerun"
export BIDL_LAUNCH_SCRIPT="$0"
exec "${script_dir}/run-bidl-rdma-hadoop-attack-8x8-20260921.sh"
