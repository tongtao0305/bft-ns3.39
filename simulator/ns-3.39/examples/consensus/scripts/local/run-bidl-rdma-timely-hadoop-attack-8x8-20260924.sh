#!/usr/bin/env bash
set -euo pipefail
readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export BIDL_CONFIG="examples/consensus/configs/paper/2026-09/bidl-rdma-timely-hadoop-attack-8x8-20260924.conf"
export BIDL_EXPERIMENT_NAME="bidl-rdma-timely-hadoop-attack-8x8-20260924"
export BIDL_CC_LABEL="TIMELY"
export BIDL_CC_MODE=7
export BIDL_LAUNCH_SCRIPT="${script_dir}/$(basename -- "${BASH_SOURCE[0]}")"
exec "${script_dir}/run-bidl-rdma-cc-hadoop-attack-8x8-20260924.sh"
