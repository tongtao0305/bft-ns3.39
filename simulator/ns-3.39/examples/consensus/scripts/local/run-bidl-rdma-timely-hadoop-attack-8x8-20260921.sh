#!/usr/bin/env bash
# Independent TIMELY launcher; it reuses the parameter-identical RDMA batch harness.
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export BIDL_CONFIG="examples/consensus/configs/paper/2026-09/bidl-rdma-timely-hadoop-attack-8x8-20260921.conf"
export BIDL_EXPERIMENT_NAME="bidl-rdma-timely-hadoop-attack-8x8-20260921"
export BIDL_CC_LABEL="TIMELY"
export BIDL_CC_MODE=7
export BIDL_LAUNCH_SCRIPT="$0"
exec "${script_dir}/run-bidl-rdma-hpcc-hadoop-attack-8x8-20260921.sh"
