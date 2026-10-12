#!/usr/bin/env bash
set -euo pipefail
readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export BIDL_CONFIG="examples/consensus/configs/paper/2026-09/bidl-rdma-hadoop-attack-8x8-pg3-20260922.conf"
export BIDL_EXPERIMENT_NAME="bidl-rdma-hpcc-hadoop-attack-8x8-pg3-20260922"
export BIDL_CC_LABEL="HPCC-PG3"
export BIDL_CC_MODE=3
export BIDL_CONSENSUS_PG=3
export BIDL_BACKGROUND_PG=3
export BIDL_FLOW_PG=3
export BIDL_LAUNCH_SCRIPT="$0"
exec "${script_dir}/run-bidl-rdma-hpcc-hadoop-attack-8x8-20260921.sh"
