#!/usr/bin/env bash
# Run this only on the Slurm controller after syncing the same repository
# revision and build products to every compute node at BIDL_NS3_DIR.
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir="${BIDL_NS3_DIR:-/home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39}"
readonly campaign="${1:?Usage: $0 <campaign-name>}"
readonly sync_dest="${BIDL_RESULTS_SYNC_DEST:?Set BIDL_RESULTS_SYNC_DEST to controller:/absolute/path/to/run-logs}"
readonly max_parallel="${BIDL_SLURM_MAX_PARALLEL:-4}"
readonly partition="${BIDL_SLURM_PARTITION:?Set BIDL_SLURM_PARTITION}"
readonly account="${BIDL_SLURM_ACCOUNT:?Set BIDL_SLURM_ACCOUNT}"

[[ "${max_parallel}" =~ ^[1-9][0-9]*$ ]] || { echo 'BIDL_SLURM_MAX_PARALLEL must be a positive integer' >&2; exit 2; }
[[ -x "${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized" ]] || {
  echo "Controller copy is missing the optimized consensus executable" >&2
  exit 1
}
mkdir -p "${ns3_dir}/run-logs/${campaign}/slurm"

cd "${ns3_dir}"
sbatch \
  --partition="${partition}" \
  --account="${account}" \
  --array="0-79%${max_parallel}" \
  --output="run-logs/${campaign}/slurm/%A_%a.out" \
  --error="run-logs/${campaign}/slurm/%A_%a.err" \
  --export="ALL,BIDL_CAMPAIGN=${campaign},BIDL_NS3_DIR=${ns3_dir},BIDL_RESULTS_SYNC_DEST=${sync_dest}" \
  "${script_dir}/bidl-rdma-slurm-local-array.sbatch"
