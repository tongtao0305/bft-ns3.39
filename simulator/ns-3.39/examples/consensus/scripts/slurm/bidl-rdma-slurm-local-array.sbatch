#!/usr/bin/env bash
# A Slurm Array task for locally replicated ns-3 trees.
#
# The controller and every compute node must contain the same synchronized tree
# at BIDL_NS3_DIR.  Results are copied back to BIDL_RESULTS_SYNC_DEST after
# every task, including failed tasks, so no shared filesystem is required.
#
# Required exported variables:
#   BIDL_CAMPAIGN           unique name for this batch
#   BIDL_RESULTS_SYNC_DEST  controller host and its run-logs directory, e.g.
#                           controller.example:/home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39/run-logs
# Optional exported variable:
#   BIDL_NS3_DIR            defaults to the identical local path on each node
#
# Submit with --array=0-79%<global-concurrency>.  Do not set an Array directive
# here so that the submit wrapper remains the single source of concurrency.
#SBATCH --job-name=bidl-rdma
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=48G
#SBATCH --time=3-00:00:00

set -euo pipefail

readonly ns3_dir="${BIDL_NS3_DIR:-/home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39}"
readonly campaign="${BIDL_CAMPAIGN:?BIDL_CAMPAIGN is required}"
readonly sync_dest="${BIDL_RESULTS_SYNC_DEST:?BIDL_RESULTS_SYNC_DEST is required}"
readonly binary="${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized"
readonly source_dir="${ns3_dir}/examples/consensus/workloads/hadoop"
readonly task_id="${SLURM_ARRAY_TASK_ID:?This script must run as a Slurm Array task}"

[[ "${task_id}" =~ ^[0-9]+$ ]] && ((task_id < 80)) || {
  echo "SLURM_ARRAY_TASK_ID must be in 0..79; got ${task_id}" >&2
  exit 2
}
[[ -x "${binary}" ]] || { echo "Missing local executable: ${binary}" >&2; exit 1; }

protocols=(hpcc dcqcn dctcp timely)
loads=(5 10 15 20 25 30 35 40 45 50)
configs_8x8=(
  examples/consensus/configs/paper/2026-09/bidl-rdma-hpcc-hadoop-attack-8x8-20260924.conf
  examples/consensus/configs/paper/2026-09/bidl-rdma-dcqcn-hadoop-attack-8x8-20260924.conf
  examples/consensus/configs/paper/2026-09/bidl-rdma-dctcp-hadoop-attack-8x8-20260924.conf
  examples/consensus/configs/paper/2026-09/bidl-rdma-timely-hadoop-attack-8x8-20260924.conf
)
configs_4x8=(
  examples/consensus/configs/paper/2026-09/bidl-rdma-hpcc-hadoop-attack-4x8-20260928.conf
  examples/consensus/configs/paper/2026-09/bidl-rdma-dcqcn-hadoop-attack-4x8-20260928.conf
  examples/consensus/configs/paper/2026-09/bidl-rdma-dctcp-hadoop-attack-4x8-20260928.conf
  examples/consensus/configs/paper/2026-09/bidl-rdma-timely-hadoop-attack-4x8-20260928.conf
)
topology_names=(8x8x128-21os 4x8x128-41os)

topology_index=$((task_id / 40))
within_topology=$((task_id % 40))
protocol_index=$((within_topology / 10))
load_index=$((within_topology % 10))
protocol="${protocols[protocol_index]}"
load="${loads[load_index]}"
topology_name="${topology_names[topology_index]}"
if ((topology_index == 0)); then
  config="${configs_8x8[protocol_index]}"
else
  config="${configs_4x8[protocol_index]}"
  corrected_topology="${ns3_dir}/run-logs/bidl-rdma-4x8x128-41os-corrected-20260928.txt"
  [[ -f "${corrected_topology}" ]] && [[ "$(awk 'NR == 1 { print $2 }' "${corrected_topology}")" == 12 ]] || {
    echo "Missing or invalid local corrected 4x8 topology: ${corrected_topology}" >&2
    exit 1
  }
fi

readonly result_dir="${ns3_dir}/run-logs/${campaign}/${topology_name}/${protocol}/hadoop-${load}G"
readonly normalized_dir="${ns3_dir}/run-logs/${campaign}/${topology_name}/${protocol}/normalized-workloads"
readonly source_flow="${source_dir}/bg-hadoop-128host-${load}G-0t1.txt"
readonly normalized_flow="${normalized_dir}/bg-hadoop-128host-${load}G-0t1.txt"
readonly summary="${result_dir}/hadoop-${load}G-summary.csv"

is_complete() {
  [[ -s "${summary}" ]] && awk -F, \
    '$1 == "completed_requests" && $2 == 30000 { ok=1 } END { exit !ok }' "${summary}"
}

sync_result() {
  local rc=$?
  trap - EXIT
  # --relative preserves run-logs/<campaign>/... below the controller's
  # BIDL_RESULTS_SYNC_DEST directory. The destination directory itself must
  # already exist and SSH keys must allow non-interactive rsync from workers.
  if ! rsync -a --relative "${ns3_dir}/./run-logs/${campaign}/${topology_name}/${protocol}/hadoop-${load}G" "${sync_dest%/}/"; then
    echo "Result synchronization failed for task ${task_id}" >&2
    ((rc == 0)) && rc=75
  fi
  exit "${rc}"
}

mkdir -p "${result_dir}" "${normalized_dir}"
trap sync_result EXIT

if is_complete; then
  echo "Already complete locally: ${topology_name}/${protocol}/hadoop-${load}G"
  exit 0
fi
[[ -f "${source_flow}" ]] || { echo "Missing local background flow: ${source_flow}" >&2; exit 1; }
[[ -f "${ns3_dir}/${config}" ]] || { echo "Missing local config: ${ns3_dir}/${config}" >&2; exit 1; }

record_count=$(wc -l <"${source_flow}")
awk -v count="${record_count}" 'BEGIN { print count } { $7 = 0; print }' \
  "${source_flow}" >"${normalized_flow}"
cp "${ns3_dir}/${config}" "${result_dir}/config.conf"
git -C "${ns3_dir}" rev-parse HEAD >"${result_dir}/git-commit.txt" 2>/dev/null || true
printf 'array_task_id=%s\ntopology=%s\nprotocol=%s\nbackground_load=%sG\nnode=%s\n' \
  "${task_id}" "${topology_name}" "${protocol}" "${load}" "$(hostname -f)" >"${result_dir}/task-metadata.txt"

cd "${ns3_dir}"
"${binary}" \
  --configFile="${config}" \
  --flowFile="run-logs/${campaign}/${topology_name}/${protocol}/normalized-workloads/$(basename -- "${normalized_flow}")" \
  --run="$((1000 + task_id))" \
  --outputPrefix="${result_dir}/hadoop-${load}G" >"${result_dir}/run.log" 2>&1
