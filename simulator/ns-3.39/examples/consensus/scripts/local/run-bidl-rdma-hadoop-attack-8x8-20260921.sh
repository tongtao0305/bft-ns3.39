#!/usr/bin/env bash
# Reproducibly runs a no-background smoke test, then six Hadoop RDMA workloads.
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly binary="${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized"
readonly config="${BIDL_CONFIG:-examples/consensus/configs/paper/2026-09/bidl-rdma-hadoop-attack-8x8-20260921.conf}"
readonly experiment_name="${BIDL_EXPERIMENT_NAME:-bidl-rdma-hadoop-attack-8x8-20260921}"
readonly launch_script="${BIDL_LAUNCH_SCRIPT:-$0}"
readonly result_dir="${ns3_dir}/run-logs/${experiment_name}"
readonly source_dir="${ns3_dir}/examples/consensus"

if [[ ! -x "${binary}" ]]; then
  echo "Missing executable: ${binary}" >&2
  exit 1
fi

mkdir -p "${result_dir}" "${result_dir}/normalized-workloads"
cp "${ns3_dir}/${config}" "${result_dir}/config.conf"
cp "${launch_script}" "${result_dir}/launch.sh"

# The supplied raw traces encode background traffic at priority group 3.  The
# experiment requires group 4, so retain every field but rewrite that column in
# run-local copies; original workload files remain untouched.
normalize_workload() {
  local load=$1
  local source="${source_dir}/bg-hadoop-s8l8h128-${load}G-1s-raw.txt"
  local target="${result_dir}/normalized-workloads/hadoop-${load}G-pg4.txt"
  [[ -f "${source}" ]] || { echo "Missing workload: ${source}" >&2; exit 1; }
  awk 'NR == 1 { print; next } { $3 = 4; print }' "${source}" >"${target}"
  printf '%s\n' "${target}"
}

mkdir -p "${result_dir}/smoke-no-background"
echo "[$(date -Is)] smoke test: no background flow" | tee "${result_dir}/launcher.log"
(
  cd "${ns3_dir}"
  exec "${binary}" --configFile="${config}" \
    --requestRate=1000 --requestCount=100 --batchSize=50 --run=1 \
    --outputPrefix="${result_dir}/smoke-no-background/smoke"
) >"${result_dir}/smoke-no-background.log" 2>&1

smoke_config="${result_dir}/smoke-no-background/smoke-config.json"
if [[ ! -f "${smoke_config}" ]] || ! rg -q '"architecture": "bidl"' "${smoke_config}" || \
   ! rg -q '"transport": "rdma"' "${smoke_config}" || \
   ! rg -q '"bidl_attack_enabled": true' "${smoke_config}" || \
   ! rg -q '"bidl_attack_target_executors": 0' "${smoke_config}"; then
  echo "Smoke test did not produce the expected BIDL/RDMA/attack configuration." >&2
  exit 1
fi
echo "[$(date -Is)] smoke test passed; launching workloads" | tee -a "${result_dir}/launcher.log"

: >"${result_dir}/pids"
for load in 5 10 15 20 25 30; do
  workload=$(normalize_workload "${load}")
  run_dir="${result_dir}/hadoop-${load}G"
  mkdir -p "${run_dir}"
  (
    cd "${ns3_dir}"
    exec "${binary}" --configFile="${config}" --flowFile="${workload}" --run="${load}" \
      --outputPrefix="${run_dir}/hadoop-${load}G"
  ) >"${run_dir}/run.log" 2>&1 &
  printf '%s hadoop-%sG\n' "$!" "${load}" | tee -a "${result_dir}/pids"
done

wait_status=0
wait || wait_status=$?
if (( wait_status == 0 )); then
  echo "[$(date -Is)] all workloads completed" | tee -a "${result_dir}/launcher.log"
else
  echo "[$(date -Is)] one or more workloads failed (status ${wait_status})" | tee -a "${result_dir}/launcher.log"
fi
exit "${wait_status}"
