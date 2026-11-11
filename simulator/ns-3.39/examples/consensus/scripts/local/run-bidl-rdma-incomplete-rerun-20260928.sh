#!/usr/bin/env bash
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly binary="${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized"
readonly config="${BIDL_CONFIG:?BIDL_CONFIG is required}"
readonly experiment_name="${BIDL_EXPERIMENT_NAME:?BIDL_EXPERIMENT_NAME is required}"
readonly cc_label="${BIDL_CC_LABEL:?BIDL_CC_LABEL is required}"
readonly cc_mode="${BIDL_CC_MODE:?BIDL_CC_MODE is required}"
readonly loads_csv="${BIDL_LOADS:?BIDL_LOADS is required}"
readonly source_dir="${ns3_dir}/examples/consensus/workloads/hadoop"
readonly result_dir="${ns3_dir}/run-logs/${experiment_name}"
readonly normalized_dir="${result_dir}/normalized-workloads"

[[ -x "${binary}" ]] || { echo "Missing executable: ${binary}" >&2; exit 1; }
[[ -f "${ns3_dir}/${config}" ]] || { echo "Missing config: ${ns3_dir}/${config}" >&2; exit 1; }
mkdir -p "${result_dir}/smoke-no-background" "${normalized_dir}"
cp "${ns3_dir}/${config}" "${result_dir}/config.conf"

echo "[$(date -Is)] ${cc_label} incomplete-rerun smoke test" | tee "${result_dir}/launcher.log"
(
  cd "${ns3_dir}"
  exec "${binary}" --configFile="${config}" --run=928 \
    --requestRate=1000 --requestCount=100 --batchSize=50 \
    --simulationTime=1 --measurementTime=1 \
    --outputPrefix="${result_dir}/smoke-no-background/smoke"
) >"${result_dir}/smoke-no-background.log" 2>&1

smoke_config="${result_dir}/smoke-no-background/smoke-config.json"
for expected in '"architecture": "bidl"' '"transport": "rdma"' \
  '"orderer_physical_ids": "0,16,32,48"' '"executor_physical_ids": "1,17,33,49"' \
  '"bidl_attack_enabled": true' '"bidl_attack_target_executors": 0' \
  '"rdma_priority_group": 3' '"effective_consensus_priority_group": 3' \
  "\"rdma_cc_mode\": ${cc_mode}"; do
  grep -Fq "${expected}" "${smoke_config}" || { echo "Smoke validation failed: ${expected}" >&2; exit 1; }
done
echo "[$(date -Is)] ${cc_label} smoke passed; mode ${cc_mode} confirmed" | tee -a "${result_dir}/launcher.log"

: >"${result_dir}/pids"
IFS=',' read -r -a loads <<<"${loads_csv}"
for load in "${loads[@]}"; do
  flow_file="${source_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  run_dir="${result_dir}/hadoop-${load}G"
  normalized_file="${normalized_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  [[ -f "${flow_file}" ]] || { echo "Missing workload: ${flow_file}" >&2; exit 1; }
  # Source traces already include the count header and background app_id=0.
  cp -- "${flow_file}" "${normalized_file}"
  mkdir -p "${run_dir}"
  (
    cd "${ns3_dir}"
    exec "${binary}" --configFile="${config}" \
      --flowFile="run-logs/${experiment_name}/normalized-workloads/bg-hadoop-128host-${load}G-0t1.txt" \
      --run="${load}928" --outputPrefix="${run_dir}/hadoop-${load}G"
  ) >"${run_dir}/run.log" 2>&1 &
  printf '%s hadoop-%sG\n' "$!" "${load}" | tee -a "${result_dir}/pids"
done

wait_status=0
while read -r pid workload; do
  if ! wait "${pid}"; then
    wait_status=1
    echo "[$(date -Is)] ${workload} failed (pid ${pid})" | tee -a "${result_dir}/launcher.log"
  fi
done <"${result_dir}/pids"
if (( wait_status == 0 )); then
  echo "[$(date -Is)] all ${cc_label} incomplete-rerun workloads completed" | tee -a "${result_dir}/launcher.log"
else
  echo "[$(date -Is)] one or more ${cc_label} incomplete-rerun workloads failed" | tee -a "${result_dir}/launcher.log"
fi
exit "${wait_status}"
