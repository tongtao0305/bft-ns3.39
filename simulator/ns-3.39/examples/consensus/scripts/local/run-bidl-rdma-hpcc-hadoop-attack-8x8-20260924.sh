#!/usr/bin/env bash
# Runs a BIDL/RDMA/HPCC smoke test, then ten independent Hadoop workloads in parallel.
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly binary="${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized"
readonly config="${BIDL_CONFIG:-examples/consensus/configs/paper/2026-09/bidl-rdma-hpcc-hadoop-attack-8x8-20260924.conf}"
readonly experiment_name="${BIDL_EXPERIMENT_NAME:-bidl-rdma-hpcc-hadoop-attack-8x8-20260924}"
readonly result_dir="${ns3_dir}/run-logs/${experiment_name}"
readonly source_dir="${ns3_dir}/examples/consensus/workloads/hadoop"
readonly launch_script="${script_dir}/$(basename -- "${BASH_SOURCE[0]}")"
readonly normalized_dir="${result_dir}/normalized-workloads"

if [[ ! -x "${binary}" ]]; then
  echo "Missing executable: ${binary}" >&2
  exit 1
fi
[[ -f "${ns3_dir}/${config}" ]] || { echo "Missing config: ${ns3_dir}/${config}" >&2; exit 1; }

mkdir -p "${result_dir}/smoke-no-background" "${normalized_dir}"
cp "${ns3_dir}/${config}" "${result_dir}/config.conf"
cp "${launch_script}" "${result_dir}/launch.sh"

echo "[$(date -Is)] HPCC smoke test: no background flow" | tee "${result_dir}/launcher.log"
(
  cd "${ns3_dir}"
  exec "${binary}" --configFile="${config}" --run=1 \
    --requestRate=1000 --requestCount=100 --batchSize=50 \
    --simulationTime=1 --measurementTime=1 \
    --outputPrefix="${result_dir}/smoke-no-background/smoke"
) >"${result_dir}/smoke-no-background.log" 2>&1

smoke_config="${result_dir}/smoke-no-background/smoke-config.json"
for expected in \
  '"architecture": "bidl"' \
  '"bidl_ingress_mode": "network"' \
  '"transport": "rdma"' \
  '"topology_file": "examples/consensus/topologies/topology-leaf-spine-8x8x128-21os.txt"' \
  '"orderer_physical_ids": "0,16,32,48"' \
  '"executor_physical_ids": "1,17,33,49"' \
  '"ingress_physical_id": 8' \
  '"client_physical_id": 9' \
  '"bidl_malicious_physical_ids": "10"' \
  '"bidl_attack_enabled": true' \
  '"bidl_attack_target_executors": 0' \
  '"bidl_attack_probability": 1' \
  '"bidl_attack_processing_us": 20' \
  '"rdma_cc_mode": 3' \
  '"rdma_priority_group": 3' \
  '"effective_consensus_priority_group": 3' \
  '"simulation_time": 1' \
  '"measurement_time": 1'; do
  grep -Fq "${expected}" "${smoke_config}" || {
    echo "Smoke validation failed; missing ${expected}" >&2
    exit 1
  }
done
echo "[$(date -Is)] HPCC smoke test passed; launching workloads" | tee -a "${result_dir}/launcher.log"

: >"${result_dir}/pids"
for load in 5 10 15 20 25 30 35 40 45 50; do
  flow_file="${source_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  run_dir="${result_dir}/hadoop-${load}G"
  [[ -f "${flow_file}" ]] || { echo "Missing workload: ${flow_file}" >&2; exit 1; }
  # These supplied Hadoop files are record-only (seven columns, no count header).
  # Their seventh column is a per-record flow index; ns-3 uses that column as
  # the legacy app_id, so normalize it to 0 to classify every record as background.
  normalized_file="${normalized_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  record_count=$(wc -l <"${flow_file}")
  awk -v count="${record_count}" 'BEGIN { print count } { $7 = 0; print }' "${flow_file}" >"${normalized_file}"
  mkdir -p "${run_dir}"
  (
    cd "${ns3_dir}"
    exec "${binary}" --configFile="${config}" \
      --flowFile="run-logs/${experiment_name}/normalized-workloads/bg-hadoop-128host-${load}G-0t1.txt" \
      --run="${load}" --outputPrefix="${run_dir}/hadoop-${load}G"
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
  echo "[$(date -Is)] all ten HPCC workloads completed" | tee -a "${result_dir}/launcher.log"
else
  echo "[$(date -Is)] one or more HPCC workloads failed (status ${wait_status})" | tee -a "${result_dir}/launcher.log"
fi
exit "${wait_status}"
