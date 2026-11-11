#!/usr/bin/env bash
# Run the Hadoop background-flow workloads using a bounded worker pool.
# Required environment: BIDL_CONFIG, BIDL_EXPERIMENT_NAME, BIDL_CC_LABEL,
# BIDL_CC_MODE.  Optional: BIDL_LOADS, BIDL_MAX_PARALLEL,
# BIDL_EXPECTED_REQUEST_COUNT, BIDL_TOPOLOGY_FILE and BIDL_LAUNCH_SCRIPT.
set -euo pipefail

readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly binary="${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized"
readonly config="${BIDL_CONFIG:?BIDL_CONFIG is required}"
readonly experiment_name="${BIDL_EXPERIMENT_NAME:?BIDL_EXPERIMENT_NAME is required}"
readonly cc_label="${BIDL_CC_LABEL:?BIDL_CC_LABEL is required}"
readonly cc_mode="${BIDL_CC_MODE:?BIDL_CC_MODE is required}"
readonly loads_csv="${BIDL_LOADS:-5,10,15,20,25,30,35,40,45,50}"
readonly max_parallel="${BIDL_MAX_PARALLEL:-2}"
readonly expected_request_count="${BIDL_EXPECTED_REQUEST_COUNT:-30000}"
readonly topology_file="${BIDL_TOPOLOGY_FILE:-}"
readonly launch_script="${BIDL_LAUNCH_SCRIPT:-${script_dir}/$(basename -- "${BASH_SOURCE[0]}")}"
readonly result_dir="${ns3_dir}/run-logs/${experiment_name}"
readonly source_dir="${ns3_dir}/examples/consensus/workloads/hadoop"
readonly normalized_dir="${result_dir}/normalized-workloads"
readonly status_file="${result_dir}/queue-status.tsv"

[[ "${max_parallel}" =~ ^[1-9][0-9]*$ ]] || { echo 'BIDL_MAX_PARALLEL must be a positive integer' >&2; exit 2; }
[[ -x "${binary}" ]] || { echo "Missing executable: ${binary}" >&2; exit 1; }
[[ -f "${ns3_dir}/${config}" ]] || { echo "Missing config: ${ns3_dir}/${config}" >&2; exit 1; }

mkdir -p "${result_dir}/smoke-no-background" "${normalized_dir}"
exec 9>"${result_dir}/queue.lock"
flock -n 9 || { echo "A queue is already managing ${result_dir}" >&2; exit 1; }
cp "${ns3_dir}/${config}" "${result_dir}/config.conf"
cp "${launch_script}" "${result_dir}/queue-launch.sh" 2>/dev/null || true

status() {
  printf '%s\t%s\t%s\t%s\t%s\n' "$(date -Is)" "$1" "$2" "$3" "$4" >>"${status_file}"
}

is_complete() {
  local summary=$1
  [[ -s "${summary}" ]] && awk -F, -v expected="${expected_request_count}" \
    'NR == 2 && $2 == expected { found=1 } END { exit !found }' "${summary}"
}

run_smoke() {
  local smoke_prefix="${result_dir}/smoke-no-background/smoke"
  echo "[$(date -Is)] ${cc_label} queued-run smoke test (workers=${max_parallel})" | tee -a "${result_dir}/launcher.log"
  (
    cd "${ns3_dir}"
    exec "${binary}" --configFile="${config}" --run=1 --requestRate=1000 --requestCount=100 \
      --batchSize=50 --simulationTime=1 --measurementTime=1 --outputPrefix="${smoke_prefix}"
  ) >"${result_dir}/smoke-no-background.log" 2>&1

  local smoke_config="${smoke_prefix}-config.json"
  local expected
  for expected in '"architecture": "bidl"' '"bidl_ingress_mode": "network"' '"transport": "rdma"' \
    '"orderer_physical_ids": "0,16,32,48"' '"executor_physical_ids": "1,17,33,49"' \
    '"ingress_physical_id": 8' '"client_physical_id": 9' '"bidl_malicious_physical_ids": "10"' \
    '"bidl_attack_enabled": true' '"bidl_attack_target_executors": 0' \
    '"bidl_attack_probability": 1' '"bidl_attack_processing_us": 20' \
    '"rdma_priority_group": 3' '"effective_consensus_priority_group": 3' \
    '"simulation_time": 1' '"measurement_time": 1' "\"rdma_cc_mode\": ${cc_mode}"; do
    grep -Fq "${expected}" "${smoke_config}" || { echo "Smoke validation failed: ${expected}" >&2; exit 1; }
  done
  if [[ -n "${topology_file}" ]]; then
    grep -Fq "\"topology_file\": \"${topology_file}\"" "${smoke_config}" || {
      echo "Smoke validation failed: topology ${topology_file}" >&2; exit 1;
    }
  fi
  echo "[$(date -Is)] smoke passed; BIDL/RDMA/attack and CC mode ${cc_mode} confirmed" | tee -a "${result_dir}/launcher.log"
}

declare -A pid_to_load=()
declare -a pending=()

on_exit() {
  local rc=$?
  if ((${#pid_to_load[@]})); then
    echo "[$(date -Is)] queue interrupted; terminating active workers" | tee -a "${result_dir}/launcher.log"
    kill "${!pid_to_load[@]}" 2>/dev/null || true
  fi
  exit "${rc}"
}
trap on_exit INT TERM

launch_one() {
  local load=$1
  local run_dir="${result_dir}/hadoop-${load}G"
  local normalized_file="${normalized_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  mkdir -p "${run_dir}"
  status "${load}G" QUEUED - -
  (
    cd "${ns3_dir}"
    exec "${binary}" --configFile="${config}" \
      --flowFile="run-logs/${experiment_name}/normalized-workloads/$(basename -- "${normalized_file}")" \
      --run="${load}" --outputPrefix="${run_dir}/hadoop-${load}G"
  ) >"${run_dir}/run.log" 2>&1 &
  local pid=$!
  pid_to_load["${pid}"]=${load}
  status "${load}G" RUNNING "${pid}" -
  echo "[$(date -Is)] started hadoop-${load}G (pid ${pid}; ${#pid_to_load[@]}/${max_parallel} slots used)" | tee -a "${result_dir}/launcher.log"
}

run_smoke
printf 'timestamp\tworkload\tstate\tpid\texit_code\n' >"${status_file}"
IFS=',' read -r -a requested_loads <<<"${loads_csv}"
for load in "${requested_loads[@]}"; do
  [[ "${load}" =~ ^[0-9]+$ ]] || { echo "Invalid load: ${load}" >&2; exit 2; }
  flow_file="${source_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  normalized_file="${normalized_dir}/bg-hadoop-128host-${load}G-0t1.txt"
  [[ -f "${flow_file}" ]] || { echo "Missing workload: ${flow_file}" >&2; exit 1; }
  if is_complete "${result_dir}/hadoop-${load}G/hadoop-${load}G-summary.csv"; then
    status "${load}G" SKIPPED_COMPLETE - 0
    continue
  fi
  # Source traces already include the count header and background app_id=0.
  cp -- "${flow_file}" "${normalized_file}"
  pending+=("${load}")
done

echo "[$(date -Is)] queue prepared: ${#pending[@]} runnable jobs, maximum ${max_parallel} concurrent workers" | tee -a "${result_dir}/launcher.log"
next=0
failed=0
while (( next < ${#pending[@]} || ${#pid_to_load[@]} > 0 )); do
  while (( next < ${#pending[@]} && ${#pid_to_load[@]} < max_parallel )); do
    launch_one "${pending[next]}"
    ((next += 1))
  done
  completed_pid=
  if wait -n -p completed_pid "${!pid_to_load[@]}"; then rc=0; else rc=$?; fi
  load=${pid_to_load["${completed_pid}"]}
  unset 'pid_to_load[$completed_pid]'
  if ((rc == 0)) && is_complete "${result_dir}/hadoop-${load}G/hadoop-${load}G-summary.csv"; then
    status "${load}G" COMPLETED "${completed_pid}" 0
    echo "[$(date -Is)] completed hadoop-${load}G; releasing slot" | tee -a "${result_dir}/launcher.log"
  else
    status "${load}G" FAILED "${completed_pid}" "${rc}"
    echo "[$(date -Is)] failed hadoop-${load}G (exit ${rc}); releasing slot" | tee -a "${result_dir}/launcher.log"
    failed=1
  fi
done

if ((failed)); then
  echo "[$(date -Is)] queue finished with failures; see ${status_file}" | tee -a "${result_dir}/launcher.log"
  exit 1
fi
echo "[$(date -Is)] queue finished successfully; see ${status_file}" | tee -a "${result_dir}/launcher.log"
