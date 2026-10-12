#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
binary="${ns3_dir}/build/examples/consensus/ns3.39-bidl-experiment-optimized"
config="examples/consensus/configs/baseline/bidl-8x8-hadoop.conf"
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir="${ns3_dir}/results/bidl-8x8-hadoop-${timestamp}"

mkdir -p "${result_dir}"
for load in 5 10 15 20 25 30; do
  flow="examples/consensus/workloads/hadoop/bg-hadoop-s8l8h128-${load}G-1s-raw.txt"
  prefix="${result_dir}/hadoop${load}G"
  (
    cd "${ns3_dir}"
    exec "${binary}" --configFile="${config}" --flowFile="${flow}" --outputPrefix="${prefix}" --run="${load}"
  ) >"${result_dir}/hadoop${load}G.log" 2>&1 &
  echo "$! hadoop${load}G" >>"${result_dir}/pids"
done

echo "Started six runs; results: ${result_dir}"
wait
