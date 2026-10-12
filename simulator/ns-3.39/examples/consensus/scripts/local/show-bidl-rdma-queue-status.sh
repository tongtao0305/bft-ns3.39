#!/usr/bin/env bash
# Usage: show-bidl-rdma-queue-status.sh <experiment-name>
set -euo pipefail

[[ $# == 1 ]] || { echo "Usage: $0 <experiment-name>" >&2; exit 2; }
readonly script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ns3_dir=$(cd -- "${script_dir}/../../../.." && pwd)
readonly result_dir="${ns3_dir}/run-logs/$1"
readonly status_file="${result_dir}/queue-status.tsv"

[[ -f "${status_file}" ]] || { echo "No queue status found: ${status_file}" >&2; exit 1; }
echo "Queue status: ${status_file}"
column -t -s $'\t' "${status_file}"
echo
echo 'Live ns-3 workers:'
pgrep -af 'ns3.39-bidl-experiment-optimized' || true
