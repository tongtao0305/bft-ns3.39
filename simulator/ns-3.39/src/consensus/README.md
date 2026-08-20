# Consensus experiment module

The module keeps the main responsibilities separate without giving every small class its own file.
Resource behavior lives with `ConsensusEngine`, fault injection lives with `ConsensusTransport`,
and RDMA workload replay lives with `ConsensusWorkloadProvider`. `ConsensusRunner` is the
common execution path for replicated protocols and the BIDL consensus-execution pipeline.

The BIDL path follows the paper's two-level workflow. The sequencer immediately assigns a sequence
number to each client transaction and broadcasts it to every orderer and executor. Executors process
transactions speculatively in sequence order. The ordering leader groups consecutive transactions by
`batchSize` or `batchTimeoutMs`, then PBFT orders only their digest list. An executor persists and
commits a block after both speculative execution and ordering have completed. A transaction whose
digest is `BidlApplication::INVALID_TRANSACTION_DIGEST` is treated as malicious or incorrect by the
lightweight fault model; its block incurs one corrective re-execution before persistence. Retransmitted
client requests keep their original sequence, and every orderer submits the same block candidate to its
local PBFT engine so a new primary can continue after a view change. Throughput samples include only
requests that are both submitted and completed inside the measurement window.

Built-in engines, transports, topologies, and switch policies are selected directly from the
experiment configuration. Invalid combinations such as RDMA over a TCP-only topology are rejected
before topology construction.

## Running experiments

Built-in presets are `pbft-tcp`, `pbft-rdma`, `bidl-tcp`, and `bidl-rdma`.

```sh
build/src/consensus/examples/ns3.39-consensus-example-default \
  --preset=pbft-rdma --requestCount=100 --outputPrefix=results/pbft-rdma

build/src/consensus/examples/ns3.39-consensus-example-default \
  --preset=bidl-rdma --requestCount=100 --outputPrefix=results/bidl-rdma
```

Configuration files use one `key=value` entry per line. Resolution order is built-in defaults,
selected preset, configuration file, then command-line overrides. See
`examples/consensus-experiment.conf`.

```sh
build/src/consensus/examples/ns3.39-consensus-example-default \
  --configFile=src/consensus/examples/consensus-experiment.conf \
  --requestCount=200
```

Workload modes are `open-loop`, `closed-loop`, `burst`, and `trace`; `auto` selects trace replay
when the flow file contains `app_id=1` records and otherwise selects open-loop generation.
`clientCount` controls synthetic clients, while multi-client trace replay discovers distinct
clients from the physical source IDs.

```sh
build/src/consensus/examples/ns3.39-consensus-example-default \
  --preset=pbft-rdma --clientCount=4 --workload=closed-loop \
  --requestCount=100 --closedLoopThinkTimeUs=20
```

Use `applicationStartTime`, `warmupTime`, `measurementTime`, and `cooldownTime` to define the
experiment phases. Latency and throughput only include requests submitted in the measurement
window. Each written run also produces `-manifest.json` with the Git revision, dirty state, build,
seed/run, phase boundaries, and completion status.

```sh
build/src/consensus/examples/ns3.39-consensus-example-default \
  --preset=pbft-rdma --batchRuns=5 --run=1 \
  --warmupTime=0.2 --measurementTime=1.0 --cooldownTime=0.2 \
  --outputPrefix=results/pbft-rdma-batch
```

When `batchRuns` is greater than one, the same entrypoint advances the ns-3 run number and writes
`-aggregate.csv` with the mean,
cross-run P95/P99, and 95% confidence-interval half-width. Consensus packets carry message type,
stable flow ID, and priority group as packet metadata. Qbb switch policies receive enqueue,
dequeue, ECN-mark, and drop events through `ObserveEnqueue`, `ObserveDequeue`, `ObserveMark`, and
`ObserveDrop`; default counters are written to `-switches.csv`.

## Validation

```sh
build/utils/ns3.39-test-runner-default --suite=consensus --verbose
build/utils/ns3.39-test-runner-default --suite=consensus-e2e --verbose
```

The unit suite checks messages, protocol behavior, resources, topology construction, configuration
precedence, and legacy trace replay. The end-to-end suite runs PBFT and BIDL over TCP and RDMA/Qbb,
including RDMA incast and long/short background workloads.

## Extension points

- Add a consensus protocol by implementing `ConsensusEngine` and adding its selection branch in
  `ConsensusExperimentConfig`.
- Add a network protocol by implementing `ConsensusTransport` and adding its selection branch in
  `ConsensusExperimentConfig`.
- Add a topology or switch behavior in the corresponding construction function in
  `consensus-experiment.cc`.
- Add transaction generators by implementing `ConsensusWorkloadProvider`; no Runner change is needed.
- Add experiment-wide wiring in `ConsensusRunner`; keep example programs as thin entrypoints.
- Add command-line parameters once in `ConsensusExperimentConfig`; they automatically become
  available to configuration files.
