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

The default BIDL service profile is intended for network-focused experiments: 20 us of sequencer
service per transaction, 10 us of effective verification, 15 us of effective execution, 5 us of
PBFT receive processing per message, and 100/500 us per block for approval and persistence. A
calibrated 10 ms consensus software residual and 2.5 ms local commit cost represent cryptography,
thread scheduling, and framework work that is outside the packet-level model. Verification and
execution are effective multi-worker service times, not
single-core cryptographic microbenchmarks. They keep a serial executor near 40 kTx/s so network
congestion can be studied without the old 250 us execution delay imposing a 4 kTx/s CPU ceiling.

The BIDL phase trace also exports paper-style measurements: phase 6 is P1 consensus
(proposal-to-decision), phase 7 is P2 verification and execution, phase 8 is P3 persistence, phase 9
is P4 execution total (P2 + P3), phase 10 is P5 commit, and phase 11 is ingress-to-commit end-to-end
latency. Named P50 and P95 values are included in `-summary.csv`.

```sh
build/src/consensus/examples/ns3.39-consensus-example-default \
  --preset=pbft-rdma --requestCount=100 --outputPrefix=results/pbft-rdma

build/src/consensus/examples/ns3.39-consensus-example-default \
  --preset=bidl-rdma --requestCount=100 --outputPrefix=results/bidl-rdma
```

Configuration files use one `key=value` entry per line. Resolution order is built-in defaults,
selected preset, configuration file, then command-line overrides. See
`examples/consensus-experiment.conf`.

`examples/bidl-k4-fat-tree.conf` selects the file-backed classic k=4 Fat-Tree topology: 16 hosts,
8 edge switches, 8 aggregation switches, and 4 core switches. Its BIDL orderers and executors are
placed one per pod, with the sequencer on a separate host.

The same configuration reserves host 3 for an optional malicious broadcaster. Enable it with
`--bidlAttackEnabled=true`. The broadcaster observes sequencer transactions, keeps each sequence
number, changes its digest, and sends the forged copy over the simulated network. Executors retain
the first digest received for each sequence and reject later conflicts; only a mismatch against the
ordered block digest causes whole-block re-execution. `bidlAttackProbability`,
`bidlAttackTargetExecutors`, `bidlAttackStartMs`, `bidlAttackStopMs`, and
`bidlAttackProcessingUs` control the attack. The summary reports forged transactions/messages/bytes,
rejected conflicts, affected blocks, and re-execution counts and latency.

`examples/bidl-attacker-bypass.conf` is a controlled asymmetric-path attack experiment. Normal
sequencer-to-executor traffic crosses switch link 10-11 together with a same-priority RDMA background
flow, while sequencer-to-malicious-to-executor traffic uses the independent 10-12-14-13 bypass. Its
150 us attack processing time is chosen so that the forgery loses the race without background
traffic but can win when the direct path is congested.

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
