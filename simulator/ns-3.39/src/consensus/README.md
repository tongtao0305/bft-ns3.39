# Consensus experiment module

The module keeps the main responsibilities separate without giving every small class its own file.
Resource behavior lives with `ConsensusEngine`, fault injection lives with `ConsensusTransport`,
and RDMA workload replay lives with `ConsensusWorkloadProvider`. `ConsensusRunner` is the
common execution path for replicated protocols and the BIDL consensus-execution pipeline.

The BIDL path follows the paper's two-level workflow. The sequencer immediately assigns a sequence
number to each client transaction and broadcasts it to every orderer and executor. Executors process
transactions speculatively in sequence order. The ordering leader groups consecutive transactions by
`batchSize` or `batchTimeoutMs`, then PBFT orders only their digest list. An executor persists and
commits a block after ordering, execution, and 2f+1 matching Orderer Persist vectors have completed.
Missing sequenced transactions are requested from the sequencer with bounded retries. Inconsistent
executor result vectors cause an abort notification. A transaction whose
digest is `BidlApplication::INVALID_TRANSACTION_DIGEST` is treated as malicious or incorrect by the
lightweight fault model; its block incurs one corrective re-execution before persistence. Retransmitted
client requests keep their original sequence, and every orderer submits the same block candidate to its
local PBFT engine so a new primary can continue after a view change. Window throughput counts
transactions completed inside the measurement window; submission-cohort latency follows slow requests through cooldown.

Built-in engines, transports, topologies, and switch policies are selected directly from the
experiment configuration. Invalid combinations such as RDMA over a TCP-only topology are rejected
before topology construction.

## Running experiments

Built-in presets are `pbft-tcp`, `pbft-rdma`, `bidl-tcp`, `bidl-rdma`, and
`zyzzyva-rdma`.

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
./ns3 run "bidl-experiment \
  --preset=pbft-rdma --requestCount=100 --outputPrefix=results/pbft-rdma"

./ns3 run "bidl-experiment \
  --preset=bidl-rdma --requestCount=100 --outputPrefix=results/bidl-rdma"
```

Configuration files use one `key=value` entry per line. Resolution order is built-in defaults,
selected preset, configuration file, then command-line overrides. See
`examples/consensus/configs/baseline/consensus-experiment.conf`.

File-backed networks use `topology=qbb-file` and `topologyFile=...`; the current topology files are
listed in `examples/consensus/topologies/`. BIDL roles are assigned to physical host IDs, so verify
the role placement against the selected topology before comparing runs.

Enable BIDL's malicious broadcaster with `bidlAttackEnabled=true`. The broadcaster can alter a
transaction digest while retaining its sequence number. `bidlAttackProbability`,
`bidlAttackTargetExecutors`, `bidlAttackStartMs`, `bidlAttackStopMs`, and
`bidlAttackProcessingUs` control injection. Executors reject conflicting copies; a mismatch against
the ordered block digest can trigger whole-block re-execution. The summary reports forged
transactions/messages/bytes, rejected conflicts, affected blocks, and re-execution counts and latency.

```sh
./ns3 run "bidl-experiment \
  --configFile=examples/consensus/configs/baseline/consensus-experiment.conf \
  --requestCount=200"
```

Workload modes are `open-loop`, `closed-loop`, `burst`, and `trace`; `auto` selects trace replay
when the flow file contains `app_id=1` records and otherwise selects open-loop generation.
`clientCount` controls synthetic clients, while multi-client trace replay discovers distinct
clients from the physical source IDs.

```sh
./ns3 run "bidl-experiment \
  --preset=pbft-rdma --clientCount=4 --workload=closed-loop \
  --requestCount=100 --closedLoopThinkTimeUs=20"
```

Use `applicationStartTime`, `warmupTime`, `measurementTime`, and `cooldownTime` to define the
experiment phases. Window throughput uses completion times, while latency follows requests submitted
in the measurement window through cooldown. Each written run also produces `-manifest.json` with the Git revision, dirty state, build,
seed/run, phase boundaries, protocol-version label, simulation boundary, and completion status.
`-candidate-events.csv` records candidate send/receive with view or epoch, instance, digest,
and mark bit; `-iors-events.csv` records switch conflict detection; `-request-events.csv`
records client submission and completion timestamps. BIDL also exports transaction and block paths.
For Zyzzyva and EZBFT, `-phases.csv` uses phase 1 for speculative execution,
2 for protocol-confirmed equivocation, 3 for replica final execution on a certified
path, and 4 for rollback or re-execution. Zyzzyva phases 5 through 9 record the
first arrival of verified double-`ORDER-REQ` evidence at a replica, a view change
started after evidence-backed accusations, a view change started without verified
equivocation evidence, rejection of an invalid ordering history, and the initial
progress timeout, respectively. This makes switch detection, evidence propagation, and
protocol response separately observable. Zyzzyva fast-path client completion is
reported separately from a replica `LOCAL-COMMIT`; the original fast path does not
send a final-commit notification to every replica.
The manifest compares BIDL executor committed block digests, Zyzzyva replica log
fingerprints, and EZBFT finalized per-instance tuple digests. Its replica progress
range counts committed transactions for BIDL, log entries for Zyzzyva, and finalized
instances for EZBFT. Zyzzyva log agreement alone is not a final-commit certificate.
`ezbft_legal_updates` counts replica-local dependency or sequence changes relative to
the authenticated Owner proposal; only a second contradictory Owner proposal increments
`ezbft_equivocations`.

```sh
./ns3 run "bidl-experiment \
  --preset=pbft-rdma --batchRuns=5 --run=1 \
  --warmupTime=0.2 --measurementTime=1.0 --cooldownTime=0.2 \
  --outputPrefix=results/pbft-rdma-batch"
```

When `batchRuns` is greater than one, the same entrypoint advances the ns-3 run number and writes
`-aggregate.csv` with the mean,
cross-run P95/P99, and 95% confidence-interval half-width. Consensus packets carry message type,
stable flow ID, and priority group as packet metadata. Qbb switch policies receive enqueue,
dequeue, ECN-mark, and drop events through `ObserveEnqueue`, `ObserveDequeue`, `ObserveMark`, and
`ObserveDrop`; default counters are written to `-switches.csv`.

## Validation

The [implementation and recovery review](PROTOCOL-RECOVERY-REVIEW.md) records
the current paper-level gaps and targeted negative probes. Existing successful
runs do not establish complete BIDL sequencer failover/canonical re-execution or
complete Zyzzyva recovery and application-work accounting.

```sh
build/utils/ns3.39-test-runner-default --suite=consensus --verbose
build/utils/ns3.39-test-runner-default --suite=consensus-e2e --verbose
build/utils/ns3.39-test-runner-default --suite=zyzzyva-recovery --verbose
```

## IORS and recovery experiments

The RDMA presets keep a 1024-byte request payload. Set `rdmaMtu=1200` for IORS
experiments so the 66-byte consensus header, the 8-byte mutable IORS extension,
and a normal request fit in one RDMA payload. Larger frames still use RDMA
fragmentation and bypass switch IORS inspection. The 128-host BIDL and Zyzzyva
IORS configurations are in `examples/consensus/configs/baseline/`.

`iorsMode` is independent of `iorsTerminalGate` and `bidlDenylistEnabled`:

- `0` disables switch inspection; `1` marks conflicts; `2` marks and applies a
  fixed bounded wait; `3` uses a bounded RTT estimate. Only an unauthorized
  BIDL sequence binding can be actively held. A Zyzzyva double proposal is
  marked and forwarded without choosing a winner.
- The terminal gate prefers the first unmarked candidate at each sequence that
  passes ordinary protocol verification. Its timeout releases the earliest
  marked candidate to the ordinary protocol path.
- Marked packets bypass candidate state at every subsequent switch. Missing
  source evidence, fragmented frames, expired/full state, or fingerprint
  collisions pass through without IORS intervention. The extension sits outside
  the protocol signature. A receiver with `IorsWireEnabled=false` removes the
  extension at the RDMA reassembly edge before application delivery.

The switch only inspects packet bytes, IP source, ingress port, and an installed
source/port map. Its source map is a simulation of data-center admission
configuration, not a simulator identity tag or a cryptographic authentication
decision. A forged source field from an authorized host on an indistinguishable
route cannot be proved malicious by this evidence alone. These experiments
use the updated parser; the edge-strip path provides compatibility for a
nonparticipating application behind the updated RDMA transport.

Zyzzyva has signed ORDER-REQ, speculative responses, commit certificates,
gap repair, checkpoints, and view-change records in the packet-level model.
`zyzzyvaRecoveryRule=0` selects the original paper's history rule for a labeled
comparison. `zyzzyvaRecoveryRule=1` selects the Velma-style corrected history
selection and is the default. Deterministic simulated per-replica authentication
binds signer identity and complete message contents while keeping cryptographic
CPU cost out of protocol throughput runs. Production key distribution and the
complete application state machine are outside this model. The recovery unit test exercises a known
multi-view certificate pattern, but is not a proof of the full Velma theorem.

BIDL's denylist applies to a conflicting client, while its transactions remain
eligible for ordered execution. This repository's BIDL path has one fixed
sequencer and PBFT orderers, so its cross-view suspicion is measured across
orderer leaders rather than the paper's rotating sequencer leaders. The model
aggregates f+1 executor reports and f+1 orderer endorsements, carries prepared
PBFT history through view change, resends accusation records alongside later
view changes, and exposes the resulting denylist size. It does not reproduce
the paper's separate organization model, all endorsement cryptography, or
the exact view-change piggyback wire format.

The manifest records IORS observed/marked/waited/already-marked and state
bypass counters, along with the selected recovery rule and denylist size.

### Three-protocol IORS comparison

The checked-in arm files under `examples/consensus/configs/baseline/` provide
paired BIDL and Zyzzyva runs: `*-no-iors.conf`, `*-detect.conf`,
`*-mark.conf`, and `*-gate.conf`. Zyzzyva's gate arm forwards both authenticated
ORDER-REQ proposals; it only delays local speculative execution for the
configured bound, after which the normal protocol handles both proposals.
`-iors-events.csv` records switch-side conflict detections. Run every arm with
the same `--seed`, `--run`, topology, and workload values; use `--batchRuns`
for repeated runs. The summary/manifest records protocol counters and selected
settings, while raw per-run files remain available for analysis.

The `ezbft-original` and `ezbft-research` labels select distinct modes in the
manifest. The latter is a research variant that adds PBFT confirmation of an
instance's final tuple; it is not an official EZBFT revision. The current
EZBFT engine is an ns-3 packet-level prototype: it models owner/epoch/instance
keys, tuple proposals and replies, client fast/slow certificates, speculative
and final counters, timeout-driven owner changes, and the research finalization
path. Its dependency representation is a 64-bit conflict-key mask and its
application execution is represented by protocol decisions. It does not yet
implement the full paper state machine, a workload read/write dependency DAG,
or reproduce the published original-protocol safety counterexample. Results
from these EZBFT configs must therefore be described as prototype-path
measurements, not as a faithful reproduction or a safety evaluation of the
published protocol. The owner-change transition requires an N-f quorum of
reports. A complete evaluation still needs the paper-level counterexample and
recovery/progress scenarios before drawing safety conclusions. EZBFT is frozen
at this IORS-prototype scope: the current regression is retained, but no new
protocol work or performance study is planned unless a regression blocks the
BIDL/Zyzzyva work.

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

## Recovery iteration 2

See [the communication matrix](IORS-COMMUNICATION-MATRIX.md) and
[implementation boundaries](PROTOCOL-RECOVERY-IMPLEMENTATION.md) for the BIDL
leader-sequencing and Zyzzyva authenticated recovery paths.
The reproducible evaluator is `examples/consensus/run-recovery-validation.py`;
`--formal` runs five seeds with paired arms, load points and wait times.
Abort notifications are terminal outcomes separate from committed throughput.

### Final communication and recovery evaluation (2026-10-14)

See [the final report](../../examples/consensus/results/recovery-client-view-2026-10-14/REPORT.md) for the stable-source 108-run evaluation, 10,000-transaction completion checks, paired Zyzzyva equivocation arms and simulation boundaries. Earlier recovery result directories are historical intermediate runs.

### Measurement and cache-repair corrections (2026-10-26)

`window_transaction_throughput` counts unique transactions completed in the
half-open measurement interval; cooldown completions do not enter its numerator.
`transaction_throughput` remains the finite-batch rate, including drain time.
Client latency percentiles follow requests submitted in the measurement interval
through cooldown. The summary also reports the cohort's submitted, incomplete,
aborted and completed sample counts; unresolved requests have no invented latency.
`request-events.csv` includes `transaction_count` for weighted throughput analysis.

BIDL cached retransmissions use `BIDL_TRANSACTION_REPAIR` (65), distinct from new
sequencing broadcasts (30). IORS allows cache repairs from the configured gateway
and orderers while retaining source mapping and the same epoch/sequence/content
bindings. Executors reject broadcaster-originated repair messages. Client ingress
already uses `CLIENT_REQUEST`; the corrected false-positive path was cached repair.

Zyzzyva phase 14 means NEW-VIEW installed, 15 application recovery started,
16 undo completed, 17 application state recovery completed, and 18 view change
entered. Phases 14–18 identify the target view in `instance_id`. Phase 17 occurs
after queued undo/replay and before draining pending requests. It does not assert
that a subsequent throughput window has returned to its pre-attack rate.
Recovery phases and completed background flows are retained through cooldown.
The evaluator requires each correct replica's phase 17 for the same target view
before reporting recovery completion; missing events remain missing.
