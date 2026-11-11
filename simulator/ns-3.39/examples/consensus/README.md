# BFT consensus experiments

This directory is the runnable, reproducible BFT experiment package.  It is
separate from `src/consensus`, which contains only the reusable ns-3 consensus
module.

## Layout

- `bidl-experiment.cc`: experiment entry point built by CMake; it is the
  common PBFT/BIDL runner.
- `configs/baseline/`: the maintained 128-host configurations for validation
  and protocol/IORS comparisons. Vary congestion control, workload, attack and
  request rate with command-line overrides instead of keeping one file per
  parameter set.
- `topologies/`: file-backed Qbb/Fat-tree/leaf-spine topologies.
- `workloads/`: flow traces and external-workload inputs.
- `scripts/local/`: local campaign launchers and status helpers.
- `scripts/slurm/consensus-slurm.py`: one entry point for preparing, submitting,
  executing and collecting Slurm array experiments.

Run from the ns-3 root with a configuration path, for example:

```sh
python3 examples/consensus/run-experiment.py \
  --config examples/consensus/configs/baseline/config-bidl-128host-noattack.conf \
  --seed 1 --output "results/bidl-noattack-$(date +%Y%m%d-%H%M%S)"
```

Output belongs in `results/` or `run-logs/`, never in this directory.  Paths in
configuration files are relative to the ns-3 root; launchers change to that
directory before invoking the binary.

The two no-attack profiles are the recommended bases for new experiment matrices:

| Configuration under `configs/baseline/` | Fixed protocol roles |
|---|---|
| `config-bidl-128host-noattack.conf` | Ten orderers, ten executors, dedicated sequencer at host 8, client at host 9 |
| `config-zyzzyva-128host-noattack.conf` | Ten replicas at the same hosts as BIDL orderers, initial primary at host 0, client at host 9 |

Both use the 128-host 4:1 leaf-spine topology, DCQCN and f=3; one client submits
50,000 one-transaction requests at 10k/s during seconds 0–5, and simulation stops
at second 20. Attacks, IORS, trace replay and synthetic background traffic are
disabled. Matrix cases should choose the matching base file, then override the
variables under study; there is no need to switch protocols inside a single
protocol's base configuration.

The additional `configs/baseline/bidl-128host.conf` profile uses the 4-spine/8-leaf 4:1
topology, DCQCN, ten orderers and ten executors, and one client submitting 50,000
requests at 10k/s during seconds 0–5. It replays the normalized Hadoop 5G/host
trace during seconds 2–3, enables sequence-tampering attacks and IORS marking
with a 1 ms terminal gate, and stops at second 20. Its sequencer is a dedicated
host (`bidlNativeSequencing=false`). Run it with:

```sh
python3 examples/consensus/run-experiment.py \
  --config examples/consensus/configs/baseline/bidl-128host.conf \
  --seed 1 --output "results/bidl-128host-$(date +%Y%m%d-%H%M%S)"
```

All 20 Hadoop traces in `workloads/hadoop/` now have a flow-count header and
seven integer columns (`src dst PG port bytes absolute_start_ns app_id`), with
`app_id=0` for every background record. They can be replayed directly without
another conversion. The repaired files replace the originals; no raw backups
are retained. Synchronize the current trace files to workers.
Completion requires both `completed_requests=50000` and
`background_completed_flows=630891` in `run-summary.csv`; the stop time does not
guarantee either. The terminal gate can fall back to executing a marked candidate
after 1 ms, rather than permanently excluding it.

Slurm uses the same `run-experiment.py` raw-data launcher as local single runs.
Neither path runs evaluation automatically. Run the commands below from the ns-3 root on the controller.

For cluster setup, configuration organization, synchronization and submission,
see the [three-node Slurm guide (Chinese)](../../../../SLURM_GUIDE.zh-CN.md).

```sh
# Prepare the maintained BIDL matrix: nine comparison arms x ten Hadoop loads,
# fixed seed=1/run=1 (90 tasks). No jobs are started.
python3 examples/consensus/scripts/slurm/consensus-slurm.py prepare \
  --output run-logs/campaign-001

# Preview submission, then omit --dry-run to submit.
python3 examples/consensus/scripts/slurm/consensus-slurm.py submit \
  run-logs/campaign-001 --partition YOUR_PARTITION --dry-run

# After jobs finish, collect worker-local results and logs onto the controller.
python3 examples/consensus/scripts/slurm/consensus-slurm.py collect \
  run-logs/campaign-001
```

Before submission, manually synchronize the project, build products, referenced
topology/flow files and the prepared campaign directory to each worker at the
same absolute paths. The default workers are `ubuntu-236` and `ubuntu-237`;
override them with `--workers HOST1 HOST2` on both `submit` and `collect`.
Each simulation uses one CPU. The Hadoop matrix budgets one simultaneous task per
worker, 16 GiB per task and a four-hour time limit; change these using
`--tasks-per-node`, `--memory` and `--time`. Resource precedence is CLI, environment,
prepared matrix `slurm`, then tool defaults; these budgets are not measured completion guarantees. Tasks are independent simulations.
The tool does not split a single simulation across workers or build/sync files.

The maintained matrix lives in `scripts/slurm/bidl-hadoop-matrix.json`.
For a custom matrix, export `example --output matrix.json`, edit it and run
`prepare --spec matrix.json --output run-logs/NEW_CAMPAIGN`. Each case can specify
a `base_config` path under the ns-3 root. Parameter precedence is base config,
`defaults`, case `parameters`, then `axes`; seeds repeat each combination.
Only 128-host file topologies are accepted. Generated per-task configurations
stay in the campaign directory, rather than adding presets to the source tree.
Campaign paths must not contain whitespace because the simulator truncates
whitespace in its output prefix.

The `run-task CAMPAIGN --task-id N` subcommand executes one prepared task locally
for diagnosis; Slurm normally supplies its task ID automatically. Configuration
and runtime fingerprints must match preparation. Topology and flow files are also
SHA-256 checked at submission and on the worker; each unique input is hashed once
during preparation and once per verification call. Existing attempt directories
are never overwritten, and incomplete workloads exit with code 3 while keeping
their results. Changes after preparation require a new campaign directory.

## 参数覆盖与实验参数核对

`run-experiment.py` 支持当前模拟器 `--help` 中的实验参数，例如：

```sh
python3 examples/consensus/run-experiment.py \
  --config examples/consensus/configs/baseline/bidl-128host.conf \
  --seed 1 --run 1 --output results/bidl-hpcc-20k \
  --rdmaCcMode=3 --requestRate=20000 --requestCount=100000
```

支持 `--key=value` 和 `--key value`，布尔参数显式写 `true/false`。
顺序为：模拟器默认值 → 选中的 preset → 基础配置 → 命令行覆盖。
更换 `preset` 时会先应用新 preset，再应用合并后的其他配置；不会自动清除
基础配置中的协议专用字段，也不会自动修改节点数量、位置、故障阈值或请求总量。
参数名称从实际可执行文件读取，拼写错误在启动实验前拒绝；数值及组合约束由 ns-3 检查。
基础配置保持不变。单次运行仅采集原始数据，不再自动生成 `result.json`、`result.csv` 或攻击曲线。每次输出包括：

| 文件 | 含义 |
|---|---|
| `base.conf` | 原基础配置的副本 |
| `input.conf` | 合并覆盖项后的可运行配置，包含本次 seed、run 和输出前缀 |
| `overrides.json` | 命令行覆盖项与运行标识 |
| `command.json`、`console.log` | 实际命令与完整控制台日志 |
| `run-config.json` | 模拟器应用默认值、preset 和配置后解析的参数 |
| `run-status.json` | 进程退出码、协议状态、宿主机耗时；不执行自动分析 |
| `run-*.csv`、`run-manifest.json` | 原始事件、原生计数和协议检查 |

`--flowFile=` 会移除基础配置中的文件回放设置，回到空默认值；
`--ordererPhysicalIds=` 等空默认字符串也可这样清除。数字、布尔值或具有非空默认值的
参数不能用空值清除。`--output` 统一管理输出前缀，不能另传 `--outputPrefix`；
该入口一次运行一个实验，不接受 `--batchRuns`。Slurm 矩阵可通过 `"flowFile": ""`
表达相同清除操作，并通过参数轴生成组合。

### 协议、拓扑和背景流量

| 类别 | 正确参数 / 当前内容 |
|---|---|
| 协议 | `preset=bidl-rdma`、`preset=zyzzyva-rdma` |
| 文件拓扑 | `topology=qbb-file`，具体文件通过 `topologyFile=examples/consensus/topologies/文件名` 指定 |
| 背景回放 | `flowFile=examples/consensus/workloads/目录/文件名`，文件时间是仿真绝对纳秒；当前数据均为 `app_id=0` |
| 背景优先级 | 文件中 PG 直接生效；`rdmaBackgroundPriorityGroup` 不会覆盖 trace 中的 PG |

核对存在的六个拓扑如下，链路均为 100Gbit/s、1μs：

| 文件 | 主机编号 | 超售比 |
|---|---|---|
| `topology-leaf-spine-4x8x128-41os.txt` | 0–127 | 4:1 |
| `topology-leaf-spine-8x8x128-21os.txt` | 0–127 | 2:1 |
| `topology-fat-tree-k8-128-41os.txt` | 0–127 | 4:1 |
| `topology-fat-tree-k8-128-21os.txt` | 0–127 | 2:1 |
| `topology-leaf-spine-8x16x256-41os.txt` | 0–255 | 4:1 |
| `topology-leaf-spine-8x16x256-21os.txt` | 0–255 | 2:1 |

单次运行入口可以选择这六个文件。当前 Slurm 准备工具按已有要求仅接受 128 主机拓扑；
保留 256 主机拓扑文件不代表已有配套的 256 主机背景流量。

| 流量目录和文件系列 | 现有数量 | 实际注入时间 |
|---|---:|---|
| `hadoop/bg-hadoop-128host-{5,10,...,50}G-0t1.txt` | 10 | 0–1s |
| `hadoop/bg-hadoop-128host-{5,10,...,50}G-2t3.txt` | 10 | 2–3s |
| `gall/bg-gall-128host-{5,10,...,50}G-20t21.txt` | 10 | **2.0–2.1s**，文件名仍保留旧标签 |
| `gsearch/bg-gsearch-128host-{5,10,...,50}G-20t21.txt` | 10 | **2.0–2.1s**，文件名仍保留旧标签 |
| `bg-hadoop-256host-...` | **0** | 当前不存在，不能直接使用清单中的通配名称 |

这些花括号是文件系列说明，运行时必须指定一个真实文件路径，不能把 `[5G-50G]`
作为文件名传入。G 表示每主机标称发送速率。流量分布和端点模式是两个变量：
GSearch/GALL/Hadoop 不等于均匀/incast 开关，回放的源、目的、大小和时间由文件记录决定。
另有内建的轮转长流/短流（`rdmaLongFlows`、`rdmaShortFlows`）和 incast
（`rdmaIncastSenders`、`rdmaIncastBursts`、`rdmaIncastFlowBytes`、
`rdmaIncastStartMs`、`rdmaIncastIntervalUs`）；内建开始偏移相对应用启动时间。
使用纯文件回放时将这些内建流量数量和 `rdmaConstantRateGbps` 设为 0，避免重复注入。

### 拥塞控制、攻击与 IORS

| 类别 | 参数与含义 |
|---|---|
| 拥塞控制 | `rdmaCcMode=1` DCQCN；`3` HPCC；`7` TIMELY；`8` DCTCP。不同模式也会选择相应的反馈/INT 路径 |
| 共同网络条件 | `enableEcn`、`qbbQcnEnabled`、`qbbPfcEnabled`、`rdmaMtu`、缓冲和 ECN 阈值；对比时保持一致，算法专用参数另行记录 |
| BIDL 攻击 | `bidlAttackEnabled=true`；`bidlAttackMode=1` 监听后将序号 `s` 改为 `s+1`，保留客户内容与认证；模式 0 是串通客户端内容替换 |
| BIDL 攻击控制 | `bidlMaliciousNodeCount`、`bidlMaliciousPhysicalIds`、`bidlAttackProbability`、`bidlAttackProcessingUs`、`bidlAttackTargetExecutors`（0=全部执行节点） |
| BIDL 攻击窗口 | `bidlAttackStartMs` / `bidlAttackStopMs` 为仿真绝对毫秒；停止值 0 表示不设停止边界 |
| Zyzzyva 攻击 | `zyzzyvaEquivocationEnabled=true`，主节点发送矛盾的 ORDER-REQ；`zyzzyvaEquivocationStartSequence` 控制起始序号，`zyzzyvaEquivocationTarget` 是逻辑副本编号 |
| Zyzzyva 攻击控制 | `zyzzyvaEquivocationDelayUs`、`zyzzyvaMaliciousReplicaIds`；`zyzzyvaEquivocationPlan` 支持 `view,primary,absolute-seconds,target` 项，以分号分隔；时间触发是在达到该时刻后的合适请求上发生 |
| Zyzzyva 恢复规则 | `zyzzyvaRecoveryRule=0` 原规则，`1` Velma 风格修正规则；比较时明确记录 |
| IORS 关闭 | `iorsMode=0`、`iorsDetectOnly=false`、`iorsTerminalGate=false` |
| IORS 标记 | `iorsMode=1`、`iorsDetectOnly=false`，交换机标记冲突消息 |
| IORS 固定等待 | `iorsMode=2`、`iorsFixedWaitUs`、`iorsMaxWaitUs`：标记后，对符合条件的 BIDL 非授权序号候选进行有界等待；不会把所有协议消息都固定等待 |
| IORS RTT 等待 | 代码还支持 `iorsMode=3`，同样只适用于符合条件的消息 |
| 仅检测 | `iorsMode=1`、`iorsDetectOnly=true`，检测但不打标记 |
| 终端门控 | 独立的 `iorsTerminalGate=true`、`iorsGateTimeoutUs=1000`：等待未标记候选，1ms 超时后回到正常协议路径；不是永久拒绝标记事务 |
| IORS 状态 | `iorsCapacity`、`iorsTtlUs`、`iorsFingerprintBits`；状态不足/过期及分片等情况会影响实际覆盖率，需看输出计数 |

切换拥塞控制通常只需覆盖 `rdmaCcMode`；切换共识协议还要核对攻击开关和角色部署。
例如，从当前 BIDL 配置切换到固定 10 副本的 Zyzzyva 正常组：

```sh
python3 examples/consensus/run-experiment.py \
  --config examples/consensus/configs/baseline/bidl-128host.conf \
  --seed 1 --run 1 --output results/zyzzyva-normal \
  --preset=zyzzyva-rdma --scenario=zyzzyva-128host \
  --bidlAttackEnabled=false --zyzzyvaEquivocationEnabled=false \
  --ordererPhysicalIds= --executorPhysicalIds= --bidlMaliciousPhysicalIds= \
  --replicaPhysicalIds=0,16,32,48,64,80,96,112,2,18 --ingressPhysicalId=0
```

这只切换协议和攻击/角色设置，其余网络、背景、请求量和资源参数继续继承基础配置。

### 规模、负载、时间与随机重复

| 项目 | 参数和约束 |
|---|---|
| 共识规模 | `replicaCount=10` 配 `faultTolerance=3`；25 配 8，满足 `n≥3f+1`。BIDL 会各创建 n 个排序和执行节点；Zyzzyva 创建 n 个副本 |
| 固定位置 | BIDL 用 `ordererPhysicalIds`、`executorPhysicalIds`；Zyzzyva 用 `replicaPhysicalIds`，其首项必须等于 `ingressPhysicalId`；客户端不能与这些角色重叠 |
| 单客户端负载 | `clientCount=1` 时，`requestRate` 是系统总速率。多个客户端时合计通常为每客户端速率乘客户端数，`requestCount` 也是每客户端数量 |
| 0–5s 注入 | `applicationStartTime=0`、`warmupTime=0`、`measurementTime=5`；10/20/30k 请求/s 分别配 `requestCount=50000/100000/150000`，每请求一笔事务时事务率相同 |
| 排空与停止 | `cooldownTime=15`、`simulationTime=20`；输入停止后，提交、重传和背景流完成仍可发生。20s 不保证所有请求或流都完成 |
| 单位 | `*Time` 窗口为秒，`*Ms` 为毫秒，`*Us` 为微秒，flow 文件启动时间为绝对纳秒 |
| 重传/恢复超时 | `requestTimeoutMs`、`retransmissionTimeoutMs`、`viewChangeTimeoutMs`；不能用大于剩余仿真时间的超时假定能观察到恢复 |
| 随机重复 | `--seed` 选择种子，`--run` 选择 ns-3 子流编号，二者都不是重复次数；未传 run 时使用基础配置的值，否则为 1 |
| 配对比较 | 不同 CC/IORS/攻击组使用相同 seed/run 和同一份流量文件；多次重复显式改变 seed 或 run |

`32.5k/s`、`40k/s` 不是代码中的硬编码吞吐上限。可达峰值依赖副本规模、资源模型、
批大小、拓扑、拥塞控制和攻击条件，不能把历史值当作所有配置的已验证上限。

固定 25 排序 + 25 执行节点时，可沿用当前 10 节点分布的前缀并扩展为以下列表，
保留定序器 8、客户端 9、攻击者 10，全部位于 0–127 主机范围且无角色重叠：

```ini
replicaCount=25
faultTolerance=8
ordererPhysicalIds=0,16,32,48,64,80,96,112,2,18,34,50,66,82,98,114,4,20,36,52,68,84,100,116,6
executorPhysicalIds=1,17,33,49,65,81,97,113,3,19,35,51,67,83,99,115,5,21,37,53,69,85,101,117,7
```

修改规模时必须同时替换完整 ID 列表；不能只把 `replicaCount` 从 10 改为 25。
Zyzzyva 的 25 副本可使用上述排序节点列表作为 `replicaPhysicalIds`，主节点设为 0，
并清除继承的 BIDL 角色列表。切换拓扑后，物理 ID 可保持一致，但共享链路和角色之间的
路径会随拓扑变化，应结合拓扑解释结果。


## 批次结束后手动统计

运行阶段只记录原始数据和 `run-status.json`，进程返回 0 表示原生协议检查完成，
3 表示未完成/观察截断，1 表示失败。Slurm 同样保留这些状态，不调用分析器。
种子统一固定 `seed=1, run=1`。重新准备 campaign 后同步更新的二进制和脚本；
旧 campaign 的运行指纹不能与新采集器混用。分析器本身不属于 worker 运行指纹，
允许收集后更新统计逻辑，而不重跑仿真。

一批任务完成并收集后，显式运行已有的 `consensus_single_run.py`：

```bash
python3 /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39/examples/consensus/consensus_single_run.py \
  /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39/run-logs/campaign-001/collected \
  --output /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39/results/analysis-campaign-001
```

可指定多个输入目录。输出必须是新目录或空目录，不修改单次实验的原始文件。
对 0–1s 注入实验，仅分析 0.25–0.75s，可追加
`--window-start 0.25 --window-stop 0.75`；它只影响离线统计，不改变注入。
未指定时沿用各次原生测量窗口。该窗口必须位于观察范围内。
**不要通过修改 warmupTime/measurementTime 来只裁剪统计：当前原生请求调度也使用这个窗口。**

| 批处理输出 | 指标与口径 |
|---|---|
| `summary.csv` | 固定窗口吞吐、完整工作负载吞吐/观察完成速率、完成率、P50/P95/P99、快慢路径、重新执行、通信和 IORS/PFC/丢包计数 |
| `throughput.csv` | 默认每 10ms 一个点，统计 `(t−250ms,t]`，除以 0.25s；仅输出完整窗口，客户端完成与 BIDL 首个/全部执行节点提交分开 |
| `recovery.csv` | 每次攻击及目标 view 的证据、首次换主、全部诚实副本安装/恢复、客户端 POM 和交换机告警时间；副本未到齐则留空，不能冒充恢复完成 |
| `paths.csv` | BIDL 定序器→执行节点、定序器→攻击者、攻击者→执行节点的应用消息时延；按完整消息身份匹配，报告未匹配计数 |
| `background.csv` | 流启动/完成/未完成数、完成率、FCT P50/P95/P99，分别统计所有流及 ≤100KiB、100KiB–10MiB、>10MiB 流 |
| `backlog.csv` | 每节点积压的时间加权均值、最大值、最后值及采样覆盖；原始 10ms 序列保留在每次实验中 |
| `analysis.json` | 统计定义、输入配置/manifest 指纹、失败列表和参考论文；空字段代表不可用，不代表零 |

新增原始采集默认启用，无需 `--diagnosticEvents=true`：

- `run-transaction-finalized.csv`：BIDL 每执行节点提交的客户端/请求/区块身份，离线去重。
- `run-request-events.csv`：新增 `completion_path`，区分 Zyzzyva `fast` / `slow`；
  客户端完成延迟与客户端发送到 BIDL 执行节点提交延迟分别报告。
- `run-transaction-path.csv`：关键 BIDL 消息默认保留，新增 message_id、view、客户端/请求、digest、纳秒时间；
  记录的是应用交给传输到完整接收的耗时，包含网络与传输排队，不是单条链路传播时延。
- `run-replica-backlog.csv`：Zyzzyva 每 10ms 的待验证消息、待应用事务、缓冲排序消息和恢复状态；
  BIDL 使用原有 `run-executor-backlog.csv`。这些不等于客户端未完成请求数。
- `run-background-events.csv`：所有实际启动流及完整接收事件；FCT 是应用提交到完整接收，不是发送端 ACK。
- `run-phases.csv`：新增 view，用于隔离多轮攻击和恢复。事件中的 node_id 是逻辑 ID。

整体吞吐分母为首次请求发送到最后客户端完成；存在未完成请求时，完整负载吞吐留空，
另报观察到的完成速率。固定窗口按完成/提交落点计数；延迟则跟踪窗口内发送的请求，
允许在排空期完成，同时报告尚未完成的样本数。快速路径成功率与重新执行率不是互补关系。
BIDL 重新执行率使用“发生过重新执行的已提交执行节点块 / 所有已提交执行节点块”，
Zyzzyva 另报诚实副本重新执行事件占执行事件比例；不能把跨副本计数直接除以客户端事务数。
单事务请求下另报失效推测执行、重新执行、取消任务的模型 CPU 服务量（跨诚实副本相加，不是墙钟恢复耗时）。
网络丢包/PFC 为设备事件汇总，不等于端到端唯一丢包数。FCT 分位数只覆盖完成流，必须结合完成率。

旧结果缺少新增字段时会明确标为 legacy 或留空；缺失原始日志不能由离线分析补造。
批处理遇到坏文件继续分析其他实验，并在 analysis.json 报错、返回非零状态。
不能根据进程结束或某个副本恢复事件就认定整个系统恢复。

指标选择参考 [Zyzzyva 的推测失效与恢复分析](https://www.cs.cornell.edu/lorenzo/papers/a7-kotla.pdf)、
[BFTSim 的延迟/吞吐及故障评估](https://www.usenix.org/conference/nsdi-08/bft-protocols-under-fire)，
以及 [RDMA Experience 对尾部完成时间和 PFC 的讨论](https://www.microsoft.com/en-us/research/wp-content/uploads/2023/03/RDMA_Experience_Paper_TR-1.pdf)。
当前仍使用项目的按事务撤销/重放模型；统计增强没有改变协议、攻击或恢复服务成本。

回归检查：

```bash
python3 -m unittest discover -s examples/consensus -p test_consensus_statistics.py
```


## BIDL Hadoop 配对实验矩阵

默认 `prepare` 使用 `scripts/slurm/bidl-hadoop-matrix.json`，复用六份基础配置中的三份 BIDL 配置，
不复制修改基础文件。Zyzzyva 配置保留供其他批次使用。

| case | 基础配置后缀 | rdmaCcMode | IORS |
|---|---|---:|---|
| bidl-normal-dcqcn | noattack | 1 | 关闭 |
| bidl-attack-dcqcn | attack | 1 | 关闭 |
| bidl-attack-iors-dcqcn | attack-iors | 1 | 标记＋终端 1ms 有界等待 |
| bidl-attack-hpcc | attack | 3 | 关闭 |
| bidl-attack-timely | attack | 7 | 关闭 |
| bidl-attack-dctcp | attack | 8 | 关闭 |
| bidl-attack-iors-hpcc | attack-iors | 3 | 标记＋终端 1ms 有界等待 |
| bidl-attack-iors-timely | attack-iors | 7 | 标记＋终端 1ms 有界等待 |
| bidl-attack-iors-dctcp | attack-iors | 8 | 标记＋终端 1ms 有界等待 |

“仅攻击”参考组仍使用 DCQCN，不表示禁用拥塞控制。所有组采用同一拓扑、ECN/PFC、
请求负载、节点位置和服务成本。攻击组均全程监听后篡改序号、概率 1、发给全部执行节点。
共享背景轴为 Hadoop 每终端 5/10/15/20/25/30/35/40/45/50G、2–3s 文件回放，
共 9×10=90 个任务；seed=1、run=1，不进行随机重复。
当前固定 Leaf-Spine 4:1、128 主机、10 个排序和 10 个执行节点、单客户端 30K/s 发送 5s（150,000 请求）、仿真 20s。
四种 CC 均有攻击/IORS 配对组，同一 CC 内比较 IORS 收益；不同 CC 间比较拥塞控制影响。
另保留无攻击/DCQCN 参考组；它不是其他 CC 的无攻击基线。

`prepare` 会另外写 `tasks.csv`，逐行列出 case、负载、CC、攻击/IORS 开关、节点数和请求/时间设置，
以及 `tasks.json` 中的输入文件 SHA-256。先审阅清单，再同步源码、构建产物、流量和 campaign 到 worker。
默认脚本与流量路径均相对 ns-3 根目录解析。

缩小批次可直接给 prepare 追加 `--hadoop-loads 5 25 50`，得到 27 项任务；
`--cases bidl-attack-dcqcn bidl-attack-iors-dcqcn` 可只选择两组。
也可导出矩阵后编辑 `axes.flowFile`。
其他参数仍来自原基础配置。需要无背景对照时，在 flowFile 轴加入空字符串。
修改请求率时，请同步修改 requestCount（固定 5s 注入时为 rate×5）。
所有 Slurm worker 继续仅采集原始数据，收集后才手动调用上面的批处理分析器。
