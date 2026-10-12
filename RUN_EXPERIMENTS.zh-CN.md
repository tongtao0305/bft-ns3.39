# 手动构建、运行与获取实验结果

本文面向当前仓库新增的 `consensus` 模块，覆盖 PBFT、BIDL、TCP/RDMA、Fat-Tree 和攻击流量实验。仓库原有的 PowerTCP、ABM 等实验入口见根目录 `README.md`；它们不是本文的主要对象。

文中的命令都假定从仓库根目录开始执行。将第一行路径改成自己的仓库路径：

```bash
export BFT_REPO=/home/tongtao/Develop/Code/bft-ns3.39
cd "$BFT_REPO/simulator/ns-3.39"
```

## 1. 环境准备

推荐使用 Ubuntu 22.04 或相近的 Linux 环境。最小 C++ 构建环境通常可以这样安装：

```bash
sudo apt update
sudo apt install -y build-essential cmake ninja-build python3 python3-dev python3-pip pkg-config
```

这个仓库已经包含 ns-3.39 源码，不需要另行下载 ns-3。NetAnim、文档、MPI 等是可选功能，运行本文的共识实验不需要它们。

先确认工具可用：

```bash
g++ --version
cmake --version
python3 --version
```

## 2. 配置和编译

仓库提供的配置脚本会使用 optimized 构建、启用 examples 和 Python bindings，但默认关闭测试：

```bash
cd "$BFT_REPO/simulator/ns-3.39"
./configure.sh
./ns3 build bidl-experiment
```

也可以执行 `./waf` 完成构建；在本仓库中它会转到当前的 ns-3/CMake 构建流程。建议日常使用 `./ns3`，因为可以明确指定目标。

确认目标已经生成：

```bash
./ns3 show targets | grep consensus
./ns3 run "bidl-experiment --help"
```

不要在脚本里写死 `build/.../ns3.39-bidl-experiment-default`。本仓库默认是 optimized 构建，实际文件名为 `ns3.39-bidl-experiment-optimized`。`./ns3 run` 会自动找到正确的二进制文件，因此后文都使用这种写法。

本文的共识程序本身不依赖 Python bindings。如果 `./configure.sh` 只因 bindings 依赖缺失而失败，可以改用以下最小 C++ 配置：

```bash
CXXFLAGS=-w ./ns3 configure \
  --build-profile=optimized \
  --enable-examples \
  --disable-tests \
  --disable-python-bindings \
  --disable-werror \
  --disable-warnings
./ns3 build bidl-experiment
```

### 可选：启用测试

`configure.sh` 默认含有 `--disable-tests`。如需运行模块测试，重新配置并构建：

```bash
CXXFLAGS=-w ./ns3 configure \
  --build-profile=optimized \
  --enable-examples \
  --enable-tests \
  --enable-python-bindings \
  --disable-werror \
  --disable-warnings
./ns3 build bidl-experiment test-runner
./ns3 run "test-runner --suite=consensus --verbose"
./ns3 run "test-runner --suite=consensus-e2e --verbose"
```

## 3. 先跑通最小实验

结果统一放在 ns-3 目录下的 `results/manual`：

```bash
mkdir -p results/manual
```

### 3.1 PBFT over RDMA

```bash
./ns3 run "bidl-experiment \
  --preset=pbft-rdma \
  --requestCount=20 \
  --simulationTime=2 \
  --outputPrefix=results/manual/pbft-smoke"
```

正常结束时，终端会打印类似以下摘要：

```text
completed=20 transactions=20 blocks=0 ... output=results/manual/pbft-smoke-*.csv
```

### 3.2 BIDL over RDMA

```bash
./ns3 run "bidl-experiment \
  --preset=bidl-rdma \
  --requestCount=20 \
  --batchSize=10 \
  --simulationTime=2 \
  --outputPrefix=results/manual/bidl-smoke"
```

正常结果应满足 `completed=20`、`transactions=20`，且 `blocks` 大于 0。BIDL 会按批提交，因此请求吞吐、事务吞吐和区块数不应混为同一个指标。

内置 preset 一共有：

- `pbft-tcp`
- `pbft-rdma`
- `bidl-tcp`
- `bidl-rdma`

可以只替换 `--preset` 来比较协议和传输方式。命令行优先级最高，完整的配置覆盖顺序是：内置默认值 → preset → 配置文件 → 命令行参数。

## 4. 跑主要场景

### 4.1 使用配置文件

仓库自带一个小型 PBFT/RDMA 配置：

```bash
./ns3 run "bidl-experiment \
  --configFile=examples/consensus/configs/baseline/consensus-experiment.conf \
  --outputPrefix=results/manual/pbft-config"
```

配置文件采用一行一个 `key=value`。建议把稳定的实验参数写入 `.conf`，只在命令行覆盖本轮变化的参数和输出目录。

### 4.2 BIDL：k=4 Fat-Tree

该拓扑包含 16 台主机、8 台 edge、8 台 aggregation 和 4 台 core 交换机：

```bash
./ns3 run "bidl-experiment \
  --configFile=examples/consensus/configs/baseline/bidl-k4-fat-tree.conf \
  --requestCount=2000 \
  --outputPrefix=results/manual/bidl-k4-baseline"
```

在相同拓扑上启用恶意广播者：

```bash
./ns3 run "bidl-experiment \
  --configFile=examples/consensus/configs/baseline/bidl-k4-fat-tree.conf \
  --requestCount=2000 \
  --bidlAttackEnabled=true \
  --outputPrefix=results/manual/bidl-k4-attack"
```

比较两次运行的 `transaction_throughput`、`bidl_end_to_end_p95`、`bidl_reexecution_events`、`bidl_affected_blocks` 和 `bidl_reexecution_p95`。除攻击开关和输出前缀外，两条命令应保持一致。

### 4.3 非对称路径攻击与背景流

下面的配置让正常路径和 RDMA 背景流共享瓶颈，而伪造流量走旁路：

```bash
./ns3 run "bidl-experiment \
  --configFile=examples/consensus/configs/baseline/bidl-attacker-bypass.conf \
  --outputPrefix=results/manual/bidl-bypass-with-background"
```

配置中的 `flowFile` 默认启用背景流。做对照实验时，复制配置文件、删除复制件中的 `flowFile=...` 一行，再使用复制件运行。不要直接修改原始配置，这样更容易复现实验和检查差异。例如：

```bash
cp examples/consensus/configs/baseline/bidl-attacker-bypass.conf /tmp/bidl-bypass-no-bg.conf
sed -i '/^flowFile=/d' /tmp/bidl-bypass-no-bg.conf
./ns3 run "bidl-experiment \
  --configFile=/tmp/bidl-bypass-no-bg.conf \
  --outputPrefix=results/manual/bidl-bypass-no-background"
```

这个场景主要观察背景拥塞是否使伪造消息抢先到达。重点比较 `bidl_rejected_conflicts`、`bidl_affected_blocks`、`bidl_reexecution_events`、事务吞吐和端到端 P95 延迟。

## 5. 多次独立运行

论文式结果不应只使用单个随机 run。下面从 run 1 开始连续执行 5 次：

```bash
./ns3 run "bidl-experiment \
  --preset=bidl-rdma \
  --requestCount=2000 \
  --warmupTime=0.2 \
  --measurementTime=1.0 \
  --cooldownTime=0.2 \
  --batchRuns=5 \
  --seed=1 \
  --run=1 \
  --outputPrefix=results/manual/bidl-rdma-batch"
```

运行后会生成：

- `bidl-rdma-batch-run-1-*` 到 `run-5-*`：每次独立运行的原始结果；
- `bidl-rdma-batch-aggregate.csv`：均值、跨 run 的 P95/P99，以及 95% 置信区间半宽。

固定 `seed` 并改变 `run` 是 ns-3 推荐的独立随机流方式。复现实验时要同时记录 seed、run、Git commit 和 dirty 状态；这些信息已自动写入 manifest。

## 6. 结果文件怎么读

若 `outputPrefix=results/manual/demo`，单次运行会写出以下文件：

| 文件 | 内容 | 常见用途 |
| --- | --- | --- |
| `demo-summary.csv` | 吞吐、延迟分位数、重传、视图切换、BIDL/攻击指标 | 首先查看的总表 |
| `demo-config.json` | 本次最终生效的全部参数 | 检查配置覆盖是否正确 |
| `demo-manifest.json` | Git commit、dirty、编译器、seed/run、测量窗口、完成状态 | 复现与审计 |
| `demo-latency.csv` | 每个客户端请求的延迟 | 画 CDF/箱线图 |
| `demo-phases.csv` | 每个实例/区块的阶段耗时 | 拆解协议阶段瓶颈 |
| `demo-messages.csv` | 各消息类型的数量和字节数 | 计算协议通信开销 |
| `demo-network.csv` | 各网卡收发、丢包、PFC 和利用率 | 定位链路瓶颈 |
| `demo-switches.csv` | 交换机策略的入队、出队、ECN、丢包计数 | 检查交换机行为 |
| `demo-queues.csv` | 队列峰值、丢包、ECN、平均驻留时间 | 分析拥塞 |
| `demo-background.csv` | 背景 RDMA flow completion time | 分析前后台干扰 |

BIDL 的论文式阶段编号为：

- phase 6：P1，共识 proposal 到 decision；
- phase 7：P2，验证与执行；
- phase 8：P3，持久化；
- phase 9：P4，执行总耗时，即 P2 + P3；
- phase 10：P5，commit；
- phase 11：入口到 commit 的端到端延迟。

`messages.csv` 中的常用消息编号为：1 client request、2 client reply、10 pre-prepare、11 prepare、12 commit、13 view-change、14 new-view、30 BIDL transaction、33 ordered block、34 persist、35 BIDL commit、100 RDMA background flow。

快速检查一次运行是否完整：

```bash
python3 -m json.tool results/manual/bidl-smoke-manifest.json
column -s, -t results/manual/bidl-smoke-summary.csv | less -S
```

应重点确认：

1. manifest 中 `status` 为 `complete`；
2. `completed_requests` 或 `completed_transactions` 符合预期；
3. `timed_out_requests`、drop、retransmission 是否异常；
4. 比较实验的 `config.json` 只存在预期的参数差异；
5. 吞吐和延迟只统计 measurement window 内提交且完成的请求。

BIDL 的外部请求会在流水线中被批处理，因此 `submitted_requests`、`measured_requests` 与 `submitted_transactions` 的语义不同。评价 BIDL 处理能力时优先使用 `transaction_throughput` 与 `completed_transactions`，延迟使用 `bidl_end_to_end_p50/p95`；不要只看到 `submitted_requests=0` 就判断实验失败。

## 7. 常用参数

| 参数 | 含义 |
| --- | --- |
| `--requestCount` | 每个合成客户端产生的请求数 |
| `--requestRate` | 每秒 offered requests |
| `--requestSize` | 每个请求的字节数 |
| `--clientCount` | 合成客户端数 |
| `--batchSize` | 每个 BIDL block 的事务数 |
| `--batchTimeoutMs` | 未满 batch 时的最长等待时间 |
| `--simulationTime` | 仿真停止时刻，单位秒 |
| `--applicationStartTime` | 应用开始时刻，默认 1 秒 |
| `--warmupTime` | 不计入统计的预热时长 |
| `--measurementTime` | 正式测量时长；0 表示使用剩余区间 |
| `--cooldownTime` | 测量结束后的冷却时长 |
| `--batchRuns` | 连续独立运行次数 |
| `--seed` / `--run` | ns-3 随机种子和独立 run 编号 |
| `--outputPrefix` | 所有结果文件的共同前缀 |

完整参数列表以当前代码为准：

```bash
./ns3 run "bidl-experiment --help"
```

## 8. 常见问题

### 找不到 `*-default` 二进制

当前配置是 optimized profile，直接使用 `./ns3 run "bidl-experiment ..."`，或者查看 `build/examples/consensus/` 中的实际文件名。

### 找不到 topology/flow 文件

配置文件里的相对路径是相对于 `simulator/ns-3.39` 解析的。务必先 `cd "$BFT_REPO/simulator/ns-3.39"` 再运行。

### 配置了请求但完成数为 0 或很少

检查 `applicationStartTime + warmupTime + measurementTime + cooldownTime` 是否超过 `simulationTime`；再检查 request rate、batch timeout、丢包、PFC、重传和 CPU/存储服务时间。BIDL 还应查看事务数和区块数，而不是只看 PBFT 风格的 request 计数。

### 修改源码后结果没有变化

先显式重编译再运行：

```bash
./ns3 build bidl-experiment
./ns3 run "bidl-experiment --configFile=... --outputPrefix=..."
```

不要复用已有的输出前缀，以免新旧文件混淆。

### 如何保证两组结果可比较

使用同一 Git commit、构建 profile、seed/run 列表、测量窗口和拓扑，只改变待研究的一个变量。保存每组 `config.json`、`manifest.json`、每次 run 的原始 CSV 及 aggregate CSV，不要只保存截图或终端摘要。

## 9. 推荐的完整执行顺序

```text
安装依赖
  → ./configure.sh
  → ./ns3 build bidl-experiment
  → 跑 PBFT/BIDL smoke test
  → 检查 manifest.status 和 summary.csv
  → 固化 .conf 配置
  → 用 batchRuns 跑多次独立 run
  → 保存 config/manifest/原始 CSV/aggregate CSV
  → 基于 summary、latency、phases、network/queues 作图和分析
```

完成以上步骤后，`results/manual/` 中的 CSV 就是后续统计、画图和撰写实验章节所需的原始结果。
