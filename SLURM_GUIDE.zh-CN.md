# 三节点 Slurm 实验运行手册

适用项目：本仓库 `simulator/ns-3.39/examples/consensus`。
适用结构：一台控制节点、两台执行节点；各执行节点使用本地磁盘保存实验结果。
以下系统命令以 Ubuntu 22.04 / systemd 为例。其他发行版需要调整包名和服务路径。

本文是操作手册，创建本文没有安装软件、修改系统配置或提交实际作业。

## 0. 你的三台机器按这个顺序部署

如果当前这台开发机同时作为一台执行节点，建议固定为以下角色：

| 机器 | 职责 |
| --- | --- |
| 当前开发机 | 执行节点 1；保存和检查 Git 版本；编译一次；也可运行 Slurm 作业 |
| 控制节点 | 运行 `slurmctld`；准备矩阵、提交作业、查看状态和收集结果 |
| 另一台执行节点 | 运行 `slurmd` 和仿真作业 |

按顺序操作：

1. 在当前开发机检查并保存一个 Git 提交。先看 `git status --short` 和 `git diff --check`，确认待提交项只包含本次代码、配置、拓扑、流量和文档；不要把 `consensus-results-*.csv` 等运行结果加入版本库。大型或被 `.gitignore` 排除的背景流量要单独同步并核对哈希。提交后记录 `git rev-parse HEAD`。
2. 在三台机器先完成 Slurm/MUNGE、主机名解析、时钟和用户配置，再从控制节点做 `srun ... hostname` 集群验收。此时还不需要提交仿真作业。
3. 从当前开发机把同一 Git 提交的源代码、配置、拓扑和背景流量手动同步到控制节点及另一执行节点。三台机器使用相同的项目绝对路径；不要同步 `.git`、旧结果或正在运行的 campaign。
4. 在当前开发机编译一次优化版，检查可执行文件和动态库。把同一份 `build/` 产物同步到控制节点和另一执行节点；这样 prepare 时记录的运行指纹和 worker 上的二进制保持一致。三台机器应使用兼容的操作系统、CPU 架构和系统运行库。
5. 在开发机记录源码提交号；再逐台比对拓扑/背景流量文件哈希、可执行文件及 ns-3 动态库。两台 worker 的 `ldd` 不得出现 `not found`。
6. 在控制节点生成两项 smoke campaign，手动同步到两台执行节点；dry-run 后提交。确认两个节点都各自成功跑完一项、结果文件齐全，再准备正式矩阵。
7. 每次改代码或输入数据，都重新保存 Git 提交（或明确记录数据版本）、同步三台机器、重编译/同步产物，并创建一个全新的 campaign。不要覆盖旧 campaign。

Git 用于保存源代码版本，手动同步用于部署到这三台机器；二者不能替代彼此。当前工作区若混有多个历史改动，先逐项审查再提交，不要直接无检查地执行 `git add -A`。

## 1. 先理解整体流程

```text
首次搭建：网络/用户/时间 → MUNGE → slurm.conf → 启动服务 → 两节点 hostname 测试

每轮实验：整理 .conf + matrix.json → 编译并冻结运行文件 → prepare
         → 同步到两个执行节点 → dry-run → submit → 观察 → collect → 检查结果
```

控制节点运行 `slurmctld`；执行节点运行 `slurmd`；三台机器运行 `munge`。
实验提交程序是 `examples/consensus/scripts/slurm/consensus-slurm.py`。
它通过标准输入把批处理脚本交给 `sbatch`，无需另外维护 `.sbatch` 文件。
执行节点会自动进入 `run-task`，再调用 `run-experiment.py`。

每项实验是一个独立的 ns-3 进程，申请 1 个 Slurm CPU。
仿真中的 128 台主机是模拟对象，不代表需要 128 台物理机器。
当前脚本不会把一个仿真实验拆成两个节点共同执行。

## 2. 确认并替换部署参数

| 参数 | 本文示例 | 你需要确认的内容 |
| --- | --- | --- |
| 控制节点 | `controller` / `10.134.180.235` | 名称和 IP 仅为占位示例 |
| 执行节点 1 | `ubuntu-236` / `10.134.180.236` | 当前环境的 hostname 是 ubuntu-236 |
| 执行节点 2 | `ubuntu-237` / `10.134.180.237` | 请确认实际主机名和 IP |
| 作业用户 | `tongtao` | 三台机器需要相同 UID/GID |
| 分区 | `compute` | 必须与 slurm.conf 中的 PartitionName 相同 |
| 项目根目录 | `/home/tongtao/Develop/Code/bft-ns3.39` | 三台机器保持相同绝对路径 |
| ns-3 根目录 | 项目根目录下 `simulator/ns-3.39` | 后续实验命令均从这里执行 |

本文命令中的主机名也用于 SSH。`submit --workers` 最终传给 Slurm 的 `--nodelist`，
因此必须是 Slurm 的 NodeName；`collect --workers` 又将它当作 SSH 目标。
请让同一名称同时适用于 Slurm 和 SSH，不要只配置一个 Slurm 不认识的 SSH 别名。

## 3. 首次安装：网络、软件、用户

### 3.1 三台机器上的基础检查

```bash
hostname -s
id tongtao
getent passwd slurm
getent hosts controller ubuntu-236 ubuntu-237
timedatectl status
```

通过 DNS 或 `/etc/hosts` 让三台机器都能正确解析上述名称。例如在每台机器的 hosts
文件中维护如下记录，IP 要替换成实际值；避免同一名称存在冲突记录：

```text
10.134.180.235 controller
10.134.180.236 ubuntu-236
10.134.180.237 ubuntu-237
```

作业用户 `tongtao` 的 UID/GID 必须一致，`slurm` 服务用户的 UID/GID 也应一致。
先比较再由管理员统一，不要在已有集群上直接修改用户编号。
缺少作业用户时，应按统一 UID/GID 创建，并给其项目目录读写权限。

### 3.2 安装软件

控制节点：

```bash
sudo apt-get update
sudo apt-get install slurmctld slurm-client munge chrony openssh-client rsync python3
```

两台执行节点分别执行：

```bash
sudo apt-get update
sudo apt-get install slurmd slurm-client munge chrony openssh-server rsync python3
sudo systemctl enable --now ssh
```

三台机器使用同一软件源、相同 Slurm 版本。安装包可能尝试启动服务；此时配置尚未就绪，
服务启动失败不代表最终部署失败。配置完成后再启动并检查。

```bash
# 三台机器
sudo systemctl enable --now chrony
chronyc tracking
srun --version
```

安装完成后再次比较用户编号。Ubuntu 软件包通常会创建 `slurm`、`munge` 服务用户。
若没有，需先创建服务用户再继续。

### 3.3 网络连通要求

三台机器之间需要双向 IP 通信。本文使用 TCP 6817（控制器）、6818（执行节点），
另外文件同步需要控制节点到执行节点的 SSH 22。
`srun` 还需要客户端与执行进程之间的回连端口，不能仅放通 6817/6818 就视为配置完成。
可在受信任集群网段允许内部通信；若必须精确限制端口，应结合 `SrunPortRange`
和本机 Slurm 版本配置防火墙。无需把这些端口开放给公网。

SSH 用于项目同步和结果收集，Slurm 启动作业不依赖 SSH。

## 4. 首次配置：MUNGE 身份认证

三台机器必须使用同一份 `/etc/munge/munge.key`，密钥不要放进仓库或公开目录。

在控制节点检查是否已有密钥：

```bash
sudo test -s /etc/munge/munge.key
```

如果已有密钥且它属于要加入的集群，保留并分发它。仅在全新集群且文件不存在时创建：

```bash
sudo install -d -o munge -g munge -m 0700 /etc/munge
sudo bash -c 'set -e; set -o noclobber; umask 077; head -c 1024 /dev/urandom > /etc/munge/munge.key'
sudo chown munge:munge /etc/munge/munge.key
sudo chmod 0400 /etc/munge/munge.key
```

在控制节点用 SSH 将密钥直接传给执行节点，远端 sudo 需要事先具备非交互授权。
没有该授权时由管理员使用安全传输方式安装同一密钥，不要放宽密钥权限来方便 scp。
下面的分发仅用于本次新建集群，不要覆盖其他运行中集群的密钥：

```bash
for worker in ubuntu-236 ubuntu-237; do
  ssh "$worker" 'sudo -n install -d -o munge -g munge -m 0700 /etc/munge' || break
  sudo cat /etc/munge/munge.key |
    ssh "$worker" 'sudo -n install -o munge -g munge -m 0400 /dev/stdin /etc/munge/munge.key' || break
done
```

确认两台都成功后，在三台机器分别执行：

```bash
sudo systemctl enable --now munge
sudo systemctl restart munge
munge -n | unmunge
```

再从控制节点验证跨节点解码：

```bash
munge -n | ssh ubuntu-236 unmunge
munge -n | ssh ubuntu-237 unmunge
```

应该看到 `STATUS: Success`。失败时先解决密钥、权限、时间或服务问题。

## 5. 首次配置：slurm.conf

### 5.1 获取每台执行节点的真实资源

两台执行节点分别运行：

```bash
slurmd -C
free -m
```

将输出中的 CPUs、Boards（如有）、SocketsPerBoard/Sockets、CoresPerSocket、
ThreadsPerCore 转写到各自 NodeName 行，保持 CPU 拓扑一致。
RealMemory 单位是 MB，设为比实测略小的值，为系统保留内存。

### 5.2 配置内容

仓库提供 `slurm.conf.example`（与本文同目录）。把控制节点信息和两行节点资源改好后，
在控制节点保存为 `/etc/slurm/slurm.conf`：

```bash
# 从仓库根目录执行，先复制到自己的编辑文件
cp slurm.conf.example /tmp/bft-slurm.conf
nano /tmp/bft-slurm.conf
sudo install -d -m 0755 /etc/slurm
sudo install -o root -g root -m 0644 /tmp/bft-slurm.conf /etc/slurm/slurm.conf
```

示例选择按 CPU 核和内存调度的基础配置，使用 `proctrack/linuxproc` 和 `task/affinity`。
它适用于先跑通可信用户的实验集群，未配置 cgroup 的硬性内存隔离。
`--mem` 在这里主要参与资源分配，不应据此认为内存已被硬限制。
需要隔离时，按所安装版本另行配置 `task/cgroup`、`proctrack/cgroup` 和 `cgroup.conf`，
不要直接混用其他版本的 cgroup 配置。

分发同一份配置到两台执行节点（同样要求远端非交互 sudo）：

```bash
for worker in ubuntu-236 ubuntu-237; do
  ssh "$worker" 'sudo -n install -d -m 0755 /etc/slurm' || break
  ssh "$worker" 'sudo -n install -o root -g root -m 0644 /dev/stdin /etc/slurm/slurm.conf' \
    < /etc/slurm/slurm.conf || break
done
```

### 5.3 创建目录并启动服务

控制节点：

```bash
sudo install -d -o slurm -g slurm -m 0755 /var/spool/slurmctld
sudo systemctl enable --now slurmctld
sudo systemctl restart slurmctld
sudo systemctl status slurmctld --no-pager
```

两台执行节点分别运行（slurmd 通常以 root 运行）：

```bash
sudo install -d -o root -g root -m 0755 /var/spool/slurmd
sudo systemctl enable --now slurmd
sudo systemctl restart slurmd
sudo systemctl status slurmd --no-pager
```

控制节点不执行仿真时无需启动 slurmd。本手册不需要 slurmdbd 或数据库；因此也不保证
`sacct` 可用。后续以 squeue、作业详情和磁盘上的结果文件验证执行情况。

### 5.4 集群验收

在控制节点执行：

```bash
scontrol ping
sinfo -N -l
scontrol show node ubuntu-236
scontrol show node ubuntu-237
srun --partition=compute --nodelist=ubuntu-236,ubuntu-237 \
  --nodes=2 --ntasks=2 --ntasks-per-node=1 hostname
```

预期：控制器 UP，两台节点空闲时为 idle，最后输出两个主机名。
如果节点为 DOWN/DRAIN，先查看节点 Reason 和 slurmd 日志、修正问题。
只有在修复原因后，才在控制节点执行例如：

```bash
sudo scontrol update NodeName=ubuntu-236 State=RESUME
```

## 6. 整理项目、构建产物与实验文件

### 6.1 目录职责

```text
bft-ns3.39/
├── SLURM_GUIDE.zh-CN.md
├── slurm.conf.example                 # 集群配置模板；真正生效的是 /etc/slurm/slurm.conf
└── simulator/ns-3.39/
    ├── build/examples/consensus/ns3.39-bidl-experiment-optimized
    ├── build/lib/                     # 同步整个目录，包含依赖库
    ├── examples/consensus/
    │   ├── configs/baseline/*.conf     # 单次实验基础参数
    │   ├── topologies/                # 拓扑输入
    │   ├── workloads/                 # 背景流量输入，含 normalized 文件
    │   ├── run-experiment.py
    │   ├── consensus_single_run.py
    │   └── scripts/slurm/consensus-slurm.py
    └── run-logs/
        ├── matrix-smoke.json          # 自定义实验矩阵
        ├── matrix-bidl.json
        └── campaign-001/              # prepare 生成，一轮实验一个新目录
```

`.conf` 管一项实验怎样模拟；`matrix.json` 管参数组合和种子；Slurm 参数管在哪台机器上、
同时跑几项、每项分配多少实际资源。不要把这些混写进同一个配置文件。

### 6.2 编译一次，分发同一套产物

在控制节点准备源代码和构建工具；也可以在任一兼容的构建机编译，再把产物送到控制节点。
项目 Python 启动器使用标准库，不依赖 ns-3 Python bindings。

```bash
# 在构建机安装基础编译工具
sudo apt-get install build-essential cmake ninja-build python3 libgsl-dev libxml2-dev libsqlite3-dev

cd /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39
./ns3 configure --build-profile=optimized --enable-examples \
  --disable-tests --disable-python-bindings --disable-werror \
  -- -DNS3_NATIVE_OPTIMIZATIONS=OFF
./ns3 build bidl-experiment

test -x build/examples/consensus/ns3.39-bidl-experiment-optimized
test -f build/lib/libns3.39-consensus-optimized.so
ldd build/examples/consensus/ns3.39-bidl-experiment-optimized
```

本仓库当前使用 `./ns3` / CMake 构建。关闭 native 优化是为了减少构建机 CPU 指令集
造成的跨机器运行问题；仍要求三台机器架构、系统运行库兼容。
如果已有正确产物，可以跳过编译。若编译提示缺少其他依赖，根据 CMake 报错补齐。
执行节点根据 `ldd` 安装对应运行库，不需要为了执行而各自重新编译。

当前脚本会为二进制、consensus 动态库、运行 Python 脚本、Slurm 脚本，以及二进制依赖的仓库内 `libns3*` 动态库记录 SHA256。应编译一次并分发同一套产物。拓扑和 flow 文件使用单独的输入指纹校验；其他外部系统动态库仍须确保三台机器兼容。

## 7. 整理实验配置：先做两项验收，再做正式矩阵

以下命令都在控制节点、ns-3 根目录执行：

```bash
cd /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39
mkdir -p run-logs
```

### 7.1 两项小实验，验证两台执行节点都能跑

导出内置矩阵，然后缩小规模。文件名必须是尚不存在的新文件：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py example \
  --output run-logs/matrix-smoke.json

python3 - <<'PY'
import json
from pathlib import Path
p = Path('run-logs/matrix-smoke.json')
spec = json.loads(p.read_text())
spec['seeds'] = [1]
spec['cases'] = [spec['cases'][0], spec['cases'][2]]
spec['axes'] = {'flowFile': ['']}
spec['defaults']['requestCount'] = 100
spec['defaults']['requestRate'] = 100
p.write_text(json.dumps(spec, indent=2) + '\n')
PY
```

这会生成 BIDL 无攻击与攻击＋IORS、无背景、固定 seed=1 的两个任务，恰好分给两台执行节点。
它验证提交链路，不代表正式负载的资源消耗。

### 7.2 正式实验矩阵示例

默认矩阵已经基于维护的 BIDL 配置，包含九组：无攻击/DCQCN，
以及 DCQCN、HPCC、TIMELY、DCTCP 各自的攻击与攻击＋IORS 配对组。背景轴为 2–3s 注入的 Hadoop 每终端 5–50G（每次递增 5G），
共 90 项，seed=1、run=1。默认是 128 主机 Leaf-Spine 4:1、单客户端 30K/s 发送 5s（150,000 请求）、仿真 20s。

可以直接 prepare，也可以先导出、审阅并调整：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py example \
  --output run-logs/matrix-bidl-hadoop.json
```

缩小负载扫描时，删除 `axes.flowFile` 中不需要的文件即可，保留 5G、25G、50G 时得到 27 项。
资源预算由矩阵 `slurm` 字段指定，默认每节点并发 1 项、16G 内存、4 小时；
命令行 `--tasks-per-node`、`--memory`、`--time` 优先，其次环境变量，再其次矩阵。
完整大背景任务的资源需求尚需实测，这些是预算值。

拓扑和流量文件必须同步到 worker。prepare 记录输入指纹，提交和执行阶段都会校验。
新生成的 tasks.csv 可直接检查所有组合；流量文件若更新，需重新 prepare。
大型 workload 可能被 Git 忽略，单纯 git clone 不保证获得这些文件，必须另行同步。

### 7.3 参数覆盖规则

优先级由低到高是：`base_config → defaults → case.parameters → axes`。
种子由矩阵的 `seeds` 决定。对于所有 case 共有的 axes，任务数为：

```text
case 数 × 每个 axis 长度的乘积 × seed 数
```

例如增加 `"axes": {"requestRate": [5000, 10000]}`，上面的 6 项就变成 12 项。
注意 requestRate、requestCount、measurementTime、simulationTime 要配套，避免流量还未完成
就结束仿真。模拟秒数与 Slurm 的实际墙钟时间上限是两个不同概念。

其他约束：

- JSON 使用 `true/false`，基础 `.conf` 使用逐行 `key=value`，同一文件不能有重复 key。
- `base_config`、topologyFile、flowFile 使用相对 ns-3 根目录的路径。
- 当前 Slurm 工具仅接受 `topology=qbb-file` 且主机数为 128 的拓扑。
- case 的 name 用于标识任务，不会自动更改协议；协议行为由 preset 和其他参数决定。
- 修改原始矩阵后需要重新 prepare；已经生成的 configs 不会自动跟随矩阵变化。

## 8. prepare：生成一轮不可变的任务目录

先用小实验验收：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py prepare \
  --spec run-logs/matrix-smoke.json \
  --output run-logs/campaign-smoke-001
```

准备正式实验时改为：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py prepare \
  --spec run-logs/matrix-bidl.json \
  --output run-logs/campaign-bidl-001
```

不要在小实验验收前提交正式实验。输出目录必须是新目录，位于 ns-3 根目录内部，
绝对路径不能包含空白。省略 --spec 会使用默认 90 项 BIDL/Hadoop 矩阵，不会自动读取基线 .conf。

生成的目录：

```text
campaign-smoke-001/
├── spec.json                # 输入矩阵副本
├── tasks.json               # 展开任务、种子、配置指纹、运行文件指纹
├── configs/task-00000.conf
├── configs/task-00001.conf
├── logs/
└── results/
```

可人工检查：

```bash
python3 -m json.tool run-logs/campaign-smoke-001/tasks.json
cat run-logs/campaign-smoke-001/configs/task-00000.conf
```

从这一步起冻结本轮配置和产物。修改配置、Python 启动器或重新编译后，要创建新一轮
campaign 并重新同步。不要删除旧结果来绕过“目录已存在”的保护。

## 9. 同步到两台执行节点

### 9.1 配置 SSH 和目标路径

控制节点使用作业用户配置公钥登录，并确认 SSH host key。可先检查是否已有密钥，
需要时运行 ssh-keygen 创建，再使用 ssh-copy-id 安装公钥：

```bash
ssh-copy-id tongtao@ubuntu-236
ssh-copy-id tongtao@ubuntu-237
ssh -o BatchMode=yes ubuntu-236 'hostname; id'
ssh -o BatchMode=yes ubuntu-237 'hostname; id'
```

让两个远端的作业用户创建相同绝对路径；若父目录权限不足，由管理员先授权：

```bash
for worker in ubuntu-236 ubuntu-237; do
  ssh "$worker" 'mkdir -p /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39' || break
done
```

### 9.2 首次同步源代码和数据

如果当前开发机就是执行节点 1，从它将已审查的工作树同步到控制节点和执行节点 2。这里不删除目标端文件，也不传 Git 元数据、旧构建缓存或运行结果：

```bash
REPO=/home/tongtao/Develop/Code/bft-ns3.39
for host in controller ubuntu-237; do
  rsync -a --protect-args \
    --exclude='.git/' \
    --exclude='simulator/ns-3.39/build/' \
    --exclude='simulator/ns-3.39/cmake-cache/' \
    --exclude='simulator/ns-3.39/run-logs/' \
    --exclude='simulator/ns-3.39/results/' \
    --exclude='consensus-results-*.csv' \
    "$REPO/" "$host:$REPO/" || break
done
```

将 `controller`、`ubuntu-237` 换成实际主机名。同步后在开发机记录 `git rev-parse HEAD`，并对比三台机器上的拓扑和 workload `sha256sum`；workload 若被 Git 忽略，也必须在这一步确认已传到。

### 9.3 编译一次并分发构建产物

在当前开发机（执行节点 1）按 6.2 节编译并检查。随后把完全相同的 `build/` 同步到控制节点和执行节点 2：

```bash
NS3_ROOT=/home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39
for host in controller ubuntu-237; do
  rsync -a --protect-args "$NS3_ROOT/build/" "$host:$NS3_ROOT/build/" || break
done
```

如果三台机器的 CPU 架构或系统运行库不兼容，就分别在各机器编译，并在每台重新 `prepare`；不要把不兼容的二进制互相拷贝。

### 9.4 每轮同步新的 campaign

在控制节点的 ns-3 根目录运行。同步到两台执行节点，包括作为开发机的执行节点 1：

```bash
CAMPAIGN=run-logs/campaign-smoke-001
for worker in ubuntu-236 ubuntu-237; do
  ssh "$worker" "mkdir -p '$NS3_ROOT/run-logs'" || break
  rsync -a --protect-args "$NS3_ROOT/$CAMPAIGN/" "$worker:$NS3_ROOT/$CAMPAIGN/" || break
done
```

每条命令都要成功；发生错误时先修复，不要继续提交。这里不使用 --delete，避免清除远端结果。
同一轮正式运行后不要再向其目录做反复覆盖同步。

### 9.5 检查执行节点动态库和文件权限

```bash
for worker in ubuntu-236 ubuntu-237; do
  ssh "$worker" "cd '$NS3_ROOT' && ldd build/examples/consensus/ns3.39-bidl-experiment-optimized" || break
done
```

不应出现 `not found`。如果需要自定义 LD_LIBRARY_PATH，在提交前设置并确保远端路径相同；
提交脚本会导出当前环境。优先修正库安装或运行路径，不要忽略加载错误。

## 10. 预览和正式提交

在控制节点 ns-3 根目录，先设置本轮名称：

```bash
CAMPAIGN=run-logs/campaign-smoke-001
```

预览：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py submit "$CAMPAIGN" \
  --partition compute --workers ubuntu-236 ubuntu-237 \
  --tasks-per-node 1 --memory 16G --time 04:00:00 --dry-run
```

确认 nodelist、chdir、日志路径和资源参数正确后，真正提交：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py submit "$CAMPAIGN" \
  --partition compute --workers ubuntu-236 ubuntu-237 \
  --tasks-per-node 1 --memory 16G --time 04:00:00
```

dry-run 会校验控制节点的任务和部分文件指纹，打印 sbatch/srun 命令；它不会检查远端
文件是否完整，也不会联系调度器验证分区或资源能否分配。

| 参数 | 作用 |
| --- | --- |
| --workers | 按顺序指定两个 Slurm 节点；任务 0/2/4…给第一个，1/3/5…给第二个 |
| --tasks-per-node | 每个节点最多同时运行多少个本轮数组任务；默认 1 |
| --memory | 每项任务申请的总内存，默认矩阵为 16G |
| --time | 每项任务的实际运行时间上限，默认矩阵为 04:00:00 |
| --account | 只有集群设置了相应 account 要求时才填写 |

如果只有一个任务，只会提交到第一个节点。至少两个任务才能让两个节点都有实验。
两个节点的数组分别固定到对应节点，节点故障时不会自动改分配到另一个节点。
两次提交同一 campaign 会启动新作业，并以 job ID 区分结果，不是“只重跑失败项”。
若第一次只成功提交一个节点，先检查 submissions.jsonl 和 squeue，处理已提交的作业后
再重试，避免重复运行。

每项占 1 CPU；例如 --tasks-per-node 4 会让本轮最多同时运行 8 项，按每任务 16G 合计申请 128G 内存。
还需逐台满足 CPU/内存资源要求，且其他 campaign 也会占用资源。
根据小实验和正式负载实测的 host_peak_rss_kib、host_elapsed_seconds 决定资源值，
不要只按机器 CPU 核数增加并发。

提交记录保存在本轮目录的 `submissions.jsonl`。日志位于各执行节点本地，
不会自动实时出现在控制节点本地同名目录。

## 11. 观察运行、查看日志、取消作业

控制节点：

```bash
squeue -u "$USER" -o '%.18i %.12j %.10T %.10M %.20R'
sinfo -N -l
cat "$CAMPAIGN/submissions.jsonl"
```

对具体 ID 查看详细原因，以下 ID 用实际值替换：

```bash
scontrol show job 12345
scontrol show job 12345_0
```

去执行节点查看日志（12345 为数组主作业 ID，0 为 task ID）：

```bash
ssh ubuntu-236 \
  "tail -n 80 '$NS3_ROOT/$CAMPAIGN/logs/12345_0.out' '$NS3_ROOT/$CAMPAIGN/logs/12345_0.err'"
```

需要取消本轮时，按 submissions.jsonl 记录的两个数组 ID 分别执行：

```bash
scancel 12345
scancel 12346
```

不要用 `scancel -u` 代替取消本轮，它会影响该用户的其他作业。

## 12. 收集结果并判断是否成功

等本轮作业结束后在控制节点执行：

```bash
python3 examples/consensus/scripts/slurm/consensus-slurm.py collect "$CAMPAIGN" \
  --workers ubuntu-236 ubuntu-237
```

它通过 rsync 复制两个执行节点本地的 results/ 和 logs/，保留远端原始数据：

```text
campaign-smoke-001/collected/
├── ubuntu-236/
│   ├── logs/<array-job-id>_<task-id>.out / .err
│   └── results/task-00000/job-<array-job-id>/
└── ubuntu-237/
    ├── logs/
    └── results/task-00001/job-<array-job-id>/
```

某项正常启动后通常包含 `input.conf`、`command.json`、`console.log`、`run-status.json`、
`slurm-task.json` 和 `run-*` 原始仿真输出；运行结束不自动分析。超时或强制终止可能来不及写完所有文件。

检查时不能只看 squeue 中作业消失，还要逐项核对：

1. tasks.json 中每个 task_id 在其分配节点上都有对应结果；缺失项从 .err/.out 查原因。
2. slurm-task.json 中 runner_exit_code 为 0，且 run-status.json 没有 artifact_error。
3. 整批收集后手动执行下面的分析命令，检查 completion_fraction、observation_truncated 和协议/安全指标。
4. 对 BIDL Hadoop 基线，另外查看 run-summary.csv：completed_requests=50000，
   background_completed_flows=630891；Slurm 包装器没有单独强制验证背景流全部完成。

Slurm run-task 返回 3 表示请求未全部完成或观察被截断，仍会保存可用结果用于诊断。
run-status.json 里的模拟器 process_exit_code 和 slurm-task.json 的 runner_exit_code 属于不同层，
要结合完成比例、退出原因和应用指标一起判断。

批次收集后统一分析（替换实际 campaign 路径，输出使用新目录）：

```bash
python3 /home/tongtao/Develop/Code/bft-ns3.39/simulator/ns-3.39/examples/consensus/consensus_single_run.py \
  run-logs/campaign-001/collected --output results/analysis-campaign-001
```

如需独立裁剪统计窗口，追加 `--window-start 0.25 --window-stop 0.75`。
指标口径和输出见 examples/consensus/README.md 的“批次结束后手动统计”。

如需重跑，可以提交同一 campaign 形成新 attempt，但它会重跑整个矩阵。
更稳妥的参数修改流程是生成新矩阵、新 campaign，只选择需要的 case/axes/seeds 再提交。
收集并验收前保留执行节点数据。

## 13. 常见故障

| 现象 | 检查与处理 |
| --- | --- |
| Could not establish a configuration source / DNS SRV lookup failed | 先确认 /etc/slurm/slurm.conf 存在；自定义路径时统一 SLURM_CONF 和服务参数 |
| 控制器无法连接 | 检查 SlurmctldHost、解析、6817、slurmctld 日志 |
| MUNGE authentication 错误 | 核对同一密钥、用户/权限、时间、munge 服务和跨节点解码 |
| 节点 INVALID_REG / Low RealMemory / DRAIN | 对照 slurmd -C 修正 CPU 拓扑与内存；看 Reason，修复后再 RESUME |
| 作业 PENDING | squeue 看 Reason；检查节点、分区、资源、时间上限和 account |
| chdir failed / 文件不存在 | 三台机器的绝对路径必须相同；检查执行用户权限、campaign 同步 |
| runtime differs / configuration fingerprint differs | 产物或配置与 prepare 时不同；整理一致后重新 prepare 新 campaign |
| error while loading shared libraries | 远端 ldd 查 not found；同步完整 build/lib，并安装兼容系统运行库 |
| Illegal instruction | 排查 native 优化和 CPU 指令兼容性，统一重编译并创建新 campaign |
| srun 卡住或回连失败 | 检查双向通信和 srun 回连端口，不只检查 6817/6818 |
| Slurm TIMEOUT / 内存不足 | 增加实际时间/内存，降低并发；结合 console.log 与资源实测 |
| run-task 退出 3 | 仿真请求未完成或截断；检查结果后调整仿真结束时间或负载，新建 campaign |
| collect SSH/rsync 失败 | 检查作业用户 SSH、host key、rsync 安装与远端路径 |

服务日志在对应机器查询：

```bash
# 控制节点
sudo journalctl -u slurmctld -n 100 --no-pager
# 执行节点
sudo journalctl -u slurmd -n 100 --no-pager
# 任意节点
sudo journalctl -u munge -n 100 --no-pager
```

## 14. 集群搭好后的日常操作清单

1. 在控制节点进入 ns-3 根目录，确认 sinfo 中两个执行节点可用。
2. 选好基础 .conf、拓扑、流量文件，编辑 matrix.json；小实验先验收。
3. 如有源码变化，编译一次并同步同一套产物，运行期间不要改公共运行文件。
4. prepare 到一个新 campaign 目录，核对任务数和生成配置。
5. 同步 examples、build 和本轮 campaign，检查远端路径和动态库。
6. submit --dry-run 检查命令，再去掉 --dry-run 正式提交。
7. squeue 观察；作业结束后 collect；核对所有任务和完成指标。
8. 验收后保留矩阵、输入、任务清单、提交记录和结果，供复现。

## 15. 参考资料

本手册的实验行为以本仓库 consensus-slurm.py、run-experiment.py 为准。
系统配置项需同时核对本机安装版本的 `man slurm.conf`，官网文档可能对应较新版本。

- [Slurm 管理员快速入门](https://slurm.schedmd.com/quickstart_admin.html)：节点角色、认证和服务启动。
- [slurm.conf](https://slurm.schedmd.com/slurm.conf.html)：集群、节点、分区与调度配置。
- [Slurm 网络配置](https://slurm.schedmd.com/network.html)：双向通信和端口要求。
