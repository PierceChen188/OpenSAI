# OpenSAI — 分布式 DAG 计算卸载平台

OpenSAI 把一个应用拆成带依赖关系的子任务图（DAG），由中心协调器智能地把子任务**放置**到合适的边缘设备执行、按依赖顺序汇总结果，并对每个任务进行**算力 / 内存 / 带宽 / 存储四维资源的分配与控制**；设备之间通过专用 QUIC 数据面**直接传输**中间结果（支持中继 / 多网卡多链路并行聚合），并配套**可视化 Web 控制台**实时呈现集群与决策过程，支撑协同感知、协同推理等数据密集型场景。

平台分三层：**接入层**（定义/提交任务、查询结果）、**智能调度层**（依赖调度、智能放置、资源分配、聚合监测）、**设备执行与网络层**（设备执行、设备间数据面、资源与链路监测）。

> 本文是面向使用者的快速上手指南；系统设计与实现细节见 [OpenSAI 软件手册](docs/OpenSAI软件手册（含说明文档+测试文档）.pdf)。

## 平台能力一览

- **DAG 任务编排**：把应用描述为带依赖的子任务图，按依赖顺序自动调度、分发、汇总。
- **智能任务放置**：就近 / 容量感知 + 健康预筛 + 整图前瞻规划，按真实数据量与链路自适应选设备（相对随机基线，完成时间最高降低 84%，详见软件手册实测效果）。
- **按任务四维资源分配与控制**：为每个任务分配 CPU 核 / 内存预算 / 传输带宽 / 存储配额，设备端跨平台施加（`cpu_affinity` 绑核 + 内存硬上限 + 带宽令牌桶限速 + 存储配额与产物回收）。
- **设备间数据面（多链路）**：中间结果经 QUIC 点对点直传，支持**中继 + 多网卡并行聚合**的多链路传输；设备出口带宽限速 + 多流公平平分；整图完成自动回收落盘产物。
- **可视化 Web 控制台**：浏览器打开即看集群三维拓扑、任务编排与提交、放置 / 资源 / 传输**决策过程**、设备拓扑编辑（内置前端 + Flask 桥接 :5000）。
- **实时监测与决策可观测**：设备资源 + 设备间链路遥测 + 协调器账本聚合为权威集群快照，并实时输出放置 / 资源 / 传输决策事件流。
- **协同感知**：跨设备拆分推理（图像分区 → 各设备提取特征 → 数据面汇聚 → 融合分类）。
- **即插即用算法**：放入一个 `Algorithm_*.py` 即被设备自动加载。
- **跨平台与多机**：Windows / POSIX 执行；TCP 星型结构可跨主机部署。

---

## 1. 环境要求

- **Windows 10/11 + Python 3.13**（已验证），无需管理员权限。
- **无需预装 Redis**：由安装脚本自动下载便携版（仅协调器所在机器需要）。
- **无需 Docker**：任务一律在设备端本地进程池中执行（绑核 / 限内存 / 限存储）。
- **可选 Node.js**：仅在自行重建 Web 控制台前端时需要；仓库已内置构建产物，无 Node 也能直接打开控制台。
- 深度学习 / 协同感知算法需额外安装 `torch` 并准备 CIFAR-10 数据（见 §6）。
- 一键脚本为 PowerShell 版（Windows）；Linux 下可参照脚本内命令手动执行。

## 2. 快速开始（三步）

在仓库根目录下：

```powershell
# ① 一次性安装：建虚拟环境 .venv、装核心依赖、下载便携版 Redis
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1

# ② 启动整套：Redis + server + 1 个 device（幂等，可重复执行）
powershell -ExecutionPolicy Bypass -File scripts\run.ps1

# ③ 端到端自检：提交一个 Pi→Sleep 的 DAG，看是否跑通
powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1
```

看到 `SMOKE TEST: PASS`（含 `Node1 pi≈3.14`）即说明整条链路正常。

停止：`powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All`（不加 `-All` 只停 server/device，保留 Redis）。全部脚本在 `scripts\` 目录。

### 打开 Web 控制台（可视化）

双击仓库根目录的 `start-console.bat`（或运行 `powershell -ExecutionPolicy Bypass -File scripts\start_console.ps1`）：会在需要时构建前端、启动 Flask 桥接（`http://127.0.0.1:5000`）并自动打开浏览器。控制台提供**集群三维拓扑、任务编排与提交、放置 / 资源 / 传输决策过程、设备拓扑编辑**。即使 server / Redis 未启动也能打开（显示"未连接"，可在顶栏开启"演示数据"浏览界面）。

## 3. 提交一个 DAG 任务

`scripts\inject_dag.py` 是一个最小注入器：向协调器提交一个两节点 DAG（`Algorithm_Pi_Calculation` → `Algorithm_Sleep_1`）并打印结果。

```powershell
.\.venv\Scripts\python.exe scripts\inject_dag.py
```

照着它改 `dag_data` 即可提交自己的 DAG；更完整的示例见 `Offloading-System\dag_offload\single_client.py`。

### DAG 数据格式（核心字段）

```python
dag_data = {'dagData': {
    'taskName': 'app1', 'taskId': '1',
    'taskCreateTime': time.time(),          # 时间戳(float) 或 "YYYY-mm-dd HH:MM:SS"
    'taskPolicy': '策略1',
    'nodes': [                              # 子任务列表（节点）
        {
            'id': 1, 'name': 'Node1',
            'form': {
                'beforeNodes': [],                  # 前驱节点 id（依赖谁）
                'afterNodes': [2],                  # 后继节点 id（谁依赖我）
                'taskAlgorithm': 'Algorithm_Pi_Calculation',  # 算法名（见 §4）
                'taskVersion': '1.0',
                'taskInputInfo': {                  # 传给算法 run() 的参数 dict
                    'inDataPath': './data/', 'outDataPath': './data/',
                    'count_num': 500000
                    # 可选资源档位：cores / mem_mb / bw_mbps（见 §5）
                },
                'directAllocationId': -1,           # -1=智能放置；>0=指定第 N 个设备
                'deviceParam': {}
            },
            'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}
        },
        # ... Node2，beforeNodes=[1] 表示它依赖 Node1 ...
    ]
}}
```

- `beforeNodes` / `afterNodes` 描述依赖关系；协调器按依赖顺序调度（前驱全完成才就绪）。
- `taskInputInfo` 整个 dict 作为 `param_dict` 传给算法的 `run()`。
- 多节点示例（含分支/汇聚）见 `Offloading-System\dag_offload\single_client.py`。

> **跨设备数据**：上游节点写到 `param_dict['_artifact_out']` 的产物，会经 **QUIC 数据面**自动拉取到下游节点所在设备（注入为下游的 `param_dict['_artifact_in']`，即 `{前驱id: 本地文件}` 映射）。这是协同感知流水线的基础——可运行示例见 §6 的协同感知 demo。

## 4. 添加自定义算法

算法文件放在 `Offloading-System\device\implements\`，约定：

1. 文件名以 `Algorithm` 开头（如 `Algorithm_Foo.py`），设备会自动识别加载。
2. 文件内：`__all__ = ['Algorithm_Foo']`，并定义**同名类**。
3. 类构造签名 `__init__(self, param_dict, data_dict)`，实现 `run(self)` 返回结果。

```python
# Offloading-System/device/implements/Algorithm_Foo.py
__all__ = ['Algorithm_Foo']

class Algorithm_Foo:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict          # = DAG 节点里的 taskInputInfo
        self.data = data_dict
    def run(self):
        n = self.param.get('count_num', 100)
        return f"foo done x{n}"
```

然后在 DAG 节点里把 `taskAlgorithm` 写成 `'Algorithm_Foo'` 即可。现成算法：`Algorithm_Pi_Calculation` / `Algorithm_Random` / `Algorithm_Sleep_1`（纯 Python，开箱即用）；`Algorithm_MatMul` / `Algorithm_ParallelMatMul`（多核计算）；`Algorithm_FeatureExtract` / `Algorithm_Fusion` / `Algorithm_CNN`（需 `torch` + CIFAR-10）。

## 5. 智能放置与资源控制

平台默认即启用智能放置，并可为每个任务分配资源档位，全部由环境变量 / 节点参数控制。机制与实现见 [OpenSAI 软件手册](docs/OpenSAI软件手册（含说明文档+测试文档）.pdf)。

### 5.1 放置（WHERE：子任务放到哪台设备）

`OPENSAI_PLACEMENT` 选模式：`plan`（默认，整图前瞻就近规划）/ `smart`（自适应贪心）/ `random`（对照基线）。节点带 `directAllocationId>0` 时按手动指定。调权重：`OPENSAI_W_XFER`（传输权重）、`OPENSAI_W_LOAD`（负载权重，↑ 更分散 / ↓ 更聚合）、`OPENSAI_DEFAULT_BW_MBPS`（未知链路兜底带宽）、`OPENSAI_PROBE_INTERVAL`（链路主动探测间隔秒）。

### 5.2 资源档位（HOW MUCH：每个任务给多少算力 / 内存 / 带宽 / 存储）

可在 DAG 节点的 `taskInputInfo` 里**显式声明**，或让协调器**自动**决定（均按设备“可用”收口、防超额）：

| 维度 | 节点显式声明 | 自动 | 设备执行机制 |
|---|---|---|---|
| CPU 核 | `cores: N` | 默认 = 总核 ÷ worker_slots | 进程池 worker `cpu_affinity` 绑核 + BLAS 线程对齐 |
| 内存 | `mem_mb: N`（运行基线**之上**的任务预算） | `OPENSAI_AUTO_MEM=1` 时按历史边际×2（地板 256MB） | 作业对象 / RLIMIT_AS 限“实时提交 + 预算” |
| 带宽 | `bw_mbps: N` | `OPENSAI_TARGET_XFER_S=秒` 时按 数据量 ÷ 目标时延 自动算 | 令牌桶限速 + 出口公平平分 |
| 存储 | `storage_mb: N` | `OPENSAI_AUTO_STORAGE=1` 时按算法历史产出自动定档 | 配额记账 + 产物 TTL / 磁盘水位 LRU 回收 |

设备 / 出口预算旋钮：`OPENSAI_TASK_WORKERS`（每设备并行槽 = 进程池大小）、`OPENSAI_DEFAULT_CORES`、`OPENSAI_DEVICE_BW_MBPS`（协调器侧每设备出口预算）、`OPENSAI_DATA_RATE_MBPS`（发送端出口预算，在并发流间公平平分）、`OPENSAI_RESERVATION_TTL`（预留超时回收秒，默认 600）。

## 6. 协同感知 demo 与深度学习算法

```powershell
.\.venv\Scripts\python.exe -m pip install -r Offloading-System\requirements-ml.txt
# 并把 CIFAR-10 数据放到 Offloading-System\data\
```

**协同感知 demo（跨设备协作，推荐先跑这个看效果）**：把一张 CIFAR 图切成左右两半，两台设备各提取特征，特征经 QUIC 数据面传输后融合分类。先 `python scripts\train_collab.py` 训练一次，再 `python scripts\demo_collab.py 10` 演示。

## 7. 监测与实验

```powershell
# 实时聚合监测视图（设备资源 + 协调器账本：预留 / 已承诺带宽 / 可用核 / 在飞 / 算法画像）
.\.venv\Scripts\python.exe scripts\show_monitor.py 10
```

量化实验脚本（放置 vs random、带宽 / 算力 / 内存 / 存储分配、出口公平、多链路聚合、异构容量、放置+资源协同等）位于 `scripts\` 目录（`exp_*.py`）。一键集成回归：`scripts\test_suite.py`（拉起集群并验证基础 DAG、跨设备数据面、扇入汇聚、就近放置、CIFAR 协同感知 5 个端到端场景）。

## 8. 多机部署

平台是 TCP 星型结构，可跨主机部署：每台 device 机器设环境变量 `OPENSAI_HOST` = 协调器局域网 IP，协调器机放行 `8888`（控制面）、各 device 放行其 `OPENSAI_DATA_PORT`（数据面 QUIC）即可。

## 9. 项目结构

```
opensai-master/
├─ README.md                  ← 本文件（使用指南）
├─ start-console.bat          ← 一键打开 Web 控制台（构建前端 + 启动桥接 + 开浏览器）
├─ frontend/                  ← Web 控制台前端源码（React + Vite + Three.js）
├─ scripts/                   ← setup/run/stop/smoke 脚本 + 演示/实验
├─ docs/
│   └─ OpenSAI软件手册（含说明文档+测试文档）.pdf   ← 软件手册（架构 / 实现 / 测试，建议通读）
└─ Offloading-System/         ← 主系统
    ├─ dag_offload/           ← ★ 入口
    │   ├─ server.py          ←  调度 + 智能放置 + 资源分配 + 分发中枢（TCP，:8888）
    │   ├─ device_client.py   ←  边缘设备执行端（心跳/监测、收子任务、绑核/限内存执行、回结果）
    │   ├─ run_device.py      ←  设备轻量启动器
    │   ├─ task_pool.py       ←  进程池 + 绑核 + 内存上限 + BLAS 线程对齐
    │   ├─ user_client.py     ←  Web 控制台桥接（Flask，:5000；服务 SPA + HTTP/SSE API）
    │   ├─ static/            ←  控制台前端构建产物（被桥接直接服务）
    │   └─ offloading_brain/  ←  放置决策：plan（前瞻）/ smart（贪心）/ random
    ├─ network/dataplane/     ←  QUIC 设备间数据面（发送/拉取；多链路：中继 + 多网卡聚合；限速/出口公平）
    ├─ device/implements/Algorithm_*.py   ← 算法库（含协同感知 FeatureExtract / Fusion）
    ├─ predis/                ← Redis 设备注册表 / 服务层
    ├─ task/ result/ config/  ← 任务模型 / 结果模型 / 配置
    └─ requirements*.txt       ← core / -ml / -optional
```

## 10. 运行须知

- **日志**：运行时日志在 `.run\server.err.log` / `.run\device.err.log`。
- **从仓库根目录启动**：`run.ps1` 会从仓库根目录拉起进程，保证路径正确。
- **Redis 客户端锁定 5.0.x**：`requirements.txt` 已锁定，与便携版 Redis 服务匹配。
- **设备上线即自动接管**：若提交任务时暂无 device 在线，任务会在设备上线后自动继续。

## 11. 文档

- 系统架构 / 实现 / 测试说明：[OpenSAI 软件手册](docs/OpenSAI软件手册（含说明文档+测试文档）.pdf)
