# OpenSAI — DAG 计算卸载平台（使用指南）

OpenSAI 是一个面向 **DAG（有向无环图）计算任务**的协同卸载平台：你把一个任务拆成若干
有依赖关系的子任务（DAG），平台负责把这些子任务**调度并分发到不同的边缘设备**上执行，
再把结果按依赖顺序汇总返回。系统分三层：**应用层**（定义/发布任务、查看结果）、
**智联服务层**（计算卸载决策、调度）、**服务定制网络层**（设备通信、资源监测与控制）。

> 这是给**第一次接触本系统**的人看的快速上手指南。想了解“系统怎么设计、能做什么、
> 哪些还没做”，请看 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 系统架构图

![架构图](Offloading-System/dag_offload/static/img.png)

---

## 1. 环境要求

- **Windows 10/11** + **Python 3.13**（已验证）。无需管理员权限。
- 不需要预装 Redis 或 Docker：
  - **Redis** 由安装脚本自动下载便携版（仅 server 所在机器需要）。
  - **Docker 可选**：装了就用容器执行任务（带 CPU/内存限制）；没装会**自动降级为本地执行**。
- 跑深度学习算法（CNN/Train）才需要额外的 `torch` 和 CIFAR-10 数据（见 §6）。

> 目前一键脚本是 PowerShell 版（Windows）。Linux 下可参考脚本里的命令手动执行。

## 2. 快速开始（三步）

在仓库根目录 `D:\opensai-master` 下：

```powershell
# ① 一次性安装：建虚拟环境 .venv、装核心依赖、下载便携版 Redis
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1

# ② 启动整套：Redis + server + 1 个 device（幂等，可重复执行）
powershell -ExecutionPolicy Bypass -File scripts\run.ps1

# ③ 端到端自检：提交一个 Pi→Sleep 的 DAG，看是否跑通
powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1
```

看到 `SMOKE TEST: PASS`（含 `Node1 pi≈3.14`）就说明整条链路正常。

停止：`powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All`
（不加 `-All` 只停 server/device/web，保留 Redis）。

脚本细节见 [scripts/README.md](scripts/README.md)。

## 3. 提交一个 DAG 任务

### 方式 A：命令行（最简单，适合自测/脚本化）

`scripts\inject_dag.py` 就是一个最小注入器（也是冒烟测试用的）。它向 server 提交一个
两节点 DAG（`Algorithm_Pi_Calculation` → `Algorithm_Sleep_1`）并打印结果。直接：

```powershell
.\.venv\Scripts\python.exe scripts\inject_dag.py
```

照着它改 `dag_data` 就能提交你自己的 DAG。也可参考更完整的示例
`Offloading-System\dag_offload\single_client.py`。

### 方式 B：Web 前端（可视化拖拽，适合演示）

见 §5。

### DAG 数据格式（核心）

一个 DAG 任务是这样的 JSON（关键字段）：

```python
dag_data = {'dagData': {
    'taskName': 'app1',                 # 任务名
    'taskId': '1',
    'taskCreateTime': time.time(),      # 时间戳（float）或 "YYYY-mm-dd HH:MM:SS"
    'taskPolicy': '策略1',
    'nodes': [                          # 子任务列表（节点）
        {
            'id': 1, 'name': 'Node1',
            'form': {
                'beforeNodes': [],              # 前驱节点 id（依赖谁）
                'afterNodes': [2],              # 后继节点 id（谁依赖我）
                'taskAlgorithm': 'Algorithm_Pi_Calculation',  # 算法名（见 §4）
                'taskVersion': '1.0',
                'taskInputInfo': {              # 传给算法 run() 的参数 dict
                    'inDataPath': './data/', 'outDataPath': './data/',
                    'count_num': 500000
                },
                'directAllocationId': -1,       # -1=随机分配设备；>0=指定第 N 个设备
                'deviceParam': {},
                'containerConfig': {'cpu_cores': 2.0, 'memory_mb': 2048}  # 可选：Docker 资源
            },
            'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}
        },
        # ... Node2，beforeNodes=[1] 表示它依赖 Node1 ...
    ]
}}
```

- `beforeNodes` / `afterNodes` 描述依赖关系；server 按依赖顺序调度（前驱全完成才就绪）。
- `taskInputInfo` 整个 dict 会作为 `param_dict` 传给算法的 `run()`。
- 更详细的多节点示例（含分支/汇聚）见
  [Offloading-System/dag_offload/README.md](Offloading-System/dag_offload/README.md)。

> 跨设备数据：上游节点写到约定路径 `param_dict['_artifact_out']` 的产物，会经 **QUIC 数据面**
> 自动拉取到下游节点所在的设备（注入为下游的 `param_dict['_artifact_in']`，是一个 `{前驱id: 本地文件}`
> 映射）。这正是“协同感知”流水线的基础——可运行示例见 [scripts/README.md](scripts/README.md) 的
> “Collaborative-perception demo”。（仅依赖关系、不走该约定的普通文件不会自动传输。）

## 4. 添加自定义算法

算法文件放在 `Offloading-System\device\implements\`，命名与写法约定：

1. 文件名以 `Algorithm` 开头，例如 `Algorithm_Foo.py`（这样才会被设备自动识别）。
2. 文件内：`__all__ = ['Algorithm_Foo']`，并定义**同名类** `Algorithm_Foo`。
3. 类的构造函数签名为 `__init__(self, param_dict, data_dict)`，并实现 `run(self)` 返回结果。

最小示例：
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
然后在 DAG 节点里把 `taskAlgorithm` 写成 `'Algorithm_Foo'` 即可。

> 也可以通过 Web 前端的“上传 .py”功能上传算法（会保存到上述目录）。
> 现成算法：`Algorithm_Pi_Calculation` / `Algorithm_Random` / `Algorithm_Sleep_1`（纯 Python，开箱即用）；
> `Algorithm_CNN` / `Algorithm_NEW_CNN` / `Algorithm_Train`（需 `torch` + CIFAR-10 数据）。

## 5. 启动 Web 前端（可选）

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -WithWeb
```
然后浏览器打开 **http://127.0.0.1:5000** ，按流程“选择 + 定制 DAG → 提交 → 查看结果”。
前端是已编译好的静态包（`dag_offload/static/`），由 `user_client.py`（Flask）提供。

## 6. 跑深度学习算法（可选）

```powershell
.\.venv\Scripts\python.exe -m pip install -r Offloading-System\requirements-ml.txt
# 并把 CIFAR-10 数据放到 Offloading-System\data\（数据较大，已被 git 忽略）
```

**协同感知 demo（跨设备协作，推荐先跑这个看效果）**：把一张 CIFAR 图切成左右两半，
两台设备各提取特征，特征经 QUIC 数据面传输后融合分类。先 `python scripts\train_collab.py`
训练一次，再 `python scripts\demo_collab.py 10` 演示。详见 [scripts/README.md](scripts/README.md)。

## 7. 多机部署

平台是 TCP 星型结构，可跨主机部署：在每台机器设环境变量 `OPENSAI_HOST` = server 的局域网 IP、
放行 8888 端口即可。完整步骤与限制见 **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)**。

## 8. 项目结构

```
opensai-master/
├─ README.md                  ← 本文件（使用指南）
├─ scripts/                   ← 一键 setup/run/stop/smoke 脚本（见 scripts/README.md）
├─ docs/
│   ├─ ARCHITECTURE.md        ← 系统架构与功能说明（强烈建议通读）
│   ├─ CODE_MAP.md            ← 真实调用链 vs 死代码地图
│   └─ DEPLOYMENT.md          ← 多机部署
├─ opensai_1.0/               ← 1.0 参考版本（git 忽略，仅作对比基线）
└─ Offloading-System/         ← 主系统
    ├─ dag_offload/           ← ★ 真正的入口
    │   ├─ server.py          ←  卸载决策 + 分发中枢（TCP，:8888）
    │   ├─ device_client.py   ←  边缘设备执行端
    │   ├─ user_client.py     ←  Web/API 前端（Flask，:5000）
    │   └─ offloading_brain/  ←  卸载决策（目前：随机/指定）
    ├─ device/implements/Algorithm_*.py   ← 算法
    ├─ device/interfaces/resource_control_interface.py  ← Docker 资源控制
    ├─ predis/                ← Redis 设备注册表 / 服务层
    ├─ task/ result/ config/  ← 任务模型 / 结果模型 / 配置
    └─ requirements*.txt       ← core / -ml / -optional
```

## 9. 日志与常见问题

- **日志**：运行时日志在 `.run\server.err.log` / `.run\device.err.log`（git 忽略）。
- **必须用脚本/从仓库根目录启动**：`run.ps1` 会从仓库根目录拉起进程（保证路径正确）。
- **Redis 客户端必须是 5.0.x**：`requirements.txt` 已锁定。新版 redis-py 会用 RESP3 的
  `HELLO` 命令，老 Redis 不认且错误被静默吞掉 → 设备注册不上、任务不分发。
- **提示“无可用在线设备…等待重试”**：说明此刻没有 device 在线，先 `run.ps1` 起一个 device；
  任务会在设备上线后自动继续（不会崩）。
- **端口 8888 被占**：`stop.ps1 -All` 清干净再 `run.ps1`。

## 10. 文档与版本

- 架构与能力：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 代码地图（活/死代码）：[docs/CODE_MAP.md](docs/CODE_MAP.md)
- 多机部署：[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
- 脚本说明：[scripts/README.md](scripts/README.md)

版本（git tag）：`v1.1-inherited`（接手原样）→ `v1.1.1`（可复现+止血修复）→
`v1.1.2`（死代码清理 + 卸载健壮性 + 多机配置）。对比某两版：`git diff v1.1-inherited v1.1.2`。
