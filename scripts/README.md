# OpenSAI run scripts (Windows / PowerShell)

One-stop scripts to set up, run, and smoke-test the OpenSAI v1.1 platform on
Windows without Docker. They use a local `.venv` and a portable Redis, so they
don't touch your global Python or require admin rights.

> If PowerShell blocks scripts, prefix with `powershell -ExecutionPolicy Bypass -File`.

## First time

```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
# add -Ml to also install torch/torchvision for the CNN/Train algorithms (large)
```

This creates `.venv`, installs `Offloading-System\requirements.txt`, and downloads
a portable Redis to `%LOCALAPPDATA%\opensai-redis`.

## Run

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run.ps1            # Redis + server + 1 device
powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -WithWeb  # also start the Flask web UI (http://127.0.0.1:5000)
```

`run.ps1` is idempotent — it stops any previous server/device first (avoids the
Windows `SO_REUSEADDR` double-bind on port 8888) and launches everything from the
repo root (so `device_monitor.py`'s relative path resolves). Logs go to `.run\`.

## Verify

```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1
```

Submits a pure-Python `Pi -> Sleep` DAG and asserts the pipeline works
(`Node1` returns ~3.14 and `finishedTime` is a small elapsed value). Exit 0 = PASS.

## Regression test suite (run after any change)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\test_suite.ps1
```

~35-45s, exit 0 only if all pass. Self-contained — starts its own clean stack
(server + 2 devices) and tears it down. The **5 integration scenarios** (`run_tests.py`):
1. basic DAG (Pi -> Sleep) — core schedule/dispatch/execute/collect
2. cross-device data plane (Produce@devA -> QUIC -> Consume@devB, sha match)
3. fan-in (2 producers -> 1 fusion node fetching from both devices)
4. locality placement (unpinned dependent chain co-locates, avoids a transfer)
5. collab perception (T6.3) — CIFAR split L/R -> FeatureExtract on 2 devices -> features
   over QUIC -> Fusion classifies (real torch model + trained weights)

Orchestrated by `test_suite.py` (Python subprocess control — robust on Windows).
Run it directly too: `.\.venv\Scripts\python.exe scripts\test_suite.py`. Add new scenarios
to `run_tests.py`'s `TESTS`. **Run after any change** to keep it green.

## Collaborative-perception demo (CIFAR-10, cross-device)

A worked example of **协同感知**: one 32×32 image is split into a left and a right
half; two devices each extract a 64-dim feature with a shared backbone; the features
travel device→device over the **QUIC data plane**; a fusion node concatenates them
and classifies. This exercises the real cross-device data path end-to-end.

Prerequisites: torch + torchvision (`scripts\setup.ps1 -Ml`, or
`pip install -r Offloading-System\requirements-ml.txt`) and the CIFAR-10 dataset in
`Offloading-System\data\` (git-ignored — download separately).

```powershell
# 1) train the tiny shared model once (~5s on CPU) -> writes weights to data\ (git-ignored)
.\.venv\Scripts\python.exe scripts\train_collab.py

# 2) run the demo: starts a clean 2-device stack and classifies N images (default 10)
.\.venv\Scripts\python.exe scripts\demo_collab.py 10
```

Expected output (accuracy ≈ the model's held-out ~40%; random baseline = 10%):

```
=== Collaborative CIFAR-10 inference across 2 devices (10 images) ===
image#1    true=ship        pred=ship        [OK]  left@dev1 right@dev2
image#6    true=automobile  pred=automobile  [OK]  left@dev1 right@dev2
...
=== accuracy: 4/10 = 40%  (random baseline = 10%) ===
=== cross-device: 10/10 images had their two halves on different devices ===
```

The algorithms are ordinary plugins (`Algorithm_FeatureExtract` / `Algorithm_Fusion`
+ shared `collab_model.py`) — adding them required **no change to task dispatch**.
`demo_collab.py` reuses `test_suite.py`'s stack startup/teardown; `inject_collab.py`
submits the per-image DAG (FE_left@dev1, FE_right@dev2 → Fusion@dev1) and reports results.

> Heavy algorithms (numpy/torch) are pre-imported on the device's **main thread** at
> startup (`ActuatorImpl.preload_algorithms()`); without this, the first import inside
> a task worker thread deadlocks on Windows. New heavy algorithms are auto-warmed.

## Experiments & monitoring (Phase 2 adaptation / resource features)

Each `exp_*` script manages its own stack (start + teardown). `show_monitor` is read-only and
needs a running stack. Prefix runs with `.\.venv\Scripts\python.exe`. Implementation details:
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) §11–§15.

| Script | What it shows |
|---|---|
| `show_monitor.py [N]` | live **server aggregated view** (T1.3): device resources + brain ledgers — placement reservations / committed egress bw / available cores / in-flight / algorithm history |
| `exp_placement_quant.py` | placement `smart`/`plan` vs `random` across data regimes (3 devices) |
| `exp_placement_scale.py` | placement at scale (24-node reduction tree × 10 devices) |
| `exp_collab_adaptation.py` | **big** 45-node intertwined collab DAG × 10 devices; adaptation ON(plan) vs OFF(random): **network-heavy makespan −31%, cross-device transfers 49.5→16/56** |
| `exp_compute_adaptation.py` | compute-heavy on **heterogeneous** devices (2 strong + 2 weak slots): capacity-aware placement beats `random` (which overloads the 1-slot devices → stragglers) |
| `exp_multidag_contention.py [K] [trials]` | K DAGs concurrently; OFF / plan-only / +bw-fair-share attribution (under heavy concurrent transfers the bottleneck is the aioquic data plane, so adaptation mainly tightens the tail) |
| `exp_bw_alloc.py` | T3.2-A per-transfer bandwidth allocation + device egress-budget accounting |
| `exp_egress_fairshare.py` | A1 sender-side egress fair-share (budget 4MB/s: 2 streams ≈ 2 each, aggregate ≈ 4 not 8; one ends → other speeds up) |
| `exp_compute_alloc.py` | T3.2-B per-task core allocation (same matmul: **1 core 105s vs 16 cores 0.2s**) |
| `exp_mem_budget.py` | B4 per-task memory budget enforced (same 400MB task: budget 100MB → killed, 800MB → ok) |
| `verify_t13.py` | T1.3 check: brain ledgers (reservation/committed-bw/in-flight) appear **live** in the aggregated view during a running DAG |

Knobs these honor (env or per-node `taskInputInfo`): `OPENSAI_PLACEMENT` (plan/smart/random),
`OPENSAI_W_XFER`/`OPENSAI_W_LOAD`, `OPENSAI_TASK_WORKERS`, `OPENSAI_DEFAULT_CORES`,
`OPENSAI_DEVICE_BW_MBPS`, `OPENSAI_DATA_RATE_MBPS`, `OPENSAI_TARGET_XFER_S`, `OPENSAI_AUTO_MEM`,
`OPENSAI_PROBE_INTERVAL`, `OPENSAI_RESERVATION_TTL`; per-node `cores` / `mem_mb` / `bw_mbps`.
See [README.md](../README.md) §7 for the usage table.

## Stop

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stop.ps1        # stop server/device/web (leave Redis)
powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All   # also stop Redis
```

## Notes

- **Redis client must stay on the 5.0.x line** (see `requirements.txt`): newer
  redis-py speaks RESP3 (`HELLO`), which Redis 5 rejects, and the failure is
  swallowed silently.
- **Execution = process pool (default, no Docker).** Tasks run in a per-device worker
  **process pool** (`dag_offload/task_pool.py`) with per-task **core-pinning** (psutil
  cpu_affinity) and **memory limits** (Windows Job Object / POSIX RLIMIT_AS) — cross-platform.
  Pool size = `OPENSAI_TASK_WORKERS`. If the pool can't build (or breaks), it falls back to
  in-process execution. Docker (`resource_control_interface.py`) is an optional/legacy path.
- ML algorithms (`Algorithm_CNN/NEW_CNN/Train`) additionally need the CIFAR-10
  dataset under `Offloading-System\data\` (git-ignored — download separately).
- The 1.0 reference baseline lives in `opensai_1.0\` (git-ignored). Compare:
  `git diff --no-index opensai_1.0\Offloading-System Offloading-System`
