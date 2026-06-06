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

~25-30s, exit 0 only if all pass. Self-contained — starts its own clean stack
(server + 2 devices) and tears it down. Covers:
1. QUIC transport unit test (`test_dataplane.py`)
2. basic DAG (Pi -> Sleep) — core schedule/dispatch/execute/collect
3. cross-device data plane (Produce@devA -> QUIC -> Consume@devB, sha match)
4. fan-in (2 producers -> 1 fusion node fetching from both devices)

Orchestrated by `test_suite.py` (Python subprocess control — robust on Windows).
The 3 integration scenarios live in `run_tests.py`; add new ones there.

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

## Stop

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stop.ps1        # stop server/device/web (leave Redis)
powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All   # also stop Redis
```

## Notes

- **Redis client must stay on the 5.0.x line** (see `requirements.txt`): newer
  redis-py speaks RESP3 (`HELLO`), which Redis 5 rejects, and the failure is
  swallowed silently.
- **Docker is optional.** Without it, `device_client` falls back to local
  execution (the in-process algorithm runner). With Docker installed, tasks run
  in per-task containers with CPU/memory limits.
- ML algorithms (`Algorithm_CNN/NEW_CNN/Train`) additionally need the CIFAR-10
  dataset under `Offloading-System\data\` (git-ignored — download separately).
- The 1.0 reference baseline lives in `opensai_1.0\` (git-ignored). Compare:
  `git diff --no-index opensai_1.0\Offloading-System Offloading-System`
