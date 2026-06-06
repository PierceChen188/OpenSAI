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
