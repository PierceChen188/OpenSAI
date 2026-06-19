"""CI 入口：依次跑【核心回归(5 场景)】+【新特性断言(多链路/存储)】，任一失败则退出码非 0。

可直接接任意 CI（GitHub Actions / 阿里云 Codeup Flow / 本地 pre-push）：
  - 需要本机可用的 Redis(127.0.0.1:6379) 与已装依赖的虚拟环境。
  - 用法： .venv/Scripts/python.exe scripts/ci.py
退出码 0 = 全绿；非 0 = 有失败（CI 据此判定流水线红/绿）。
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

STAGES = [
    ('regression (5 scenarios)', os.path.join(HERE, 'test_suite.py')),
    ('feature assertions (multipath/storage)', os.path.join(HERE, 'test_features.py')),
]


def main():
    results = []
    for name, script in STAGES:
        print(f"\n{'#' * 70}\n# CI stage: {name}\n# {script}\n{'#' * 70}", flush=True)
        rc = subprocess.call([PY, script])
        results.append((name, rc))
        print(f"# stage '{name}' exit={rc}", flush=True)
    print(f"\n{'=' * 70}\n  CI SUMMARY\n{'=' * 70}", flush=True)
    ok = True
    for name, rc in results:
        print(f"  [{'PASS' if rc == 0 else 'FAIL'}] {name} (exit={rc})", flush=True)
        ok = ok and rc == 0
    print(f"\n{'==== CI: ALL GREEN ====' if ok else '==== CI: RED ===='}", flush=True)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
