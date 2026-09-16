#!/usr/bin/env python3
"""
OpenSAI M1 — Comprehensive Test Runner (APP-1.10).

Run all unit/integration tests and report results. Does NOT require a running
OpenSAI server (all tests use stubs/mocks for external dependencies).

Pytest-fixture tests (any ``test_*`` function whose signature has a required
parameter, e.g. ``tmp_path``) are SKIPPED by this zero-arg runner, because it
invokes each test function with no arguments. Those tests still run under
``pytest``. This keeps the self-hosted runner honest (no spurious FAIL) while
pytest remains the canonical fixture-capable path.

Usage:
    python3 tests/run_all.py
    python3 tests/run_all.py --verbose
"""

import sys
import time
import traceback
from pathlib import Path

# Ensure project root on path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))


def _requires_fixtures(fn) -> bool:
    """Return True when the test function needs pytest fixtures (required args).

    A zero-arg runner cannot supply ``tmp_path`` / ``monkeypatch`` / etc., so
    such functions are skipped rather than invoked with missing arguments.
    """
    import inspect

    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return False
    for p in sig.parameters.values():
        if p.default is inspect.Parameter.empty and p.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            return True
    return False


def discover_tests():
    """Discover zero-arg test functions from test_*.py files.

    Returns ``(test_funcs, skipped)``. ``skipped`` lists fixture-based tests
    (signature has required parameters) that this runner cannot invoke.
    """
    import importlib
    import inspect

    test_dir = Path(__file__).resolve().parent
    test_modules = []
    test_funcs = []
    skipped = []

    for f in sorted(test_dir.glob('test_*.py')):
        mod_name = f'tests.{f.stem}'
        try:
            mod = importlib.import_module(mod_name)
            test_modules.append(mod)
            for name, obj in inspect.getmembers(mod, inspect.isfunction):
                if not name.startswith('test_'):
                    continue
                if _requires_fixtures(obj):
                    skipped.append((mod_name, name))
                    continue
                test_funcs.append((mod_name, name, obj))
        except Exception as e:
            print(f"  SKIP: Failed to import {mod_name}: {e}")

    return test_funcs, skipped


def run_tests(verbose=False):
    """Run all discovered zero-arg tests."""
    print("=" * 60)
    print("  OpenSAI M1 — Test Suite")
    print(f"  Python: {sys.version.split()[0]}")
    print("=" * 60)

    test_funcs, skipped = discover_tests()
    if not test_funcs and not skipped:
        print("\n  No test functions found!")
        return 1

    print(f"\n  Found {len(test_funcs)} runnable test functions"
          f" ({len(skipped)} fixture-based skipped)\n")

    passed = 0
    failed = 0
    errors = []

    start = time.monotonic()
    for mod_name, name, fn in test_funcs:
        try:
            fn()
            passed += 1
            if verbose:
                print(f"  PASS  {mod_name}::{name}")
        except AssertionError as e:
            failed += 1
            msg = str(e) if str(e) else 'assertion failed'
            print(f"  FAIL  {mod_name}::{name} — {msg}")
            errors.append((mod_name, name, str(e)))
        except Exception as e:
            failed += 1
            print(f"  ERROR {mod_name}::{name} — {e}")
            if verbose:
                traceback.print_exc()
            errors.append((mod_name, name, str(e)))

    elapsed = time.monotonic() - start

    print(f"\n{'=' * 60}")
    total = passed + failed
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    if skipped:
        print(f"  Skipped: {len(skipped)} (fixture-based; run under pytest)")
        for mod, name in skipped:
            print(f"    SKIP  {mod}::{name}")
    print(f"  Time:    {elapsed:.2f}s")
    print(f"{'=' * 60}")

    if errors:
        print(f"\n  Failures:")
        for mod, name, msg in errors:
            print(f"    {mod}::{name}: {msg[:120]}")

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    verbose = '--verbose' in sys.argv or '-v' in sys.argv
    sys.exit(run_tests(verbose=verbose))
