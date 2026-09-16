#!/usr/bin/env python3
"""
OpenSAI M1 End-to-End Demo (APP-1.9 + CLU-1.9 + RES-1.9 + MON-1.9).

Demonstrates the full OpenSAI pipeline using the opensai_client SDK:

  1. Connect to the OpenSAI server
  2. Discover cluster capabilities (algorithms, devices, status)   [RES-1.9, MON-1.9]
  3. Submit a cross-device data-plane DAG (ProduceBlob -> ConsumeBlob)  [APP-1.9]
  4. Submit a collaborative perception DAG (FeatureExtract + Fusion)   [APP-1.9]
  5. Wait for and display results
  6. Show resource usage after execution                            [RES-1.9]

Placement is handled by the server's placement_service (CLU-1.9).

Usage
-----
    python3 scripts/demo_m1.py [--host HOST] [--port PORT] [--timeout SECONDS]

    # Skip collab scenario (no torch/CIFAR):
    python3 scripts/demo_m1.py --skip-collab

Environment
-----------
    OPENSAI_HOST      — Server host (default: 127.0.0.1)
    OPENSAI_PORT      — Server port (default: 8888)
    OPENSAI_TIMEOUT   — Operation timeout (default: 30)

Exit codes
----------
    0 — All scenarios passed
    1 — Connection failed
    2 — Scenario failed
"""

import argparse
import json
import logging
import os
import sys
import time

# Ensure the project root is on sys.path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

OFFLOADING_PATH = os.path.join(_PROJECT_ROOT, 'Offloading-System')
if os.path.isdir(OFFLOADING_PATH) and OFFLOADING_PATH not in sys.path:
    sys.path.insert(0, OFFLOADING_PATH)

from opensai_client import OpenSAIClient, OpenSAIClientError, TimeoutError

log = logging.getLogger('demo_m1')


# ── helper ─────────────────────────────────────────────────────────────────

def _section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _ok(msg: str):
    print(f"  [OK]    {msg}")


def _info(msg: str):
    print(f"  [INFO]  {msg}")


def _warn(msg: str):
    print(f"  [WARN]  {msg}")


def _fail(msg: str):
    print(f"  [FAIL]  {msg}")


# ── demo scenarios ─────────────────────────────────────────────────────────

def demo_discovery(client: OpenSAIClient):
    """RES-1.9 + MON-1.9: Discover cluster capabilities."""
    _section("Phase 1: Cluster Discovery")

    # Algorithms
    algos = client.list_algorithms()
    _ok(f"Registered algorithms: {len(algos)}")
    for a in algos[:8]:
        _info(f"  - {a}")
    if len(algos) > 8:
        _info(f"  ... and {len(algos) - 8} more")

    # Devices (RES-1.9)
    devices = client.list_devices()
    _ok(f"Online devices: {len(devices)}")
    for d in devices:
        if isinstance(d, dict):
            name = d.get('device_name', d.get('deviceNo', 'unknown'))
            slots = d.get('worker_slots', d.get('workerSlots', '?'))
            cpu = d.get('used_cpu', d.get('usedCpu', '?'))
            delay = d.get('delay', '?')
            _info(f"  {name}: {slots} slots, CPU {cpu}%, delay {delay}ms")

    # Cluster status (RES-1.9)
    status = client.cluster_status()
    _ok("Cluster status:")
    _info(f"  Online: {status.get('online_devices', '?')} devices")
    _info(f"  Slots:  {status.get('used_slots', '?')}/{status.get('total_slots', '?')} used "
          f"({status.get('utilization_pct', '?')}%)")
    anomalies = status.get('anomalies', [])
    if anomalies:
        _warn(f"  {len(anomalies)} anomaly alerts")
        for a in anomalies:
            _warn(f"    - {a.get('device')}: {a.get('type')} = {a.get('value_ms', a.get('value_pct'))}")
    else:
        _ok("  No anomalies detected")

    # Events (MON-1.9)
    events = client.get_events()
    _ok(f"Decision events available: {len(events)} recent")
    return True


def demo_cross_device(client: OpenSAIClient) -> bool:
    """APP-1.9: ProduceBlob -> ConsumeBlob cross-device data plane demo.

    The server's placement_service (CLU-1.9) should automatically place
    Producer and Consumer on different devices to demonstrate QUIC data-plane
    cross-device transfer.
    """
    _section("Phase 2: Cross-Device Data Plane (ProduceBlob -> ConsumeBlob)")

    spec = {
        'taskName': 'demo_cross_device',
        'taskPolicy': 'p',
        'nodes': [
            {
                'id': 1,
                'name': 'Producer',
                'algorithm': 'Algorithm_ProduceBlob',
                'params': {'size_mb': 1, 'inDataPath': './data/', 'outDataPath': './data/'},
                'dependsOn': [],
                'directAllocationId': -1,  # CLU-1.9: auto-placement
            },
            {
                'id': 2,
                'name': 'Consumer',
                'algorithm': 'Algorithm_ConsumeBlob',
                'dependsOn': [1],
                'directAllocationId': -1,  # CLU-1.9: auto-placement
            },
        ],
    }

    _info("Submitting DAG: ProduceBlob -> ConsumeBlob")
    try:
        from opensai_client.validator import ClientValidator
        validator = ClientValidator(client)
        dagdata = validator.validate_or_raise(spec)
        job_id = client.submit_dag(dagdata)
        _ok(f"DAG submitted: job_id = {job_id}")
    except Exception as e:
        _fail(f"Submission failed: {e}")
        return False

    _info(f"Waiting for result (timeout=60s)...")
    try:
        result = client.wait_result(job_id, timeout=60)
    except TimeoutError:
        _fail("Timed out waiting for result")
        return False
    except Exception as e:
        _fail(f"Result retrieval failed: {e}")
        return False

    # Verify cross-device placement
    nodes = result.get('dagResult', result).get('nodes', [])
    if len(nodes) >= 2:
        p_dev = nodes[0].get('result', {}).get('deviceId', '?')
        c_dev = nodes[1].get('result', {}).get('deviceId', '?')
        if p_dev != c_dev:
            _ok(f"Cross-device confirmed: Producer@{p_dev}, Consumer@{c_dev}")
        else:
            _warn(f"Both nodes on same device {p_dev} (may be single-device cluster)")

    _ok(f"DAG completed in {result.get('dagResult', result).get('finishedTime', '?')}s")
    return True


def demo_collab_perception(client: OpenSAIClient) -> bool:
    """APP-1.9: FeatureExtract (left/right) -> Fusion collaborative demo.

    Requires torch + CIFAR-10 data. Runs FeatureExtract on two devices
    (left/right image halves) and Fusion to classify.

    The server's placement_service (CLU-1.9) should place FE nodes on
    different devices to demonstrate collaborative inference.
    """
    _section("Phase 3: Collaborative Perception (FeatureExtract + Fusion)")

    spec = {
        'taskName': 'demo_collab',
        'taskPolicy': 'p',
        'nodes': [
            {
                'id': 1,
                'name': 'FE_left',
                'algorithm': 'Algorithm_FeatureExtract',
                'params': {'image_index': 0, 'half': 'left',
                           'inDataPath': './data/', 'outDataPath': './data/'},
                'dependsOn': [],
                'directAllocationId': 1,
            },
            {
                'id': 2,
                'name': 'FE_right',
                'algorithm': 'Algorithm_FeatureExtract',
                'params': {'image_index': 0, 'half': 'right',
                           'inDataPath': './data/', 'outDataPath': './data/'},
                'dependsOn': [],
                'directAllocationId': 2,
            },
            {
                'id': 3,
                'name': 'Fusion',
                'algorithm': 'Algorithm_Fusion',
                'dependsOn': [1, 2],
                'directAllocationId': 1,
            },
        ],
    }

    _info("Submitting DAG: FeatureExtract(left+right) -> Fusion")
    try:
        from opensai_client.validator import ClientValidator
        validator = ClientValidator(client)
        dagdata = validator.validate_or_raise(spec)
        job_id = client.submit_dag(dagdata)
        _ok(f"DAG submitted: job_id = {job_id}")
    except Exception as e:
        _fail(f"Submission failed: {e}")
        return False

    _info(f"Waiting for result (timeout=120s)...")
    try:
        result = client.wait_result(job_id, timeout=120)
    except TimeoutError:
        _fail("Timed out waiting for result")
        return False
    except Exception as e:
        _fail(f"Result retrieval failed: {e}")
        return False

    # Report results
    nodes = result.get('dagResult', result).get('nodes', [])
    for n in nodes:
        name = n.get('name', '?')
        dev = n.get('result', {}).get('deviceId', '?')
        _info(f"  {name} -> device {dev}")

    fusion_node = next((n for n in nodes if n.get('name') == 'Fusion'), None)
    if fusion_node:
        result_info = fusion_node.get('ResultInfo', '?')
        import re
        m = re.search(r'class=(\d+)', str(result_info))
        if m:
            _ok(f"Fusion classified as: class {m.group(1)}")
        else:
            _ok(f"Fusion completed (raw output: {str(result_info)[:80]})")

    _ok("Collaborative perception demo complete")
    return True


def demo_resource_view(client: OpenSAIClient):
    """RES-1.9: Show resource usage after demos."""
    _section("Phase 4: Post-Demo Resource View")

    devices = client.list_devices()
    _ok(f"Devices after demos: {len(devices)}")
    status = client.cluster_status()
    _ok(f"Utilization: {status.get('utilization_pct', '?')}%")
    _info(f"  Used slots: {status.get('used_slots', '?')}/{status.get('total_slots', '?')}")

    # Show RES-1.9: list_algorithms + cluster_status
    _info("Resource summary available via: opensai devices list / opensai cluster status")


# ── main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='OpenSAI M1 End-to-End Demo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python3 scripts/demo_m1.py
  python3 scripts/demo_m1.py --host 192.168.1.100 --port 8888
  python3 scripts/demo_m1.py --skip-collab
        ''',
    )
    parser.add_argument('--host', default=os.environ.get('OPENSAI_HOST', '127.0.0.1'))
    parser.add_argument('--port', type=int, default=int(os.environ.get('OPENSAI_PORT', '8888')))
    parser.add_argument('--timeout', type=int, default=int(os.environ.get('OPENSAI_TIMEOUT', '30')))
    parser.add_argument('--skip-collab', action='store_true',
                        help='Skip collaborative perception scenario (requires torch/CIFAR)')
    parser.add_argument('--skip-all', action='store_true',
                        help='Only run discovery phase (no DAG submission)')
    args = parser.parse_args()

    # Logging setup
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    print("=" * 60)
    print("  OpenSAI M1 MVP — End-to-End Demo")
    print(f"  Server: {args.host}:{args.port}")
    print("=" * 60)

    # Connect
    _section("Connect")
    client = OpenSAIClient(host=args.host, port=args.port, timeout=args.timeout)
    try:
        client.connect()
        _ok(f"Connected to {args.host}:{args.port}")
    except Exception as e:
        _fail(f"Failed to connect: {e}")
        return 1

    try:
        # Phase 1: Discovery (RES-1.9 + MON-1.9)
        demo_discovery(client)

        if args.skip_all:
            _info("--skip-all: skipping DAG submission scenarios")
            return 0

        # Phase 2: Cross-device data plane (APP-1.9 + CLU-1.9)
        ok1 = demo_cross_device(client)

        # Phase 3: Collaborative perception (APP-1.9 + CLU-1.9)
        ok2 = True
        if not args.skip_collab:
            ok2 = demo_collab_perception(client)
        else:
            _section("Phase 3: Collaborative Perception")
            _info("Skipped (--skip-collab)")

        # Phase 4: Resource view (RES-1.9)
        demo_resource_view(client)

        # Summary
        _section("Demo Summary")
        passed = sum([1, int(ok1), int(ok2)])
        total = 3 if not args.skip_collab else 2
        print(f"  Scenarios passed: {passed}/{total}")
        if ok1 and ok2:
            print("  Status: ALL PASSED")
            return 0
        else:
            print("  Status: SOME FAILED")
            return 2

    finally:
        client.close()
        _info("Connection closed")


if __name__ == '__main__':
    sys.exit(main())
