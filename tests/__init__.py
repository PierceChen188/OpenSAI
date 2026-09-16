"""
Comprehensive test suite for OpenSAI M1 MVP (APP-1.10).

Run all tests:
    python3 -m pytest tests/ -v
    python3 tests/run_all.py

Coverage:
    - SDK imports and construction
    - Transport layer (NET-1.1 + NET-1.5)
    - Schema validation (APP-1.3)
    - Client validator (APP-1.4)
    - Async wait (APP-1.5)
    - AgentSkill packaging (APP-1.7)
    - CLI parsing (APP-1.8)
    - Agent Control (APP-1.6)
    - DAG conversion (to_dagdata)
    - Algorithm whitelist rejection
    - Cycle detection
    - JSON I/O (file and stdin paths)
    - Exception hierarchy (I1 fix)
"""
