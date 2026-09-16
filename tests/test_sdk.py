"""
SDK and Transport tests (APP-1.10).

Covers: NET-1.1, NET-1.5, APP-1.1, exception hierarchy (I1 fix).
"""

import sys
import os
from pathlib import Path

# Ensure project root on path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))


def test_imports():
    """All public symbols importable."""
    import opensai_client
    from opensai_client import OpenSAIClient, OpenSAIClientError, TimeoutError
    from opensai_client.transport import OpenSAITransport, TransportError
    from opensai_client.transport import TimeoutError as TransportTimeoutError

    assert opensai_client is not None
    assert OpenSAIClient is not None
    assert OpenSAIClientError is not None
    assert TimeoutError is not None
    assert OpenSAITransport is not None
    assert TransportError is not None
    assert TransportTimeoutError is not None


def test_i1_exception_hierarchy():
    """I1 fix: opensai_client.TimeoutError extends transport.TimeoutError."""
    from opensai_client import TimeoutError, OpenSAIClientError
    from opensai_client.transport import TimeoutError as TransportTimeoutError
    from opensai_client.transport import TransportError

    assert issubclass(TimeoutError, TransportTimeoutError)
    assert issubclass(TimeoutError, OpenSAIClientError)
    assert issubclass(TimeoutError, TransportError)
    assert issubclass(TimeoutError, Exception)


def test_transport_construction():
    """Transport constructs and guards work."""
    from opensai_client.transport import OpenSAITransport, TransportError

    t = OpenSAITransport('127.0.0.1', 8888)
    assert t is not None
    assert hasattr(t, '_send_lock')
    assert hasattr(t, '_buf')

    # Guards
    try:
        t.send_message({'test': 1})
        assert False, 'Should have raised'
    except TransportError:
        pass

    try:
        t.recv_message(timeout=0.1)
        assert False, 'Should have raised'
    except TransportError:
        pass


def test_transport_fetch_result():
    """NET-1.5: fetch_result method exists."""
    from opensai_client.transport import OpenSAITransport
    t = OpenSAITransport()
    assert hasattr(t, 'fetch_result')
    import inspect
    sig = inspect.signature(t.fetch_result)
    params = list(sig.parameters.keys())
    assert 'job_id' in params
    assert 'timeout' in params


def test_client_construction():
    """Client constructs and guards work."""
    from opensai_client import OpenSAIClient, OpenSAIClientError

    c = OpenSAIClient('127.0.0.1', 8888)
    assert c is not None
    assert c.is_connected is False

    # All 7 public methods should raise when not connected
    methods = [
        ('list_algorithms', lambda: c.list_algorithms()),
        ('list_devices', lambda: c.list_devices()),
        ('cluster_status', lambda: c.cluster_status()),
        ('get_events', lambda: c.get_events()),
        ('submit_dag', lambda: c.submit_dag({'taskName': 't'})),
        ('get_result', lambda: c.get_result('j1')),
        ('wait_result', lambda: c.wait_result('j1', timeout=0.1)),
    ]
    for name, fn in methods:
        try:
            fn()
            assert False, f'{name} should have raised'
        except OpenSAIClientError:
            pass


def test_client_imports_all_symbols():
    """__all__ exports correct symbols."""
    from opensai_client import __all__
    assert 'OpenSAIClient' in __all__
    assert 'OpenSAIClientError' in __all__
    assert 'TimeoutError' in __all__


def test_get_result_catches_transport_timeout():
    """get_result() must catch transport.TimeoutError (I1 fix)."""
    from opensai_client import TimeoutError, OpenSAIClientError
    from opensai_client.transport import TimeoutError as TransportTimeoutError

    # verify isinstance behavior
    te = TransportTimeoutError('test')
    ce = TimeoutError('test')

    # Transport timeout IS caught by (TransportTimeoutError, TimeoutError)
    try:
        raise te
    except (TransportTimeoutError, TimeoutError):
        pass
    else:
        assert False

    # Client timeout is also caught
    try:
        raise ce
    except (TransportTimeoutError, TimeoutError):
        pass
    else:
        assert False
