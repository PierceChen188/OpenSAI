"""
Async wait utilities for OpenSAI DAG results (APP-1.5).

Provides polling-based result retrieval with status tracking, designed for
Agent tool timeout models. Wraps the existing OpenSAIClient.get_result()
and wait_result() with structured status responses.

Usage:
    from opensai_client.wait import ResultWaiter

    waiter = ResultWaiter(client)
    status = waiter.get_result(job_id)
    # {'status': 'running'|'completed'|'timeout', 'result': dict|None}

    result = waiter.wait_result(job_id, timeout=60)
    # Blocks until completion or timeout
"""

__all__ = ['ResultWaiter', 'ResultStatus']

import time
import logging

log = logging.getLogger(__name__)


class ResultStatus:
    """Canonical status values for DAG job results."""
    RUNNING = 'running'
    COMPLETED = 'completed'
    TIMEOUT = 'timeout'
    ERROR = 'error'
    UNKNOWN = 'unknown'


class ResultWaiter:
    """High-level result waiting with structured status tracking.

    Parameters
    ----------
    client : OpenSAIClient
        A connected OpenSAIClient instance.
    poll_interval : float
        Seconds between polls when using get_result (default 0.5).
    """

    def __init__(self, client, poll_interval: float = 0.5):
        self._client = client
        self._poll_interval = poll_interval

    # ── non-blocking status query ──────────────────────────────────────

    def get_result(self, job_id: str) -> dict:
        """Non-blocking result query with status.

        Parameters
        ----------
        job_id : str
            Job identifier returned by submit_dag().

        Returns
        -------
        dict
            {'status': str, 'result': dict|None}
            - status = 'completed' when result is available
            - status = 'running' when job is still pending
            - status = 'timeout' when the poll timed out
        """
        try:
            result = self._client.get_result(job_id)
        except Exception as e:
            log.warning('get_result error for job %s: %s', job_id, e)
            return {'status': ResultStatus.ERROR, 'result': None, 'error': str(e)}

        if result is not None:
            return {'status': ResultStatus.COMPLETED, 'result': result}
        return {'status': ResultStatus.RUNNING, 'result': None}

    # ── blocking wait ──────────────────────────────────────────────────

    def wait_result(self, job_id: str, timeout: float = 60) -> dict:
        """Block until the DAG job completes or times out.

        Uses a polling loop with the configured poll_interval to avoid
        blocking the Agent's main event loop for too long on any single
        recv call. Fits the Agent tool timeout model: the Agent can set
        ``timeout`` based on its own deadline and get a structured response.

        Parameters
        ----------
        job_id : str
            Job identifier returned by submit_dag().
        timeout : float
            Maximum total seconds to wait (default 60).

        Returns
        -------
        dict
            {'status': str, 'result': dict|None}
        """
        from opensai_client import TimeoutError as ClientTimeoutError
        from opensai_client.transport import TimeoutError as TransportTimeoutError

        deadline = time.monotonic() + timeout

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {'status': ResultStatus.TIMEOUT, 'result': None}

            try:
                # Use the client's get_result with a short timeout
                result = self._client.get_result(job_id)
            except (ClientTimeoutError, TransportTimeoutError):
                # Internal poll timeout — keep waiting if total deadline not exceeded
                time.sleep(self._poll_interval)
                continue
            except Exception as e:
                log.warning('wait_result error for job %s: %s', job_id, e)
                return {'status': ResultStatus.ERROR, 'result': None, 'error': str(e)}

            if result is not None:
                return {'status': ResultStatus.COMPLETED, 'result': result}

            # No result yet — sleep and retry
            time.sleep(self._poll_interval)

    # ── convenience ────────────────────────────────────────────────────

    def poll_until(self, job_id: str, timeout: float = 60,
                   on_progress: callable = None) -> dict:
        """Poll with optional progress callback.

        Parameters
        ----------
        job_id : str
            Job identifier.
        timeout : float
            Maximum seconds to wait.
        on_progress : callable, optional
            Called on each poll with (elapsed: float, remaining: float).

        Returns
        -------
        dict
            {'status': str, 'result': dict|None}
        """
        deadline = time.monotonic() + timeout
        started = time.monotonic()

        while True:
            elapsed = time.monotonic() - started
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                return {'status': ResultStatus.TIMEOUT, 'result': None}

            status = self.get_result(job_id)
            if status['status'] in (ResultStatus.COMPLETED, ResultStatus.ERROR):
                return status

            if on_progress:
                on_progress(elapsed, remaining)

            time.sleep(self._poll_interval)
