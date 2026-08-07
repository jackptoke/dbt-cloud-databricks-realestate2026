"""HTTP fetching with rate-limit-aware retries.

The job's ``for_each_task.concurrency`` bounds how many suburbs are fetched at
once, which is only indirectly a request rate: actual rate is roughly
``concurrent_iterations / avg_request_seconds``. Response times vary, so the
concurrency cap alone will occasionally overshoot. This module handles the
overshoot.

Retrying here rather than relying on task retries matters: a task retry re-runs
the whole suburb, re-fetching pages that already succeeded and burning more
quota. Retrying in-process re-issues only the request that failed.
"""

from __future__ import annotations

import logging
import random

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
MAX_BACKOFF = 60.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RateLimited(requests.RequestException):
    """429. Carries the server's Retry-After when it supplies one."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class UnexpectedResponse(ValueError):
    """A 2xx response that is not usable JSON.

    Deliberately NOT a RequestException, so tenacity does not retry it. An
    empty body or an HTML error page means the request was wrong — wrong
    endpoint, wrong parameters — and repeating it just burns quota and delays
    the real error by the length of the backoff schedule.
    """


def _wait(retry_state) -> float:
    """Honour Retry-After when present, otherwise exponential backoff.

    Jitter is not decoration: without it, every concurrent iteration that gets
    429'd in the same instant retries in the same instant, reproducing the burst
    that caused the throttle.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        return min(float(retry_after), MAX_BACKOFF)

    base = min(2.0 ** retry_state.attempt_number, MAX_BACKOFF)
    return base * (0.5 + random.random() / 2)


def _log_retry(retry_state) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    log.warning(
        "Request failed (attempt %s): %s — retrying in %.1fs",
        retry_state.attempt_number,
        exc,
        retry_state.next_action.sleep if retry_state.next_action else 0,
    )


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=_wait,
    stop=stop_after_attempt(6),
    before_sleep=_log_retry,
    reraise=True,
)
def get_json(
    url: str,
    *,
    headers: dict | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> dict:
    """GET a URL and return parsed JSON, retrying throttles and 5xx.

    A 4xx other than 429 is not retried — a malformed query or a bad API key
    will fail identically on every attempt, and retrying only wastes quota.
    """
    client = session or requests
    response = client.get(url, headers=headers, timeout=timeout)

    if response.status_code == 429:
        raw = response.headers.get("Retry-After")
        try:
            retry_after = float(raw) if raw is not None else None
        except ValueError:
            # Retry-After may be an HTTP-date rather than seconds. Fall back to
            # backoff rather than trying to parse it.
            retry_after = None
        raise RateLimited(f"429 Too Many Requests for {url}", retry_after)

    if response.status_code in RETRYABLE_STATUS:
        raise requests.HTTPError(
            f"{response.status_code} from {url}", response=response
        )

    response.raise_for_status()

    # A 204, or a 200 with an empty body, is what a wrong endpoint looks like
    # on this API. Without this check the failure surfaces as a bare
    # "JSONDecodeError: Expecting value: line 1 column 1 (char 0)" from deep
    # inside requests, which says nothing about which URL was at fault.
    if response.status_code == 204 or not response.content:
        raise UnexpectedResponse(
            f"{response.status_code} with an empty body from {url}. "
            "Usually the wrong endpoint or a parameter combination the API "
            "treats as 'no content'."
        )

    try:
        return response.json()
    except ValueError as exc:
        content_type = response.headers.get("content-type", "unknown")
        raise UnexpectedResponse(
            f"Response from {url} is not JSON (content-type: {content_type}, "
            f"{len(response.content)} bytes). First 200 chars: "
            f"{response.text[:200]!r}"
        ) from exc
