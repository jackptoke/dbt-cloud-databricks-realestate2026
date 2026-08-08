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
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
)

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
MAX_BACKOFF = 60.0
# A Retry-After longer than this is a quota window, not a burst — waiting it out
# would hold the cluster for the length of the pause and still probably fail.
# Give up immediately instead, so the error names the real problem.
MAX_RETRY_AFTER = 300.0
# Per-wait caps do not bound the total. Five waits at MAX_RETRY_AFTER is 1500s
# on a single page — 83% of the for_each task's 1800s timeout, spent before the
# crawl is killed mid-suburb.
#
# stop_after_delay is checked when DECIDING to retry, so the real ceiling is
# this budget plus one final wait (<=MAX_RETRY_AFTER) plus one final request
# (<=DEFAULT_TIMEOUT): about 930s worst case, not 600. Still comfortably inside
# the task timeout, which is the point — a persistently throttled request gives
# up while the task still has time to fail cleanly and be retried by the job.
MAX_TOTAL_RETRY_SECONDS = 600.0
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


def _is_retryable(exc: BaseException) -> bool:
    """Decide retryability by what actually failed, not by exception ancestry.

    ``retry_if_exception_type(RequestException)`` looks right and is wrong:
    ``raise_for_status`` raises ``HTTPError``, which IS a ``RequestException``,
    so every 401, 403 and 404 was retried six times with backoff — the exact
    behaviour the docstring below promises it avoids. A bad key or an exhausted
    plan would burn six requests per page and spend minutes in backoff before
    reporting a failure that was certain on the first attempt.
    """
    if isinstance(exc, RateLimited):
        # A throttle that asks us to disappear for longer than we are willing
        # to wait is not worth retrying — see MAX_RETRY_AFTER.
        return exc.retry_after is None or exc.retry_after <= MAX_RETRY_AFTER

    if isinstance(exc, requests.HTTPError):
        response = exc.response
        return response is not None and response.status_code in RETRYABLE_STATUS

    # Transport-level failures: the request never got a verdict, so repeating
    # it is meaningful. Everything else (MissingSchema, InvalidURL, TooManyRedirects)
    # is a defect in how we built the request and will fail identically forever.
    return isinstance(
        exc,
        (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError),
    )


def _wait(retry_state) -> float:
    """Honour Retry-After when present, otherwise exponential backoff.

    Jitter is not decoration: without it, every concurrent iteration that gets
    429'd in the same instant retries in the same instant, reproducing the burst
    that caused the throttle.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        # Capped at MAX_RETRY_AFTER, not MAX_BACKOFF: a server asking for 120s
        # means 120s, and sleeping 60 instead just earns a second 429. Anything
        # above the cap never reaches here — _is_retryable rejects it outright.
        return min(float(retry_after), MAX_RETRY_AFTER)

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
    retry=retry_if_exception(_is_retryable),
    wait=_wait,
    stop=stop_after_attempt(6) | stop_after_delay(MAX_TOTAL_RETRY_SECONDS),
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
