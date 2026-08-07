"""Per-suburb backfill markers, stored beside the landing zone.

    <landing_root>/_state/<dataset>/state=<state>/suburb=<suburb>.json

A marker records that a suburb's full history has been fetched at least once.
It exists so the nightly incremental run can refuse to start on a suburb that
has never been backfilled, rather than quietly giving it one month of history
while its neighbours have twenty — the coverage artefact suburbs.py warns about.

### Why a file and not a control table

The task already holds write access to this volume, so markers need no SQL
connection, no warehouse, and no extra credential — which keeps the ingest path
runnable on any compute, serverless included.

It is also more honest than inferring state from the data. "Some rows exist for
Horsham" is not "Horsham's backfill completed": a backfill that died on page 180
leaves rows behind and would read as done, capping that suburb's history
forever. A marker is written only after every page has landed, so a partial run
leaves none and gets retried.

### Placement

`_state/` sits beside the per-dataset folders rather than inside them. The
pipeline's Auto Loader streams read `<landing_root>/<dataset>/`, so nothing here
is ever mistaken for data — but that safety depends on those load paths staying
dataset-scoped. Point one at the volume root and it would ingest this
bookkeeping.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from realestate2026.ingest.landing import _slug

log = logging.getLogger(__name__)


def marker_path(*, landing_root: str, dataset: str, suburb: str, state: str) -> str:
    return (
        f"{landing_root.rstrip('/')}/_state/{dataset}"
        f"/state={_slug(state)}/suburb={_slug(suburb)}.json"
    )


def read_marker(*, landing_root: str, dataset: str, suburb: str, state: str) -> dict | None:
    """Return the marker, or None when the suburb has never been backfilled."""
    path = marker_path(
        landing_root=landing_root, dataset=dataset, suburb=suburb, state=state
    )
    try:
        with open(path, "rb") as handle:
            return json.loads(handle.read())
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        # A corrupt marker is treated as absent: re-running a backfill is
        # wasteful but safe, whereas trusting an unreadable one is not.
        log.warning("Marker at %s is unreadable — treating as absent", path)
        return None


def write_marker(
    *, landing_root: str, dataset: str, suburb: str, state: str, summary: dict
) -> str:
    """Record a completed backfill. Call only after every page has landed."""
    path = marker_path(
        landing_root=landing_root, dataset=dataset, suburb=suburb, state=state
    )
    payload = {
        **summary,
        "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(json.dumps(payload, indent=2, default=str).encode())

    log.info("Wrote backfill marker %s", path)
    return path
