"""Writes raw records into the Unity Catalog landing volume.

Layout:

    /Volumes/<catalog>/landing/raw/<dataset>/<k1>=<v1>/<k2>=<v2>/<filename>

One deterministic object per call, at a path derived entirely from its inputs.
A re-run with the same inputs overwrites in place rather than appending, so
replays cannot inflate the landing zone. Bronze stays append-only —
de-duplication happens in dbt staging, where it is visible and testable.

Newline-delimited JSON rather than Parquet: the landing zone is a faithful
record of what the API said, and JSON survives upstream schema drift without a
write-time schema. Parquet is the right call from bronze onward.

This is the Databricks port of the Airflow version, which addressed the same
storage as ``abfss://landing@<account>.dfs.core.windows.net/raw/...`` through
the Azure SDK. The volume is EXTERNAL over that identical container, so the
bytes land in the same place — but Unity Catalog owns the credential, which
removes the service principal, the client secret, and the azure-storage
dependency from this code entirely.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Iterable, Mapping

log = logging.getLogger(__name__)


def _slug(value: str) -> str:
    """Make a string safe for a Hive partition directory.

    Spark reads ``key=value`` directories as columns, and chokes on spaces,
    slashes and encoded characters. "St Kilda" must not become "St%20Kilda".
    """
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower())
    return cleaned.strip("-") or "unknown"


def land_ndjson(
    records: Iterable[dict],
    *,
    dataset: str,
    partitions: Mapping[str, str],
    filename: str,
    landing_root: str,
) -> dict:
    """Write ``records`` as newline-delimited JSON to the landing volume.

    Produces:
        <landing_root>/<dataset>/<k1>=<v1>/<k2>=<v2>/<filename>

    One object per call, at a path derived entirely from its inputs. Two
    consequences worth being deliberate about:

    * A re-run with the same inputs overwrites rather than appends, so replays
      cannot inflate the landing zone.
    * Concurrent for_each iterations never collide, provided the partition
      values differ — which is why page number belongs in ``filename``.

    ``partitions`` must be ordered (a plain dict is, in 3.7+); the directory
    order is the partition column order Auto Loader infers.
    """
    parts = "/".join(f"{k}={_slug(v)}" for k, v in partitions.items())
    directory = f"{landing_root.rstrip('/')}/{dataset}/{parts}"
    path = f"{directory}/{filename}"

    buffer = bytearray()
    count = 0
    for record in records:
        buffer += json.dumps(record, separators=(",", ":"), default=str).encode()
        buffer += b"\n"
        count += 1

    if count == 0:
        # Write nothing rather than an empty object: Auto Loader would have no
        # schema to infer from, and an empty file is indistinguishable from a
        # truncated one.
        log.warning("No records for %s — skipping write", path)
        return {"path": None, "record_count": 0, "bytes": 0}

    # Volumes are exposed as a POSIX filesystem on Databricks compute, so the
    # whole Azure DataLake client the Airflow version needed collapses to this.
    os.makedirs(directory, exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(bytes(buffer))

    log.info("Wrote %s records (%s bytes) to %s", count, len(buffer), path)
    return {
        "path": path,
        "record_count": count,
        "bytes": len(buffer),
    }
