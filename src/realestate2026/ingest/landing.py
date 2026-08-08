"""Writes raw records into the Unity Catalog landing volume.

Layout:

    /Volumes/<catalog>/landing/raw/<dataset>/<k1>=<v1>/<k2>=<v2>/<filename>

One deterministic object per call, at a path derived entirely from its inputs.
A re-run with the same inputs overwrites in place rather than appending, so
replays cannot inflate the landing zone. Bronze stays append-only —
de-duplication happens in dbt staging, where it is visible and testable.

Overwriting is only half of replay safety, and the half that is easy to
overlook is the other one. Auto Loader tracks files by path, so it ignores a
path it has already seen unless ``cloudFiles.allowOverwrites`` is set: without
it, a re-landed file with CORRECTED content is silently never ingested. The
bronze pipeline sets that option, which is why an overwrite here reaches bronze
at all — and why bronze may hold both versions, leaving staging's
``(listingId, ingest_date)`` dedup to pick the later one.

A shrinking result set is the case overwriting cannot cover on its own: if an
earlier attempt landed ten pages and this one lands three, pages four to ten
are not overwritten, they are simply stale. They carry real listing ids, so no
dedup downstream can tell them from live ones. ``prune_stale_pages`` deletes
them, and the caller runs it once the page loop has completed.

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


PAGE_FILE = re.compile(r"^page=(\d+)\.jsonl$")


def _slug(value: str) -> str:
    """Make a string safe for a Hive partition directory.

    Spark reads ``key=value`` directories as columns, and chokes on spaces,
    slashes and encoded characters. "St Kilda" must not become "St%20Kilda".
    """
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower())
    return cleaned.strip("-") or "unknown"


def partition_dir(
    *, dataset: str, partitions: Mapping[str, str], landing_root: str
) -> str:
    """``<landing_root>/<dataset>/<k1>=<v1>/<k2>=<v2>``.

    ``dataset`` is slugged like the partition values are. It arrives from a CLI
    argument, and while every caller passes one of three literals today, a path
    component that is interpolated raw is one refactor away from escaping the
    landing root entirely.
    """
    parts = "/".join(f"{k}={_slug(v)}" for k, v in partitions.items())
    return f"{landing_root.rstrip('/')}/{_slug(dataset)}/{parts}"


def _write_atomic(path: str, payload: bytes) -> None:
    """Write ``payload`` to ``path``, avoiding a readable half-written file.

    A crash mid-write would otherwise leave truncated NDJSON that Auto Loader is
    perfectly happy to ingest — the last line is a fragment, and the rescued-data
    column absorbs it without anything failing loudly.

    Volumes are FUSE-backed, and rename support there has varied by runtime, so
    a failed rename falls back to writing in place rather than failing the page.
    The fallback is strictly no worse than what this replaces.

    The temp name leads with an underscore on purpose: Spark's input machinery,
    Auto Loader included, skips files whose name starts with ``_`` or ``.``. A
    crash between the write and the rename therefore leaves something the
    pipeline ignores rather than a second copy of the page to ingest.
    """
    directory, _, base = path.rpartition("/")
    tmp = f"{directory}/_tmp.{base}" if directory else f"_tmp.{base}"
    try:
        with open(tmp, "wb") as handle:
            handle.write(payload)
        os.replace(tmp, path)
    except OSError:
        log.warning("Atomic rename unavailable for %s — writing in place", path)
        with open(path, "wb") as handle:
            handle.write(payload)
        try:
            os.remove(tmp)
        except OSError:
            pass


def prune_stale_pages(
    *,
    dataset: str,
    partitions: Mapping[str, str],
    landing_root: str,
    keep_pages: int,
) -> list[str]:
    """Delete ``page=NNNN.jsonl`` files above ``keep_pages`` in this partition.

    These are the leftovers of an earlier, longer run of the same crawl — a
    failed attempt that got further than this one, or a day when the source
    genuinely had more results. Overwriting cannot remove them because this run
    never writes to those paths at all, and they are indistinguishable from live
    data downstream: real listing ids, correct partition, just no longer
    returned by the source.

    Only files this module's own naming produces are touched, and only within
    the one partition directory the caller just wrote.
    """
    directory = partition_dir(
        dataset=dataset, partitions=partitions, landing_root=landing_root
    )
    removed = []
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return removed

    for name in names:
        match = PAGE_FILE.match(name)
        if not match or int(match.group(1)) <= keep_pages:
            continue
        path = f"{directory}/{name}"
        try:
            os.remove(path)
            removed.append(path)
        except OSError:
            # Worth a warning, not a failure: the crawl itself succeeded, and
            # failing it here would discard pages that did land.
            log.warning("Could not remove stale page %s", path)

    if removed:
        log.warning(
            "Removed %s stale page file(s) from %s left by a longer earlier run: %s",
            len(removed), directory, ", ".join(sorted(removed)),
        )
    return removed


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
    directory = partition_dir(
        dataset=dataset, partitions=partitions, landing_root=landing_root
    )
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
    _write_atomic(path, bytes(buffer))

    log.info("Wrote %s records (%s bytes) to %s", count, len(buffer), path)
    return {
        "path": path,
        "record_count": count,
        "bytes": len(buffer),
    }
