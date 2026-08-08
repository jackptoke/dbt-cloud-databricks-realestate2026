"""Fetch every page for one suburb/channel and land it as NDJSON.

This is the entry point each ``for_each_task`` iteration runs. One invocation
handles one suburb end to end: page 1 (which carries the result count), then
pages 2..N sequentially over a shared connection.

### Why this is one task rather than three

The Airflow version split this across ``get_first_page``, ``chunk_pages`` and a
mapped ``get_pages_chunk`` with CHUNK_SIZE=25. Both of those existed to work
around Airflow, not the problem: ``expand()`` caps at max_map_length (1024), and
~2s of scheduling overhead per task made one-task-per-page cost more in
orchestration than in fetching.

Neither applies here. A for_each iteration is a process, so pages loop in
memory with no per-page overhead at all, and the requests.Session that the
chunked version introduced to amortise TLS handshakes now covers the whole
suburb rather than 25 pages of it.

### Partial failure

A run that fails partway has already landed some pages. That is safe because
landing paths are deterministic and overwritten — a retry re-lands the same
paths rather than accumulating new ones — and because a completed run prunes any
page files above its own page count, so a shorter retry cannot leave the tail of
a longer failed attempt behind as live-looking data. See landing.py.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from datetime import date, datetime, timezone

import requests

from realestate2026.ingest.http import get_json
from realestate2026.ingest.landing import land_ndjson, prune_stale_pages
from realestate2026.ingest.realty_au import (
    assert_locality_resolved,
    build_url,
    extract_listings,
    page_count,
)
from realestate2026.ingest.state import read_marker, write_marker

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://realty-in-au.p.rapidapi.com/properties/list"
PAGE_SIZE = 30

# The API serves at most 1500 results for a query, then keeps returning
# listings from within that same window. Requesting beyond it costs quota and
# lands duplicates: a single sold listing was observed 214 times across 263
# pages of Horsham. Capping makes the ceiling explicit; the WARNING below makes
# the resulting truncation visible.
#
# The ceiling is a RESULT count, so the page cap has to be derived from the page
# size rather than written down. Hardcoding 50 was only correct at the default
# page_size of 30: at --page_size=10 it would allow 500 results and silently
# treat the other 1000 as absent, reporting no truncation at all.
MAX_RESULTS = 1500


def max_pages_for(page_size: int) -> int:
    return math.ceil(MAX_RESULTS / page_size)


def fetch_suburb(
    *,
    suburb: str,
    state: str,
    channel: str,
    dataset: str,
    landing_root: str,
    ingest_date: date,
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
    page_size: int = PAGE_SIZE,
    max_sold_age_months: int | None = None,
    max_pages: int | None = None,
    require_backfill_marker: bool = False,
    write_backfill_marker: bool = False,
) -> dict:
    max_pages = max_pages or max_pages_for(page_size)

    if require_backfill_marker and not read_marker(
        landing_root=landing_root, dataset=dataset, suburb=suburb, state=state
    ):
        # Failing beats proceeding. An incremental run against a never-
        # backfilled suburb succeeds and lands one month of history, which is
        # indistinguishable downstream from a suburb that genuinely has one
        # month of sales — and permanently skews any suburb-to-suburb
        # comparison. See suburbs.py on coverage artefacts.
        raise SystemExit(
            f"{suburb}, {state} ({dataset}) has no backfill marker: its full "
            "history has never been fetched, so an incremental run would give "
            "it a fraction of the history its neighbours have. Run the "
            "realestate_backfill job for this suburb first."
        )

    headers = {
        "x-rapidapi-host": "realty-in-au.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }

    def url_for(page: int) -> str:
        return build_url(
            base_url=base_url,
            suburb=suburb,
            state=state,
            page=page,
            channel=channel,
            page_size=page_size,
            surrounding_suburbs=False,
            max_sold_age_months=max_sold_age_months,
        )

    partitions = {
        "ingest_date": ingest_date.isoformat(),
        "state": state,
        "suburb": suburb,
    }

    def land(payload: dict, page: int) -> dict:
        return land_ndjson(
            extract_listings(payload),
            dataset=dataset,
            partitions=partitions,
            filename=f"page={page:04d}.jsonl",
            landing_root=landing_root,
        )

    summaries = []
    with requests.Session() as session:
        session.headers.update(headers)

        # Page 1 does double duty: it is real data that must be landed, and it
        # carries totalResultsCount. Splitting those apart would cost an extra
        # request against a rate-limited API for nothing.
        payload = get_json(url=url_for(1), session=session)

        # Before landing anything: an unresolvable suburb returns an unfiltered
        # national search rather than an error.
        assert_locality_resolved(payload, suburb, state)

        available = page_count(payload=payload, page_size=page_size)
        summaries.append(land(payload, 1))

        pages = min(available, max_pages)
        if available > max_pages:
            log.warning(
                "%s, %s (%s): source reports %s pages but serves at most %s. "
                "Fetching %s; the remaining %s pages are UNREACHABLE and those "
                "listings will be missing. Narrow the query "
                "(--max_sold_age_months, or slice by price band) to get under "
                "the cap.",
                suburb, state, channel, available, max_pages,
                pages, available - pages,
            )

        for page in range(2, pages + 1):
            try:
                summaries.append(land(get_json(url=url_for(page), session=session), page))
            except Exception:
                # Names the page; the traceback alone does not.
                log.exception("Failed page %s for %s, %s", page, suburb, state)
                raise

    # Reached only when every page landed, which is what makes it safe to treat
    # anything above `pages` as debris rather than as data still being written.
    stale = prune_stale_pages(
        dataset=dataset,
        partitions=partitions,
        landing_root=landing_root,
        keep_pages=pages,
    )

    # A page with no records writes no file, so counting summaries would report
    # pages that do not exist — including "1 page" for a suburb that landed
    # nothing at all.
    pages_landed = sum(1 for s in summaries if s["path"])
    records = sum(s["record_count"] for s in summaries)
    truncated = available > max_pages

    log.info(
        "%s, %s (%s): landed %s pages, %s records",
        suburb, state, channel, pages_landed, records,
    )
    if not records:
        # Not an error. A suburb can legitimately have nothing on the market
        # tonight, and the sold channel with max_sold_age_months=1 is expected
        # to come back empty for small suburbs in a quiet month.
        log.info(
            "%s, %s (%s): no listings — the source resolved the locality and "
            "returned nothing, which is a valid result, not a failure.",
            suburb, state, channel,
        )

    result = {
        "suburb": suburb,
        "state": state,
        "channel": channel,
        "pages_landed": pages_landed,
        "pages_fetched": len(summaries),
        "pages_available": available,
        "stale_pages_removed": len(stale),
        "record_count": records,
        "truncated": truncated,
        "max_sold_age_months": max_sold_age_months,
    }

    # Written last, and only on the path where every page landed — an
    # exception above leaves no marker, so a partial backfill is retried
    # rather than mistaken for a complete one.
    #
    # "Every page landed" is necessary but not sufficient. The marker's whole
    # claim is that this suburb's FULL history has been fetched, and two
    # successful runs cannot honestly make that claim: one that stopped at the
    # page cap, and one that asked for a slice of history in the first place.
    # The backfill job's own instructions recommend slicing with
    # max_sold_age_months to get under the cap, so certifying a slice would
    # quietly grant the coverage guarantee to suburbs that hold a fraction of
    # their history — the exact artefact the marker exists to prevent, made
    # invisible because the nightly run would then accept them.
    if write_backfill_marker:
        if truncated:
            log.warning(
                "NOT writing a backfill marker for %s, %s (%s): the source "
                "reported %s pages and only %s are reachable, so this run "
                "cannot have fetched the full history. Narrow the query and "
                "re-run.",
                suburb, state, dataset, available, max_pages,
            )
        elif max_sold_age_months is not None:
            log.warning(
                "NOT writing a backfill marker for %s, %s (%s): this run "
                "covered only the last %s month(s). A marker certifies full "
                "history; run without --max_sold_age_months to earn one.",
                suburb, state, dataset, max_sold_age_months,
            )
        else:
            write_marker(
                landing_root=landing_root,
                dataset=dataset,
                suburb=suburb,
                state=state,
                summary=result,
            )

    return result


def _optional_int(value: str) -> int | None:
    """Parse a job parameter that may legitimately be empty.

    Databricks renders parameters as ``--name=value``, so an unset job
    parameter arrives as ``--max_sold_age_months=`` rather than as an absent
    flag. argparse's type=int rejects that outright, which would fail every
    full backfill.

    Note the underscores throughout this CLI: job-level ``parameters`` are
    appended automatically using the parameter's own name, and job parameter
    names cannot contain hyphens. A hyphenated flag here would be unreachable
    from a job parameter.
    """
    value = (value or "").strip()
    return int(value) if value else None


def _flag(value: str) -> bool:
    """Parse a boolean job parameter.

    store_true cannot be used for the same reason: named_parameters always
    emits ``--name=``, and argparse refuses an explicit value for a flag that
    takes none. So these are real options with truthy values.
    """
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _api_key(scope: str, key: str) -> str:
    """The RapidAPI key, from the environment or the secret scope.

    Environment first so local runs and tests need nothing but an env var.
    dbutils second because spark_env_vars does not exist on serverless compute
    — without this fallback, choosing serverless for the ingest tasks would
    mean no way to reach the secret at all.
    """
    from_env = os.environ.get("REALESTATE_API_KEY", "").strip()
    if from_env:
        return from_env

    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # noqa: BLE001
        # Not just ImportError. Off-cluster, importing the runtime constructs a
        # Config() and authenticates, so a stale local profile raises ValueError
        # from deep inside the SDK — which is a confusing way to be told "there
        # is no key here". Either way the answer is the same: no key.
        return ""

    try:
        return (dbutils.secrets.get(scope=scope, key=key) or "").strip()
    except Exception:  # noqa: BLE001 - any failure here means "no key", and
        # the caller's error message is more useful than this one's traceback.
        log.warning("Could not read %s/%s from the secret scope", scope, key)
        return ""


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suburb", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--channel", required=True, choices=["buy", "rent", "sold"])
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["buy_properties", "rent_properties", "sold_properties"],
        help="Landing folder. Constrained because it becomes a path component.",
    )
    parser.add_argument("--landing_root", required=True, help="/Volumes/<catalog>/landing/raw")
    parser.add_argument(
        "--ingest_date",
        default=None,
        help=(
            "Partition date (YYYY-MM-DD). Pass {{job.start_time.iso_date}} from the "
            "job so the partition matches the schedule's day, not the cluster's UTC "
            "clock — an 11pm Melbourne run is already tomorrow in UTC."
        ),
    )
    parser.add_argument("--base_url", default=DEFAULT_BASE_URL)
    parser.add_argument("--page_size", type=int, default=PAGE_SIZE)
    parser.add_argument(
        "--max_pages",
        type=_optional_int,
        default=None,
        help=(
            "Override the page cap. Empty derives it from --page_size and the "
            f"source's {MAX_RESULTS}-result ceiling."
        ),
    )
    parser.add_argument(
        "--max_sold_age_months",
        type=_optional_int,
        default=None,
        help="Sold channel only. 1 for a daily sweep; empty for a full backfill.",
    )
    parser.add_argument(
        "--require_backfill_marker",
        type=_flag,
        default=False,
        help="Fail if this suburb has never been backfilled. For the nightly run.",
    )
    parser.add_argument(
        "--write_backfill_marker",
        type=_flag,
        default=False,
        help="Record a completed backfill on success. For the backfill job.",
    )
    parser.add_argument(
        "--secret_scope",
        default="realestate",
        help="Scope holding the API key, read when REALESTATE_API_KEY is unset.",
    )
    parser.add_argument("--secret_key", default="rapidapi_key")
    args = parser.parse_args(argv)

    api_key = _api_key(args.secret_scope, args.secret_key)
    if not api_key:
        # Deliberately not a parameter: task parameters are rendered in the run
        # UI and the event log, so a key passed that way is a key on screen.
        parser.error(
            "No API key. Set REALESTATE_API_KEY (spark_env_vars on a classic "
            f"cluster) or grant this task READ on secret scope "
            f"{args.secret_scope!r}. Never pass it as a task parameter."
        )

    if args.ingest_date:
        ingest_date = datetime.strptime(args.ingest_date, "%Y-%m-%d").date()
    else:
        ingest_date = datetime.now(timezone.utc).date()
        log.warning(
            "No --ingest-date given; defaulting to %s (UTC). Pass it explicitly "
            "from the job to avoid a date that disagrees with the schedule.",
            ingest_date,
        )

    result = fetch_suburb(
        suburb=args.suburb,
        state=args.state,
        channel=args.channel,
        dataset=args.dataset,
        landing_root=args.landing_root,
        ingest_date=ingest_date,
        api_key=api_key,
        base_url=args.base_url,
        page_size=args.page_size,
        max_sold_age_months=args.max_sold_age_months,
        max_pages=args.max_pages,
        require_backfill_marker=args.require_backfill_marker,
        write_backfill_marker=args.write_backfill_marker,
    )

    # Success is "the crawl completed", not "the crawl found something". Exiting
    # non-zero on an empty suburb failed the whole for_each task, and since the
    # three channels are chained one behind the other, a single quiet suburb in
    # buy or rent SKIPPED the sold channel for that night — losing the only
    # channel whose history cannot be re-fetched later. Genuine failures still
    # raise, and an exception is still a non-zero exit.
    log.info("Result: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
