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
paths rather than accumulating new ones — and because a completed run prunes
page files above its own page count WITHIN ITS OWN QUERY SCOPE, so a shorter
retry cannot leave the tail of a longer failed attempt behind as live-looking
data.

The scope qualifier is the whole safety property, not a detail: one partition
directory is shared by every query that targets the same suburb and date, and
their page counts are unrelated. Pruning across scopes destroyed 48 pages of
unrepeatable sold history. See ``query_scope`` below and landing.page_filename.
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
from realestate2026.ingest.landing import land_ndjson, page_filename, prune_stale_pages
from realestate2026.ingest.realty_au import (
    assert_locality_resolved,
    build_url,
    extract_listings,
    page_count,
)
from realestate2026.ingest.state import read_marker, write_marker

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://realty-in-au.p.rapidapi.com/properties/list"

# Doubles as the sentinel for "default query" in query_scope, so changing it is
# not a free tuning knob: pages already landed under the bare `page=NNNN.jsonl`
# name were produced at the OLD size, and new runs would still call themselves
# the default scope while computing a different page count from them. Change it
# only alongside a scope tag that distinguishes the two.
PAGE_SIZE = 30

# channel and dataset are separate CLI arguments, and the landing path is keyed
# on dataset while the page count depends on channel. Pairing them by convention
# would mean --channel=rent --dataset=sold_properties writes rent-shaped page
# counts into the sold partition under the default scope, where pruning would
# then treat the sold crawl's pages as stale.
DATASET_FOR_CHANNEL = {
    "buy": "buy_properties",
    "rent": "rent_properties",
    "sold": "sold_properties",
}

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


def query_scope(
    *,
    max_sold_age_months: int | None,
    page_size: int,
    max_pages_override: int | None,
) -> str | None:
    """Name the query shape, for everything that changes the PAGE COUNT.

    Landed pages are tagged with this so two different queries against the same
    suburb and date cannot share a filename namespace — see landing.page_filename
    for why that matters, and what it cost to learn.

    Every input that moves `pages` belongs here, which is all three of these:
    `available` is ceil(totalResultsCount / page_size), and the cap is either
    --max_pages or max_pages_for(page_size). Tagging only max_sold_age_months
    left the same data loss reachable one axis over — `--page_size=100` or
    `--max_pages=10` against a partition holding a default full crawl would
    still have pruned the pages beyond its own shorter count.

    Returns None for the default query, which keeps the bare `page=NNNN.jsonl`
    name and so needs no migration of anything already landed.
    """
    # An explicit --max_pages equal to what would be derived anyway describes
    # the same query, so it must not open a second namespace for it.
    if max_pages_override == max_pages_for(page_size):
        max_pages_override = None

    if (
        max_sold_age_months is None
        and page_size == PAGE_SIZE
        and max_pages_override is None
    ):
        return None

    bits = []
    if max_sold_age_months is not None:
        bits.append(f"m{max_sold_age_months}")
    if page_size != PAGE_SIZE:
        bits.append(f"s{page_size}")
    if max_pages_override is not None:
        bits.append(f"c{max_pages_override}")
    # Must stay within [a-z0-9]+ to match landing.PAGE_FILE.
    return "".join(bits)


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
    expected = DATASET_FOR_CHANNEL.get(channel)
    if expected is not None and dataset != expected:
        raise SystemExit(
            f"--channel={channel} does not go with --dataset={dataset}. The "
            f"landing path is keyed on dataset while the page count comes from "
            f"channel, so a mismatch writes one channel's page counts into "
            f"another's partition, where pruning would treat the resident pages "
            f"as stale. Use --dataset={expected}."
        )

    # Captured before the default is resolved: query_scope needs to know whether
    # the caller asked for a cap, not what the cap ended up being.
    max_pages_override = max_pages
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

    # The query scope, which the partition path does not capture. A full crawl
    # and a one-month crawl of the same suburb on the same day land in the same
    # directory and produce completely different page counts, so they must not
    # share a filename namespace. See landing.page_filename.
    scope = query_scope(
        max_sold_age_months=max_sold_age_months,
        page_size=page_size,
        max_pages_override=max_pages_override,
    )

    def land(payload: dict, page: int) -> dict:
        return land_ndjson(
            extract_listings(payload),
            dataset=dataset,
            partitions=partitions,
            filename=page_filename(page, scope=scope),
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
                "Fetching %s; the remaining %s pages are unreachable BY THIS "
                "QUERY and those listings will be missing. Narrow it with "
                "--max_sold_age_months to get under the cap — a window that "
                "fits returns complete, where this returns the most relevant "
                "%s. (build_url exposes no price parameters, so price-band "
                "slicing is not available.)",
                suburb, state, channel, available, max_pages,
                pages, available - pages, max_pages * page_size,
            )

        for page in range(2, pages + 1):
            try:
                summaries.append(land(get_json(url=url_for(page), session=session), page))
            except Exception:
                # Names the page; the traceback alone does not.
                log.exception("Failed page %s for %s, %s", page, suburb, state)
                raise

    # Reached only when every page landed, which is what makes it safe to treat
    # anything above `pages` IN THIS SCOPE as debris rather than as data still
    # being written. Another scope's pages in the same directory are not this
    # run's to judge.
    stale = prune_stale_pages(
        dataset=dataset,
        partitions=partitions,
        landing_root=landing_root,
        keep_pages=pages,
        scope=scope,
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
    # "Every page landed" is necessary but not sufficient, and the two
    # insufficient cases are NOT alike.
    #
    # A TRUNCATED unsliced run is certified, with the truncation recorded. Be
    # precise about why, because the obvious reason is wrong: it is NOT that the
    # rest is unreachable. sortType is `relevance` (realty_au.build_url), so an
    # unsliced Ararat crawl returns the 1,500 most RELEVANT of ~4,260 — and a
    # narrowed query returns a different, complete window that reaches records
    # this run never saw. A slice sequence really can accumulate more than this.
    #
    # It is certified anyway because refusing stranded the suburbs that need it
    # most. Horsham (~7,890 regional sales), Nhill (~2,160) and Ararat (~4,260)
    # are all over the 1,500 ceiling, so for three of five watched suburbs every
    # unsliced run truncates and every narrowed run is a slice: no route to a
    # marker existed at all. ingest_sold runs with require_backfill_marker, so
    # those three would have failed every night and their sold history would
    # never have landed. A knowingly-partial certification beats no ingest.
    #
    # A SLICED run writes nothing. It cannot certify a suburb — that would hand
    # the coverage guarantee to a suburb holding one month of history, which is
    # the artefact the marker exists to prevent — and it deliberately does not
    # record what it covered either.
    #
    # That recording existed briefly and was removed. It kept needing answers to
    # questions only a consumer can settle (does "covered" mean ever-covered or
    # last-observed? do nested windows subsume? does re-certification reset?),
    # and there is no consumer: ROADMAP.md still lists exposing `_state/` as a
    # dbt source as future work. A schema guessed ahead of its reader is a
    # schema that gets guessed differently every time someone looks at it.
    #
    # It was also a second, worse copy of something already in the warehouse.
    # Page files are scope-tagged and `_source_file` carries the landing path
    # through bronze into int_listings_unioned, so which windows were crawled,
    # when, and how much each returned is already answerable:
    #
    #     select regexp_extract(_source_file, '/([a-z0-9]+)\\.page=', 1) as scope,
    #            crawled_on, count(*)
    #     from int_listings_unioned where channel = 'sold' group by all
    #
    # — from data that is loaded, tested, and has consumers today.
    if write_backfill_marker:
        if max_sold_age_months is not None:
            log.warning(
                "NOT writing a backfill marker for %s, %s (%s): this run covered "
                "only the last %s month(s). A slice cannot certify a suburb — do "
                "one unsliced run to earn the marker, then slices to fill in. "
                "What this run covered is recoverable from the scope tag on its "
                "landed page files.",
                suburb, state, dataset, max_sold_age_months,
            )
        else:
            if truncated:
                log.warning(
                    "Certifying %s, %s (%s) as backfilled DESPITE truncation: "
                    "the source reported %s pages and serves only %s, so this "
                    "run holds roughly %s%% of the region's sales — the most "
                    "RELEVANT ones, not the most recent. Narrower queries would "
                    "reach records this did not; certifying anyway because "
                    "refusing leaves the suburb with no sold ingest at all. The "
                    "marker records truncated=true and pages_available=%s.",
                    suburb, state, dataset, available, max_pages,
                    round(100 * max_pages / available), available,
                )
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
