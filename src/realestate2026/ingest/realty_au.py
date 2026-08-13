"""Payload handling for the realty-in-au RapidAPI endpoint.

Kept apart from landing.py because everything here is a
statement about one vendor's response shape. When the vendor changes, this is
the only file that should need touching.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator

log = logging.getLogger(__name__)

PAGE_SIZE = 30


class PayloadShapeError(ValueError):
    """The response did not look like what this module expects.

    Raised rather than returning empty: a silently empty extraction produces a
    green task and an empty landing zone, which is far more expensive to notice
    than a failed task.
    """


def extract_listings(payload: dict) -> Iterator[dict]:
    """Yield individual listings from a search response.

    Listings are nested under ``tieredResults[*].results[*]`` — the top level
    is an envelope of query metadata, and tiers group exact-suburb matches
    (tier 1) separately from surrounding-suburb matches (tier 2+) when
    ``surroundingSuburbs=true``.

    The tier is attached as ``_tier`` because it is a real signal — a "sold in
    Beaufort" analysis usually means tier 1 only — and it exists nowhere on the
    listing itself, so flattening without it loses information irrecoverably.
    """
    if "tieredResults" not in payload:
        raise PayloadShapeError(
            f"No 'tieredResults' in response. Top-level keys: {sorted(payload)[:12]}"
        )

    total = 0
    for tier_block in payload["tieredResults"]:
        tier = tier_block.get("tier")
        results = tier_block.get("results") or []
        for listing in results:
            total += 1
            yield {**listing, "_tier": tier}

    if total == 0:
        # A legitimately empty page is possible past the end of a result set,
        # so this is a warning rather than an error. Consistently empty pages
        # mean the page count is wrong.
        log.warning("Response contained tieredResults but no listings")


def assert_locality_resolved(payload: dict, suburb: str, state: str) -> None:
    """Fail when the API did not recognise the requested suburb.

    An unresolvable location is NOT an error upstream. The API drops the
    location filter and returns an unfiltered national search, which looks like
    a wildly successful query:

        Underbank, VIC  ->  resolvedLocalities: []   totalResultsCount: 7,829,597
        Beaufort,  VIC  ->  resolvedLocalities: [{display: "Beaufort, VIC 3373",
                                                  precision: "suburb"}]   828

    Left unchecked, 7.8 million Australia-wide listings land under
    suburb=underbank and silently poison every suburb-level metric,
    dim_property and the yield mart. Nothing downstream could detect it —
    the rows are individually valid.

    So: a populated resolvedLocalities is the contract, and this is the only
    place that can enforce it.
    """
    resolved = payload.get("resolvedLocalities") or []
    if not resolved:
        raise PayloadShapeError(
            f"{suburb!r}, {state} did not resolve to a locality. The API "
            f"returned {payload.get('totalResultsCount')} UNFILTERED results — "
            "it silently drops the location filter rather than erroring. "
            "Check the suburb is gazetted (estates and new developments "
            "often are not)."
        )


def page_count(payload: dict, page_size: int = PAGE_SIZE) -> int:
    """Total pages implied by the envelope's result count."""
    total = payload.get("totalResultsCount")
    if total is None:
        raise PayloadShapeError(
            f"No 'totalResultsCount' in response. Top-level keys: {sorted(payload)[:12]}"
        )
    return math.ceil(int(total) / page_size)


def build_url(
    base_url: str,
    *,
    suburb: str,
    state: str,
    page: int,
    channel: str = "sold",
    page_size: int = PAGE_SIZE,
    surrounding_suburbs: bool = False,
    max_sold_age_months: int | None = None,
) -> str:
    """Build a search URL.

    FULL PARAMETER REFERENCE: see API_PARAMETERS.md beside this file. It records
    every parameter the vendor documents, which of them actually work on the
    sold channel, and — just as usefully — the ~25 spellings that are silently
    ignored, so they are not retested. Read it before adding a parameter here.

    Centralised so the page-1 request and the mapped page requests cannot
    drift apart — differing query strings would silently return differently
    ordered or filtered result sets.

    ``surrounding_suburbs`` defaults to False so the query does not add tier-2
    results on top. With it on, roughly a quarter of the Beaufort results were
    neighbouring suburbs — listings that a query for those suburbs would return
    anyway, so the extra requests re-download data against a limited quota.

    NOTE: this does NOT make the query suburb-scoped. ``type=region`` below
    means the search resolves to a REGION, and every locality in it comes back
    as a tier-1 match. Measured on the first crawl:

        horsham   ->  25 suburbs, 5,785 of 7,890 actually in Horsham
        nhill     ->  14 suburbs,   890 of 2,160 actually in Nhill  (41%)
        beaufort  ->   1 suburb,    828 of   828                   (100%)

    That matters because the 1,500-result ceiling applies per query, so for
    Nhill 59% of the budget is spent on neighbours.

    WHAT CONTROLS THE SCOPE, verified 2026-08-13: not ``type`` and not
    ``searchLocationSubtext`` — both are ignored on their own. It is the
    ``searchLocation`` STRING:

        "Horsham, VIC"       -> "Horsham - Greater Region, VIC"   7,892
        "Horsham, VIC 3400"  -> "Horsham, VIC 3400"               6,873
        "Natimuk, VIC"       -> "Natimuk, VIC 3409"                 115

    A locality sharing its name with a region resolves to the REGION unless the
    postcode is supplied; smaller localities resolve to themselves either way.
    Appending the postcode therefore gives each locality its own 1,500 budget
    instead of 32 sharing one. Not done here yet because it changes the landing
    path namespace — see query_scope in fetch_suburb.py.

    ``max_sold_age_months`` restricts the sold channel to recent sales. Units
    are MONTHS, verified against the API:

        Beaufort      828 results ->  7 with maxSoldAge=1  (2026-07-06..07-31)
        Wyndham Vale 9371 results -> 47 with maxSoldAge=1

    That is the difference between a 313-page daily sweep and a 2-page one.
    Leave it None for a full historical backfill. It has no effect on the buy
    channel, where every listing is current by definition.

    Note the API silently ignores unrecognised parameters — `maxAge` and
    `dateFrom` both return the unfiltered count — so a typo here degrades to a
    full fetch rather than an error. Any change to this name should be checked
    against a known result count.

    There is NO lower bound on sold date. `minSoldAge`, `soldAgeFrom`,
    `dateSoldFrom`, `soldFrom` and `minAge` were all tested and ignored, so
    maxSoldAge only ever gives cumulative windows from today — it cannot reach
    past the point where the running total hits the 1,500 ceiling.

    ``sortType`` is the lever that can. Each ordering is a DIFFERENT 1,500-row
    window into the same set: for Horsham 3400 `relevance` opens at 2026-07
    while `sold-price-asc` opens at 2008-10, so a union across orderings reaches
    records no single query returns. Valid values are in API_PARAMETERS.md. A
    sort never changes totalResultsCount, so it must be verified by comparing
    the first listing id — checking the count is how it was wrongly written off.
    """
    from urllib.parse import urlencode

    params = {
        "page": page,
        "pageSize": page_size,
        "sortType": "relevance",
        "channel": channel,
        "surroundingSuburbs": "true" if surrounding_suburbs else "false",
        "searchLocation": f"{suburb}, {state}",
        "searchLocationSubtext": "Region",
        "type": "region",
        "ex-under-contract": "false",
    }

    # Added only when set, so a full backfill sends no filter at all rather
    # than a sentinel the API might interpret.
    if max_sold_age_months is not None:
        params["maxSoldAge"] = str(max_sold_age_months)

    return f"{base_url.rstrip('?')}?{urlencode(params)}"
