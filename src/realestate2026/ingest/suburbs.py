"""The suburbs every channel watches.

Single source of truth for the sold, buy and rent ingest tasks. Add a suburb
here and all three pick it up on the next bundle deploy — the job's for_each
tasks are driven by ``as_json()`` below.

That matters for more than convenience. The conformed dimensions and the
cross-channel metrics assume the channels cover the same ground: gross rental
yield divides annualised rent by median sold price for the same suburb and
property type, so a suburb present in one channel and missing from another
produces a NULL yield rather than a wrong one — but only because the join is
explicit. Comparisons between suburbs are worse: a suburb watched on sold since
January against one added last week will show a difference that is an artefact
of coverage, not of the market.

Kept in code rather than a job parameter deliberately. The watched set is part
of what the data means, and a change to it should be reviewable and
attributable in git rather than made silently in the Jobs UI.

Adding a suburb does NOT backfill it. The next run fetches it from scratch, so
its history starts then; existing suburbs keep theirs. Any suburb-versus-suburb
comparison should check first_seen_date in dim_suburb.
"""

from __future__ import annotations

# Australian state and territory codes, for the sanity check below.
_VALID_STATES = {"VIC", "NSW", "QLD", "WA", "SA", "TAS", "ACT", "NT"}

# Suburb names must match what the API's searchLocation accepts — the display
# form, correctly spaced and capitalised. "Wyndham Vale", not "wyndham-vale".
# The landing writer slugs them for partition paths; do not pre-slug here.
SUBURBS: list[dict[str, str]] = [
    {"suburb": "Ararat", "state": "VIC"},
    {"suburb": "Beaufort", "state": "VIC"},
    {"suburb": "Horsham", "state": "VIC"},
    {"suburb": "Nhill", "state": "VIC"},
    {"suburb": "Stawell", "state": "VIC"},
]


def as_json() -> str:
    """The suburb list as a JSON string, for a for_each task's `inputs`.

    Printed by `python -m realestate2026.ingest.suburbs` so the job YAML can be
    regenerated from the same source of truth the tasks import, rather than
    maintaining a second copy in the bundle.
    """
    import json

    return json.dumps(SUBURBS, separators=(",", ":"))


def _validate() -> None:
    """Fail at import time rather than silently fetching nothing.

    A malformed state code does not error at the API — it returns zero results,
    which looks exactly like a suburb with no listings. Raising here makes the
    mistake obvious when the task starts instead of surfacing weeks later as a
    suburb that mysteriously has no data.
    """
    seen = set()
    for entry in SUBURBS:
        suburb, state = entry.get("suburb"), entry.get("state")
        if not suburb or not state:
            raise ValueError(f"SUBURBS entry missing suburb or state: {entry!r}")
        if state not in _VALID_STATES:
            raise ValueError(
                f"SUBURBS entry {entry!r} has state {state!r}; "
                f"expected one of {sorted(_VALID_STATES)}"
            )
        key = (suburb.strip().lower(), state)
        if key in seen:
            # A duplicate would fetch the same suburb twice, burning quota and
            # landing the second copy over the first.
            raise ValueError(f"SUBURBS contains {suburb}, {state} more than once")
        seen.add(key)


_validate()


if __name__ == "__main__":
    print(as_json())
