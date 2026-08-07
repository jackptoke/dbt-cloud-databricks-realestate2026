"""Publish the watched suburb list as a task value, for for_each to iterate.

The alternative is pasting the JSON into `inputs:` in the job YAML, which makes
a second copy of the watched set — the one thing suburbs.py exists to prevent.
A suburb added there but not here would be silently unwatched, and the coverage
artefact that produces is invisible in the data.

This way the list has exactly one definition. Add a suburb to suburbs.py,
redeploy, and all three channels pick it up with no YAML change.

Downstream tasks read it as:

    {{tasks.list_suburbs.values.suburbs}}
"""

from __future__ import annotations

import logging
import sys

from realestate2026.ingest.suburbs import SUBURBS

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Importing suburbs.py already ran _validate(), so a malformed entry has
    # failed by now — before any quota is spent.
    from databricks.sdk.runtime import dbutils

    dbutils.jobs.taskValues.set(key="suburbs", value=SUBURBS)
    log.info(
        "Published %s suburbs: %s",
        len(SUBURBS),
        ", ".join(f"{s['suburb']} {s['state']}" for s in SUBURBS),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
