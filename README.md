# realestate2026

An end-to-end lakehouse for Australian residential property listings: scraped
from the realty-in-au API, landed as raw JSON, and modelled into a star schema.

Everything is one Databricks Asset Bundle — ingestion, the declarative pipeline,
the dbt project, and the jobs that orchestrate them.

```text
realty-in-au API
      │  python_wheel_task, one for_each iteration per suburb
      ▼
/Volumes/<catalog>/landing/raw/<dataset>/ingest_date=/state=/suburb=/page=NNNN.jsonl
      │  Lakeflow Declarative Pipeline, Auto Loader
      ▼
<catalog>.bronze.{buy,rent,sold}_properties      streaming tables, append-only
      │  dbt: staging → intermediate
      ▼
<catalog>.silver.stg_* / int_*                   deduplicated, typed, conformed
      │  dbt: marts
      ▼
<catalog>.gold.{dim_*, bridge_*, fct_*}          star schema
```

Three jobs, deliberately decoupled:

| job | trigger | does |
| --- | --- | --- |
| `realestate_ingest` | 23:00 Australia/Melbourne | fetches all three channels for every watched suburb |
| `realestate_medallion` | file arrival in the landing volume | bronze via Auto Loader, then silver and gold via dbt |
| `realestate_backfill` | manual, per suburb | one suburb's full sold history; writes the backfill marker |

Splitting ingestion from transformation means **anything** that lands files
feeds bronze — the nightly run and ad-hoc backfills alike — with no wiring
between the jobs.

---

## Repository layout

```text
databricks.yml                  the single bundle definition
resources/
  realestate2026_etl.pipeline.yml   Auto Loader: landing → bronze
  realestate_ingest.job.yml         nightly 23:00: API → landing
  realestate_medallion.job.yml      on file arrival: bronze → silver → gold
  realestate_backfill.job.yml       manual: one suburb's full sold history
src/realestate2026/
  ingest/                       API client, landing writer, CLI entry points
transform/                      the dbt project
  models/staging/               1:1 with bronze; dedup, cast, rename, parse
  models/intermediate/          union across channels, explode arrays
  models/marts/                 dimensions, bridges, facts
  macros/generate_schema_name.sql
tests/  fixtures/               pytest scaffolding for the wheel
```

---

## Environments

Separated by **catalog**, both in the same workspace:

| | dev | prod |
| --- | --- | --- |
| catalog | `realestate` | `realestate_prod` |
| landing | `/Volumes/realestate/landing/raw` | `/Volumes/realestate_prod/landing/raw` |
| schemas | `bronze` / `silver` / `gold` in each | |

The schema axis is already spent on the medallion layers, so environments had
nowhere to go but the catalog axis. This keeps `silver` literally `silver` in
both environments rather than `dbt_toke_silver` — see `generate_schema_name`
below.

Both landing volumes are EXTERNAL over one ADLS container
(`abfss://landing@realestatestorage2026.dfs.core.windows.net/`), at prefixes
`raw` and `raw-prod`. The container is covered by a single external location, so
prod needed no new Azure resources.

---

## The star schema

`fct_listing_snapshot` is the centre — one row per listing per crawl date,
across all three channels. The three channel-specific facts hang off the columns
only their channel has.

**Dimensions** — `dim_property`, `dim_location`, `dim_agent`, `dim_agency`,
`dim_property_type`, `dim_feature`, `dim_date`

**Bridges** — `bridge_listing_agent`, `bridge_listing_feature`
(listings carry up to 2 agents and many features)

**Facts** — `fct_listing_snapshot`, `fct_sale`, `fct_rental`, `fct_inspection`

The load-bearing idea is **separating the property from the listing**. A listing
is an advertisement; a property is a physical dwelling. The same house appears in
`buy` while advertised and in `sold` once it transacts. Conforming on property is
what makes "what did this house list for versus sell for" answerable — and it
turns out 640 properties in the dev dataset sold twice and 82 sold three times,
across a 2007–2026 span. That repeat-sales structure is the most interesting
thing in the data.

---

## Design decisions

### Structure

**One bundle at the repo root.** There were briefly two `databricks.yml` files,
one nested. The CLI resolves the *nearest* one walking up from your working
directory, so `bundle deploy` did different things depending on where you stood,
with no error either way.

**One virtualenv for both toolchains.** `databricks-connect` caps Python at
`<3.13` and dbt-core 1.12 runs on 3.12, so a single 3.12 interpreter serves the
bundle and the dbt project. dbt lives in a `[dependency-groups]` entry rather
than a second `pyproject.toml`.

**The dbt project lives inside the bundle** (`transform/`). A `dbt_task` reads
its project from a workspace path the bundle deployed, so it has to ship with
everything else.

**`generate_schema_name` is overridden.** dbt's default *concatenates*
`target.schema` with the model's `+schema`, giving `dbt_toke_silver`. The
override returns the custom schema verbatim, so models land in `silver` and
`gold` exactly.

### Ingestion

**One `for_each` iteration per suburb, pages looped in-process.** The Airflow
original split this into three tasks with 25-page chunks, to work around
`max_map_length` (1024) and ~2s of per-task scheduling overhead. Neither applies
to a process that can loop, so the chunking machinery disappeared — and the
`requests.Session` now covers a whole suburb instead of 25 pages.

**`list_suburbs` publishes the suburb list as a task value.** Pasting the JSON
into `inputs:` would create a second copy of the watched set; a suburb added to
`suburbs.py` but not the YAML would be silently unwatched, and that coverage gap
is invisible in the data. Now there is one definition.

**Backfill is a separate, manually-triggered job.** A full sold history is
thousands of requests against a rate-limited key that the nightly run depends on.
It happens when someone is watching — never automatically at 11pm because a
marker was missing.

**Backfill markers are files, not a control table.**
`<landing_root>/_state/<dataset>/state=/suburb=.json`, written only after every
page lands. The task already has volume write access, so this needs no SQL
connection, no warehouse, and no extra credential — and it stays runnable on any
compute. Inferring state from the data instead would conflate "some rows exist"
with "the backfill completed": a run that died on page 180 leaves rows behind and
would read as done, capping that suburb's history permanently.

**The nightly run refuses a suburb with no marker.** An incremental run against a
never-backfilled suburb succeeds and lands one month of history — indistinguishable
downstream from a suburb that genuinely has one month of sales, and permanently
skewing any suburb-to-suburb comparison.

**`buy` and `rent` have no age filter and no marker check.** They are *state*,
not events: every run wants the full current set. Only `sold` has history, so
only `sold` can be incremental.

**Ingestion is scheduled; transformation is file-arrival triggered.** These
started as one job, which forced a choice: a schedule couldn't react to
backfills, and a file-arrival trigger fired on the job's own writes and ran Auto
Loader mid-crawl. Splitting them dissolves the conflict — `realestate_ingest`
runs on a clock, `realestate_medallion` reacts to files, and any producer feeds
the medallion.

**The trigger debounces for 15 minutes.** `wait_after_last_change_seconds` must
exceed the longest gap *within* a crawl, or the trigger fires mid-run.
A nightly crawl is ~2 minutes of fetching, so 900s is generous by design: it
absorbs a 429 backoff chain (six attempts, up to 60s each), a slow suburb, or a
gap between `for_each` iterations. It also batches consecutive backfills into a
single bronze load. Latency costs nothing when the run starts at 11pm.

**Job parameters are appended to a `python_wheel_task` automatically**, as
`--<name>=<value>`. Referencing them *also* via `named_parameters` passes each
twice, and the two spellings won't match — job parameter names can't contain
hyphens. Hence the CLI speaks underscores throughout, and `named_parameters`
carries only what isn't already a job parameter.

**The API key is a secret; the base URL is not.** Credentials authenticate;
endpoints are configuration. Putting the URL in a scope would cost reviewability
in git, deployability across workspaces, and readable error messages, for no
security gain. `spark_env_vars` puts the key only on `ingest_runner`, not on the
dbt cluster that never reads it.

**CLI flags take explicit values, not `store_true`.** Databricks renders
`named_parameters` as `--key=value` *always*, so an unset parameter arrives as
`--flag=`. argparse rejects an explicit value for `store_true`, and `type=int`
rejects `''`. Hence `_flag` and `_optional_int`.

### Bronze

**The pipeline owns bronze; dbt only ever reads it.** Bronze tables are declared
as dbt *sources*, never materialised into. Two writers on one Delta table is the
classic way to corrupt a streaming table.

**Checkpoints and schema locations are unset.** Declarative Pipelines manages
both per streaming table. Setting them by hand — as every plain Structured
Streaming tutorial does — fights the managed state.

**Column mapping is enabled.** The source carries `agency.logo.links.hero image`,
and Delta rejects spaces in column names under the default protocol. Column
mapping handles the whole class of awkward keys at once, which matters for
scraped payloads that will keep producing them.

**Bronze lands the mess faithfully.** No parsing, no dedup, `_rescued_data` on.
It's what made the pagination duplication and the `postcode`/`postCode` collision
*visible* rather than silently absorbed.

### Silver and gold

**Deduplication happens in silver, where it's visible and testable.** The
`unique` test on `listing_id` is what stops the pagination duplication from
quietly returning.

**Price parsing extracts every dollar figure into an array.** The four observed
shapes — `$607,000`, `$1,000,000 - $1,100,000`, `$1,500,000 to $1,600,000`,
`$1,124,000 <marketing copy>` — plus `Contact agent` and `ALL OFFERS CONSIDERED`
all fall out of the array's length, with no pattern per shape.

**Channel prices stay in separate columns through to gold.** A weekly rent and a
sale price are different measures; merging them into one `price` column would
pass every test and quietly ruin every average.

**`property_key` falls back to the listing id when the address is withheld.**
Withheld listings all carry the literal string `Address available on request`, so
hashing the address merged every withheld listing in a suburb into one phantom
dwelling — 39 "sales" of one Beaufort property, across 7 property types.
`is_property_identified` flags them so repeat-sales analysis can exclude them.

**Facts use Unknown members (`-1`), not null foreign keys.** `dim_date`,
`dim_agent` and `dim_agency` each carry one, and the facts `coalesce` onto it.
That's what lets every `relationships` test be strict — a null FK passes
silently and hides exactly the kind of gap this project keeps finding.

**Surrogate keys are hashes of natural keys, computed independently per model.**
No lookup joins at build time, so models build in parallel and a fact can't lose
rows to a missing dimension row. The `relationships` tests verify the hashes
agree.

---

## Operating it

```bash
# deploy
databricks bundle deploy -t dev          # or -t prod

# fetch now, rather than waiting for 23:00 (dev schedules are paused)
databricks bundle run realestate_ingest -t dev

# bronze + silver + gold now, bypassing the 15-minute file-arrival debounce
databricks bundle run realestate_medallion -t dev

# backfill one suburb's full sold history, then certify it
databricks bundle run realestate_backfill -t prod \
  --params suburb=Ararat,state=VIC,write_backfill_marker=true

# large suburbs exceed the API ceiling — slice, then certify the last pass
databricks bundle run realestate_backfill -t prod \
  --params suburb=Horsham,state=VIC,max_sold_age_months=12
```

Local dbt work runs against dev:

```bash
source .venv/bin/activate        # or use direnv; .envrc puts .venv/bin on PATH
cd transform && dbt build
```

### One-time setup

Neither of these is created by `bundle deploy`:

```bash
databricks secrets create-scope realestate
databricks secrets put-secret realestate rapidapi_key
```

and the target catalog with `bronze`/`silver`/`gold` schemas plus a `landing.raw`
external volume.

---

## Known data-quality issues

**The API serves at most 1,500 results per query** — 50 pages at the default
page size of 30. Beyond that it returns listings from within the same window, so
extra pages are duplicates: one sold listing appeared 214 times across 263 pages
of Horsham. `MAX_RESULTS` and `max_pages_for()` stop the wasted requests and log
a warning naming how many pages are unreachable — but the listings past the cap
**cannot be retrieved** without narrowing the query. This is the one problem no
amount of downstream work fixes.

A run that hits this cap will not be certified as a backfill: `fetch_suburb`
declines to write the marker for a truncated run, and for a run narrowed with
`--max_sold_age_months`, because neither has seen the suburb's full history.

**`modifiedDate` is always empty.** `{"value": ""}` on every record sampled, so
the source provides no change signal. `ingest_date` is the only version axis,
which means any SCD2 treatment has to be derived from daily snapshots.

**`address.postcode` and `address.postCode` both exist**, differing only in case.
Spark resolves column names case-insensitively, so Auto Loader keeps `postCode`
and parks the lowercase twin in `_rescued_data` — which is why `_rescued_data` is
non-null on 100% of bronze rows. Values are identical; nothing is lost.

**14 sold listings have no `dateSold`** despite `status = Sold` (0.2%). The
`not_null` test warns rather than fails, with a threshold that errors if the
count grows past 50.

**~1% of listings withhold the street address.** See `property_key` above.

**Only one crawl date exists so far.** Every time-based measure — days on
market, price reductions, asking-versus-achieved — is derived *across*
observations, so it needs the nightly run to accumulate history. The model
supports them; the data does not yet.

---

## Gotchas that cost time

Things that pass `bundle validate` and fail at runtime, or fail in ways whose
error message points somewhere else:

- **A `file_arrival` trigger URL must end with `/`.** Enforced by the Jobs API
  at deploy, not by the bundle schema.
- **`spark_env_vars` is the job mechanism for secrets.** `env_vars` exists in the
  schema but belongs to Databricks Apps.
- **`store_true` is unusable with `named_parameters`**, which always emit
  `--flag=`. argparse rejects an explicit value for a flag that takes none.
- **A typo'd model name in a dbt YAML silently drops its tests.** dbt warns
  rather than errors, so the build stays green with untested models.
- **An unused bundle variable is never flagged.** Declaring one nothing reads
  fails only at runtime, where the variable was needed.
- **`num_workers: 0` alone hangs.** A single-node cluster also needs
  `spark.master`, `spark.databricks.cluster.profile` and the `ResourceClass` tag.
- **Databricks Connect runs `spark` remotely but plain Python locally.** An
  `os.makedirs('/Volumes/...')` in a Connect session touches your laptop.
- **Cluster logs contain a benign `CommandLineHelper$` ERROR** during library
  install. The real task failure is in the run *output*, not the cluster log.
