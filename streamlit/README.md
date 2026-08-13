# realestate2026 — investor dashboard

A Streamlit app over `realestate.gold`, answering one question:
**what has this market done, and is this listing priced fairly against it?**

Scope, and the reasoning behind what is in and out, lives in [SCOPE.md](SCOPE.md).

```text
app.py                    entry point; st.navigation over the four pages
lib/data.py               warehouse connection, cached mart loaders
lib/ui.py                 palette, number formats, shared captions
views/market_explorer.py  landing — prices, supply, affordability
views/capital_growth.py   repeat sales — the differentiator
views/listing_valuation.py  the interactive page; needs user input
views/coverage.py         what the data cannot answer, and why
```

## Running locally

The app authenticates through the Databricks SDK's `Config()`, which reads the
environment. Nothing here stores a credential.

```bash
uv pip install -r requirements.txt

DATABRICKS_CONFIG_PROFILE=realestate_dev \
DATABRICKS_WAREHOUSE_ID=cf2a73a5f7ab6a80 \
REALESTATE_CATALOG=realestate \
streamlit run app.py
```

`REALESTATE_CATALOG` is required — there is no default, deliberately. Set it to
`realestate` for dev or `realestate_prod` for production data.

**Caching gotcha.** Every loader is wrapped in `st.cache_data`, keyed on the
function's own bytecode. Editing a *helper* the loader calls does not invalidate
it, so a change to `lib/data.py` internals can leave stale frames in a running
process. Restart the server rather than relying on hot reload when you change
the data layer.

## Deploying

**Live: https://dashboard-production-4932.up.railway.app** — Railway project
`realestate-dashboard`, service `dashboard`, reading `realestate_prod`.

The same code runs on Railway, on Databricks Apps and on a laptop without
modification, because authentication is delegated entirely to the SDK's
`Config()`: it accepts a PAT, an OAuth service principal, or platform-injected
credentials, and nothing in the app knows which is in use.

### Railway (primary)

`railway.json` pins the start command — Streamlit must bind `$PORT` and
`0.0.0.0` or Railway's healthcheck never passes — and points the check at
`/_stcore/health`.

Required service variables:

| variable | value |
| --- | --- |
| `REALESTATE_CATALOG` | `realestate_prod` |
| `DATABRICKS_HOST` | the workspace URL |
| `DATABRICKS_WAREHOUSE_ID` | the SQL warehouse id |
| `DATABRICKS_CLIENT_ID` | service principal application id |
| `DATABRICKS_CLIENT_SECRET` | its OAuth secret |

`REALESTATE_CATALOG` deliberately has **no default**. A default would let one
missing variable turn a production deployment into a silent development one, so
an unset value is a startup error with a named list of what is missing.

```bash
cd streamlit
railway up --service dashboard
```

The app reads through a service principal holding `USE CATALOG` on
`realestate_prod`, plus `USE SCHEMA` and `SELECT` on `realestate_prod.gold`, and
`CAN_USE` on the warehouse. Nothing else — verified: the same credentials are
refused on the dev catalog. Rotate by issuing a new secret with
`databricks service-principal-secrets-proxy create <sp-id>` and updating the
Railway variable; the old one can then be deleted independently.

### Databricks Apps (alternative)

`app.yaml` takes the warehouse from a `valueFrom` resource rather than a
hardcoded id, so register a SQL warehouse resource named `sql-warehouse` on the
app first. The platform injects the service principal credentials itself.

```bash
databricks apps deploy <app-name> --profile realestate_dev
```

## Design notes worth knowing before changing anything

**Everything is loaded whole and filtered in pandas.** The gold layer is ~40k
rows. A query per interaction would put a warehouse round trip behind every
filter change for no benefit. If the crawl footprint grows by an order of
magnitude, push filtering back into SQL.

**Decimals are normalised at the boundary.** Databricks `DECIMAL` arrives as
`decimal.Decimal` in object columns. They print and compare fine, so the problem
stays hidden until something multiplies one by a float and raises `TypeError` on
a page nobody exercised. `lib/data._floats` converts them once, in `query()`.

**The valuation verdict is stated relative to the typical premium, never
against zero.** Asking prices sit above sold prices everywhere — negotiating
room, plus live stock being a different mix from sold stock. Across every
verdict the typical listing asks a few percent above its indexed comparables,
and the page reports each listing against *that*, not against parity. Comparing
to zero would call most of the market overpriced and would undo, at the last
step, the time-indexation the mart does specifically to avoid that.

**Comparables are time-indexed, and the page says so.** Both the raw and indexed
medians are shown along with the rate applied and the median comparable age, so
a reader can undo the adjustment. Hiding it would make the headline number
unfalsifiable.

**Known limit — location within an LGA.** Comparables pool across a whole
council area, so a premium pocket is priced against its region: a Halls Gap
lifestyle property reads far above a Northern Grampians median that includes
Stawell. That is location premium, not mispricing. Suburb-level comparables
would fix it and reach only 452 of 674 listings against 567 at LGA level — a
visible, explainable bias was preferred over a coverage gap that is neither.

**The app may read `gold` and nothing else.** Its service principal is granted
`SELECT` on `realestate_prod.gold` only, so a query touching `silver` or
`bronze` works for a developer whose profile sees everything and fails in
production. That is not a hypothetical — deriving the crawl year from
`silver.int_listings_unioned` passed locally and 500'd on Railway. Anything the
app needs must be published into a gold model first.

```bash
grep -n "silver\|bronze" lib/data.py views/*.py   # must return nothing
```

## Verifying a change

There is no test suite for the UI. The check that has caught real bugs is
driving it headless and looking for Streamlit exception blocks:

```python
page.goto("http://localhost:8511/capital-growth", wait_until="networkidle")
page.locator("[data-testid='stException']").count()   # must be 0
```

Scroll before screenshotting — Streamlit renders below-the-fold content lazily,
so a full-page capture without scrolling silently omits half the page.
