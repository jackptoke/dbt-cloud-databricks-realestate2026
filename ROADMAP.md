# Next phase: answering an investor's questions

The pipeline is built and running. The next phase is making it *useful* — gold
tables that answer the questions a property investor actually asks, then a way
to see them.

---

## The questions

Grouped by whether the data can answer them **today**, because three of them
can't yet and it's better to know that before building the table.

### Answerable now

| # | Question | Basis |
| --- | --- | --- |
| 1 | Which suburb gives the best rental yield? | weekly rent × 52 ÷ median sale price, by suburb × property type × bedrooms |
| 2 | Where have prices grown fastest? | median sold price by suburb × year, 2007–2026 |
| 3 | What has this specific property done historically? | repeat sales — 640 properties sold twice, 82 three times |
| 4 | Is this listing priced above its comparables? | asking price vs median sold price/m² for the same suburb, type and bedroom count |
| 5 | Which agencies dominate a suburb? | `bridge_listing_agent`, `dim_agency` |

### Needs accumulated history — the nightly run is what unlocks these

| # | Question | Blocked on |
| --- | --- | --- |
| 6 | How long do properties take to sell? | a listing must be observed across many `ingest_date`s |
| 7 | Which listings have cut their price? | same — price change is a delta between observations |
| 8 | What discount do sellers accept? | a `buy` listing must later appear in `sold` for the same `property_key` |

Question 8 is the most valuable one in the whole set, and the model already
supports it — `property_key` links a listing to its eventual sale. It just needs
weeks of running. Worth stating plainly: **the daily cadence is the product.**
A one-off crawl is a snapshot; the measures investors care about live in the
differences between snapshots.

---

## Data constraints to design around

**Rental volume is thin.** 21 listings in Ararat, 16 in Horsham, 7 in Stawell,
≤2 everywhere else. Any yield figure below ~10 rentals is noise. Options: widen
the suburb list, aggregate to a region, or publish yield with a sample-size
column and let the consumer judge. The last is the honest choice and the easiest.

**Watched vs spillover suburbs.** `dim_location` has 119 suburbs; 5 are crawled.
The rest arrive as neighbouring-suburb results and have arbitrary, unstated
coverage. Every suburb-comparison mart must exclude them or flag them, or it will
rank Dimboola against Ararat as if they were measured the same way.

**Sold history is truncated for large suburbs.** The 50-page API ceiling means
Ararat has 1,500 of ~4,260 sales, biased toward whatever the API returns first.
Price-trend series must carry that caveat; the backfill markers record
`truncated: true` per suburb, so it can be surfaced rather than hidden.

**Only one crawl date exists.** Nothing time-based works until the nightly run
accumulates.

---

## Gold tables to build

```text
dim_location            + is_watched_suburb        flag, driven by suburbs.py
mart_suburb_yield       suburb × property_type × bedrooms
mart_suburb_price_trend suburb × year × property_type
mart_repeat_sales       property_key with ≥2 sales
mart_listing_valuation  one row per current buy listing, vs its comparables
mart_suburb_scorecard   one row per watched suburb — the headline table
```

**`mart_suburb_yield`** — median weekly rent, median sale price, gross yield
`(rent × 52 / price)`, plus `rental_sample_size` and `sale_sample_size` so a
yield computed from 3 rentals is visibly different from one computed from 30.

**`mart_suburb_price_trend`** — median sale price per year with a transaction
count, and a `is_truncated_history` flag from the backfill marker.

**`mart_repeat_sales`** — first sale, last sale, years held, total growth and
annualised growth. Restricted to `is_property_identified = true`, since withheld
addresses can't be proven to be the same dwelling. This is the strongest evidence
of capital growth in the dataset because it holds the property constant.

**`mart_listing_valuation`** — the "is it overpriced?" table. For each current
`buy` listing: asking price, the median sold price/m² for its suburb + type +
bedroom band, the implied comparable value, and the variance. Needs a minimum
comparable count to produce a verdict; below that it returns null rather than a
confident-looking guess.

**`mart_suburb_scorecard`** — one row per watched suburb joining the above:
median price, yield, 5-year growth, active listings, sales per year. The table a
dashboard's landing page reads.

---

## Visualisation

Recommendation: **Databricks AI/BI dashboards first**, Streamlit later if an
interactive tool proves necessary.

| | fit |
| --- | --- |
| **AI/BI dashboards** | Native, no infrastructure, inherits UC permissions, SQL-only. Genie lets you ask "which suburb has the best yield" in English against the marts. Fastest path from table to answer. |
| **Streamlit** (as a Databricks App) | Right when you need *input* — "paste a listing URL, tell me if it's overpriced". More control, more to maintain. |
| **Power BI** | Strongest if the audience already lives there. Adds a refresh path and a second semantic layer to keep in sync. For a portfolio project it demonstrates less than the native option. |

The marts are plain Delta tables either way, so this decision is reversible and
doesn't need making before the tables exist.

---

## Next week

**Monday — verify and stabilise**

- Check the first unattended nightly: did `realestate_ingest` complete, did the
  file-arrival trigger fire ~15 min later, did dbt build?
- Resolve the dbt profile-name question in prod if it surfaced.
- Backfill the four remaining suburbs so `ingest_sold` stops failing.
- Confirm a second `ingest_date` lands — the first proof that history accumulates.

**Tuesday — foundations for comparison**

- Add `is_watched_suburb` to `dim_location`, sourced from `suburbs.py` (a seed
  keeps it in one place).
- Add the backfill-marker state as a dbt source so `is_truncated_history` is
  queryable rather than buried in a JSON file.
- Expand the suburb list — the thin rental counts are the binding constraint on
  every yield number, and more suburbs is the only real fix. Backfill each.

**Wednesday — the growth and yield marts**

- `mart_suburb_price_trend`, `mart_repeat_sales`.
- `mart_suburb_yield` with explicit sample sizes.
- Tests: no yield without a minimum sample; no negative growth over a 1-day hold;
  every mart restricted to watched suburbs.

**Thursday — valuation**

- `mart_listing_valuation` and `mart_suburb_scorecard`.
- This is the one with genuine modelling judgement in it: which comparables
  count, how to band bedrooms, what minimum sample justifies a verdict. Expect to
  iterate on the definition more than the SQL.

**Friday — make it visible**

- An AI/BI dashboard over `mart_suburb_scorecard`: yield by suburb, price trend,
  the current listings ranked by variance from comparable value.
- Try Genie against the marts and see whether the column names hold up to plain
  English questions. They're a user interface now.

**Running in the background all week:** every night adds an `ingest_date`. By
Friday there should be five, which is the first point at which questions 6–8
stop being hypothetical.
