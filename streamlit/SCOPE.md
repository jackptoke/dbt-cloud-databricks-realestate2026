# Streamlit dashboard — scope

Companion to `ROADMAP.md`. That document listed the questions; this one checks
them against what is actually in `realestate.gold` today, and scopes the app
around what survives.

Profiled against the `realestate_dev` profile, catalog `realestate`, on
2026-08-12. Single crawl date in the data: **2026-08-06**.

**The app answers one question: _what has this market done, and is this listing
priced fairly against it?_** Capital growth and valuation, reported at **Local
Government Area** grain. Rental yield is out of scope for v1 — see
[Why rental is deferred](#why-rental-is-deferred).

---

## What is actually in gold

| table | rows | notes |
| --- | --- | --- |
| `fct_sale` | 6,792 | 20 years, 2007–2026. 6,195 with a price, 597 withheld, 6,708 address-identified |
| `fct_listing_snapshot` | 7,551 | **one** crawl date: 674 buy, 49 rent, 6,828 sold |
| `fct_rental` | 49 | too thin to surface — deferred, not deleted |
| `fct_inspection` | 43 | too thin to build on |
| `dim_property` | 6,609 | |
| `dim_location` | 119 | suburb grain; `locality` is unusable, it just echoes `suburb` |
| `dim_agency` / `dim_agent` | 118 / 311 | |
| `bridge_listing_feature` | 16,842 | genuinely rich |
| `mart_suburb_price_trend` | 816 | already built; regrain to LGA |

Sales depth is real and it is recent-weighted: 1,072 sales in 2025, 946 in 2024,
849 in 2023, tapering to double digits before 2015. Median price moves
$106k (2015) → $415k (2026), so a growth story is well supported.

At **suburb** grain the depth is badly unbalanced. Horsham 1,312 sales, Ararat
1,227, Stawell 945, Beaufort 821, Nhill 604, Dimboola 468 — then a cliff into a
long tail of suburbs with a single sale. That imbalance is what the LGA rollup
below fixes.

---

## Geography: report at LGA, not suburb

Source: [`michalsn/australian-suburbs`](https://github.com/michalsn/australian-suburbs)
(`data/suburbs.csv`, MIT). Vendored as a dbt seed — **this costs no API quota**,
it is a static file, not more scraping.

### The join holds

117 of 119 suburbs resolve:

| outcome | n | detail |
| --- | --- | --- |
| exact on `(suburb, postcode)` | 116 | |
| unique-name fallback | 1 | Beaufort pc 3469 — a source postcode error; Beaufort is 3373 |
| absent from the CSV | 2 | Grass Flat and Arapiles, both 3409, 1 sale each — hand-override to Horsham |

Three sales out of 6,792 unresolved. **Join on `(suburb, postcode)`, never on
name alone**: Bellfield is both 3081 (Banyule, Melbourne) and 3381 (Northern
Grampians, near Halls Gap), and a name-only join silently drags a Melbourne LGA
into the Grampians.

### LGA is the grain the crawler already collects at

This is the real argument, and it was not the one we went looking for. Mapping
each crawl region to the LGAs of the listings it returned:

```text
crawl 'ararat'   -> Ararat (Rural City)          100%
crawl 'beaufort' -> Pyrenees (Shire)             100%
crawl 'horsham'  -> Horsham (Rural City)         100%
crawl 'nhill'    -> Hindmarsh (Shire)            100%
crawl 'stawell'  -> Northern Grampians (Shire)    95%   (+64 listings Ararat)
```

`realty_au.build_url` sends `type=region`, and the region it resolves is
effectively the LGA. Two consequences worth more than the aggregation itself:

- **The 1,500-result cap applies per LGA.** `is_truncated_history` becomes
  exactly correct at this grain, replacing the `bool_and` approximation over
  crawl regions currently in `mart_suburb_price_trend`.
- **The "region centre vs by-catch" problem dissolves.** `ROADMAP.md` planned an
  `is_region_centre` flag because suburbs were measured to uneven depth. At LGA
  grain the unit of measurement _is_ the unit of collection, so there is no
  uneven depth left to flag. That flag is no longer needed.

### What it buys

Balanced reporting units instead of a cliff:

| LGA | suburbs | sales | population | med income | med price 24+ | price/income |
| --- | --- | --- | --- | --- | --- | --- |
| Ararat (Rural City) | 25 | 1,540 | 13,426 | $29,369 | $396,250 | 13.5 |
| Hindmarsh (Shire) | 14 | 1,498 | 5,739 | $26,739 | $260,000 | **9.7** |
| Horsham (Rural City) | 25 | 1,496 | 19,729 | $32,309 | $403,125 | 12.5 |
| Northern Grampians (Shire) | 47 | 1,428 | 10,898 | $27,219 | $349,000 | 12.8 |
| Pyrenees (Shire) | 1 | 821 | 6,335 | $24,969 | $400,000 | **16.0** |

A tail of Loddon (4 sales), Buloke (2) and Southern Grampians (1) sits below
these and should be grouped as "other" rather than charted.

**The valuation page improves materially.** Comparables at LGA × property type ×
bedrooms rather than suburb × type × bedrooms lift coverage from **452 to 566 of
674 live buy listings** (67% → 84%), over 122 denser cells instead of 295 sparse
ones.

**Demographics come free** and give an affordability ratio — Hindmarsh at 9.7
against Pyrenees at 16.0 is a genuine investor signal for one join.

### Caveats to carry

- **Median income cannot be median-of-medianed.** The CSV publishes it per
  suburb. The LGA figures above are population-weighted means; label them as
  approximations, or report income at suburb grain only.
- **Population and area must be summed over every CSV suburb in the LGA**, not
  only the crawled ones, or the totals under-report.
- **Vintage.** The repo was last updated 2020 on ABS SSC 2016 boundaries. Rural
  Victorian LGA boundaries have been stable since the 1994 amalgamations, so
  this is safe, but record it in the seed's provenance comment.
- **Suburb grain is not thrown away.** `dim_location` stays at suburb grain with
  LGA as an attribute, so drill-down remains available.

---

## Question-by-question verdict

### Answerable well — these are the app

**Capital growth on the same dwelling (repeat sales).** 601 properties sold two
or more times with an identified address, a real date and a non-withheld price.
548 sold twice, 46 three times, 7 four times. Median holding period 3.0 years,
median annualised growth 9.8%, and 556 of the 601 pairs were held a year or
more. This is the strongest thing in the dataset.

It is also the metric that exercises the hardest modelling in the pipeline.
`fct_sale` collapses observations into events, then collapses four disjoint
listing-id bands into a single sale via `property_key` — the work in commit
`b77e316`. Repeat sales is what that work was for, and it holds the dwelling
constant, which is the methodologically defensible way to measure growth
(Case-Shiller works this way). It needs no accumulated crawl history.

**Is this listing priced above its comparables.** At LGA × type × bedrooms,
sold since 2024, priced and not withheld: **566 of 674 live buy listings** land
in a cell with ≥5 comparables. A real answer for five in six listings, and an
honest "not enough comparables" for the rest.

**Price trend by LGA and year.** `mart_suburb_price_trend` already has the
shape — quartiles and a truncation flag. Regrain it to LGA.

**Affordability.** Median price against median household income per LGA. New,
cheap, and directly investor-relevant.

**Feature premium.** 16,842 feature rows: Garage 3,390, Carport 2,069, Shed 846,
Split-system A/C 700, Dishwasher 535, Ensuite 323.

Note for whoever builds this panel: **pool will not work.** `Swimming Pool -
Inground` is 16 listings and `Above Ground` is 5, out of 6,609 properties. Rural
Victoria. Garage, shed, ensuite and A/C all have the volume; pool does not.

**Agency concentration.** Clear league table — Wes Davidson 744, Harcourts
Horsham 729, Monaghans Stawell 644, Ray White Ararat 592.

### Not answerable — one crawl date

Days on market, price reductions, discount-to-asking, listing velocity. All four
need a second `ingest_date` and there is exactly one. This is a _stronger_
blocker than `ROADMAP.md` assumed: it is not only days-on-market that is
blocked, it is every time-based measure, because `fct_listing_snapshot`
currently has no time axis at all.

---

## Why rental is deferred

`fct_rental` has 49 rows in total, and pooling to LGA does not rescue them:

```text
Ararat (Rural City)          21
Horsham (Rural City)         16
Northern Grampians (Shire)    9
Hindmarsh (Shire)             3
```

At suburb × type × bedrooms — the grain any yield figure with filters needs —
there are 21 cells and only two clear five observations (Ararat 3-bed house 11,
Horsham 3-bed house 9; thirteen cells hold exactly one rental).

A yield panel built on this would be two credible numbers wearing the costume of
an analysis. Publishing it with sample sizes attached would be honest, but it
would still be the weakest thing on the page, and it would dilute an app that is
otherwise well supported by 6,792 sales.

**Decision: rental is out of v1.** `fct_rental` and the rent channel stay in the
pipeline — they are correct, they cost nothing, and they accumulate. Yield
becomes a v2 feature once the suburb list widens. `mart_suburb_yield` is
dropped from the build list; page 5 carries rental volume as a labelled gap so
the omission is visible and deliberate rather than silent.

### Adopted from the design review

Several suggestions from the earlier review are kept and are reflected below:
supply mix, price distribution box plots, the agency league table, feature
premium, explicit row counts and crawl date on the page, labelled placeholders
for the blocked time-based metrics, and the discipline of keeping the app tight
rather than a wall of charts.

---

## Scope

### Page 1 — Market Explorer _(landing)_

Filters: LGA (5 reportable), property type, bedrooms. Optional suburb drill-down.

- KPI row — median sold price last 12m, sales last 12m, median asking price,
  active listings, 10-year growth, price-to-income ratio
- Median sold price by year, with a q1–q3 band; flagged where truncated
- Price distribution by property type — box plot, so spread is visible
- Supply mix — active buy listings vs sales, by property type

### Page 2 — Capital Growth _(the differentiator)_

- Scatter: holding period vs annualised growth, one point per repeat-sale pair,
  601 of them
- Median annualised growth by LGA with n beside every number
- Table: address, first sale, last sale, years held, total and annualised growth
- Stated caveat: identified addresses only, non-withheld prices only

### Page 3 — Listing Valuation _(the interactive hook)_

- Select a live buy listing, or filter down to a shortlist
- Asking price vs comparable median, variance %, comparable count
- The comparables themselves in a table, so the verdict is auditable
- "Insufficient comparables" for the 108 listings below threshold — no
  confident-looking guess

This page is the reason the answer is Streamlit and not an AI/BI dashboard. Per
`ROADMAP.md`'s own criterion, Streamlit earns its place when the app needs
_input_. Everything else on this list AI/BI could do.

### Page 4 — Market Texture _(optional, build if time allows)_

- Feature premium: median price with vs without garage, shed, ensuite, A/C
- Agency league table by LGA, off `bridge_listing_agent`

### Page 5 — Coverage & Caveats _(small, always visible)_

Crawl date, row counts per table, which LGAs have truncated sold history, and
the suburb→LGA match rate with its three unresolved sales. Labelled gaps for
what is deliberately not shown: rental yield (49 rentals), inspections (43), and
the four time-based metrics blocked on a second `ingest_date`.

If the schedule tightens, cut pages 4 and 1 in that order. Pages 2, 3 and 5 are
the ones that would be hard to reproduce from a generic scrape.

---

## Build order

Compute in dbt, not in Streamlit. The app stays a thin read layer, the marts
stay reusable by AI/BI and Genie, and the logic stays tested.

**1. Geography — DONE, built and tested 2026-08-12.**

| object | status |
| --- | --- |
| `seeds/australian_suburbs.csv` | 15,286 rows, all states, vendored verbatim |
| `seeds/suburb_lga_overrides.csv` | 2 rows: Grass Flat, Arapiles → Horsham (Rural City) |
| `models/staging/stg_suburbs.sql` | one row per `ssc_code`; pads postcodes, fixes the upstream `local_goverment_area` typo, normalises join keys |
| `dim_location` | + `lga_key`, `lga_match_method`, suburb population / income / area / elevation |
| `dim_lga` | 539 rows (538 LGAs + Unknown); population, area, density, weighted income, weighted centroid, `has_listing_data` |

Outcome: 119 of 119 suburbs resolve — 116 exact, 1 name fallback, 2 override,
**0 unmatched**. `dim_location` stays at 119 rows, so nothing fanned out. All
6,792 sales land in an LGA. Full suite: 113 pass, 0 errors, 2 pre-existing
warnings unrelated to this work.

Deliberately **not** built into `dim_lga`: the truncation flag. It is a
property of a crawl, not of a council, and `mart_lga_price_trend` is the right
owner. `dim_lga` stays reference geography plus demographics.

**2. The two marts that gate pages 2 and 3 — DONE, built and tested
2026-08-12.**

| model | grain | result |
| --- | --- | --- |
| `mart_repeat_sales` | property_key with ≥2 sales | 601 properties over 1,262 sales; median 3.0 yrs held, +38.8% total, 9.7% CAGR |
| `mart_lga_growth_rate` | one row per LGA with listings | 8 rows; 5 on their own rate, 3 on the national fallback |
| `mart_listing_valuation` | one row per live buy listing | 674 rows, 537 with a verdict, comparables time-indexed |

Postcode was also normalised at source in the same pass — a
`normalise_postcode` macro applied in the three staging models, so every
downstream join gets a zero-padded four-character string and no model
reformats it. A no-op on today's data by design, which is why it was safe to
add: it makes a current coincidence into a guarantee without moving a single
surrogate key.

**3. The remaining marts — DONE 2026-08-12.**

| model | grain | result |
| --- | --- | --- |
| `int_crawl_coverage` | one row per crawl region | the single definition of truncation |
| `mart_lga_price_trend` | LGA × year × property type | 344 rows, 2007–2026 |
| `mart_lga_scorecard` | one row per LGA | 8 rows, 5 reportable |
| `mart_data_coverage` | one row per LGA | owns `is_truncated_history` |

**4. The app — DONE 2026-08-12.** Four pages under `streamlit/`, verified
headless against live data with zero exception blocks. See
[README.md](README.md).

### Two bugs the build surfaced

**Truncation was computed three different ways and two disagreed.** `max_page`
was taken over the rows belonging to each LGA rather than over the crawl as a
whole, so a capped crawl whose last Ararat listing fell on page 49 made Ararat
look complete — while Buloke's 3 stray listings from that same capped crawl
topped out at page 21 and made *it* look complete too. The ceiling is a property
of the crawl, not of the slice an LGA receives. Now defined once in
`int_crawl_coverage` and consumed by all three marts.

**Comparables banded land by bedrooms.** Every vacant block reports 0 bedrooms,
so a whole LGA's land sat in one cell: Horsham's held 129 sales spanning 306 m²
to 845,700 m². A 53-hectare parcel read **+3,809%** against 756 m² suburban
blocks. Land now bands on area via the `comparable_band` macro — worst case
dropped to +1,242% (a genuine Halls Gap location premium), median land variance
17.1% → 10.6%, and 54 listings correctly lost a verdict they should never have
had.

### Why the truncation caveat stays (API investigation, 2026-08-13)

The dashboard's truncation warnings are not provisional — they reflect a hard
limit that was investigated properly and written up in
`src/realestate2026/ingest/API_PARAMETERS.md`.

The source serves at most **1,500 results per query** (50 pages x 30). Page 51
returns page 50 verbatim, forever — verified independently twice. `pageSize` is
vendor-capped at 30, so there is no shortcut. Our `MAX_RESULTS = 1500` matches
the ceiling exactly rather than causing it.

There is **no lower bound on sold date** — five spellings tested, all silently
ignored — so `maxSoldAge` can only give cumulative windows from today and can
never step past the cap. That is why Horsham stops at 2022.

Full history *is* reachable, but only by opening multiple 1,500-row windows into
the same set and taking their union: scope by `"<Suburb>, <STATE> <postcode>"`
so each locality gets its own budget, vary `sortType` (each ordering is a
different window — `sold-price-asc` reaches 2008 where `relevance` stops at
2022), and partition with exact bedroom bands, `propertyTypes` and
`minimumCars`. That is an ingest change of real size and it is deliberately not
being done now.

**Until it is, every price series in this dashboard is drawn from a truncated,
relevance-biased subset, and the coverage page says so from data rather than
prose.** Nothing here needs revisiting when the ingest improves — the marts read
truncation from `int_crawl_coverage`, so the caveats will simply stop firing.

### No windowed growth KPI, deliberately

The landing page has no "10-year growth". The result cap bites hardest where the
market is busiest, so history depth runs *inverse* to activity — Horsham reaches
back 3.9 years, Pyrenees 19.1. Three of the five cannot reach ten years and
Horsham cannot reach five, so any windowed comparison would compare different
spans of time and read as a market difference. Growth is the repeat-sales CAGR
instead: a per-property rate, comparable across all five.

### Valuation calibration — RESOLVED by time-indexation

The first cut compared today's asking prices against sold prices up to two and a
half years old, in a market compounding at 7–13%. Median variance came out
**+21.7%** and the ±10% band called 58% of listings overpriced — mostly the
calendar, not the vendor.

Fixed by indexing each comparable sale forward to the crawl date at its own
LGA's repeat-sales growth rate, via a new `mart_lga_growth_rate`. Indexing the
individual sales rather than the resulting median matters: sales within a cell
have different ages, so a median of mixed vintages has no single age to correct
by.

| | median variance | q1 | q3 |
| --- | --- | --- | --- |
| raw comparables | +21.7% | −7.7% | +56.4% |
| **time-indexed** | **+8.3%** | −17.3% | +38.1% |

Verdict split moved from 58 / 19 / 22 (above / in line / below) to **48.8 / 19.9
/ 31.3**. Median comparable age is 1.09 years and the median uplift applied is
12.3%.

Rates used — all five measured LGAs clear the 20-pair reliability threshold
comfortably; the three with no repeat-sale evidence borrow the national 9.68%
and between them hold 7 sales:

| LGA | pairs | rate | source |
| --- | --- | --- | --- |
| Hindmarsh (Shire) | 165 | 13.48% | lga |
| Ararat (Rural City) | 146 | 7.47% | lga |
| Pyrenees (Shire) | 89 | 8.43% | lga |
| Northern Grampians | 81 | 11.74% | lga |
| Horsham (Rural City) | 75 | 8.25% | lga |
| Buloke / Loddon / Sthn Grampians | 0 | 9.68% | national |

**The residual +8.3% is real and should stay.** Asking prices carry negotiating
room and live stock is a different mix from sold stock — that is a genuine
asking premium, not a measurement artifact, and recentring the band on it would
hide a fact worth knowing. Page 3 should show a listing's variance against the
+8.3% typical premium rather than against zero. Both the raw and indexed
medians are published, so the adjustment is auditable and reversible.

`mart_suburb_price_trend` is superseded by `mart_lga_price_trend` — keep or drop
depending on whether suburb drill-down survives design.

Tests worth writing: every `dim_location` row resolves to an LGA or an explicit
unknown; no valuation verdict below the comparable threshold; no repeat-sale
growth over a sub-30-day hold; grain uniqueness on every mart.

---

## Deployment

A Databricks App running Streamlit, added to the existing bundle as an app
resource so it deploys with `databricks bundle deploy`. Reads the gold schema
through the SQL warehouse already in `databricks.yml`
(`dbt_warehouse_id: cf2a73a5f7ab6a80`), via the app's service principal, which
inherits Unity Catalog permissions.

The whole gold layer is ~40k rows. Every mart can be pulled into memory behind
`st.cache_data` on startup; no query-per-interaction is needed, so filtering is
instant.

---

## Open decisions

1. **Comparable window.** Sold since 2024 gives 566 of 674 listings a verdict at
   LGA grain. Widening to 2023 covers more but mixes in a market ~8% cheaper.
   Worth testing both before fixing it.
2. ~~**Seed scope.**~~ Settled: the full 15,286-row CSV is vendored verbatim,
   all states, with no filter in staging. `dim_lga` therefore covers all 538
   Australian LGAs and `has_listing_data` narrows it to the 8 in play.
3. **Suburb drill-down.** Keep suburb as a second level under LGA, or report LGA
   only? Suggest keeping it — the data supports it and it costs one filter.
4. **Price basis.** `sale_price_aud` is the low end of a range where the source
   gave one. Fine for medians, slightly low. Leave as is, note it on page 5.
