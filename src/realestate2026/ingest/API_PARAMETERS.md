# realty-in-au `/properties/list` — parameter reference

Vendor documentation plus **what was actually verified against the live API** on
2026-08-13, against `Horsham, VIC 3400` (6,873 sold) and `Beaufort, VIC` (828).

Read this before adding a parameter. Roughly 45 requests were spent
rediscovering things that are written down here now.

## The one rule that makes testing hard

**The API silently ignores parameters it does not recognise.** A misspelled
parameter returns the full unfiltered result set — which reads as "the filter
matched everything" rather than "there is no such filter". So:

- Test every parameter against a **known baseline** and treat an identical count
  as IGNORED, not as a result.
- Test **sorts differently**: a sort never changes `totalResultsCount`, it
  changes the order. Compare the **first listing id**, not the count. Getting
  this wrong is how `sortType` was written off as non-functional on the first
  pass, which was the most costly error in the whole investigation.

## The hard ceiling

`totalResultsCount` reports the true size of the result set, but **only the
first 1,500 records (50 pages × 30) are ever servable**. Page 51 and beyond
return page 50 verbatim — not an error, not an empty page, the same 30 rows
forever. Verified independently twice.

`pageSize` is capped at 30 by the vendor, so a larger page size cannot buy
depth. `MAX_RESULTS = 1500` in `fetch_suburb.py` matches this exactly and should
stay.

**The escape is not paging — it is opening more windows.** See "Getting past
1,500" below.

## Parameters

| parameter | type | verified behaviour on `channel=sold` |
| --- | --- | --- |
| `page` | int | 1-based. Clamps at 50; higher pages repeat page 50. |
| `pageSize` | int | **Max 30**, vendor-enforced. |
| `sortType` | string | **WORKS.** See sort values below — each ordering is a different window into the set. |
| `channel` | string | `buy` \| `rent` \| `sold`. Required. |
| `propertyTypes` | string | **WORKS.** Comma-separated for multiple. Note the **plural** — `propertyType` singular is ignored. |
| `surroundingSuburbs` | bool | Adds tier-2 neighbouring results. |
| `searchLocation` | string | **Granularity depends on the string.** See below — this is the most important finding. |
| `searchLocationSubtext` | string | Vendor says: use the value from the auto-complete endpoint. |
| `type` | string | Vendor says: use the value from the auto-complete endpoint. |
| `minimumBedrooms` | int | **WORKS.** |
| `maximumBedrooms` | int | **WORKS.** Composes with the minimum to give exact bedroom bands. |
| `minimumLandSize` | int (m²) | Documented; untested. |
| `minimumBathroom` | int | Documented. **Singular** — `minimumBathrooms` plural is ignored. |
| `minimumCars` | int | **WORKS.** `maximumCars` is NOT documented and appeared to be ignored. |
| `minimumPrice` | int | Documented but **IGNORED on `sold`** — verified, no effect. Probably buy/rent only. |
| `maximumPrice` | int | Documented but **IGNORED on `sold`** — verified, no effect. |
| `ex-under-contract` | bool | |
| `constructionStatus` | string | `established` \| `new`. Untested. |
| `keywords` | string | Facilities, comma-separated (`pool,garage`). Untested. |
| `maxSoldAge` | int (months) | **WORKS.** Upper bound on age only — no lower bound exists. |

### `sortType` values

`relevance` · `new-asc` · `new-desc` · `price-asc` · `price-desc` ·
`sold-relevance` · `sold-date-desc` · `sold-price-desc` · `sold-price-asc`

Observed first result for Horsham 3400 — note how far apart the windows sit:

```
relevance         sold 2026-07-30    (== sold-relevance)
sold-date-desc    sold 2026-08-03
sold-price-asc    sold 2008-10-30
sold-price-desc   sold 2011-11-18
new-asc           sold 2014-09-17
```

### `searchLocation` — suburb vs region

The string determines the granularity, and this is easy to get wrong:

```
"Horsham, VIC"        -> resolves to "Horsham - Greater Region, VIC"   7,892
"Horsham, VIC 3400"   -> resolves to "Horsham, VIC 3400"               6,873
"Natimuk, VIC"        -> resolves to "Natimuk, VIC 3409"                 115
```

A locality whose name is **also a region name** resolves to the region unless
the postcode is supplied. Every smaller locality resolves to itself either way.
The region form makes 32 localities share one 1,500 budget; the postcode form
gives each its own.

`type=suburb` and `searchLocationSubtext=Suburb` do **not** change this on their
own — the resolution is driven entirely by `searchLocation`. The vendor's
guidance is to take all three values from the `/auto-complete` endpoint, which
is the robust route and has not yet been wired up.

### Confirmed IGNORED (do not retry)

All returned the unfiltered baseline exactly:

`type=suburb` · `type=locality` · `searchLocationSubtext=Suburb` ·
`minSoldAge` · `soldAgeFrom` · `dateSoldFrom` · `soldFrom` · `minAge` ·
`dateFrom` · `minPrice` · `maxPrice` · `priceFrom` · `priceTo` ·
`minBedrooms` · `numBedrooms` · `beds` · `bedroomsRange` ·
`minimumBathrooms` (plural) · `landSizeFrom` · `propertyType` (singular) ·
`propertyTypes[]` · `maximumCars` · `sortType=dateSold-asc` ·
`sortType=date-asc` · `sortType=oldest`

## Getting past 1,500

There is **no lower bound on sold date**, so `maxSoldAge` alone can never reach
older records: it gives cumulative windows from today, and the window stops
growing once it hits the cap. Requesting more pages is useless.

What does work is opening **multiple 1,500-record windows** into the same set
and taking the union:

1. **Scope tightly.** Use `"<Suburb>, <STATE> <postcode>"` so each locality gets
   its own budget instead of sharing the region's.
2. **Vary `sortType`.** Each ordering exposes a different 1,500. `sold-price-asc`
   surfaced 2008 sales that the `relevance` window (2022–2026) never returns.
   Up to ~6 usable orderings.
3. **Partition into sub-1,500 sets**, which are then retrievable *completely*:
   - `minimumBedrooms` + `maximumBedrooms` for exact bedroom bands
   - `propertyTypes`
   - `minimumCars`
   - `maxSoldAge` for a recent window

Measured for Horsham 3400 (6,873 total, 1,500 reachable unsliced):

```
exact 2-bed                   858   complete in one query
exact 3-bed                 3,458   needs further splitting
  3-bed + minimumCars=3       422   complete
exact 4-bed+                1,536   just over — split by type or cars
unit                          504   complete
townhouse                     324   complete
unitblock                      24   complete
```

Partitioning plus sort variation should cover the whole set. The cost is one
request per page per partition, so partition only as finely as the cap requires
— check `totalResultsCount` first (one request) and only split when it exceeds
1,500.

## Reproducing

The probe used to establish all of this is not committed; it is a handful of
page-1 requests comparing `totalResultsCount` and the first listing id against
a baseline. Rebuild it from the two rules at the top if needed. Roughly 45
requests covered every parameter in this document.
