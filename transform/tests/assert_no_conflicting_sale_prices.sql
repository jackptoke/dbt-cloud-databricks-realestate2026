{{ config(severity = 'warn') }}

-- Two source records for one sale, both quoting an EXACT price, and the prices
-- disagree. One of them is wrong and nothing in the data says which.
--
-- fct_sale deduplicates these to one row per (property_key, sold_date), so by
-- the time a sale reaches gold the disagreement is gone — which is exactly why
-- this test reads the intermediate model instead. The tie-break there picks the
-- highest listing_id, a rule chosen for stability, not for accuracy.
--
-- A warning rather than an error on purpose. The nightly build should not fail
-- because an upstream aggregator disagrees with itself, but the count should be
-- visible: 2 groups today, and if it climbs steeply the tie-break is deciding
-- more than it should and the sale price series needs a harder look.
--
-- Exact prices only. A range that overlaps an exact figure is not a conflict —
-- it is the same sale reported at lower precision, which the dedup handles by
-- preferring the exact record.

with exact_priced_sales as (
    select
        property_key,
        sold_date,
        sale_price_low_aud as price
    from {{ ref('int_listings_unioned') }}
    where channel = 'sold'
      and sold_date is not null
      and not is_price_withheld
      -- low = high is how an exact price presents after price parsing
      and sale_price_low_aud = sale_price_high_aud
)

select
    property_key,
    sold_date,
    count(distinct price) as distinct_exact_prices,
    min(price) as lowest,
    max(price) as highest
from exact_priced_sales
group by property_key, sold_date
having count(distinct price) > 1
