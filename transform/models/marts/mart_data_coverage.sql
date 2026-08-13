{{ config(materialized = 'table') }}

-- Grain: one row per LGA that has listing data.
--
-- The honesty table. Everything a reader needs to judge how much weight the
-- other marts can carry, in one place, so the dashboard's caveats page reads
-- from data rather than from hardcoded prose that will drift.
--
-- Three separate things limit this dataset, and they are easy to confuse:
--
--   DEPTH   the source caps a region query at 1,500 results, so a busy LGA's
--           history stops early. Inverse to activity — the busiest LGA has the
--           shallowest history.
--   BREADTH the crawl covers 8 LGAs of western Victoria, not the state.
--   TIME    every measure needing day-over-day change is blocked until a second
--           ingest_date lands. crawl_dates below is the count that matters, and
--           while it reads 1 nothing time-based can be computed at all.
--
-- Rentals are counted but nothing is built on them: 49 across the whole
-- dataset, which is why yield is out of scope. Counting them here keeps the
-- omission visible and deliberate rather than silent.

with reference_date as (
    select max(crawled_on) as crawled_on
    from {{ ref('int_listings_unioned') }}
),

-- Suburb resolution quality, straight off dim_location's match tiers.
suburbs as (
    select
        lga_key,
        count(*)                                          as suburbs,
        count_if(lga_match_method = 'exact')              as suburbs_matched_exact,
        count_if(lga_match_method = 'name')               as suburbs_matched_by_name,
        count_if(lga_match_method = 'override')           as suburbs_matched_by_override,
        count_if(lga_match_method = 'unmatched')          as suburbs_unmatched
    from {{ ref('dim_location') }}
    group by lga_key
),

-- Channel volumes and the crawl evidence, from the conformance view so the
-- landing path is still available.
listings as (
    select
        l.lga_key,
        u.channel,
        u.listing_id,
        u.crawled_on,
        regexp_extract(u._source_file, '/suburb=([a-z0-9-]+)/', 1)                   as crawl_region
    from {{ ref('int_listings_unioned') }} u
    join {{ ref('dim_location') }} l
      on l.location_key = {{ dbt_utils.generate_surrogate_key(['u.suburb', 'u.postcode', 'u.state_code']) }}
),

channels as (
    select
        lga_key,
        count(distinct case when channel = 'sold' then listing_id end) as sold_listings,
        count(distinct case when channel = 'buy'  then listing_id end) as buy_listings,
        count(distinct case when channel = 'rent' then listing_id end) as rent_listings,
        count(distinct crawled_on)                                     as crawl_dates,
        min(crawled_on)                                                as first_crawled_on,
        max(crawled_on)                                                as last_crawled_on,
        count(distinct case when channel = 'sold' then crawl_region end) as supplying_crawl_regions
    from listings
    group by lga_key
),

-- Truncation, from the ONE definition in int_crawl_coverage. An LGA's history
-- is incomplete only if EVERY crawl that supplied it hit the ceiling: a crawl
-- that finished returns every sold listing in its region, so a single complete
-- supplier makes the LGA whole regardless of what the others did.
--
-- Crucially the cap is read per CRAWL, not per the slice of a crawl this LGA
-- received. Deriving it from the LGA's own rows is what previously made Ararat
-- look complete because one capped crawl's last Ararat listing fell on page 49.
lga_crawls as (
    select distinct
        l.lga_key,
        cc.crawl_region,
        cc.hit_result_cap
    from listings l
    join {{ ref('int_crawl_coverage') }} cc on l.crawl_region = cc.crawl_region
    where l.channel = 'sold'
),

truncation as (
    select
        lga_key,
        bool_and(hit_result_cap)                as is_truncated_history,
        max(case when hit_result_cap then 1 else 0 end) = 1 as any_supplier_truncated,
        count(*)                                as crawl_regions
    from lga_crawls
    group by lga_key
),

sales as (
    select
        l.lga_key,
        count(*)                                                     as sales,
        count_if(s.sold_date is null)                                as sales_without_date,
        count_if(s.is_price_withheld)                                as sales_with_withheld_price,
        count_if(not s.is_property_identified)                       as sales_without_identified_address,
        count_if(s.source_listing_count > 1)                         as sales_consolidated_from_duplicates,
        min(s.sold_date)                                             as earliest_sale_date,
        max(s.sold_date)                                             as latest_sale_date
    from {{ ref('fct_sale') }} s
    join {{ ref('dim_location') }} l on s.location_key = l.location_key
    group by l.lga_key
),

valuation as (
    select
        lga_key,
        count(*)                                    as live_listings,
        count_if(has_verdict)                       as listings_with_verdict,
        count_if(no_verdict_reason = 'no asking price')          as no_verdict_no_price,
        count_if(no_verdict_reason = 'too few comparable sales') as no_verdict_few_comps,
        count_if(no_verdict_reason = 'no comparable sales')      as no_verdict_no_comps
    from {{ ref('mart_listing_valuation') }}
    group by lga_key
)

select
    g.lga_key,
    g.local_government_area,
    g.state_code,

    -- geography resolution
    coalesce(sb.suburbs, 0)                     as suburbs,
    coalesce(sb.suburbs_matched_exact, 0)       as suburbs_matched_exact,
    coalesce(sb.suburbs_matched_by_name, 0)     as suburbs_matched_by_name,
    coalesce(sb.suburbs_matched_by_override, 0) as suburbs_matched_by_override,
    coalesce(sb.suburbs_unmatched, 0)           as suburbs_unmatched,

    -- volumes by channel
    coalesce(ch.sold_listings, 0)               as sold_listings,
    coalesce(ch.buy_listings, 0)                as buy_listings,
    -- Counted, never surfaced. 49 rentals dataset-wide is why yield is deferred.
    coalesce(ch.rent_listings, 0)               as rent_listings,

    -- DEPTH: how far the sold history actually reaches, and whether the cap cut
    -- it short. A truncated LGA's 1,500 sales are the most RELEVANT ones the
    -- source held, not a random sample, so the median is biased and not merely
    -- noisier.
    coalesce(sa.sales, 0)                       as sales,
    sa.earliest_sale_date,
    sa.latest_sale_date,
    cast(months_between(rd.crawled_on, sa.earliest_sale_date) / 12 as decimal(5, 1)) as history_years,
    coalesce(tr.is_truncated_history, true)     as is_truncated_history,
    -- Partial truncation: at least one supplying crawl was capped while another
    -- finished. The LGA's history is whole, but unevenly sourced.
    coalesce(tr.any_supplier_truncated, false)  as any_supplier_truncated,
    coalesce(ch.supplying_crawl_regions, 0)     as supplying_crawl_regions,

    -- known gaps in the sold data itself
    coalesce(sa.sales_without_date, 0)                    as sales_without_date,
    coalesce(sa.sales_with_withheld_price, 0)             as sales_with_withheld_price,
    coalesce(sa.sales_without_identified_address, 0)      as sales_without_identified_address,
    coalesce(sa.sales_consolidated_from_duplicates, 0)    as sales_consolidated_from_duplicates,

    -- valuation answerability
    coalesce(vl.live_listings, 0)               as live_listings,
    coalesce(vl.listings_with_verdict, 0)       as listings_with_verdict,
    coalesce(vl.no_verdict_no_price, 0)         as no_verdict_no_price,
    coalesce(vl.no_verdict_few_comps, 0)        as no_verdict_few_comps,
    coalesce(vl.no_verdict_no_comps, 0)         as no_verdict_no_comps,

    -- TIME: the count that gates every day-over-day measure. While this is 1,
    -- days-on-market, price reductions, discount-to-asking and listing velocity
    -- are all uncomputable, not merely imprecise.
    coalesce(ch.crawl_dates, 0)                 as crawl_dates,
    ch.first_crawled_on,
    ch.last_crawled_on,
    coalesce(ch.crawl_dates, 0) > 1             as supports_time_series,

    coalesce(sa.sales, 0) >= {{ var('scorecard_min_sales') }} as is_reportable

from {{ ref('dim_lga') }} g
cross join reference_date rd
left join suburbs   sb on g.lga_key = sb.lga_key
left join channels  ch on g.lga_key = ch.lga_key
left join sales     sa on g.lga_key = sa.lga_key
left join valuation vl on g.lga_key = vl.lga_key
left join truncation tr on g.lga_key = tr.lga_key
where g.has_listing_data
