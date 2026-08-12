{{ config(materialized = 'table') }}

-- Grain: one row per LGA x sold year x property type.
--
-- The LGA-grain successor to mart_suburb_price_trend. Both are kept: this one
-- backs the dashboard, the suburb version stays for drill-down.
--
-- An AGGREGATE fact, which is what the mart_ prefix warns about — coarser than
-- fct_sale, so never join it to anything at listing grain.
--
-- Quartiles, not just the median. A median drawn from five sales spread over a
-- $400k range is not a central tendency, and nothing in a median column says
-- so. sales + q1 + q3 let a reader see that for themselves.
--
-- TRUNCATION IS EXACT AT THIS GRAIN, and that is the main reason the model
-- exists. The source caps a region query at 1,500 results, and each crawl
-- region resolves to one LGA, so the cap applies per LGA. The suburb-grain
-- version had to approximate it with bool_and over whichever crawls happened to
-- supply a suburb; here the unit of measurement is the unit of collection.
--
-- Read every series against is_truncated_history. Truncation is INVERSE to
-- market activity — the busiest LGA is capped soonest and therefore has the
-- shallowest history. Horsham's 1,496 sales reach back only to September 2022,
-- while Pyrenees, quiet enough to sit under the cap, is complete from 2007. A
-- cross-LGA comparison of any long window is therefore comparing different
-- spans of time, not different markets.

with sales as (
    select
        l.lga_key,
        s.property_type_key,
        s.sale_price_aud,
        year(s.sold_date) as sold_year
    from {{ ref('fct_sale') }} s
    join {{ ref('dim_location') }} l on s.location_key = l.location_key
    where s.sold_date is not null
      and s.sale_price_aud is not null
),

-- Truncation is NOT recomputed here. mart_data_coverage owns the one
-- definition, derived from int_crawl_coverage, because the ceiling applies to a
-- CRAWL rather than to whichever slice of it an LGA received — deriving it
-- locally from this model's own rows is what previously had two marts
-- disagreeing about Ararat.
coverage as (
    select lga_key, is_truncated_history, supplying_crawl_regions
    from {{ ref('mart_data_coverage') }}
),

latest_crawl as (
    select max(year(crawled_on)) as current_year
    from {{ ref('int_listings_unioned') }}
)

select
    s.sold_year,

    -- conformed foreign keys
    s.lga_key,
    s.property_type_key,

    -- Attributes taken from the dimensions the keys point at, never from
    -- dim_property: that is Type 1, so a dwelling relisted under a different
    -- type would carry only its latest one and split a single group into two
    -- rows with disagreeing text.
    g.local_government_area,
    g.state_code,
    t.property_type,

    -- measures. sale_price_aud is the LOW end of a price range where the source
    -- gave one, so a year with range-priced sales reads slightly low. It is a
    -- floor rather than a midpoint.
    count(*)                                            as sales,
    min(s.sale_price_aud)                               as min_price_aud,
    cast(percentile(s.sale_price_aud, 0.25) as bigint)  as q1_price_aud,
    cast(median(s.sale_price_aud) as bigint)            as median_price_aud,
    cast(percentile(s.sale_price_aud, 0.75) as bigint)  as q3_price_aud,
    max(s.sale_price_aud)                               as max_price_aud,

    coalesce(c.is_truncated_history, true)              as is_truncated_history,
    coalesce(c.supplying_crawl_regions, 0)              as supplying_crawl_regions,

    -- The current year is a part year and must not be charted as though it were
    -- complete. Derived from the crawl rather than hardcoded so it stays true
    -- as the pipeline accumulates.
    s.sold_year >= lc.current_year                      as is_partial_year

from sales s
join {{ ref('dim_lga') }}           g on s.lga_key           = g.lga_key
join {{ ref('dim_property_type') }} t on s.property_type_key = t.property_type_key
left join coverage c on s.lga_key = c.lga_key
cross join latest_crawl lc
group by
    s.sold_year, s.lga_key, s.property_type_key,
    g.local_government_area, g.state_code, t.property_type,
    c.is_truncated_history, c.supplying_crawl_regions, lc.current_year
