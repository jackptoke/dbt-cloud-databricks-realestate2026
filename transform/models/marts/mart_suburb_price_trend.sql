{{ config(materialized = 'table') }}

-- Grain: one row per location × sold year × property type.
--
-- An AGGREGATE fact, which is what the mart_ prefix warns about: its grain is
-- coarser than fct_sale's, so it must never be joined to dim_property or
-- anything else at listing grain. It carries location_key and
-- property_type_key so it stays conformed to the same dimensions the atomic
-- facts use — joining on the suburb string would work today and break the first
-- time two suburbs share a name across postcodes.
--
-- Quartiles, not just the median. A median drawn from five sales spread over a
-- $400k range is not a central tendency, and nothing in a median column says
-- so. sales + q1 + q3 let a reader see that for themselves.

-- Attributes come from the dimensions the keys point at, NOT from dim_property.
-- Taking the key from fct_sale (what the property was WHEN IT SOLD) and the
-- text from dim_property (what it is NOW) mixes two versions of the same thing:
-- dim_property is Type 1, so a dwelling relisted under a different property
-- type carries only its latest one. That produced two rows for a single
-- (location_key, sold_year, property_type_key) — same key, disagreeing text —
-- and the grain test caught it. Sourcing both from dim_location and
-- dim_property_type makes text and key consistent by construction.
with sales as (
    select
        location_key,
        property_type_key,
        sale_price_aud,
        year(sold_date) as sold_year
    from {{ ref('fct_sale') }}
    where sold_date is not null
      and sale_price_aud is not null
),

-- Truncation is a property of the CRAWL, not of the suburb. Queries resolve to
-- a REGION (realty_au.build_url), so one crawl of "Horsham" returns listings
-- across 25 suburbs and the 1,500-result ceiling applies to the region as a
-- whole. A small suburb can therefore have incomplete history purely because it
-- sits inside a busy region.
--
-- Read from int_listings_unioned rather than fct_sale because _source_file is
-- where the evidence lives: the landing path carries both the crawl region and
-- the page number, so a crawl that reached the page cap is visible in data that
-- is already loaded and tested. No dependency on the backfill markers, which
-- sit in a JSON file beside the landing zone that nothing reads.
crawls as (
    select
        suburb,
        regexp_extract(_source_file, '/suburb=([a-z0-9-]+)/', 1) as crawl_region,
        max(cast(regexp_extract(_source_file, 'page=([0-9]+)[.]jsonl', 1) as int)) as max_page
    from {{ ref('int_listings_unioned') }}
    where channel = 'sold'
      and _source_file is not null
    group by 1, 2
),

coverage as (
    select
        suburb,
        -- Truncated only if EVERY crawl that supplied this suburb hit the cap.
        -- A complete crawl of a region returns every sold listing in it,
        -- including this suburb's, so one complete supplier is enough to make
        -- the suburb's history whole regardless of what other crawls did.
        bool_and(max_page >= {{ var('sold_page_cap') }}) as is_truncated_history,
        count(distinct crawl_region) as supplying_crawl_regions
    from crawls
    group by 1
)

select
    s.sold_year,

    -- conformed foreign keys
    s.location_key,
    s.property_type_key,

    -- dimension attributes, carried for readability. location_key is a hash of
    -- exactly (suburb, postcode, state_code) and property_type_key a hash of
    -- property_type, so these are functionally dependent on the keys and cannot
    -- refine the grain.
    l.suburb,
    l.postcode,
    l.state_code,
    t.property_type,

    -- measures. sale_price_aud is the LOW end of a price range where the source
    -- gave one, so a suburb-year with range-priced sales reads slightly low.
    -- 0.1% of the main id band is range-priced, so the effect is small, but it
    -- is a floor rather than a midpoint.
    count(*)                                            as sales,
    min(s.sale_price_aud)                               as min_price_aud,
    cast(percentile(s.sale_price_aud, 0.25) as bigint)  as q1_price_aud,
    cast(median(s.sale_price_aud) as bigint)            as median_price_aud,
    cast(percentile(s.sale_price_aud, 0.75) as bigint)  as q3_price_aud,
    max(s.sale_price_aud)                               as max_price_aud,

    -- Read the median against this. Four of five crawl regions currently hit
    -- the cap, and the 1,500 they return are the most RELEVANT listings, not a
    -- random sample — so a truncated median is not merely noisier, it is drawn
    -- from a biased subset.
    coalesce(c.is_truncated_history, true)              as is_truncated_history,
    coalesce(c.supplying_crawl_regions, 0)              as supplying_crawl_regions
from sales s
join {{ ref('dim_location') }} l on s.location_key = l.location_key
join {{ ref('dim_property_type') }} t on s.property_type_key = t.property_type_key
left join coverage c on l.suburb = c.suburb
group by
    s.sold_year, s.location_key, s.property_type_key,
    l.suburb, l.postcode, l.state_code, t.property_type,
    c.is_truncated_history, c.supplying_crawl_regions
