{{ config(materialized = 'table') }}

-- Grain: one row per completed sale. 20 years of history, and a property can
-- appear many times — that repeat-sales structure is the point.

with sales as (
    select *
    from {{ ref('int_listings_unioned') }}
    where channel = 'sold'
    -- One row per event, not per observation. int_listings_unioned carries a
    -- row per listing per crawl date, so without this the same sale would
    -- appear once for every night it was still being advertised.
    qualify row_number() over (
        partition by listing_id order by crawled_on desc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['listing_id']) }} as sale_key,
    listing_id,

    property_key,
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agent_id, \'-1\')']) }}       as agent_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agency_id, \'-1\')']) }}      as agency_key,
    {{ dbt_utils.generate_surrogate_key(['property_type']) }}                    as property_type_key,
    -- 14 sales have no date; they point at the Unknown member rather than null
    coalesce(cast(date_format(sold_date, 'yyyyMMdd') as int), -1)                as sold_date_key,

    sold_date,
    sale_price_low_aud,
    sale_price_high_aud,
    sale_price_low_aud as sale_price_aud,
    is_price_withheld,
    sale_price_low_aud <> sale_price_high_aud as is_price_range,

    bedrooms,
    bathrooms,
    parking_spaces,
    land_size_m2,
    is_property_identified
from sales
