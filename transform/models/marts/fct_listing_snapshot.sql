{{ config(materialized = 'table') }}

-- Grain: one row per listing per crawl date, across all three channels.
-- Channel-specific prices stay in separate columns — a weekly rent and a sale
-- price are different measures and must never share one.

with listings as (select * from {{ ref('int_listings_unioned') }})

select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'crawled_on']) }} as listing_snapshot_key,

    -- degenerate dimension
    listing_id,
    channel,

    -- foreign keys
    property_key,
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agent_id, \'-1\')']) }}       as agent_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agency_id, \'-1\')']) }}      as agency_key,
    {{ dbt_utils.generate_surrogate_key(['property_type']) }}                    as property_type_key,
    cast(date_format(crawled_on, 'yyyyMMdd') as int)                             as crawled_date_key,

    -- measures
    sale_price_low_aud,
    sale_price_high_aud,
    list_price_low_aud,
    list_price_high_aud,
    weekly_rent_aud,
    bond_aud,
    bedrooms,
    bathrooms,
    parking_spaces,
    land_size_m2,

    -- flags
    is_price_withheld,
    is_auction,
    is_featured,
    is_property_identified,
    status_label
from listings
