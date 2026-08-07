{{ config(materialized = 'table') }}

-- One row per physical dwelling. Attributes are taken from the property's most
-- recent listing — a house sold in 2009 and relisted in 2024 should describe
-- itself as it is now.

with listings as (select * from {{ ref('int_listings_unioned') }}),

latest_per_property as (
    select *
    from listings
    qualify row_number() over (
        partition by property_key
        order by coalesce(sold_date, crawled_on) desc, _ingested_at desc
    ) = 1
)

select
    property_key,
    is_property_identified,
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,

    street_address,
    suburb,
    locality,
    postcode,
    state_code,
    latitude,
    longitude,
    is_address_public,

    property_type,
    construction_status,
    land_size_m2,
    bedrooms,
    bathrooms,
    parking_spaces
from latest_per_property
