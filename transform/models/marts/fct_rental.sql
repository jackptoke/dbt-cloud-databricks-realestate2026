{{ config(materialized = 'table') }}

with rentals as (
    select *
    from {{ ref('int_listings_unioned') }}
    where channel = 'rent'
    -- One row per event, not per observation. int_listings_unioned carries a
    -- row per listing per crawl date, so without this the same rental would
    -- appear once for every night it was still being advertised.
    qualify row_number() over (
        partition by listing_id order by crawled_on desc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['listing_id']) }} as rental_key,
    listing_id,

    property_key,
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agent_id, \'-1\')']) }}       as agent_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agency_id, \'-1\')']) }}      as agency_key,
    {{ dbt_utils.generate_surrogate_key(['property_type']) }}                    as property_type_key,
    coalesce(cast(date_format(available_from, 'yyyyMMdd') as int), -1)           as available_from_key,

    available_from,
    weekly_rent_aud,
    weekly_rent_aud * 52 / 12 as monthly_rent_aud,
    bond_aud,
    -- bond is conventionally ~4 weeks' rent; null when the source omits it
    bond_aud / nullif(weekly_rent_aud, 0) as bond_weeks,

    bedrooms,
    bathrooms,
    parking_spaces,
    is_property_identified
from rentals
