{{ config(materialized = 'table') }}

-- Factless fact: the event's occurrence is the fact. Thin for now — 30 of 674
-- buy listings have inspections and only 1 has an auction.

with inspections as (select * from {{ ref('int_listing_inspections') }}),

listings as (
    select listing_id, property_key, suburb, postcode, state_code,
           agent_id, agency_id, property_type
    from {{ ref('int_listings_unioned') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['i.listing_id', 'i.starts_at']) }} as inspection_key,
    i.listing_id,
    i.channel,

    l.property_key,
    {{ dbt_utils.generate_surrogate_key(['l.suburb', 'l.postcode', 'l.state_code']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(l.agent_id, \'-1\')']) }}           as agent_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(l.agency_id, \'-1\')']) }}          as agency_key,
    cast(date_format(i.starts_at, 'yyyyMMdd') as int)                                  as inspection_date_key,

    i.is_auction,
    i.starts_at,
    i.ends_at,
    datediff(minute, i.starts_at, i.ends_at) as duration_minutes
from inspections i
left join listings l on i.listing_id = l.listing_id
