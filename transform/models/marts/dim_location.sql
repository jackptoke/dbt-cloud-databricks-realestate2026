{{ config(materialized = 'table') }}

-- Suburb-level geography. Centroid is averaged over listings with coordinates;
-- ~7% of sold listings have none, so they simply don't contribute.

with listings as (select * from {{ ref('int_listings_unioned') }}),

aggregated as (
    select
        suburb,
        postcode,
        state_code,
        max(locality)  as locality,
        avg(latitude)  as centroid_latitude,
        avg(longitude) as centroid_longitude
    from listings
    where suburb is not null
    group by suburb, postcode, state_code
)

select
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,
    *
from aggregated
