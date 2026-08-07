{{ config(materialized = 'table') }}

with types as (
    select distinct property_type
    from {{ ref('int_listings_unioned') }}
    where property_type is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['property_type']) }} as property_type_key,
    property_type,
    initcap(property_type) as property_type_name,
    property_type in ('residential land', 'acreage/semi-rural', 'mixed farming', 'lifestyle')
        as is_land_or_rural
from types
