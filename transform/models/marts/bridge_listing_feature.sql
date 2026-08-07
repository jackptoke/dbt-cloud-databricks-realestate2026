{{ config(materialized = 'table') }}

select
    listing_feature_key,
    listing_id,
    {{ dbt_utils.generate_surrogate_key(['feature_name']) }} as feature_key,
    feature_count,
    channel
from {{ ref('int_listing_features') }}
