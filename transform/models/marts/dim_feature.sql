{{ config(materialized = 'table') }}

with features as (
    select
        feature_name,
        max(feature_section) as feature_section,
        max(feature_group)   as feature_group,
        max(case when feature_count is not null then true else false end) as is_countable
    from {{ ref('int_listing_features') }}
    where feature_name is not null
    group by feature_name
)

select
    {{ dbt_utils.generate_surrogate_key(['feature_name']) }} as feature_key,
    *
from features
