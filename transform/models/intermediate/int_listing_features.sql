{{ config(materialized = 'view') }}

-- One row per (listing, feature). propertyFeatures is an array of sections each
-- holding an array of strings, so this is a double explode. Roughly half the
-- strings encode a count ("Garage: 2"); the rest are bare flags.

{% set channels = ['sold', 'buy', 'rent'] %}

with exploded as (
{% for channel in channels %}
    select
        listing_id,
        channel,
        section.section as feature_section,
        section.label   as feature_group,
        feature         as feature_raw,
        _ingested_at
    from {{ ref('stg_' ~ channel ~ '_properties') }}
    lateral view explode(propertyFeatures) a as section
    lateral view explode(section.features)  b as feature
{% if not loop.last %}    union all{% endif %}
{% endfor %}
),

parsed as (
    select
        listing_id,
        channel,
        feature_section,
        feature_group,
        feature_raw,
        case
            when feature_raw like '%: %' then trim(split_part(feature_raw, ': ', 1))
            else trim(feature_raw)
        end as feature_name,
        case
            when feature_raw like '%: %'
                then try_cast(trim(split_part(feature_raw, ': ', 2)) as int)
        end as feature_count,
        _ingested_at
    from exploded
),

deduplicated as (
    select *
    from parsed
    qualify row_number() over (
        partition by listing_id, feature_name order by feature_count desc nulls last
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'feature_name']) }} as listing_feature_key,
    *
from deduplicated
