{{ config(materialized = 'view') }}

-- Only buy and rent carry inspectionsAndAuctions; sold listings have none.

{% set channels = ['buy', 'rent'] %}

{% for channel in channels %}
select
    listing_id,
    channel,
    event.auction                     as is_auction,
    cast(event.startTime as timestamp) as starts_at,
    cast(event.endTime   as timestamp) as ends_at,
    event.dateDisplay                 as date_display,
    _ingested_at
from {{ ref('stg_' ~ channel ~ '_properties') }}
lateral view explode(inspectionsAndAuctions) t as event
{% if not loop.last %}union all{% endif %}
{% endfor %}
