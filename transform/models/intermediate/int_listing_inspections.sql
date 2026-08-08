{{ config(materialized = 'view') }}

-- Only buy and rent carry inspectionsAndAuctions; sold listings have none.
--
-- Grain: one row per inspection or auction EVENT — deliberately NOT one row per
-- event per crawl. Staging is one row per listing per crawl date, so exploding
-- it directly re-emits the same open-home every night the listing stays
-- advertised: a listing on market for N nights yields N copies of each event,
-- and fct_inspection joins that against N listing rows for N² in total. The
-- event is a fixed real-world occurrence, so the crawl that observed it is
-- metadata about the observation, not part of the grain.

{% set channels = ['buy', 'rent'] %}

with exploded as (
{% for channel in channels %}
    select
        listing_id,
        channel,
        crawled_on,
        event.auction                      as is_auction,
        cast(event.startTime as timestamp) as starts_at,
        cast(event.endTime   as timestamp) as ends_at,
        event.dateDisplay                  as date_display,
        _ingested_at
    from {{ ref('stg_' ~ channel ~ '_properties') }}
    lateral view explode(inspectionsAndAuctions) t as event
    {% if not loop.last %}union all{% endif %}
{% endfor %}
),

deduplicated as (
    select
        *,
        -- The first crawl that advertised this event, kept as an attribute:
        -- how far ahead an agent lists an open home is a real signal, and it
        -- would be lost if only the surviving row's crawl date were carried.
        min(crawled_on) over (
            partition by listing_id, channel, starts_at
        ) as first_seen_on
    from exploded
    qualify row_number() over (
        partition by listing_id, channel, starts_at
        order by crawled_on desc, _ingested_at desc
    ) = 1
)

select
    listing_id,
    channel,
    is_auction,
    starts_at,
    ends_at,
    date_display,
    first_seen_on,
    crawled_on as last_seen_on,
    _ingested_at
from deduplicated
