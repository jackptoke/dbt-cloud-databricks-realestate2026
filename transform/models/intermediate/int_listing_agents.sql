{{ config(materialized = 'view') }}

-- One row per (listing, agent), exploded per channel then unioned. Reading the
-- staging models directly rather than int_listings_unioned keeps the differing
-- `listers` struct shapes out of the conformance layer: rent listings have no
-- agentId field at all.

{% set channels = ['sold', 'buy', 'rent'] %}

with exploded as (
{% for channel in channels %}
    select
        listing_id,
        channel,
        agency_id,
        agent.id                as agent_id,
        {% if channel == 'rent' -%}
        cast(null as string)
        {%- else -%}
        agent.agentId
        {%- endif %} as agent_uuid,
        agent.name              as agent_name,
        agent.jobTitle          as job_title,
        agent.email             as email,
        agent.phoneNumber       as phone_number,
        agent.mobilePhoneNumber as mobile_phone_number,
        agent.website           as website,
        agent.powerProfile      as is_power_profile,
        agent.mainPhoto.uri     as photo_uri,
        _ingested_at
    from {{ ref('stg_' ~ channel ~ '_properties') }}
    lateral view explode(listers) t as agent
{% if not loop.last %}    union all{% endif %}
{% endfor %}
),

filtered as (
    select *
    from exploded
    -- 1,110 of 7,440 exploded rows in sold are empty padding entries
    -- (null id, agentId and name). agent.id is the only reliable key.
    where agent_id is not null
),

deduplicated as (
    -- A listing can name the same agent twice across `lister` and `listers`.
    select *
    from filtered
    qualify row_number() over (
        partition by listing_id, agent_id order by _ingested_at desc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'agent_id']) }} as listing_agent_key,
    *
from deduplicated

