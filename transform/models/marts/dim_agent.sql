{{ config(materialized = 'table') }}

with agents as (select * from {{ ref('int_listing_agents') }}),

latest_per_agent as (
    select *
    from agents
    qualify row_number() over (
        partition by agent_id order by _ingested_at desc
    ) = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['agent_id']) }} as agent_key,
        agent_id,
        agent_uuid,
        agent_name,
        job_title,
        email,
        phone_number,
        mobile_phone_number,
        website,
        is_power_profile,
        photo_uri,
        false as is_unknown
    from latest_per_agent
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}, '-1',
        cast(null as string), 'Unknown Agent', cast(null as string),
        cast(null as string), cast(null as string), cast(null as string),
        cast(null as string), cast(null as boolean), cast(null as string),
        true
)

select * from final
union all select * from unknown_member
