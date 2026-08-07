{{ config(materialized = 'table') }}

select
    listing_agent_key,
    listing_id,
    {{ dbt_utils.generate_surrogate_key(['agent_id']) }} as agent_key,
    channel
from {{ ref('int_listing_agents') }}
