{{ config(materialized = 'table') }}

with listings as (select * from {{ ref('int_listings_unioned') }}),

latest_per_agency as (
    select *
    from listings
    where agency_id is not null
    qualify row_number() over (
        partition by agency_id order by _ingested_at desc
    ) = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['agency_id']) }} as agency_key,
        agency_id,
        agency_name,
        agency_email,
        agency_phone,
        agency_website,
        agency_street_address,
        agency_suburb,
        agency_state,
        agency_postcode,
        agency_logo_url,
        false as is_unknown
    from latest_per_agency
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}, '-1', 'Unknown Agency',
        cast(null as string), cast(null as string), cast(null as string),
        cast(null as string), cast(null as string), cast(null as string),
        cast(null as string), cast(null as string), true
)

select * from final
union all select * from unknown_member
