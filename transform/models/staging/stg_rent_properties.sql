{{ config(materialized = 'table') }}

with source as (
    select * from {{ source('bronze', 'rent_properties') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by listingId
        order by _ingested_at desc, _source_file
    ) = 1
),

price_parsed as (
    select
        *,
        -- Uniform in this channel: "$350 per week". Same extraction as the other
        -- two so the three staging models stay structurally identical.
        transform(
            regexp_extract_all(price.display, '\\$([0-9,]+)', 1),
            amount -> cast(replace(amount, ',', '') as decimal(12, 0))
        ) as price_amounts
    from deduplicated
),

renamed as (
    select
        -- keys
        listingId       as listing_id,
        agencyListingId as agency_listing_id,
        case
            when address.showAddress
             and address.streetAddress <> 'Address available on request'
                then {{ dbt_utils.generate_surrogate_key([
                        'upper(trim(address.streetAddress))',
                        'upper(trim(address.suburb))',
                        'address.postCode'
                    ]) }}
            -- Withheld addresses all carry the literal "Address available on
            -- request", so hashing them merges every withheld listing in a
            -- suburb into one phantom dwelling (39 "sales" in Beaufort).
            -- Give each its own property instead.
            else {{ dbt_utils.generate_surrogate_key(['listingId']) }}
        end as property_key,

        coalesce(address.showAddress, false)
            and address.streetAddress <> 'Address available on request'
            as is_property_identified,
        agency.agencyId as agency_id,
        -- agency attributes (flattened here so the struct's shape can't break
        -- the union the way `listers` did)
        agency.name                  as agency_name,
        agency.email                 as agency_email,
        agency.phoneNumber           as agency_phone,
        agency.website               as agency_website,
        agency.address.streetAddress as agency_street_address,
        agency.address.suburb        as agency_suburb,
        agency.address.state         as agency_state,
        agency.address.postcode      as agency_postcode,
        agency.logo.links.default    as agency_logo_url,
        lister.id       as agent_id,

        -- event
        'rent' as channel,
        ingest_date as crawled_on,

        -- price / tenancy terms
        price.display as price_display,
        try_element_at(price_amounts, 1) as weekly_rent_aud,
        cardinality(price_amounts) = 0   as is_price_withheld,
        bond.value as bond_aud,
        -- NOTE: unlike dateSold.value (ISO), this is display-formatted
        -- ("28 Aug 2026"), so a plain cast silently yields null.
        -- dateAvailable.dateDisplay is often the literal "Available now".
        try_to_date(dateAvailable.date, 'd MMM yyyy') as available_from,
        coalesce(applyOnline, false) as is_apply_online,

        -- address
        address.streetAddress      as street_address,
        address.suburb             as suburb,
        address.locality           as locality,
        address.postCode           as postcode,
        address.subdivisionCode    as state_code,
        address.location.latitude  as latitude,
        address.location.longitude as longitude,
        address.showAddress        as is_address_public,

        -- property
        propertyType as property_type,
        coalesce(features.general.bedrooms,      generalFeatures.bedrooms.value)      as bedrooms,
        coalesce(features.general.bathrooms,     generalFeatures.bathrooms.value)     as bathrooms,
        coalesce(features.general.parkingSpaces, generalFeatures.parkingSpaces.value) as parking_spaces,

        -- listing attributes
        title,
        description,
        prettyUrl    as listing_url,
        productDepth as product_depth,
        _tier        as tier,
        status.label as status_label,
        featured  as is_featured,
        signature as is_signature,
        midtier   as is_midtier,
        standard  as is_standard,

        -- kept nested for the intermediate layer
        listers,
        propertyFeatures,
        inspectionsAndAuctions,

        -- audit
        state  as crawl_state,
        suburb as crawl_suburb,
        _source_file,
        _ingested_at,
        _rescued_data
    from price_parsed
)

select * from renamed
