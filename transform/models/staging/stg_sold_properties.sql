{{ config(materialized = 'table') }}

with source as (
    select * from {{ source('bronze', 'sold_properties') }}
),

deduplicated as (
    -- The scraper pages past the source's 50-page / 1500-result cap, so one
    -- listing can repeat up to 214x within a single ingest_date. Bronze keeps
    -- every copy; silver keeps one.
    select *
    from source
    qualify row_number() over (
        -- Partitioned by (listingId, ingest_date), NOT listingId alone. The
        -- duplication this removes is the scraper paging past the source's
        -- 1500-result cap, which happens WITHIN one crawl. Partitioning by
        -- listingId alone would also collapse the same listing across crawls,
        -- silently reducing fct_listing_snapshot to one row per listing and
        -- destroying the time series every day-over-day measure depends on.
        partition by listingId, ingest_date
        order by _ingested_at desc, _source_file
    ) = 1
),

price_parsed as (
    select
        *,
        -- price.display is free text: "$607,000", "Range: $200,000 - $240,000",
        -- or "Contact agent". Pull out every dollar figure; the 1 / 2 / 0 cases
        -- then fall out of the array length.
        transform(
            regexp_extract_all(price.display, '\\$([0-9,]+)', 1),
            amount -> cast(replace(amount, ',', '') as decimal(12, 0))
        ) as price_amounts
    from deduplicated
),

renamed as (
    select
        -- keys
        listingId as listing_id,
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
        'sold' as channel,
        cast(dateSold.value as date) as sold_date,
        ingest_date as crawled_on,

        -- price
        price.display as price_display,
        try_element_at(price_amounts, 1) as sale_price_low_aud,
        coalesce(
            try_element_at(price_amounts, 2),
            try_element_at(price_amounts, 1)
        ) as sale_price_high_aud,
        cardinality(price_amounts) = 0 as is_price_withheld,

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
        propertyType        as property_type,
        propertyTypeId      as property_type_id,
        propertyTypeDisplay as property_type_display,
        constructionStatus  as construction_status,
        coalesce(features.general.bedrooms,      generalFeatures.bedrooms.value)      as bedrooms,
        coalesce(features.general.bathrooms,     generalFeatures.bathrooms.value)     as bathrooms,
        coalesce(features.general.parkingSpaces, generalFeatures.parkingSpaces.value) as parking_spaces,
        case when landSize.unit = 'm2' then landSize.value end as land_size_m2,
        landSize.display as land_size_display,

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

        -- kept nested for the intermediate layer to explode into bridges
        listers,
        propertyFeatures,

        -- audit
        state  as crawl_state,
        suburb as crawl_suburb,
        _source_file,
        _ingested_at,
        _rescued_data
    from price_parsed
)

select * from renamed
