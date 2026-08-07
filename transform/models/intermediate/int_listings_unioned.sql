{{ config(materialized = 'view') }}

-- One row per listing across all three channels. This is the conformance point:
-- every dimension downstream reads from here, so an agent or a property means
-- the same thing whether it came from a rental, a sale, or an active listing.
--
-- Channel-specific measures stay in their own columns rather than collapsing
-- into a generic "price" — a weekly rent and a sale price are not the same
-- measure, and merging them would survive every test we have.

with sold as (
    select
        listing_id, property_key, is_property_identified, agency_id, agent_id,
        channel, crawled_on,
        cast(null as string)  as agency_listing_id,

        -- prices
        price_display, is_price_withheld,
        sale_price_low_aud, sale_price_high_aud,
        cast(null as decimal(12, 0)) as list_price_low_aud,
        cast(null as decimal(12, 0)) as list_price_high_aud,
        cast(null as decimal(12, 0)) as weekly_rent_aud,
        cast(null as bigint)         as bond_aud,

        -- dates
        sold_date,
        cast(null as date)      as available_from,
        cast(null as timestamp) as auction_start_at,

        -- address
        street_address, suburb, locality, postcode, state_code,
        latitude, longitude, is_address_public,

        -- property
        property_type, construction_status, land_size_m2,
        bedrooms, bathrooms, parking_spaces,

        -- listing
        title, description, listing_url, product_depth, tier, status_label,
        is_featured, is_signature, is_midtier, is_standard,
        cast(null as boolean) as is_auction,

        -- agency
        agency_name,
        agency_email,
        agency_phone,
        agency_website,
        agency_street_address,
        agency_suburb,
        agency_state,
        agency_postcode,
        agency_logo_url,

        -- audit
        _source_file, _ingested_at
    from {{ ref('stg_sold_properties') }}
),

buy as (
    select
        listing_id, property_key, is_property_identified, agency_id, agent_id,
        channel, crawled_on, agency_listing_id,

        price_display, is_price_withheld,
        cast(null as decimal(12, 0)) as sale_price_low_aud,
        cast(null as decimal(12, 0)) as sale_price_high_aud,
        list_price_low_aud, list_price_high_aud,
        cast(null as decimal(12, 0)) as weekly_rent_aud,
        cast(null as bigint)         as bond_aud,

        cast(null as date) as sold_date,
        cast(null as date) as available_from,
        auction_start_at,

        street_address, suburb, locality, postcode, state_code,
        latitude, longitude, is_address_public,

        property_type, construction_status, land_size_m2,
        bedrooms, bathrooms, parking_spaces,

        title, description, listing_url, product_depth, tier, status_label,
        is_featured, is_signature, is_midtier, is_standard,
        is_auction,

        -- agency
        agency_name,
        agency_email,
        agency_phone,
        agency_website,
        agency_street_address,
        agency_suburb,
        agency_state,
        agency_postcode,
        agency_logo_url,

        _source_file, _ingested_at
    from {{ ref('stg_buy_properties') }}
),

rent as (
    select
        listing_id, property_key, is_property_identified, agency_id, agent_id,
        channel, crawled_on, agency_listing_id,

        price_display, is_price_withheld,
        cast(null as decimal(12, 0)) as sale_price_low_aud,
        cast(null as decimal(12, 0)) as sale_price_high_aud,
        cast(null as decimal(12, 0)) as list_price_low_aud,
        cast(null as decimal(12, 0)) as list_price_high_aud,
        weekly_rent_aud, bond_aud,

        cast(null as date)      as sold_date,
        available_from,
        cast(null as timestamp) as auction_start_at,

        street_address, suburb, locality, postcode, state_code,
        latitude, longitude, is_address_public,

        property_type,
        cast(null as string) as construction_status,
        cast(null as bigint) as land_size_m2,
        bedrooms, bathrooms, parking_spaces,

        title, description, listing_url, product_depth, tier, status_label,
        is_featured, is_signature, is_midtier, is_standard,
        cast(null as boolean) as is_auction,

        -- agency
        agency_name,
        agency_email,
        agency_phone,
        agency_website,
        agency_street_address,
        agency_suburb,
        agency_state,
        agency_postcode,
        agency_logo_url,

        _source_file, _ingested_at
    from {{ ref('stg_rent_properties') }}
)

select * from sold
union all select * from buy
union all select * from rent
