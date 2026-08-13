{{ config(materialized = 'table') }}

-- Grain: one row per property that has sold at least twice.
--
-- An AGGREGATE fact — the mart_ prefix warns that its grain is coarser than
-- fct_sale's. Never join it to fct_sale; a property here summarises two to four
-- rows there.
--
-- This is the strongest evidence of capital growth the dataset holds, because
-- it holds the DWELLING constant. A median-price series compares whatever
-- happened to sell in 2019 against whatever happened to sell in 2025, so it
-- moves when the mix of housing moves, not only when prices do. Comparing a
-- property against itself removes that entirely. It is the same reasoning
-- behind the Case-Shiller index.
--
-- It also depends on work the pipeline already did: fct_sale collapses
-- observations into events and reconciles the four disjoint listing-id bands
-- the source publishes under, so `property_key` genuinely identifies one
-- dwelling across two decades. Without that dedup, a single sale republished
-- under a second listing id would appear here as an instant 0%-growth "repeat".

with qualifying_sales as (
    select
        property_key,
        sale_key,
        sold_date,
        sale_price_aud,
        location_key,
        property_type_key,
        bedrooms,
        bathrooms,
        parking_spaces,
        land_size_m2
    from {{ ref('fct_sale') }}
    -- Identified addresses only. A withheld address gets a property_key derived
    -- from its listing_id, so two withheld sales of one dwelling are already
    -- separate properties here and could never pair up. Including them would
    -- not add pairs, it would only imply the filter was considered and rejected.
    where is_property_identified
      and sold_date is not null
      and sale_price_aud is not null
      -- A withheld price cannot anchor a growth calculation.
      and not is_price_withheld
),

sequenced as (
    select
        *,
        -- sale_key breaks the tie so the sequence is deterministic when a
        -- property has two sales on one date. fct_sale's grain makes that
        -- nearly impossible, but "nearly" is not a guarantee to build ordering
        -- on.
        row_number() over (partition by property_key order by sold_date, sale_key) as sale_seq,
        count(*)     over (partition by property_key)                              as sale_count
    from qualifying_sales
),

paired as (
    select
        property_key,
        max(sale_count) as sale_count,

        min_by(sold_date,      sale_seq) as first_sold_date,
        min_by(sale_price_aud, sale_seq) as first_sale_price_aud,
        max_by(sold_date,      sale_seq) as last_sold_date,
        max_by(sale_price_aud, sale_seq) as last_sale_price_aud,

        -- Dimensionality from the LATEST sale: what the dwelling was when it
        -- last changed hands. Taking it from the first sale would describe a
        -- 2009 listing's view of a property that has since been renovated,
        -- subdivided or reclassified.
        max_by(location_key,      sale_seq) as location_key,
        max_by(property_type_key, sale_seq) as property_type_key,
        max_by(bedrooms,          sale_seq) as bedrooms,
        max_by(bathrooms,         sale_seq) as bathrooms,
        max_by(parking_spaces,    sale_seq) as parking_spaces,
        max_by(land_size_m2,      sale_seq) as land_size_m2
    from sequenced
    where sale_count >= 2
    group by property_key
),

measured as (
    select
        *,
        datediff(last_sold_date, first_sold_date) as days_held,
        -- nullif guards a zero or absent first price. Neither exists in the
        -- current data; both would produce a divide-by-zero rather than a
        -- wrong number, which is the failure mode worth having.
        last_sale_price_aud - first_sale_price_aud                              as price_change_aud,
        (last_sale_price_aud / nullif(first_sale_price_aud, 0) - 1) * 100        as total_growth_pct
    from paired
)

select
    m.property_key,

    -- conformed foreign keys
    m.location_key,
    l.lga_key,
    m.property_type_key,
    coalesce(cast(date_format(m.first_sold_date, 'yyyyMMdd') as int), -1) as first_sold_date_key,
    coalesce(cast(date_format(m.last_sold_date,  'yyyyMMdd') as int), -1) as last_sold_date_key,

    -- carried for readability; functionally dependent on the keys above
    l.suburb,
    l.postcode,
    l.state_code,
    g.local_government_area,
    t.property_type,

    m.sale_count,
    -- Three sales means the window spans more than one ownership, so
    -- "years held" is the property's observed history rather than any single
    -- owner's holding period. 53 of the 601 properties are in this position.
    m.sale_count > 2 as spans_multiple_ownerships,

    m.first_sold_date,
    m.first_sale_price_aud,
    m.last_sold_date,
    m.last_sale_price_aud,

    m.days_held,
    cast(m.days_held / 365.25 as decimal(6, 2))     as years_held,
    m.price_change_aud,
    cast(m.total_growth_pct as decimal(10, 2))      as total_growth_pct,

    -- Compound annual growth rate, and null rather than absurd below the
    -- threshold: a 10% gain over 10 days annualises to more than 3,000%, which
    -- is arithmetically correct and tells the reader nothing true. Total growth
    -- above stays populated for those rows, so a short hold is reported rather
    -- than hidden.
    case
        when m.days_held >= {{ var('repeat_sale_min_annualise_days') }}
             and m.first_sale_price_aud > 0
        then cast(
            (power(m.last_sale_price_aud / m.first_sale_price_aud, 365.25 / m.days_held) - 1) * 100
            as decimal(10, 2)
        )
    end as annualised_growth_pct,

    m.days_held < {{ var('repeat_sale_min_annualise_days') }} as is_short_hold,

    m.bedrooms,
    m.bathrooms,
    m.parking_spaces,
    m.land_size_m2
from measured m
join {{ ref('dim_location') }}      l on m.location_key      = l.location_key
join {{ ref('dim_property_type') }} t on m.property_type_key = t.property_type_key
join {{ ref('dim_lga') }}           g on l.lga_key           = g.lga_key
