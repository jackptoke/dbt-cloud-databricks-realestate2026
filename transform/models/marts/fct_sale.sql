{{ config(materialized = 'table') }}

-- Grain: one row per completed SALE. 20 years of history, and a property can
-- appear many times — that repeat-sales structure is the point.
--
-- Reaching that grain takes two collapses, and they remove different things.

with sold_listings as (
    select *
    from {{ ref('int_listings_unioned') }}
    where channel = 'sold'
    -- (1) One row per listing, not per observation. int_listings_unioned
    -- carries a row per listing per crawl date, so without this the same sale
    -- would appear once for every night it was still being advertised.
    qualify row_number() over (
        partition by listing_id order by crawled_on desc
    ) = 1
),

grouped as (
    select
        *,
        -- (2) identifies the real-world EVENT rather than the record of it. A
        -- property cannot sell twice on one day, so this is the sale's natural
        -- key.
        --
        -- The source publishes one sale under more than one listingId. It
        -- appears to aggregate several upstream feeds: listing ids fall into
        -- four disjoint bands (<10M legacy, ~130-160M, ~200-210M, ~700M) with
        -- NO ids in between, three of them still live. 34 sales arrive twice,
        -- and 29 of those pairs sit inside a single band — so this is not only
        -- cross-feed duplication, and band membership says nothing about which
        -- copy is right. Same-band pairs disagree on price as readily as
        -- cross-band ones.
        --
        -- An undated sale cannot be grouped with anything: falling back to
        -- listing_id keeps it a singleton instead of merging every undated sale
        -- of a property into one. Withheld-address listings need no special
        -- case — their property_key is already derived from listing_id, so they
        -- are singletons by construction, which is right. You cannot tell
        -- whether two withheld listings are one dwelling.
        concat_ws(
            '|',
            property_key,
            coalesce(cast(sold_date as string), concat('undated-', listing_id))
        ) as sale_group
    from sold_listings
),

sales as (
    select
        *,
        -- How many source records were collapsed into this row. 1 for almost
        -- everything; >1 marks a consolidated sale, so the dedup is visible in
        -- the data rather than only in this comment.
        count(*) over (partition by sale_group) as source_listing_count
    from grouped
    qualify row_number() over (
        partition by sale_group
        order by
            -- Fidelity first, provenance never. An exact price beats a range
            -- ("Range: $150,000 - $180,000" against a flat "$160,000" for the
            -- same Beaufort sale), and any price beats a withheld one.
            case
                when is_price_withheld then 2
                when sale_price_low_aud = sale_price_high_aud then 0
                else 1
            end,
            -- Nothing left to judge on: two equally confident records. Highest
            -- id wins purely so the choice is stable across runs. Where the two
            -- disagree on price, assert_no_conflicting_sale_prices flags it —
            -- this tie-break resolves the row, it does not resolve the truth.
            listing_id desc
    ) = 1
)

select
    -- Keyed on the surviving listing rather than on (property_key, sold_date).
    -- Keeping listing_id in the key preserves the trace back to the exact
    -- source record these numbers came from, and it stays unique because one
    -- listing survives per sale. Trade-off worth knowing: if a better source
    -- record appears for an already-consolidated sale, the winner changes and
    -- so does this key. A natural key over (property_key, sold_date) would be
    -- stable under that churn but would collide across a property's undated
    -- sales, of which there are 14.
    {{ dbt_utils.generate_surrogate_key(['listing_id']) }} as sale_key,
    listing_id,
    source_listing_count,

    property_key,
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agent_id, \'-1\')']) }}       as agent_key,
    {{ dbt_utils.generate_surrogate_key(['coalesce(agency_id, \'-1\')']) }}      as agency_key,
    {{ dbt_utils.generate_surrogate_key(['property_type']) }}                    as property_type_key,
    -- 14 sales have no date; they point at the Unknown member rather than null
    coalesce(cast(date_format(sold_date, 'yyyyMMdd') as int), -1)                as sold_date_key,

    sold_date,
    sale_price_low_aud,
    sale_price_high_aud,
    sale_price_low_aud as sale_price_aud,
    is_price_withheld,
    sale_price_low_aud <> sale_price_high_aud as is_price_range,

    bedrooms,
    bathrooms,
    parking_spaces,
    land_size_m2,
    is_property_identified
from sales
