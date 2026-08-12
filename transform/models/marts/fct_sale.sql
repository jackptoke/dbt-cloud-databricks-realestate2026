{{ config(materialized = 'table') }}

-- Grain: one row per completed SALE. 20 years of history, and a property can
-- appear many times — that repeat-sales structure is the point.
--
-- Reaching that grain takes three collapses, and they remove different things.

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

same_date_collapsed as (
    select
        *,
        -- How many source records were collapsed at this stage. Summed across
        -- the next collapse too, so the published count is the total.
        count(*) over (partition by sale_group) as same_date_records
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
),

-- (3) The same sale reported on ADJACENT DAYS rather than the same day. The
-- collapse above keys on an exact date match, so two feeds that disagree by
-- 24 hours produce two sales of one dwelling: 239 Western Highway sold for
-- $425,000 on both 2022-03-16 and 2022-03-17, and Lot 2/9 Krause Road for
-- $720,000 on 2025-03-03 and 2025-03-21 under ids in two different bands.
--
-- PRICE EQUALITY is the whole test, and the reason this collapse is safe.
-- Selling the same dwelling twice inside a month is unlikely but perfectly
-- possible — a quick flip, or a fall-through and immediate resale — and such a
-- pair would be real history worth keeping. What makes it implausible is the
-- price landing on exactly the same figure. So a pair that disagrees on price
-- survives as two sales: 59 High Street at $207,000 then $230,000 four days
-- later stays two rows, because nothing in the data can tell a fast resale
-- from a corrected figure, and inventing an answer would be worse than keeping
-- both.
--
-- A withheld or absent price is NOT a matching price. It is an unknown one,
-- and an unknown cannot establish that two records describe one event, so
-- those rows are excluded from this collapse and remain separate sales. Three
-- pairs currently sit in that state.
--
-- Gaps-and-islands over (property, price) rather than a self-join: a property
-- genuinely sold twice at the same price years apart must not merge, and a
-- session boundary on the day gap expresses that directly.
near_duplicate_flags as (
    select
        *,
        case
            -- Nothing to compare on: keep as its own sale.
            when sold_date is null
              or is_price_withheld
              or sale_price_low_aud is null
            then 1
            when datediff(
                     sold_date,
                     lag(sold_date) over (
                         partition by property_key, sale_price_low_aud
                         order by sold_date
                     )
                 ) <= {{ var('sale_duplicate_window_days') }}
            then 0
            else 1
        end as starts_new_sale
    from same_date_collapsed
),

sessionised as (
    select
        *,
        sum(starts_new_sale) over (
            partition by property_key, sale_price_low_aud
            order by sold_date
            rows between unbounded preceding and current row
        ) as sale_session
    from near_duplicate_flags
),

sales as (
    select
        *,
        sum(same_date_records) over (
            partition by property_key, sale_price_low_aud, sale_session
        ) as source_listing_count
    from sessionised
    qualify row_number() over (
        partition by property_key, sale_price_low_aud, sale_session
        -- Earliest date wins: the first report of a sale, with the later copy
        -- treated as the republication. Which of two adjacent dates is the
        -- contract and which the settlement is not knowable from this feed, so
        -- this is a stable convention rather than a claim about the truth.
        order by sold_date, listing_id desc
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
