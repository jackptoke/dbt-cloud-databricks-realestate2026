{{ config(materialized = 'table') }}

-- Grain: one row per live for-sale listing, as at the most recent crawl.
--
-- Answers "is this priced above what comparable properties actually sold for?"
-- — the one question in the set that needs user input, and therefore the reason
-- the front end is an app rather than a dashboard.
--
-- Comparables are (LGA, property type, bedrooms) over a recent window. LGA
-- rather than suburb on purpose: the listings API is queried by region and each
-- crawl region resolves to a single LGA, so the LGA is the unit the data is
-- collected at. It also fills the cells — at suburb grain only 452 of 674
-- listings reach five comparables, at LGA grain 567 do.
--
-- Where the evidence runs out the mart says so. Below the minimum comparable
-- count every derived figure is null and `has_verdict` is false, rather than a
-- median of two sales dressed up as a valuation.
--
--
-- TIME INDEXATION — why the comparable median is not a plain median.
--
-- An asking price is quoted today; a comparable sold up to two and a half years
-- ago, with a median staleness of 1.19 years. In a market compounding at 7-13%
-- depending on the LGA, comparing the two untouched builds a systematic upward
-- bias into every variance figure: measured on raw medians, the median listing
-- came out +21.7% above comparables and the symmetric band called 58% of the
-- market overpriced. Most of that was the calendar, not the vendor.
--
-- So each comparable sale is indexed forward to the crawl date at its own LGA's
-- repeat-sales growth rate before the median is taken. Indexing the individual
-- SALES rather than the resulting median matters, because the sales in a cell
-- have different ages and a median of mixed vintages has no single age to
-- correct by.
--
-- The rate comes from mart_lga_growth_rate, which derives it from repeat sales
-- — the same dwelling sold twice — rather than from a median-price series that
-- would move with the mix of what sold.
--
-- Both medians are published. `comparable_median_price_aud` is the raw
-- observed figure and `comparable_median_indexed_aud` is what the verdict is
-- computed against, so the size of the adjustment is always visible and a
-- reader who distrusts it can fall back to the raw number.
--
-- What indexation does NOT remove: asking prices carry negotiating room, and
-- live stock is a different mix from recently-sold stock. Some positive skew is
-- therefore real and should survive. The verdict is still better read as a
-- ranking than as an accusation.
--
-- KNOWN LIMIT — location within an LGA. Comparables are pooled across the whole
-- LGA, so a premium pocket is priced against its whole council area. A Halls
-- Gap lifestyle block reads +1,242% against a Northern Grampians median that
-- includes Stawell, and a McKenzie Creek house reads +300% against Horsham.
-- Both are location premium, not mispricing.
--
-- This is the deliberate trade made when the grain moved from suburb to LGA:
-- suburb-level comparables reflect location but reach only 452 of 674 listings,
-- against 567 at LGA level. Narrowing the geography would trade a bias that is
-- visible and explainable for a coverage gap that is neither. Revisit once the
-- crawl footprint is wide enough that suburb cells fill.

with latest_crawl as (
    -- Only the most recent observation of each listing. Today there is one
    -- crawl date and this is a no-op; once history accumulates it is what keeps
    -- the mart describing the CURRENT market rather than every listing ever
    -- advertised.
    select
        max(crawled_date_key)                                          as crawled_date_key,
        to_date(cast(max(crawled_date_key) as string), 'yyyyMMdd')     as crawled_on
    from {{ ref('fct_listing_snapshot') }}
    where channel = 'buy'
),

current_listings as (
    select s.*
    from {{ ref('fct_listing_snapshot') }} s
    join latest_crawl c on s.crawled_date_key = c.crawled_date_key
    where s.channel = 'buy'
),

-- The buy-side equivalent of fct_sale's cross-band dedup (commit b77e316). The
-- source aggregates several upstream feeds, so one property advertised on two
-- portals arrives as two listing_ids — 217 Grampians Road appears as 149758016
-- and 151512868, both at $595,000, and 5025 Western Highway appears once in the
-- ~150M id band and again in the ~700M band.
--
-- Matched on ADDRESS, never on bedrooms. Bedroom counts are agent-entered and
-- agents disagree: a study counted as a fourth bedroom, or a plain typo, would
-- split one property into two rows precisely when the dedup is most needed.
-- property_key already encodes street address, suburb and postcode.
--
-- Address alone is not sufficient either, and the two exceptions are real:
--
--   House-and-land packages. 8 Omaroo Court is a 4-bedroom house at $740,000
--   AND bare residential land at $221,550 — same address, same 758 m2, two
--   genuinely purchasable offerings. Five of these exist.
--
--   Address collisions. Lot 2 Watsons Lane is TWO parcels, 32,800 m2 at
--   $390,000 and 246,000 m2 at $2,000,000, sharing a street-address string and
--   therefore a property_key. Merging them would delete a two-million-dollar
--   listing.
--
-- PRICE is what separates these from genuine duplicates, and the margin is
-- wide. True duplicates sit 0.0%, 0.0%, 0.0% and 9.7% apart; the house-and-land
-- pairs run 66.9% to 77.0% apart and the Watsons Lane parcels 80.5%. There is
-- nothing between 10% and 66%, so a 20% threshold is not a tuned parameter — it
-- sits in an empty band. Land size within 5% is carried as independent
-- corroboration, since two parcels could in principle be priced alike.
--
-- Both guards must agree before two rows are treated as one offering. That bias
-- is deliberate: a false merge silently deletes a real listing, a false split
-- merely leaves a duplicate visible in offer_listing_count.
duplicate_groups as (
    select
        *,
        count(*) over (partition by property_key)          as offer_listing_count,
        min(land_size_m2) over (partition by property_key) as min_land_size_m2,
        max(land_size_m2) over (partition by property_key) as max_land_size_m2,
        min(list_price_low_aud) over (partition by property_key) as min_ask_aud,
        max(list_price_low_aud) over (partition by property_key) as max_ask_aud,
        row_number() over (
            partition by property_key
            order by
                -- A quoted price beats a withheld one: the whole mart is a
                -- price comparison, so the copy without one is useless here.
                case when is_price_withheld or list_price_low_aud is null then 1 else 0 end,
                -- Then the lower asking price. Where two agencies advertise the
                -- same dwelling at different figures — 76 Robins Road at
                -- $720,000 and $650,000 — the lower is the one a buyer can
                -- actually transact at, and it is the conservative choice for a
                -- mart whose output is "is this overpriced".
                list_price_low_aud asc,
                -- Nothing left to judge on. Highest id purely so the winner is
                -- stable across runs, matching fct_sale's tie-break.
                listing_id desc
        ) as offer_rank
    from current_listings
),

live_listings as (
    select *
    from duplicate_groups
    where offer_rank = 1
       -- Either guard failing means these are distinct offerings, so every row
       -- survives rather than only the winner.
       or (min_land_size_m2 is not null and max_land_size_m2 > min_land_size_m2 * 1.05)
       or (min_ask_aud is not null and max_ask_aud > min_ask_aud * 1.20)
),

-- Comparable sales, their band and their indexed value all come from
-- mart_comparable_sale. That model owns the definition so this mart and the
-- dashboard's evidence table cannot drift apart — see its header.
indexed_sales as (
    select * from {{ ref('mart_comparable_sale') }}
),

comparables as (
    select
        lga_key,
        property_type_key,
        comparable_band,
        count(*)                                             as comparable_sales,
        cast(median(sale_price_aud) as bigint)               as comparable_median_price_aud,
        cast(median(indexed_price_aud) as bigint)            as comparable_median_indexed_aud,
        cast(percentile(indexed_price_aud, 0.25) as bigint)  as comparable_q1_indexed_aud,
        cast(percentile(indexed_price_aud, 0.75) as bigint)  as comparable_q3_indexed_aud,
        cast(median(age_years) as decimal(5, 2))             as comparable_median_age_years,
        max(applied_growth_pct)                              as applied_growth_pct,
        max(growth_rate_source)                              as growth_rate_source
    from indexed_sales
    group by lga_key, property_type_key, comparable_band
),

joined as (
    select
        s.listing_snapshot_key,
        s.listing_id,
        s.property_key,
        s.location_key,
        l.lga_key,
        s.property_type_key,
        s.crawled_date_key,

        l.suburb,
        l.postcode,
        l.state_code,
        g.local_government_area,
        t.property_type,
        t.is_land_or_rural,

        s.bedrooms,
        s.bathrooms,
        s.parking_spaces,
        s.land_size_m2,
        {{ comparable_band('t.is_land_or_rural', 's.bedrooms', 's.land_size_m2') }} as comparable_band,

        -- The LOW end of an advertised range, matching fct_sale.sale_price_aud,
        -- which is also the low end where the source gave a range. Comparing
        -- like with like matters more here than picking the "fairest" figure:
        -- a midpoint asking price against a low-end sold price would read as
        -- systematic overpricing that is really just two different measures.
        -- list_price_high_aud is carried so a consumer can take the other view.
        s.list_price_low_aud  as asking_price_aud,
        s.list_price_high_aud as asking_price_high_aud,
        s.is_price_withheld,
        s.is_auction,

        -- How many source listings this row stands for. 1 for almost
        -- everything; >1 marks a cross-feed duplicate that was collapsed, so
        -- the dedup is visible in the data rather than only in a comment —
        -- the same convention fct_sale.source_listing_count follows.
        s.offer_listing_count,

        c.comparable_sales,
        c.comparable_median_price_aud,
        c.comparable_median_indexed_aud,
        c.comparable_q1_indexed_aud,
        c.comparable_q3_indexed_aud,
        c.comparable_median_age_years,
        c.applied_growth_pct,
        c.growth_rate_source
    from live_listings s
    join {{ ref('dim_location') }}      l on s.location_key      = l.location_key
    join {{ ref('dim_property_type') }} t on s.property_type_key = t.property_type_key
    join {{ ref('dim_lga') }}           g on l.lga_key           = g.lga_key
    -- LEFT: a listing with no comparable cell is still a listing. It appears
    -- with has_verdict = false rather than dropping out of the mart, so the
    -- coverage figure on the dashboard is honest about its own denominator.
    left join comparables c
           on l.lga_key           = c.lga_key
          and s.property_type_key = c.property_type_key
          and {{ comparable_band('t.is_land_or_rural', 's.bedrooms', 's.land_size_m2') }} = c.comparable_band
),

assessed as (
    select
        *,
        coalesce(comparable_sales, 0) >= {{ var('valuation_min_comparables') }}
            and asking_price_aud is not null
            and not is_price_withheld
            as has_verdict
    from joined
)

select
    listing_snapshot_key,
    listing_id,

    -- conformed foreign keys
    property_key,
    location_key,
    lga_key,
    property_type_key,
    crawled_date_key,

    suburb,
    postcode,
    state_code,
    local_government_area,
    property_type,

    bedrooms,
    bathrooms,
    parking_spaces,
    land_size_m2,
    comparable_band,

    asking_price_aud,
    asking_price_high_aud,
    is_price_withheld,
    is_auction,
    offer_listing_count,

    -- Null unless the verdict stands up. Every derived measure below is gated
    -- on has_verdict rather than merely flagged by it, so a consumer that
    -- ignores the flag still cannot read a number that was never supported.
    case when has_verdict then coalesce(comparable_sales, 0) end as comparable_sales,
    case when has_verdict then comparable_median_price_aud end   as comparable_median_price_aud,
    case when has_verdict then comparable_median_indexed_aud end as comparable_median_indexed_aud,
    case when has_verdict then comparable_q1_indexed_aud end     as comparable_q1_indexed_aud,
    case when has_verdict then comparable_q3_indexed_aud end     as comparable_q3_indexed_aud,

    -- How much work the indexation did. A reader can undo it from these two
    -- columns alone, which is the point of publishing them.
    case when has_verdict then comparable_median_age_years end   as comparable_median_age_years,
    case when has_verdict then applied_growth_pct end            as applied_growth_pct,
    case when has_verdict then growth_rate_source end            as growth_rate_source,

    -- Against the INDEXED median. The raw comparison is still recoverable from
    -- asking_price_aud and comparable_median_price_aud above.
    case
        when has_verdict then cast(
            (asking_price_aud / nullif(comparable_median_indexed_aud, 0) - 1) * 100
            as decimal(10, 2)
        )
    end as variance_from_comparable_pct,

    case
        when not has_verdict then null
        when asking_price_aud / nullif(comparable_median_indexed_aud, 0) - 1
             > {{ var('valuation_in_line_band_pct') }} / 100.0 then 'above comparables'
        when asking_price_aud / nullif(comparable_median_indexed_aud, 0) - 1
             < -{{ var('valuation_in_line_band_pct') }} / 100.0 then 'below comparables'
        else 'in line'
    end as valuation_verdict,

    -- Why a listing has no verdict, so the dashboard can say which of the three
    -- it is instead of a bare "unavailable".
    case
        when has_verdict then null
        when is_price_withheld or asking_price_aud is null then 'no asking price'
        -- Split by WHY the band is null. Land bands on area and dwellings on
        -- bedrooms, so one message cannot serve both: telling the owner of a
        -- bedroom-less house that it is "a land or rural listing" is simply
        -- false, and the two are fixed by different missing fields.
        when comparable_band is null and is_land_or_rural then 'no land size recorded'
        when comparable_band is null then 'no bedroom count recorded'
        when coalesce(comparable_sales, 0) = 0 then 'no comparable sales'
        else 'too few comparable sales'
    end as no_verdict_reason,

    has_verdict
from assessed
