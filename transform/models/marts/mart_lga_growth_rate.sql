{{ config(materialized = 'table') }}

-- Grain: one row per LGA that has listing data — the growth rate used to index
-- historical prices forward to today.
--
-- Estimated from repeat sales rather than from a median-price series, because
-- the median-price series moves with the MIX of what sold. Indexing a
-- comparable forward by a mix-contaminated rate would import that contamination
-- into every valuation. Repeat sales hold the dwelling constant, so what is
-- left is closer to price movement alone.
--
-- Median of per-property CAGRs, not a mean: the distribution has a long right
-- tail (a renovated cottage can post 40% a year) and a mean would follow it.
--
-- Estimated over ALL repeat-sale pairs, not only recent ones, which is worth
-- justifying since the indexation only reaches back about a year. Restricting
-- to pairs whose last sale fell in 2022 or later changes the answer by under a
-- percentage point in every LGA — 13.5 to 13.5, 7.5 to 7.0, 8.4 to 8.1, 11.7 to
-- 10.7, 8.3 to 8.3 — so the rate is stable across the window and the larger
-- sample is the better estimator. That agreement is itself the evidence for
-- using a single rate rather than a term structure.

with pairs as (
    select
        lga_key,
        annualised_growth_pct
    from {{ ref('mart_repeat_sales') }}
    -- Null on short holds by construction, which is the intended filter: a
    -- six-week flip says nothing about the market's trend rate.
    where annualised_growth_pct is not null
),

-- The fallback for an LGA with too little repeat-sale evidence of its own.
-- Pooled across every pair rather than averaging the per-LGA medians, so it is
-- weighted by evidence instead of treating a 165-pair LGA and a 3-pair one as
-- equally informative.
national as (
    select median(annualised_growth_pct) as fallback_growth_pct
    from pairs
),

by_lga as (
    select
        lga_key,
        count(*)                                             as repeat_sale_pairs,
        median(annualised_growth_pct)                        as lga_median_growth_pct,
        percentile(annualised_growth_pct, 0.25)              as q1_growth_pct,
        percentile(annualised_growth_pct, 0.75)              as q3_growth_pct
    from pairs
    group by lga_key
)

select
    g.lga_key,
    g.local_government_area,
    g.state_code,

    coalesce(b.repeat_sale_pairs, 0)                    as repeat_sale_pairs,
    cast(b.lga_median_growth_pct as decimal(6, 2))      as lga_median_growth_pct,
    cast(b.q1_growth_pct as decimal(6, 2))              as q1_growth_pct,
    cast(b.q3_growth_pct as decimal(6, 2))              as q3_growth_pct,

    -- The single control on a bad rate. An LGA below the threshold borrows the
    -- national figure rather than indexing everything by a median drawn from
    -- three properties, which is why no separate clamp is needed: the
    -- thin-evidence case never reaches the arithmetic.
    coalesce(b.repeat_sale_pairs, 0) >= {{ var('valuation_min_growth_pairs') }} as is_reliable,

    cast(
        case
            when coalesce(b.repeat_sale_pairs, 0) >= {{ var('valuation_min_growth_pairs') }}
            then b.lga_median_growth_pct
            else n.fallback_growth_pct
        end as decimal(6, 2)
    ) as applied_growth_pct,

    case
        when coalesce(b.repeat_sale_pairs, 0) >= {{ var('valuation_min_growth_pairs') }}
        then 'lga'
        else 'national'
    end as growth_rate_source

from {{ ref('dim_lga') }} g
left join by_lga b on g.lga_key = b.lga_key
cross join national n
-- Only LGAs we actually hold listings in. The other 530 would be rows of pure
-- fallback describing markets this pipeline has never observed.
where g.has_listing_data
