{{ config(materialized = 'table') }}

-- Grain: one row per LGA that has listing data. The dashboard's landing table.
--
-- Joins the other marts into the handful of numbers an investor reads first:
-- what does it cost, what has it returned, how much is on the market, and how
-- affordable is it against local incomes.
--
-- NO WINDOWED GROWTH KPI, and the omission is the considered part of this
-- model. The obvious headline would be "10-year growth", but the source caps
-- each region query at 1,500 results and the cap bites hardest where the market
-- is busiest, so history depth runs INVERSE to activity:
--
--     Horsham (Rural City)        1,496 sales     back to 2022-09    3.9 yrs
--     Northern Grampians (Shire)  1,428 sales     back to 2019-03    7.4 yrs
--     Ararat (Rural City)         1,539 sales     back to 2017-08    9.0 yrs
--     Hindmarsh (Shire)           1,498 sales     back to 2008-10   17.8 yrs
--     Pyrenees (Shire)              807 sales     back to 2007-06   19.1 yrs
--
-- Three of the five cannot reach back ten years at all, and Horsham cannot
-- reach five. Any windowed comparison would silently compare different spans of
-- time across LGAs and read as a market difference. So growth here is the
-- repeat-sales CAGR from mart_lga_growth_rate: a per-property rate rather than a
-- window difference, which makes it the only growth figure comparable across
-- all five. history_years and earliest_sale_date are published beside it so the
-- depth behind every other measure stays visible.

with reference_date as (
    -- "Now" is the crawl, not the wall clock. A model whose KPIs shift because
    -- it was rebuilt on a different day is not reproducible.
    select
        max(crawled_on)                                as crawled_on,
        add_months(max(crawled_on), -12)               as twelve_months_ago
    from {{ ref('int_listings_unioned') }}
),

sales as (
    select
        l.lga_key,
        s.sale_price_aud,
        s.sold_date
    from {{ ref('fct_sale') }} s
    join {{ ref('dim_location') }} l on s.location_key = l.location_key
    where s.sold_date is not null
      and s.sale_price_aud is not null
      and not s.is_price_withheld
),

sales_summary as (
    select
        s.lga_key,
        count(*)                                                     as total_sales,
        min(s.sold_date)                                             as earliest_sale_date,
        max(s.sold_date)                                             as latest_sale_date,

        count_if(s.sold_date > r.twelve_months_ago)                  as sales_12m,
        cast(median(case when s.sold_date > r.twelve_months_ago
                         then s.sale_price_aud end) as bigint)       as median_sold_price_12m_aud,
        cast(percentile(case when s.sold_date > r.twelve_months_ago
                             then s.sale_price_aud end, 0.25) as bigint) as q1_sold_price_12m_aud,
        cast(percentile(case when s.sold_date > r.twelve_months_ago
                             then s.sale_price_aud end, 0.75) as bigint) as q3_sold_price_12m_aud
    from sales s
    cross join reference_date r
    group by s.lga_key
),

-- Live supply, from the deduplicated valuation mart rather than raw snapshots,
-- so a property advertised on two portals is counted once.
listings_summary as (
    select
        lga_key,
        count(*)                                            as active_listings,
        cast(median(asking_price_aud) as bigint)            as median_asking_price_aud,
        count_if(has_verdict)                               as listings_with_verdict,
        cast(median(variance_from_comparable_pct) as decimal(6, 2)) as median_variance_from_comparable_pct
    from {{ ref('mart_listing_valuation') }}
    group by lga_key
),

repeat_summary as (
    select
        lga_key,
        count(*)                                          as repeat_sale_properties,
        cast(median(years_held) as decimal(5, 1))         as median_years_held,
        cast(median(total_growth_pct) as decimal(8, 1))   as median_total_growth_pct
    from {{ ref('mart_repeat_sales') }}
    group by lga_key
),

-- One owner for truncation, same as everywhere else.
truncation as (
    select lga_key, is_truncated_history, history_years
    from {{ ref('mart_data_coverage') }}
)

select
    g.lga_key,
    g.local_government_area,
    g.state_code,

    -- Demographics. approx_weighted_mean_income is a population-weighted mean
    -- of per-suburb medians, not a true median — see dim_lga.
    g.population,
    g.area_sqkm,
    g.population_per_sqkm,
    g.approx_weighted_mean_income,
    g.suburbs_with_listings,

    -- Current market
    coalesce(ls.active_listings, 0)             as active_listings,
    ls.median_asking_price_aud,
    ss.sales_12m,
    ss.median_sold_price_12m_aud,
    ss.q1_sold_price_12m_aud,
    ss.q3_sold_price_12m_aud,

    -- Affordability. Against the weighted-mean income above, so it inherits
    -- that approximation — a ratio for ranking LGAs, not a lending calculation.
    cast(
        ss.median_sold_price_12m_aud / nullif(g.approx_weighted_mean_income, 0)
        as decimal(6, 1)
    ) as price_to_income_ratio,

    -- Growth. The repeat-sales rate, comparable across LGAs where a windowed
    -- figure would not be. growth_rate_source says whether it is this LGA's own
    -- evidence or the national fallback.
    gr.applied_growth_pct                       as annual_growth_pct,
    gr.growth_rate_source,
    gr.is_reliable                              as is_growth_rate_reliable,
    coalesce(rs.repeat_sale_properties, 0)      as repeat_sale_properties,
    rs.median_years_held,
    rs.median_total_growth_pct,

    -- Valuation coverage
    coalesce(ls.listings_with_verdict, 0)       as listings_with_verdict,
    ls.median_variance_from_comparable_pct,

    -- History depth. Read every price measure above against these.
    coalesce(ss.total_sales, 0)                 as total_sales,
    ss.earliest_sale_date,
    ss.latest_sale_date,
    tr.history_years,
    coalesce(tr.is_truncated_history, true)     as is_truncated_history,

    -- Below this, an LGA is a handful of stray sales from a neighbouring
    -- region's crawl rather than a market. Loddon, Buloke and Southern
    -- Grampians hold 4, 2 and 1 sales between them; the dashboard defaults to
    -- the reportable five and this is the flag that draws the line.
    coalesce(ss.total_sales, 0) >= {{ var('scorecard_min_sales') }} as is_reportable

from {{ ref('dim_lga') }} g
left join sales_summary    ss on g.lga_key = ss.lga_key
left join listings_summary ls on g.lga_key = ls.lga_key
left join repeat_summary   rs on g.lga_key = rs.lga_key
left join truncation       tr on g.lga_key = tr.lga_key
left join {{ ref('mart_lga_growth_rate') }} gr on g.lga_key = gr.lga_key
where g.has_listing_data
