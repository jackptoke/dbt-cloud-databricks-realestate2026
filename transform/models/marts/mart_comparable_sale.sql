{{ config(materialized = 'table') }}

-- Grain: one row per sale that qualifies as a comparable, with its band and its
-- indexed value already computed.
--
-- The evidence layer under mart_listing_valuation. That mart publishes the
-- verdict; this publishes the individual sales the verdict rests on, so the
-- dashboard can show its working without re-deriving anything.
--
-- WHY THIS EXISTS AS A MODEL rather than a CTE. The valuation page lists the
-- comparables beside the verdict they justify, and the app was reproducing the
-- comparable_band macro and the date-window var in Python to fetch them. Two
-- implementations of one definition, and the divergence would have been silent:
-- change `valuation_comparable_from` or a band boundary and the table would
-- quietly list different sales than the median it claims to explain. An audit
-- trail that can drift from what it audits is worse than none.
--
-- So the definition lives here once. mart_listing_valuation aggregates this,
-- and the app filters it. Neither restates the rules.
--
-- Small by construction — one crawl's worth of recent sales, ~2.5k rows — so
-- materialising it costs nothing and the app can read it whole.

with latest_crawl as (
    select
        max(crawled_date_key)                                      as crawled_date_key,
        to_date(cast(max(crawled_date_key) as string), 'yyyyMMdd') as crawled_on
    from {{ ref('fct_listing_snapshot') }}
    where channel = 'buy'
),

-- Sales recent enough to describe the market a buyer is standing in. Withheld
-- and unpriced sales cannot inform a median; a sale with no band cannot be
-- matched to a cell at all.
qualifying as (
    select
        s.sale_key,
        s.property_key,
        l.lga_key,
        s.location_key,
        s.property_type_key,
        {{ comparable_band('t.is_land_or_rural', 's.bedrooms', 's.land_size_m2') }} as comparable_band,
        s.sold_date,
        s.sale_price_aud,
        s.bedrooms,
        s.bathrooms,
        s.land_size_m2,
        c.crawled_on,
        c.crawled_date_key,
        datediff(c.crawled_on, s.sold_date) / 365.25 as years_before_crawl
    from {{ ref('fct_sale') }} s
    join {{ ref('dim_location') }} l      on s.location_key = l.location_key
    join {{ ref('dim_property_type') }} t on s.property_type_key = t.property_type_key
    cross join latest_crawl c
    where s.sold_date >= cast('{{ var("valuation_comparable_from") }}' as date)
      and s.sale_price_aud is not null
      and not s.is_price_withheld
      and {{ comparable_band('t.is_land_or_rural', 's.bedrooms', 's.land_size_m2') }} is not null
)

select
    q.sale_key,
    q.property_key,

    -- conformed keys
    q.lga_key,
    q.location_key,
    q.property_type_key,
    q.crawled_date_key,

    -- the cell this sale informs
    q.comparable_band,

    -- readability
    g.local_government_area,
    dl.suburb,
    dp.street_address,
    t.property_type,
    q.bedrooms,
    q.bathrooms,
    q.land_size_m2,

    q.sold_date,
    q.sale_price_aud,
    cast(q.years_before_crawl as decimal(5, 2)) as age_years,

    -- Indexed forward to the crawl date at the LGA's own repeat-sales rate.
    -- greatest(...,0) guards the direction: a sale dated after the crawl would
    -- otherwise be discounted backwards, and a bad sold_date should not quietly
    -- reduce a comparable.
    gr.applied_growth_pct,
    gr.growth_rate_source,
    cast(
        q.sale_price_aud
        * power(1 + gr.applied_growth_pct / 100.0, greatest(q.years_before_crawl, 0))
        as bigint
    ) as indexed_price_aud,

    -- The configured window floor, published so consumers never restate it.
    cast('{{ var("valuation_comparable_from") }}' as date) as comparable_from_date

from qualifying q
join {{ ref('mart_lga_growth_rate') }} gr on q.lga_key = gr.lga_key
join {{ ref('dim_lga') }} g               on q.lga_key = g.lga_key
join {{ ref('dim_location') }} dl         on q.location_key = dl.location_key
join {{ ref('dim_property') }} dp         on q.property_key = dp.property_key
join {{ ref('dim_property_type') }} t     on q.property_type_key = t.property_type_key
