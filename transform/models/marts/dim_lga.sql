{{ config(materialized = 'table') }}

-- Grain: one row per Local Government Area per state — 538 nationally, plus an
-- Unknown member.
--
-- Built from the full vendored reference rather than filtered to the LGAs we
-- currently hold listings for. A dimension is allowed to describe members no
-- fact points at yet (dim_date already spans to 2028), and adding a state to
-- the crawl should not require rebuilding the geography. `has_listing_data`
-- exists so the dashboard can filter to the handful that matter today without
-- the model pretending the rest do not exist.
--
-- Why this grain is the reporting level: the listings API is queried with
-- type=region, and each crawl region maps to a single LGA at 95-100% —
-- 'ararat' to Ararat (Rural City), 'nhill' to Hindmarsh (Shire), and so on. The
-- LGA is therefore the unit the data is actually COLLECTED at, which makes it
-- the unit the source's 1,500-result cap applies to as well. Aggregating here
-- is reporting at the grain of collection, not smoothing over it.

with reference as (select * from {{ ref('stg_suburbs') }}),

aggregated as (
    select
        lga_key,
        local_government_area,
        state_code,
        max(state_name)                            as state_name,
        max(is_unincorporated)                     as is_unincorporated,

        count(*)                                   as suburb_count,
        sum(population)                            as population,
        sum(area_sqkm)                             as area_sqkm,

        -- Population-weighted MEAN of per-suburb medians, and named that way on
        -- purpose. The reference file publishes median income per suburb, and a
        -- median of medians is not a median — there is no way to recover a true
        -- LGA median from suburb-level summaries. Anything that needs an exact
        -- figure should read median_income from dim_location at suburb grain.
        --
        -- Cast to bigint BEFORE multiplying, not after. Both columns are int,
        -- so Spark types the product int and evaluates it before sum() widens
        -- anything: the largest product in the current file is Point Cook's
        -- 2,131,568,868 against an INT_MAX of 2,147,483,647, which is 0.74% of
        -- headroom. This warehouse runs ANSI mode, so crossing that line throws
        -- ARITHMETIC_OVERFLOW and aborts the build rather than wrapping. The
        -- seed is explicitly meant to be re-downloaded, so one upstream refresh
        -- is all it would take.
        cast(
            sum(cast(median_income as bigint) * cast(population as bigint))
            / nullif(sum(case when median_income is not null then cast(population as bigint) end), 0)
            as int
        )                                          as approx_weighted_mean_income,

        -- Population-weighted centroid: where the people are, not where the
        -- polygon's middle is. An LGA like Hindmarsh is 11,400 km2 of mostly
        -- empty land, so a geometric centre would plot the label in a paddock.
        sum(latitude  * population) / nullif(sum(case when latitude  is not null then population end), 0) as centroid_latitude,
        sum(longitude * population) / nullif(sum(case when longitude is not null then population end), 0) as centroid_longitude,

        false                                      as is_unknown
    from reference
    group by lga_key, local_government_area, state_code
),

-- How much of our own listing footprint sits in each LGA. Kept as a count
-- rather than a bare flag so the coverage page can say "25 of this LGA's
-- suburbs appear in our data" instead of just "yes".
coverage as (
    select
        lga_key,
        count(*) as suburbs_with_listings
    from {{ ref('dim_location') }}
    group by lga_key
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as lga_key,
        'Unknown LGA'          as local_government_area,
        cast(null as string)   as state_code,
        cast(null as string)   as state_name,
        cast(null as boolean)  as is_unincorporated,
        cast(null as bigint)   as suburb_count,
        cast(null as bigint)   as population,
        cast(null as double)   as area_sqkm,
        cast(null as int)      as approx_weighted_mean_income,
        cast(null as double)   as centroid_latitude,
        cast(null as double)   as centroid_longitude,
        true                   as is_unknown
),

members as (
    select * from aggregated
    union all
    select * from unknown_member
)

select
    m.lga_key,
    m.local_government_area,
    m.state_code,
    m.state_name,
    m.is_unincorporated,

    m.suburb_count,
    m.population,
    m.area_sqkm,
    -- Rural Victorian LGAs run well under 10 people per km2; the ratio is the
    -- quickest way to tell a town-centred council from a farming one.
    case
        when m.area_sqkm > 0 then cast(m.population as double) / m.area_sqkm
    end as population_per_sqkm,
    m.approx_weighted_mean_income,

    m.centroid_latitude,
    m.centroid_longitude,

    -- Coverage. Joined after the union so the Unknown member reports its own
    -- footprint too: if suburbs ever fail to resolve, the count lands here
    -- rather than vanishing.
    coalesce(c.suburbs_with_listings, 0) as suburbs_with_listings,

    -- ...but the Unknown member is explicitly NOT a place with listing data,
    -- however many unresolved suburbs pile into it. has_listing_data is the
    -- filter every LGA-grain consumer uses — mart_lga_growth_rate builds its
    -- row set from it — so without this clause a single unresolved suburb would
    -- promote Unknown to a reportable LGA and create one comparable cell
    -- pooling unrelated suburbs from anywhere in the country, each handed a
    -- shared growth rate and a shared median. Silently, and with a
    -- confident-looking verdict on the other end.
    coalesce(c.suburbs_with_listings, 0) > 0 and not m.is_unknown as has_listing_data,

    m.is_unknown
from members m
left join coverage c on m.lga_key = c.lga_key
