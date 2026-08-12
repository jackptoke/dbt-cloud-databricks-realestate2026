{{ config(materialized = 'table') }}

-- Suburb-level geography. Centroid is averaged over listings with coordinates;
-- ~7% of sold listings have none, so they simply don't contribute.
--
-- Also the point where each suburb is resolved to its Local Government Area,
-- against the vendored ABS reference in stg_suburbs. LGA is the level the
-- marts report at: the listings API is queried with type=region, and each
-- crawl region resolves to one LGA at 95-100%, so the LGA is the grain the
-- data is actually collected at rather than an arbitrary rollup. Suburb grain
-- is kept here so drill-down stays available.

-- One row per listing before averaging, not one per listing per crawl:
-- otherwise a dwelling that sat on market for 60 nights would pull the centroid
-- 60 times harder than one that sold in a day, and the centroid would drift
-- every night for reasons that have nothing to do with geography.
with listings as (
    select *
    from {{ ref('int_listings_unioned') }}
    qualify row_number() over (
        partition by listing_id
        order by crawled_on desc, channel
    ) = 1
),

aggregated as (
    select
        suburb,
        postcode,
        state_code,
        max(locality)  as locality,
        avg(latitude)  as centroid_latitude,
        avg(longitude) as centroid_longitude
    from listings
    where suburb is not null
    group by suburb, postcode, state_code
),

-- The listings feed writes 'Vic'; the reference file writes 'VIC'. Case is the
-- only difference left to reconcile — postcode arrives already normalised by
-- the staging layer's normalise_postcode macro, on both sides of this join, so
-- it is used as-is rather than reformatted here.
targets as (
    select
        *,
        lower(trim(suburb))     as suburb_norm,
        upper(trim(state_code)) as state_code_norm
    from aggregated
),

reference as (select * from {{ ref('stg_suburbs') }}),

-- The set of LGAs that genuinely exist, used to validate overrides below.
lga_list as (
    select distinct lga_key, local_government_area, local_government_area_norm, state_code
    from reference
),

-- Tier 1: exact match on (suburb, postcode, state).
--
-- Collapsed to that grain first because stg_suburbs is per-SSC and two ABS
-- polygons can share a name and postcode — Green Point NSW 2251. Joining
-- without this would duplicate the suburb and double every measure hung off it.
exact_lookup as (
    select
        suburb_norm,
        postcode,
        state_code,
        -- Deterministic pick by population where two polygons disagree. They
        -- agree in the current file; lga_candidates makes a future
        -- disagreement visible instead of arbitrary.
        max_by(lga_key, population)               as lga_key,
        count(distinct lga_key)                   as lga_candidates,
        sum(population)                           as population,
        sum(area_sqkm)                            as area_sqkm,
        max_by(median_income, population)         as median_income,
        max_by(elevation_m, population)           as elevation_m
    from reference
    group by 1, 2, 3
),

-- Tier 2: the postcode did not match but the name is unambiguous within the
-- state. Rescues source postcode errors — Beaufort arrives on 3469, which is
-- not a Beaufort postcode; the real one is 3373.
--
-- The `having` clause is what makes this safe. Bellfield is 3081 (Banyule, in
-- Melbourne) and 3381 (Northern Grampians, near Halls Gap); a name-only match
-- without the guard would drag a metropolitan LGA into the Grampians. Requiring
-- exactly one candidate LGA excludes precisely that case.
name_lookup as (
    select
        suburb_norm,
        state_code,
        max(lga_key)      as lga_key,
        sum(population)   as population,
        sum(area_sqkm)    as area_sqkm,
        max(median_income) as median_income
    from reference
    group by 1, 2
    having count(distinct lga_key) = 1
),

-- Tier 0, applied FIRST: hand-assigned mappings for localities the reference
-- file omits, and the lever for correcting one it gets wrong.
--
-- Joined to lga_list rather than trusting the seed's spelling. A typo'd LGA
-- name therefore resolves to nothing and the suburb falls through to Unknown,
-- rather than minting a surrogate key that no dim_lga row will ever have.
override_lookup as (
    select
        lower(trim(ov.suburb))                           as suburb_norm,
        {{ normalise_postcode('ov.postcode') }}          as postcode,
        upper(trim(ov.state_code))                       as state_code,
        l.lga_key
    from {{ ref('suburb_lga_overrides') }} ov
    join lga_list l
      on lower(trim(ov.local_government_area)) = l.local_government_area_norm
     and upper(trim(ov.state_code))            = l.state_code
),

resolved as (
    select
        t.*,
        coalesce(o.lga_key, e.lga_key, n.lga_key) as matched_lga_key,
        -- Which tier produced the answer. Carried into the dimension because
        -- the coverage page reports it: a match rate is only meaningful
        -- alongside how the matches were made.
        case
            when o.lga_key is not null then 'override'
            when e.lga_key is not null then 'exact'
            when n.lga_key is not null then 'name'
            else 'unmatched'
        end as lga_match_method,
        coalesce(e.population, n.population)       as population,
        coalesce(e.area_sqkm, n.area_sqkm)         as area_sqkm,
        coalesce(e.median_income, n.median_income) as median_income,
        e.elevation_m,
        coalesce(e.lga_candidates, 0)              as reference_lga_candidates
    from targets t
    left join override_lookup o
      on t.suburb_norm = o.suburb_norm
     and t.postcode = o.postcode
     and t.state_code_norm = o.state_code
    left join exact_lookup e
      on t.suburb_norm = e.suburb_norm
     and t.postcode = e.postcode
     and t.state_code_norm = e.state_code
    left join name_lookup n
      on t.suburb_norm = n.suburb_norm
     and t.state_code_norm = n.state_code
)

select
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode', 'state_code']) }} as location_key,

    -- Mandatory FK. Unresolved suburbs point at dim_lga's Unknown member rather
    -- than carrying a null, matching how dim_agent, dim_agency and dim_date
    -- already handle theirs.
    coalesce(matched_lga_key, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as lga_key,
    lga_match_method,

    suburb,
    locality,
    postcode,
    state_code,
    centroid_latitude,
    centroid_longitude,

    -- Suburb-grain demographics from the reference file. Null where the suburb
    -- did not resolve. Median income is a genuine per-suburb median here —
    -- unlike the LGA-level figure in dim_lga, which can only be approximated.
    population,
    area_sqkm,
    median_income,
    elevation_m,

    -- How many distinct LGAs the reference offered for this (suburb, postcode,
    -- state). Always 1 today. Selected rather than left in the CTE because the
    -- exact_lookup comment claims this makes a future disagreement visible, and
    -- a counter that never leaves the query makes nothing visible —
    -- assert_no_ambiguous_suburb_lga is what turns it into a signal.
    reference_lga_candidates
from resolved
