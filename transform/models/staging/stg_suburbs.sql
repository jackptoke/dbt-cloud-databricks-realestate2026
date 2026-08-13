{{ config(materialized = 'table') }}

-- Reference geography for the whole of Australia: every ABS state suburb (SSC)
-- and the Local Government Area it belongs to. Sourced from a vendored seed,
-- not from the listings API — this model costs no request quota.
--
-- Grain: one row per ssc_code, which is the ONLY unique key in the source.
-- (suburb, postcode, state) is not: Green Point NSW 2251 is two distinct ABS
-- polygons (ssc 11753 and 11754). Anything joining on name and postcode has to
-- collapse this model first or the join fans out — dim_location does exactly
-- that, and keeps the collapse visible rather than hiding it here, because the
-- faithful per-SSC grain is what dim_lga needs to sum populations correctly.

with source as (
    select * from {{ ref('australian_suburbs') }}
),

cleaned as (
    select
        cast(ssc_code as string) as ssc_code,

        -- Two forms of every text join key: the display form and the match
        -- form. Downstream joins use the _norm columns exclusively, so casing
        -- drift in either the listings feed or the reference file cannot
        -- silently drop a match.
        trim(suburb)                                       as suburb_name,
        lower(trim(suburb))                                as suburb_norm,

        -- 313 of 15,286 postcodes lost a leading zero upstream — NT 0886 is
        -- stored as '886'. Irrelevant to the current Victorian footprint (all
        -- 3xxx), wrong everywhere it does apply, and cheaper to correct once
        -- here than in every consumer. Same macro the listings feed uses, so
        -- both sides of every postcode join are normalised identically.
        {{ normalise_postcode('postcode') }}               as postcode,

        -- Jervis Bay, Christmas Island, Norfolk Island and the Cocos Islands
        -- carry an empty state in the source. 'OT' is the ABS code for Other
        -- Territories; leaving it blank would make state_code nullable for no
        -- reason and put a null inside a surrogate key.
        case
            when trim(coalesce(state, '')) = '' then 'OT'
            else upper(trim(state))
        end                                                as state_code,
        nullif(trim(coalesce(state_name, '')), '')         as state_name,

        -- The source header misspells this as `local_goverment_area`. The seed
        -- is kept byte-identical to upstream so it can be re-downloaded and
        -- diffed, so the correction lands here.
        trim(local_goverment_area)                         as local_government_area,
        lower(trim(local_goverment_area))                  as local_government_area_norm,

        nullif(trim(coalesce(urban_area, '')), '')         as urban_area,
        nullif(trim(coalesce(statistic_area, '')), '')     as statistic_area,
        nullif(trim(coalesce(type, '')), '')               as suburb_type,
        nullif(trim(coalesce(timezone, '')), '')           as timezone,

        -- try_cast rather than cast: the current file parses cleanly, but this
        -- is vendored third-party data that will be re-downloaded. A future
        -- row with a stray character should null one measure, not fail the run.
        try_cast(elevation as int)                         as elevation_m,
        try_cast(population as int)                        as population,
        try_cast(median_income as int)                     as median_income,
        try_cast(sqkm as double)                           as area_sqkm,
        try_cast(lat as double)                            as latitude,
        try_cast(lng as double)                            as longitude
    from source
)

select
    ssc_code,

    -- Keyed on (LGA, state), never on LGA name alone. 'Campbelltown (City)'
    -- exists in both NSW and SA, and 'Unincorporated' spans six jurisdictions,
    -- so a name-only key would merge unrelated councils into one dimension row.
    {{ dbt_utils.generate_surrogate_key(['local_government_area_norm', 'state_code']) }} as lga_key,

    suburb_name,
    suburb_norm,
    postcode,
    state_code,
    state_name,

    local_government_area,
    local_government_area_norm,
    -- 'Unincorporated' is not a council; it is the absence of one. Flagged so
    -- rankings and league tables can drop it deliberately.
    local_government_area_norm = 'unincorporated' as is_unincorporated,

    urban_area,
    statistic_area,
    suburb_type,
    timezone,

    elevation_m,
    population,
    median_income,
    area_sqkm,
    latitude,
    longitude
from cleaned
