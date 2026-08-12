-- Every row in suburb_lga_overrides must name an LGA that actually exists in
-- the reference file, for the state it claims.
--
-- An error, not a warning, and the only test here that is. dim_location joins
-- overrides against the reference list rather than trusting their spelling, so
-- a typo does not corrupt anything — but it does silently do NOTHING, and a
-- hand-written override that quietly fails to apply is worse than one that
-- fails loudly. The whole point of the seed is that someone decided the answer
-- by hand; if that decision is not taking effect, the build should say so.
--
-- Fires on a misspelled council name, a name that has changed since the ABS
-- 2016 vintage, or the right name paired with the wrong state.

with overrides as (
    select
        suburb,
        postcode,
        upper(trim(state_code))                  as state_code,
        lower(trim(local_government_area))       as local_government_area_norm
    from {{ ref('suburb_lga_overrides') }}
),

reference as (
    select distinct
        state_code,
        local_government_area_norm
    from {{ ref('stg_suburbs') }}
)

select o.*
from overrides o
left join reference r
  on o.local_government_area_norm = r.local_government_area_norm
 and o.state_code = r.state_code
where r.local_government_area_norm is null
