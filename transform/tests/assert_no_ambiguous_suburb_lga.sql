{{ config(severity = 'warn') }}

-- A (suburb, postcode, state) that the reference file places in more than one
-- Local Government Area.
--
-- dim_location's exact_lookup resolves these with max_by(lga_key, population),
-- which is deterministic but arbitrary — it picks the more populous polygon's
-- council with no way of knowing that is the right one. Zero rows today: no
-- group in the current reference has more than one LGA, so the tie-break has
-- never actually fired.
--
-- A warning rather than an error because the pipeline stays correct when it
-- fires — every suburb still resolves to exactly one LGA, just possibly the
-- wrong one of two. What it costs is confidence, not integrity.
--
-- When it fires: check which council the suburb genuinely belongs to and add a
-- row to suburb_lga_overrides. Tier 0 runs ahead of the exact match, so an
-- override settles it permanently rather than leaving the tie-break to decide.

select
    location_key,
    suburb,
    postcode,
    state_code,
    lga_match_method,
    reference_lga_candidates
from {{ ref('dim_location') }}
where reference_lga_candidates > 1
