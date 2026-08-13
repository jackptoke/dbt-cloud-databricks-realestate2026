{{ config(severity = 'warn') }}

-- Suburbs in the listings feed that no tier of the LGA lookup could place.
--
-- A warning rather than an error, because the failure mode is expansion, not
-- corruption: crawl a new region and any locality the ABS reference omits
-- lands here. The marts stay correct — those suburbs point at dim_lga's
-- Unknown member and are excluded from LGA rollups rather than silently
-- inflating one.
--
-- What to do when it fires: check the suburb is real and sits where you think
-- it does, then add a row to the suburb_lga_overrides seed. Two localities are
-- already there (Grass Flat and Arapiles, both near Natimuk), which is the
-- pattern to follow.
--
-- The count matters more than the presence. Two unmatched suburbs carrying one
-- sale each is noise; twenty carrying hundreds means the reference file's
-- vintage has drifted past the crawl footprint and the seed needs refreshing.

select
    location_key,
    suburb,
    postcode,
    state_code,
    lga_match_method
from {{ ref('dim_location') }}
where lga_match_method = 'unmatched'
