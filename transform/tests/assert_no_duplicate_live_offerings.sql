-- Two rows in mart_listing_valuation describing what looks like one offering.
--
-- The mart collapses cross-portal duplicates on property_key where land size
-- agrees within 5% AND asking price within 20%. This asserts nothing of that
-- shape survived, so the dedup is verified rather than assumed.
--
-- Deliberately NOT a check that property_key is unique — it isn't, and should
-- not be. A house-and-land package is two real offerings at one address, and
-- Lot 2 Watsons Lane is two parcels sharing an address string. Both are
-- separated by the price or land guard, and both belong in the mart. Testing
-- property_key uniqueness would fail on correct data.
--
-- Bedrooms are deliberately absent from the grouping: agents disagree on
-- whether a study counts, so a bedroom-aware test would mask exactly the
-- duplicates it is supposed to catch.

select
    property_key,
    count(*)                    as surviving_rows,
    min(land_size_m2)           as min_land_size_m2,
    max(land_size_m2)           as max_land_size_m2,
    min(asking_price_aud)       as min_ask_aud,
    max(asking_price_aud)       as max_ask_aud
from {{ ref('mart_listing_valuation') }}
group by property_key
having count(*) > 1
   and (min(land_size_m2) is null or max(land_size_m2) <= min(land_size_m2) * 1.05)
   and (min(asking_price_aud) is null or max(asking_price_aud) <= min(asking_price_aud) * 1.20)
