{{ config(materialized = 'view') }}

-- One row per sold crawl region: how deep it paged, and whether it hit the
-- source's result ceiling.
--
-- THE ONE DEFINITION of crawl truncation. Three marts need it and each had been
-- deriving it locally, which produced two different answers for the same LGA.
--
-- The bug that motivated this: max_page was being computed over only the rows
-- belonging to a given LGA rather than over the crawl as a whole. The 'stawell'
-- crawl paged to 50 and was clearly capped, but its last Ararat-suburb listing
-- happened to fall on page 49 — so Ararat looked like it had one complete
-- supplier and was reported untruncated. The same mistake ran the other way for
-- Buloke, whose 3 stray listings from that capped crawl topped out at page 21
-- and made it look complete.
--
-- The ceiling applies to the CRAWL, not to whichever slice of it a region
-- happens to receive. Aggregating per crawl_region, once, is the whole fix.
--
-- Read from int_listings_unioned rather than fct_sale because _source_file is
-- where the evidence lives: the landing path carries both the crawl region and
-- the page number, so a crawl that reached the cap is visible in data that is
-- already loaded and tested. No dependency on the backfill markers, which sit
-- in a JSON file beside the landing zone that nothing reads.

with sold_pages as (
    select
        regexp_extract(_source_file, '/suburb=([a-z0-9-]+)/', 1)                as crawl_region,
        cast(regexp_extract(_source_file, 'page=([0-9]+)[.]jsonl', 1) as int)   as page_number,
        listing_id,
        crawled_on
    from {{ ref('int_listings_unioned') }}
    where channel = 'sold'
      and _source_file is not null
)

select
    crawl_region,
    max(page_number)                as max_page,
    count(distinct listing_id)      as listings,
    min(crawled_on)                 as first_crawled_on,
    max(crawled_on)                 as last_crawled_on,

    -- A crawl that landed this many pages exhausted the source's 1,500-result
    -- budget, so whatever it did not return is missing rather than absent. The
    -- 1,500 it did return are the most RELEVANT listings, not a random sample,
    -- which makes a truncated series biased and not merely noisier.
    max(page_number) >= {{ var('sold_page_cap') }} as hit_result_cap
from sold_pages
where crawl_region <> ''
group by crawl_region
