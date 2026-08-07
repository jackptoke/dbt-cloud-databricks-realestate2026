{{ config(materialized = 'table') }}

-- Spans 2007 because sold_date reaches back 20 years — far wider than the
-- crawl window. Includes an Unknown member (-1) so facts can keep a mandatory
-- date FK even for the 14 sales with no date.

with spine as (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2007-01-01' as date)",
        end_date   = "cast('2028-01-01' as date)"
    ) }}
),

dates as (
    select
        cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
        cast(date_day as date)                         as full_date,
        year(date_day)                                 as year_number,
        quarter(date_day)                              as quarter_number,
        month(date_day)                                as month_number,
        date_format(date_day, 'MMMM')                  as month_name,
        date_format(date_day, 'yyyy-MM')               as year_month,
        day(date_day)                                  as day_of_month,
        dayofweek(date_day)                            as day_of_week,
        date_format(date_day, 'EEEE')                  as day_name,
        weekofyear(date_day)                           as week_of_year,
        dayofweek(date_day) in (1, 7)                  as is_weekend,
        false                                          as is_unknown
    from spine
),

unknown_member as (
    select
        -1, cast(null as date), cast(null as int), cast(null as int),
        cast(null as int), cast(null as string), cast(null as string),
        cast(null as int), cast(null as int), cast(null as string),
        cast(null as int), cast(null as boolean), true
)

select * from dates
union all select * from unknown_member
