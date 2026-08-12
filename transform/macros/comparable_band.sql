{% macro comparable_band(is_land_or_rural, bedrooms, land_size_m2) -%}
    {#-
        What makes two properties comparable, beyond suburb and type.

        For a DWELLING that is the bedroom count. For LAND it is emphatically
        not: a vacant block reports 0 bedrooms whether it is a 300 m2 infill lot
        or a 200-hectare farm, so banding land by bedrooms puts every parcel in
        an LGA into a single cell. Horsham's "residential land, 0 bedrooms" cell
        held 129 sales spanning 306 m2 to 845,700 m2 and $37,500 to $1,568,000,
        with a median block of 756 m2 — so any acreage was priced against
        suburban blocks. That produced a 53-hectare parcel reading +3,809%
        against its comparables, and it is why 44% of verdicts were land types
        with a 95th-percentile variance of 199% against 86% for dwellings.

        So land bands on AREA instead. Boundaries follow how rural property is
        actually transacted rather than a neat log scale: a residential block, a
        large block, a half-to-two hectare lot, a hobby farm, a small holding, a
        farm. Within a band, price per square metre is at least the same order
        of magnitude.

        Bands are deliberately coarse. Narrower ones would be more homogeneous
        and would empty the cells — and an empty cell is the right answer only
        when the market really is that thin, not because the banding was greedy.
        A parcel with no recorded area gets no band and therefore no verdict,
        which is correct: nothing is known about what it should be compared to.

        Returned as a STRING so both branches share one column, and prefixed so
        'beds:0' can never collide with 'land:<1000'.
    -#}
    case
        when {{ is_land_or_rural }} then
            case
                when {{ land_size_m2 }} is null           then null
                when {{ land_size_m2 }} <      1000       then 'land:<1000m2'
                when {{ land_size_m2 }} <      5000       then 'land:1000-5000m2'
                when {{ land_size_m2 }} <     20000       then 'land:0.5-2ha'
                when {{ land_size_m2 }} <    100000       then 'land:2-10ha'
                when {{ land_size_m2 }} <    500000       then 'land:10-50ha'
                else                                           'land:50ha+'
            end
        when {{ bedrooms }} is null then null
        else concat('beds:', cast({{ bedrooms }} as string))
    end
{%- endmacro %}
