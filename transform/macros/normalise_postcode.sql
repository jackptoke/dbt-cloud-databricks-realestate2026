{% macro normalise_postcode(column) -%}
    {#-
        One definition of what a postcode looks like: a four-character string,
        zero-padded, or null. Applied at the staging boundary so every model
        downstream can treat `postcode` as already correct and join on it
        directly, rather than re-formatting it at each use.

        Australian postcodes are identifiers, not quantities. The NT range
        starts 0800 and the ACT 0200, so anything that lets them become numeric
        loses the leading zero and breaks every join silently — which is exactly
        what the vendored ABS reference did to 313 of its rows.

        Currently a no-op on the listings feed: all 7,551 rows already arrive as
        clean four-character strings. That is the point. It costs nothing today
        and it is the difference between that being true by luck and true by
        construction.

        REJECT rather than coerce. The shape is tested before padding, because
        lpad is only safe in one direction: lpad('30810', 4, '0') silently
        returns '3081' and lpad('VIC 3081', 4, '0') returns 'VIC '. Truncation
        is the dangerous failure here, not nullity — this macro feeds the
        property_key surrogate hash, so a five-digit typo coerced down to four
        would collide with a real property at that postcode and merge two
        dwellings into one key, surfacing downstream as a fabricated repeat
        sale. A null postcode simply produces a distinct key, which is wrong in
        a way that stays visible and cannot corrupt a neighbour's identity.

        Anything that is not one to four digits therefore becomes null, and
        stg_suburbs' not_null test on the result is what makes that loud.
    -#}
    case
        when trim(cast({{ column }} as string)) rlike '^[0-9]{1,4}$'
        then lpad(trim(cast({{ column }} as string)), 4, '0')
    end
{%- endmacro %}
