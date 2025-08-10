{#
    This macro returns the decade
#}

{% macro get_decade(datetime) %}

    cast((extract(year from {{ datetime }}) / 10) * 10 as int64)

{% endmacro %}