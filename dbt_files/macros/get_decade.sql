{#
    This macro returns the decade
#}

{% macro get_decade(datetime) %}

    (floor(extract(year from {{ datetime }}) / 10) * 10)

{% endmacro %}