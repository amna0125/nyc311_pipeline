{#
    Scrub the dataset's sentinel "missing" values down to a real NULL.

    NYC 311 uses several placeholders that mean "no value" but are not NULL:
    empty string, 'Unspecified', 'N/A', '0 Unspecified', 'Unknown'. This macro
    trims whitespace and collapses all of those to NULL so downstream models and
    tests treat them consistently.

    Usage:  {{ clean_sentinel('borough') }}
#}
{% macro clean_sentinel(column) %}
    nullif(
        nullif(
            nullif(
                nullif(
                    nullif(trim({{ column }}), ''),
                    'Unspecified'
                ),
                'N/A'
            ),
            '0 Unspecified'
        ),
        'Unknown'
    )
{% endmacro %}
