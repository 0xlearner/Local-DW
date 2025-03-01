{% macro convert_timezone(column) %}
    {% if target.type == 'postgres' %}
        timezone('UTC', {{ column }}::timestamp)
    {% elif target.type == 'snowflake' %}
        CONVERT_TIMEZONE('UTC', {{ column }})
    {% else %}
        {{ column }}
    {% endif %}
{% endmacro %}

{% macro current_timestamp_utc() %}
    {% if target.type == 'postgres' %}
        (current_timestamp AT TIME ZONE 'UTC')
    {% elif target.type == 'snowflake' %}
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
    {% else %}
        current_timestamp
    {% endif %}
{% endmacro %}
