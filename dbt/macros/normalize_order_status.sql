{% macro normalize_order_status(status_column) %}

    case
        when {{ status_column }} is null then null
        when upper(trim({{ status_column }})) = 'PENDING' then 'PLACED'
        else upper(trim({{ status_column }}))
    end

{% endmacro %}