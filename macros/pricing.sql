{% macro discounted_amount(extended_price, discount, scale=2) %}
    (-1 * {{ extended_price }} * {{discount}} )::decimal(16, {{ scale }})
{% endmacro %}