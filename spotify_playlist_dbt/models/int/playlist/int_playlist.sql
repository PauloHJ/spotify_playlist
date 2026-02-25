{{ config(materialized = "table") }}

-- Computing Premium Ratios for this and past month
with premium_ratio_calc as (
    select
        *,
        {% for mau_type in ["mau", "mau_previous_month"] %}
            safe_divide(premium_{{ mau_type }}, {{ mau_type }})
                as {{ mau_type }}_premium_ratio
            {% if not loop.last %} , {% endif %}
        {% endfor %}
    from {{ ref('stg_playlist') }}
),

growth_ratio_calc as (
    select
        *,
        {% for mau_type in ["mau", "premium_mau"] %}
            safe_divide(
                {{ mau_type }} - {{ mau_type }}_previous_month,
                {{ mau_type }}_previous_month
            )
                as {{ mau_type }}_growth_ratio
            {% if not loop.last %} , {% endif %}
        {% endfor %}
    from premium_ratio_calc
)

select *
from growth_ratio_calc
