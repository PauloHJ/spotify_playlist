-- tests/right_tail_outliers.sql
{% test right_tail_outliers(model, column_name, quantile) %}

with quantile_limit as (
  select
    approx_quantiles({{ column_name }}, 10000) [offset(cast({{ quantile }} * 1000 as int64))] as q_limit
  from {{ model }}
),

flagged_outliers as (
  select *
  from {{ model }}, quantile_limit
  where {{ column_name }} > q_limit
)

select *
from flagged_outliers

{% endtest %}
