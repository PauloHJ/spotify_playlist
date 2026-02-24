{{ config(materialized = "view") }}

with baseline_log_calc as (
  select
  playlist_tokens,
  monthly_stream30s,
  avg(ln(monthly_stream30s + 1))over() as baseline_monthly_streams
  from {{ ref('stg_playlist') }}
),
log_token as (
  select
  token,
  baseline_monthly_streams,
  ln(monthly_stream30s + 1) as log_monthly_stream30s,
from baseline_log_calc, unnest(playlist_tokens) as token
),
token_uplift as (
  select
  token,
  avg(log_monthly_stream30s) - max(baseline_monthly_streams) as avg_log_monthly_stream30s
from log_token
group by
  token
)
select
  *
from token_uplift