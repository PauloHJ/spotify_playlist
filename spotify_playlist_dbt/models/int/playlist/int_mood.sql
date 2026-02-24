{{ config(materialized = "view") }}

with mood_metrics as (
    select
        mood,
        count(distinct playlist_uri) as mood_playlist_count,
        sum(streams) as mood_streams,
        sum(monthly_stream30s) as mood_stream30s,
        sum(mau) as mood_mau,
        avg(ln(1+monthly_stream30s)) as log_mood_stream30s,
        sum(mau_previous_month) as mood_mau_previous_month,
    from {{ ref('stg_playlist') }}, unnest(playlist_mood) as mood
    group by mood
)

select
    *,
    row_number() over(order by mood_stream30s desc) as mood_ranking
from mood_metrics



--with baseline_log_calc as (
--  select
--  playlist_mood,
--  monthly_stream30s,
--  avg(ln(monthly_stream30s + 1))over() as baseline_monthly_streams
--  from {{ ref('stg_playlist') }}
--),
--log_token as (
--  select
--  mood,
--  baseline_monthly_streams,
--  ln(monthly_stream30s + 1) as log_monthly_stream30s,
--from baseline_log_calc, unnest(playlist_mood) as mood
--),
--token_uplift as (
--  select
--  mood,
--  avg(log_monthly_stream30s) - max(baseline_monthly_streams) as avg_log_monthly_stream30s
--from log_token
--group by
--  mood
--)
--select
--  *
--from token_uplift