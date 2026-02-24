{{ config(materialized = "view") }}

with genre_metrics as (
    select
        genre,
        count(distinct playlist_uri) as genre_playlist_count,
        sum(streams) as genre_streams,
        sum(monthly_stream30s) as genre_stream30s,
        sum(mau) as genre_mau,
        avg(ln(1+monthly_stream30s)) as log_genre_stream30s,
        sum(mau_previous_month) as genre_mau_previous_month,
    from {{ ref('stg_playlist') }}, unnest(playlist_genre) as genre
    group by genre
)

select
    *,
    row_number() over(order by genre_stream30s desc) as genre_ranking
from genre_metrics


--with baseline_log_calc as (
--  select
--  playlist_genre,
--  monthly_stream30s,
--  avg(ln(monthly_stream30s + 1))over() as baseline_monthly_streams
--  from {{ ref('stg_playlist') }}
--),
--log_token as (
--  select
--  genre,
--  baseline_monthly_streams,
--  ln(monthly_stream30s + 1) as log_monthly_stream30s,
--from baseline_log_calc, unnest(playlist_genre) as genre
--),
--token_uplift as (
--  select
--  genre,
--  avg(log_monthly_stream30s) - max(baseline_monthly_streams) as avg_log_monthly_stream30s
--from log_token
--group by
--  genre
--)
--select
--  *
--from token_uplift