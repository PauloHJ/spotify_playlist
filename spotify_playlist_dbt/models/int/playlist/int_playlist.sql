{{ config(materialized = "table") }}

-- Recomputing Premium Ratios as they're missing
with premium_ratio_calc as (
    select
        * except (
            premium_mau,
            premium_mau_previous_month,
            mau_premium_ratio,
            mau_previous_month_premium_ratio
        ),
        {% for mau_type in ["mau", "mau_previous_month"] %}
            if(
                {{ mau_type }}_premium_ratio is null,
                safe_divide(premium_{{ mau_type }}, {{ mau_type }}),
                {{ mau_type }}_premium_ratio
            ) as {{ mau_type }}_premium_ratio,
            if(
                premium_{{ mau_type }} is null,
                {{ mau_type }}_premium_ratio * {{ mau_type }},
                premium_{{ mau_type }}
            )
                as premium_{{ mau_type }},
        {% endfor %}
        {% for col in ["tokens", "genre", "mood"] %}
            array_length(playlist_{{ col }}) as n_{{ col }},
        {% endfor %}
        count(distinct playlist_uri) over(partition by owner) as playlists_by_owner
    from {{ ref('stg_playlist') }}
),

playlist_genre_metrics as(
    select
        p.playlist_uri,
        avg(g.genre_stream30s) as avg_genre_stream30s,
        avg(g.genre_streams) as avg_genre_streams,
        avg(g.genre_mau) as avg_genre_mau,
        avg(g.genre_mau_previous_month) as avg_genre_mau_previous_month,
        sum(g.genre_playlist_count) as genre_playlist_count,
        sum(g.log_genre_stream30s) as log_genre_stream30s,
        min(g.genre_ranking) as top_genre_ranking,
        sum(if(g.genre_ranking <= 5, g.genre_stream30s, 0)) as n_genres_in_top_10
    from premium_ratio_calc as p, unnest(playlist_genre) as pg
    inner join {{ ref('int_genre') }} as g
        on pg = g.genre
    group by p.playlist_uri
    --select
    --    p.playlist_uri,
    --    avg(g.avg_log_monthly_stream30s) as playlist_genre_uplift
    --from premium_ratio_calc as p, unnest(playlist_genre) as pg
    --inner join {{ ref('int_genre') }} as g
    --    on pg = g.genre
    --group by p.playlist_uri
),

playlist_mood_metrics as(
    select
        p.playlist_uri,
        avg(m.mood_stream30s) as avg_mood_stream30s,
        avg(m.mood_streams) as avg_mood_streams,
        avg(m.mood_mau) as avg_mood_mau,
        avg(m.mood_mau_previous_month) as avg_mood_mau_previous_month,
        min(m.mood_ranking) as top_mood_ranking,
        sum(m.mood_playlist_count) as mood_playlist_count,
        sum(m.log_mood_stream30s) as log_mood_stream30s,
        sum(if(m.mood_ranking <= 5, m.mood_stream30s, 0)) as n_moods_in_top_10
    from premium_ratio_calc as p, unnest(playlist_mood) as pm
    inner join {{ ref('int_mood') }} as m
        on pm = m.mood
    group by p.playlist_uri
    --select
    --    p.playlist_uri,
    --    avg(g.avg_log_monthly_stream30s) as playlist_mood_uplift
    --from premium_ratio_calc as p, unnest(playlist_mood) as pg
    --inner join {{ ref('int_mood') }} as g
    --    on pg = g.mood
    --group by p.playlist_uri
),

playlist_token_metrics as(
    select
        p.playlist_uri,
        avg(t.avg_log_monthly_stream30s) as playlist_token_uplift,
    from premium_ratio_calc as p, unnest(playlist_tokens) as pt
    inner join {{ ref('int_token') }} as t
        on pt = t.token
    group by p.playlist_uri
),

-- Extra metrics relevant to the study
metric_calc as (
    select
        *,
        monthly_stream30s  / mau as streams_p_mau,
        safe_divide(stream30s, streams) as daily_stream_retention, --Out of all streams today, ratio of those longer  >30s
        safe_divide(skippers, users) as skipper_user_ratio, --Proportion of skippers vs all users
        n_artists / n_tracks as artist_diversity,
        monthly_owner_stream30s / monthly_stream30s as monthly_owner_stream_engagement,
        ntile(4) over(order by n_tracks) as playlist_size_quantile,
        if(streams = 0, 1, ntile(4) over(partition by streams = 0 order by streams)) as playlist_stream_quantile,
        if(stream30s = 0, 1, ntile(4) over(partition by stream30s = 0 order by stream30s)) as playlist_stream30s_quantile
    from premium_ratio_calc
)

select
    p.*,
    coalesce(g.avg_genre_stream30s, 0) as avg_genre_stream30s,
    coalesce(g.avg_genre_streams, 0) as avg_genre_streams,
    coalesce(g.avg_genre_mau, 0) as avg_genre_mau,
    coalesce(g.avg_genre_mau_previous_month, 0) as avg_genre_mau_previous_month,
    coalesce(g.top_genre_ranking, 0) as top_genre_ranking,
    coalesce(g.n_genres_in_top_10, 0) as n_genres_in_top_10,
    coalesce(g.log_genre_stream30s, 0) as log_genre_stream30s,
    coalesce(g.genre_playlist_count, 0) as genre_playlist_count,
    
    
    coalesce(m.avg_mood_stream30s, 0) as avg_mood_stream30s,
    coalesce(m.avg_mood_streams, 0) as avg_mood_streams,
    coalesce(m.avg_mood_mau, 0) as avg_mood_mau,
    coalesce(m.avg_mood_mau_previous_month, 0) as avg_mood_mau_previous_month,
    coalesce(m.top_mood_ranking, 0) as top_mood_ranking,
    coalesce(m.n_moods_in_top_10, 0) as n_moods_in_top_10,
    coalesce(m.log_mood_stream30s, 0) as log_mood_stream30s,
    coalesce(m.mood_playlist_count, 0) as mood_playlist_count,
    --coalesce(m.playlist_mood_uplift, 0) as playlist_mood_uplift,
    --coalesce(g.playlist_genre_uplift, 0) as playlist_genre_uplift,
    coalesce(t.playlist_token_uplift, 0) as playlist_token_uplift
from premium_ratio_calc as p
left join playlist_genre_metrics as g
    on p.playlist_uri = g.playlist_uri
left join playlist_mood_metrics as m
    on p.playlist_uri = m.playlist_uri
left join playlist_token_metrics as t
    on p.playlist_uri = t.playlist_uri
 


--(df["stream30s"].divide(df["streams"])) < 1