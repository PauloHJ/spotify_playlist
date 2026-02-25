{{ config(materialized = "view") }}

with source as (
    select *
    from {{ source('playlist_dataset', 'playlist_summary_external') }}
),

-- Transforming UNICODE text
escaped_unicode as (
    select
        * except (tokens),
        json_value(concat('"', tokens, '"')) as tokens
    from source
),

-- Converting into ARRAY structure
-- Treating duplicate tokens
expanded_token as (
    select
        * except (tokens),
        array(
            select distinct trim(token) as pt
            from
                unnest(split(regexp_replace(tokens, r'[\[\]]', ''), ','))
                    as token
            where trim(token) != ''
        ) as playlist_tokens
    from escaped_unicode
),

-- Compressing Genre & Mood columns into an array with their respective Ranking.
-- Easier to scale as an array withuot disrupting the schema,
-- rather that columnar growth
clean_genre_mood as (
    select
        * except (genre_1, genre_2, genre_3, mood_1, mood_2, mood_3),
        {% for col in ["genre", "mood"] %}
            array(
                select distinct {{ col }}_list
                from
                    unnest(
                        [
                            struct(
                                {{ col }}_1 as {{ col }}, 1 as {{ col }}_rank
                            ),
                            struct(
                                {{ col }}_2 as {{ col }}, 2 as {{ col }}_rank
                            ),
                            struct(
                                {{ col }}_3 as {{ col }}, 3 as {{ col }}_rank
                            )
                        ]
                    ) as {{ col }}_list
                where
                    {{ col }} is not null
                    and {{ col }} != '-'
            ) as playlist_{{ col }}

            {% if not loop.last %},{% endif %}
        {% endfor %}
    from expanded_token
)

select
    playlist_uri,
    owner,
    streams,
    stream30s,
    dau,
    wau,
    mau,
    mau_previous_month,
    mau_both_months,
    users,
    skippers,
    owner_country,
    n_tracks,
    n_local_tracks,
    n_artists,
    n_albums,
    monthly_stream30s,
    monthly_owner_stream30s,
    playlist_tokens,
    playlist_genre,
    playlist_mood
from clean_genre_mood
