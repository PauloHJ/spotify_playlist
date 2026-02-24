{{ config(materialized = "view") }}

with source as (
    select *
    from {{ source('playlist_dataset', 'playlist_revision_raw') }}
),

-- Removing identified duplicates
deduplicated_playlist as (
    select *
    from source
    qualify
        1 = row_number() over (partition by playlist_uri)
),

-- Inconsistent owner (username) hashing
-- Escalated to source team, protecting owner in the meantime
hashed_owner as (
    select
        *,
        case
            when
                (
                    (
                        length(owner) = 32
                        and regexp_contains(owner, r'^[a-f0-9]+$')
                    )
                    or owner = 'spotify'
                ) then owner
            else to_hex(md5(owner))
        end as hash_owner
    from deduplicated_playlist
),

-- Propagating owner hashing into playlist_uri
hashed_playlist_uri as (
    select
        * except (playlist_uri, owner, hash_owner),
        hash_owner as owner,
        if(
            owner != hash_owner,
            regexp_replace(playlist_uri, owner, hash_owner),
            playlist_uri
        ) as playlist_uri
    from hashed_owner
),

-- Converting token JSON -> ARRAY
-- Removing leftover quotes
-- Treating duplicate tokens
expanded_token as (
    select
        * except (tokens),
        array(
            select distinct trim(array_tokens, '"') as clean_tokens
            from unnest(json_extract_array(tokens)) as array_tokens
        ) as playlist_tokens
    from hashed_playlist_uri
),

clean_genre_mood as (
    select
        * except (genre_1, genre_2, genre_3, mood_1, mood_2, mood_3),
        {% for col in ["genre", "mood"] %}
            array(
                select distinct {{ col }}
                from
                    unnest([{{ col }}_1, {{ col }}_2, {{ col }}_3]) as {{ col }}
                where
                    {{ col }} is not null
                    and {{ col }} != '-'
            ) as playlist_{{ col }}

            {% if not loop.last %},{% endif %}
        {% endfor %}
    from expanded_token
),

-- Cast Premium MAU fields from FLOAT -> INT
type_casting as (
    select
        * except (premium_mau, premium_mau_previous_month),
        {% for col in ["premium_mau", "premium_mau_previous_month"] %}
            cast({{ col }} as int64) as {{ col }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from clean_genre_mood
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
    playlist_mood,
    premium_mau_previous_month,
    premium_mau,
    mau_premium_ratio,
    mau_previous_month_premium_ratio
from type_casting
