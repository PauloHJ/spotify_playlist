from google.cloud import bigquery

PLAYLIST_REVISION_SCHEMA = [
    bigquery.SchemaField("playlist_uri", "STRING", description="The key, Spotify uri of the playlist"),
    bigquery.SchemaField("owner", "STRING", description="Playlist owner, Spotify username"),
    bigquery.SchemaField("streams", "INTEGER", description="Number of streams from playlist today"),
    bigquery.SchemaField("stream30s", "INTEGER", description="Number of streams over 30 sec from playlist today"),
    bigquery.SchemaField(
        "dau",
        "INTEGER",
        description="Number of Daily Active Users. Users with a stream over 30 sec from playlist today",
    ),
    bigquery.SchemaField(
        "wau",
        "INTEGER",
        description="Number of Weekly Active Users. Users with a stream over 30 seconds from playlist in past week",
    ),
    bigquery.SchemaField(
        "mau",
        "INTEGER",
        description="Number of Monthly Active Users. Users with a stream over 30 seconds from playlist in past month",
    ),
    bigquery.SchemaField(
        "mau_previous_month", "INTEGER", description="Number of Monthly Active Users in the month prior to this one"
    ),
    bigquery.SchemaField(
        "mau_both_months",
        "INTEGER",
        description="Number of users that were active on the playlist both this and the previous month",
    ),
    bigquery.SchemaField(
        "users", "INTEGER", description="Number of users streaming (all streams) from this playlist this month"
    ),
    bigquery.SchemaField(
        "skippers", "INTEGER", description="Number of users who skipped more than 90 percent of their streams today"
    ),
    bigquery.SchemaField("owner_country", "STRING", description="Country of the playlist owner"),
    bigquery.SchemaField("n_tracks", "INTEGER", description="Number of tracks in playlist"),
    bigquery.SchemaField(
        "n_local_tracks", "INTEGER", description="Change in number of tracks on playlist since yesterday"
    ),
    bigquery.SchemaField("n_artists", "INTEGER", description="Number of unique artists in playlist"),
    bigquery.SchemaField("n_albums", "INTEGER", description="Number of unique albums in playlist"),
    bigquery.SchemaField("monthly_stream30s", "INTEGER", description="Number of streams over 30 seconds this month"),
    bigquery.SchemaField(
        "monthly_owner_stream30s",
        "INTEGER",
        description="Number of streams over 30 seconds by playlist owner this month",
    ),
    bigquery.SchemaField(
        "tokens", "STRING", description="List of playlist title tokens, stopwords and punctuation removed"
    ),
    bigquery.SchemaField(
        "genre_1", "STRING", description="No. 1 Genre by weight of playlist tracks, from Gracenote metadata"
    ),
    bigquery.SchemaField(
        "genre_2", "STRING", description="No. 2 Genre by weight of playlist tracks, from Gracenote metadata"
    ),
    bigquery.SchemaField(
        "genre_3", "STRING", description="No. 3 Genre by weight of playlist tracks, from Gracenote metadata"
    ),
    bigquery.SchemaField(
        "mood_1", "STRING", description="No. 1 Mood by weight of playlist tracks, from Gracenote metadata"
    ),
    bigquery.SchemaField(
        "mood_2", "STRING", description="No. 2 Mood by weight of playlist tracks, from Gracenote metadata"
    ),
    bigquery.SchemaField(
        "mood_3", "STRING", description="No. 3 Mood by weight of playlist tracks, from Gracenote metadata"
    ),
    bigquery.SchemaField(
        "premium_mau_previous_month",
        "FLOAT",
        description="Number of Monthly Active Users in the month prior to this one that had a Premium subscription. A subset of mau_previous_months (as defined above).",
    ),
    bigquery.SchemaField(
        "premium_mau",
        "FLOAT",
        description="Number of Monthly Active Users that have a Premium subscription. A subset of mau (as defined above).",
    ),
    bigquery.SchemaField(
        "mau_premium_ratio",
        "FLOAT",
        description="Proportion of Monthly Active Users in past month with Premium subscription, calculated as premium_mau divided by mau.",
    ),
    bigquery.SchemaField(
        "mau_previous_month_premium_ratio",
        "FLOAT",
        description="Proportion of Monthly Active Users in the month prior to this one with Premium subscription, calculated as premium_mau_previous_month divided by mau_previous_month.",
    ),
    bigquery.SchemaField(
        "meta_change_timestamp", "TIMESTAMP", description="Date and time of record creation/change. UTC."
    ),
    bigquery.SchemaField("meta_integration_identifier", "STRING", description="Source data origin"),
]
