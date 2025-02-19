CREATE TABLE user_engagement_events_transformed (
    client_id String,
    event_name String,
    session_engaged String,
    page_location String,
    page_title String,
    session_count String,
    session_id String,
    session_time DateTime,
    page_referrer Nullable(String),
    consumed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY consumed_at;