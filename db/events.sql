CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), event_id);

CREATE TABLE IF NOT EXISTS cinema.kafka_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                     'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'cinema-clickhouse-v1',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         kafka_num_consumers = 3,
         kafka_flush_interval_ms = 700;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.consume_events TO cinema.events AS
SELECT event_id, user_id, movie_id, toString(event_type) AS event_type,
       timestamp, toString(device_type) AS device_type, session_id,
       toUInt32(progress_seconds) AS progress_seconds
FROM cinema.kafka_events;
