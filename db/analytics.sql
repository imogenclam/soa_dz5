CREATE TABLE IF NOT EXISTS cinema.first_views
(
    user_id String,
    first_day SimpleAggregateFunction(min, Date)
)
ENGINE = AggregatingMergeTree
ORDER BY user_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.collect_first_views TO cinema.first_views AS
SELECT user_id, min(toDate(timestamp)) AS first_day
FROM cinema.events
WHERE event_type = 'VIEW_STARTED'
GROUP BY user_id;

-- на существующей базе добавляем пропущенные первые просмотры
INSERT INTO cinema.first_views (user_id, first_day)
SELECT history.user_id, history.first_day
FROM (
    SELECT user_id, min(toDate(timestamp)) AS first_day
    FROM cinema.events
    WHERE event_type = 'VIEW_STARTED'
    GROUP BY user_id
) AS history
LEFT JOIN (
    SELECT user_id, min(first_day) AS known_day FROM cinema.first_views GROUP BY user_id
) AS known ON history.user_id = known.user_id
WHERE known.known_day IS NULL OR history.first_day < known.known_day
SETTINGS join_use_nulls = 1;

CREATE TABLE IF NOT EXISTS cinema.daily_metrics
(
    day Date,
    metric LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (metric, dimension);

CREATE TABLE IF NOT EXISTS cinema.daily_metrics_stage AS cinema.daily_metrics
ENGINE = MergeTree
PARTITION BY day
ORDER BY (metric, dimension);
