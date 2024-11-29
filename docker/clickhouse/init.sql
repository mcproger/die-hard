CREATE TABLE IF NOT EXISTS event_log
(
    `event_type` String,
    `event_date_time` DateTime64(6),
    `environment` String,
    `event_context` String,
    `metadata_version` Int32 DEFAULT 1,
    `sign` Int8,
)
ENGINE = VersionedCollapsingMergeTree(sign, event_date_time)
PARTITION BY toYYYYMM(event_date_time)
ORDER BY (event_date_time, event_type)
SETTINGS index_granularity = 8192
