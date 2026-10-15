-- Athena external table for querying CDC events in S3.
-- Run this in the Athena query editor after CDC events have been delivered.
--
-- Replace <CDC_BUCKET_NAME> with your actual S3 bucket name.
-- This DDL matches the flattened output from the Lambda transform.
-- If deployed WITHOUT the transform, the raw CDC event format differs.

CREATE DATABASE IF NOT EXISTS dsql_cdc;

CREATE EXTERNAL TABLE IF NOT EXISTS dsql_cdc.cdc_events (
    op       STRING COMMENT 'Operation type: c=INSERT, u=UPDATE, d=DELETE',
    `table`  STRING COMMENT 'Source table name',
    `schema` STRING COMMENT 'Source schema name',
    db       STRING COMMENT 'Source database name',
    cluster  STRING COMMENT 'DSQL cluster identifier',
    tx_id        STRING COMMENT 'DSQL transaction identifier (source.txId)',
    ts           STRING COMMENT 'Transaction commit timestamp (ISO 8601)',
    ts_ms        BIGINT COMMENT 'Transaction commit timestamp (epoch milliseconds, source.ts_ms)',
    commit_ts_ns BIGINT COMMENT 'Transaction commit timestamp (epoch nanoseconds, source.ts_ns); total order of transactions',
    `before`     STRING COMMENT 'Deletes only: primary key columns of the deleted row (JSON)',
    `after`      STRING COMMENT 'Inserts/updates: full row image after the change (JSON)'
)
PARTITIONED BY (
    `year`  STRING,
    `month` STRING,
    `day`   STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://<CDC_BUCKET_NAME>/cdc-events/'
TBLPROPERTIES (
    'has_encrypted_data'        = 'true',
    'projection.enabled'        = 'true',
    'projection.year.type'      = 'integer',
    'projection.year.range'     = '2025,2030',
    'projection.month.type'     = 'integer',
    'projection.month.range'    = '1,12',
    'projection.month.digits'   = '2',
    'projection.day.type'       = 'integer',
    'projection.day.range'      = '1,31',
    'projection.day.digits'     = '2',
    'storage.location.template' = 's3://<CDC_BUCKET_NAME>/cdc-events/year=${year}/month=${month}/day=${day}/'
);

-- Sample: count events by operation type for today
-- SELECT op, COUNT(*) AS cnt
-- FROM dsql_cdc.cdc_events
-- WHERE year = CAST(YEAR(current_date) AS VARCHAR)
--   AND month = LPAD(CAST(MONTH(current_date) AS VARCHAR), 2, '0')
--   AND day = LPAD(CAST(DAY(current_date) AS VARCHAR), 2, '0')
-- GROUP BY op;

-- Sample: change history for one row, in commit order.
-- Row images are JSON strings; timestamptz columns such as created_at are
-- encoded as microseconds since the Unix epoch.
-- SELECT op, ts,
--        COALESCE(json_extract_scalar("after", '$.id'),
--                 json_extract_scalar("before", '$.id')) AS id,
--        json_extract_scalar("after", '$.email') AS email,
--        from_unixtime(CAST(json_extract_scalar("after", '$.created_at') AS BIGINT) / 1e6) AS created_at
-- FROM dsql_cdc.cdc_events
-- WHERE COALESCE(json_extract_scalar("after", '$.id'),
--                json_extract_scalar("before", '$.id')) = '<row-id>'
-- ORDER BY commit_ts_ns;
