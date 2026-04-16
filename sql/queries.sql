-- =============================================================================
-- Example Athena Queries for DSQL CDC Iceberg Tables
-- =============================================================================
-- Database: dsql_cdc_iceberg
-- Tables:   cdc_events     (append-only audit log)
--           current_state  (merge/upsert view with tombstone deletes)


-- 1. Current state (live rows only — excludes tombstoned deletes)
SELECT id, name, email, created_at, _cdc_commit_ts_ns
FROM "dsql_cdc_iceberg"."current_state"
WHERE _is_deleted = false
ORDER BY _cdc_commit_ts_ns DESC;


-- 2. Full CDC audit trail (ordered by transaction commit timestamp)
SELECT id, name, email, created_at,
       _cdc_op,
       _cdc_tx_id,
       _cdc_ts_ms,
       _cdc_commit_ts_ns,
       FROM_UNIXTIME(_cdc_ts_ms / 1000) AS event_time,
       _cdc_source_table
FROM "dsql_cdc_iceberg"."cdc_events"
ORDER BY _cdc_commit_ts_ns ASC, _cdc_ts_ms ASC;


-- 3. Time-travel: query a specific snapshot
-- SELECT *
-- FROM "dsql_cdc_iceberg"."cdc_events" FOR VERSION AS OF <snapshot_id>;


-- 4. Snapshot listing
SELECT *
FROM "dsql_cdc_iceberg"."cdc_events$snapshots"
ORDER BY committed_at DESC;


-- 5. History
SELECT *
FROM "dsql_cdc_iceberg"."cdc_events$history"
ORDER BY made_current_at DESC;


-- 6. Operation distribution
SELECT _cdc_op,
       CASE _cdc_op
           WHEN 'c' THEN 'CREATE'
           WHEN 'u' THEN 'UPDATE'
           WHEN 'd' THEN 'DELETE'
           ELSE 'UNKNOWN'
       END AS operation_name,
       COUNT(*) AS event_count
FROM "dsql_cdc_iceberg"."cdc_events"
GROUP BY _cdc_op
ORDER BY event_count DESC;


-- 7. Daily event volume
SELECT DATE(FROM_UNIXTIME(_cdc_ts_ms / 1000)) AS event_date,
       COUNT(*) AS total_events,
       SUM(CASE WHEN _cdc_op = 'c' THEN 1 ELSE 0 END) AS creates,
       SUM(CASE WHEN _cdc_op = 'u' THEN 1 ELSE 0 END) AS updates,
       SUM(CASE WHEN _cdc_op = 'd' THEN 1 ELSE 0 END) AS deletes
FROM "dsql_cdc_iceberg"."cdc_events"
GROUP BY DATE(FROM_UNIXTIME(_cdc_ts_ms / 1000))
ORDER BY event_date;


-- 8. Deleted records (from audit trail)
SELECT id, name, email, created_at,
       _cdc_tx_id,
       _cdc_commit_ts_ns,
       FROM_UNIXTIME(_cdc_ts_ms / 1000) AS deleted_at
FROM "dsql_cdc_iceberg"."cdc_events"
WHERE _cdc_op = 'd'
ORDER BY _cdc_commit_ts_ns DESC;


-- 9. Tombstoned rows in current_state (sanity check)
SELECT id, name, email, created_at, _cdc_commit_ts_ns
FROM "dsql_cdc_iceberg"."current_state"
WHERE _is_deleted = true
ORDER BY _cdc_commit_ts_ns DESC;
