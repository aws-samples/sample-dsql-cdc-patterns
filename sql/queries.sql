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


-- =============================================================================
-- Ordering-safe current state (reconstructed from the append-only log)
-- =============================================================================
-- The current_state table is maintained by Firehose merge mode, which applies
-- upserts in the order Firehose PROCESSES records -- it does NOT compare
-- _cdc_commit_ts_ns. Under out-of-order delivery, a stale UPDATE can overwrite
-- a newer one, or a late UPDATE can un-tombstone a deleted row.
--
-- DSQL guarantees source.ts_ns (stored here as _cdc_commit_ts_ns) establishes a
-- TOTAL ORDER of transactions. The append-only cdc_events table retains every
-- version, so we can reconstruct a correct current state by taking the latest
-- event per id by that total-order key -- independent of Firehose merge order.

-- 10. Ordering-safe current state as a reusable Athena view.
--     Run once; then query "current_state_ordered" like any table.
CREATE OR REPLACE VIEW "dsql_cdc_iceberg"."current_state_ordered" AS
WITH ranked AS (
    SELECT id, name, email, created_at, _cdc_op, _cdc_commit_ts_ns,
           ROW_NUMBER() OVER (
               PARTITION BY id
               ORDER BY _cdc_commit_ts_ns DESC, _cdc_ts_ms DESC
           ) AS rn
    FROM "dsql_cdc_iceberg"."cdc_events"
)
SELECT id, name, email, created_at, _cdc_commit_ts_ns
FROM ranked
WHERE rn = 1
  AND _cdc_op <> 'd';   -- drop ids whose latest state is a delete

-- 11. Query the ordering-safe current state
SELECT id, name, email, created_at, _cdc_commit_ts_ns
FROM "dsql_cdc_iceberg"."current_state_ordered"
ORDER BY _cdc_commit_ts_ns DESC;

-- 12. Detect merge-ordering anomalies: rows where the Firehose merge table
--     disagrees with the ordering-safe reconstruction (email diff or a row that
--     the log says is deleted but current_state still shows as live).
WITH ranked AS (
    SELECT id, name, email, _cdc_op, _cdc_commit_ts_ns,
           ROW_NUMBER() OVER (
               PARTITION BY id
               ORDER BY _cdc_commit_ts_ns DESC, _cdc_ts_ms DESC
           ) AS rn
    FROM "dsql_cdc_iceberg"."cdc_events"
),
latest AS (
    SELECT id, name, email, _cdc_op, _cdc_commit_ts_ns
    FROM ranked
    WHERE rn = 1
)
SELECT cs.id,
       cs.email      AS merge_email,
       latest.email  AS ordered_email,
       cs._is_deleted        AS merge_is_deleted,
       (latest._cdc_op = 'd') AS ordered_is_deleted,
       cs._cdc_commit_ts_ns   AS merge_commit_ts_ns,
       latest._cdc_commit_ts_ns AS ordered_commit_ts_ns
FROM "dsql_cdc_iceberg"."current_state" cs
JOIN latest ON latest.id = cs.id
WHERE cs._is_deleted <> (latest._cdc_op = 'd')
   OR cs.email IS DISTINCT FROM latest.email;
