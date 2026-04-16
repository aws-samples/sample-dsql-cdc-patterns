# Amazon Aurora DSQL CDC to Apache Iceberg Architecture

## Data Flow

```
Amazon Aurora DSQL (events table)
    |
    | CDC stream (change data capture)
    v
Amazon Kinesis Data Streams    |
    +---> Amazon Data Firehose (append) ---> AWS Lambda (flatten) ---> Iceberg: cdc_events
    |
    +---> Amazon Data Firehose (merge)  ---> AWS Lambda (flatten) ---> Iceberg: current_state
                                                                            |
                                                                            v
                                                                     Amazon Athena queries
```

## Components

| Component | Service | Purpose |
|-----------|---------|---------|
| Source database | Amazon Aurora DSQL | Serverless PostgreSQL-compatible database with CDC support |
| CDC stream | Amazon Aurora DSQL | Streams row-level changes (INSERT/UPDATE/DELETE) to Amazon Kinesis via change data capture |
| Event stream | Amazon Kinesis Data Streams | Buffers CDC events; consumed by two Amazon Data Firehose streams |
| Append pipeline | Amazon Data Firehose + AWS Lambda | Flattens CDC events, inserts every change as a new row |
| Merge pipeline | Amazon Data Firehose + AWS Lambda | Flattens CDC events, applies upserts to maintain current state (deletes are tombstoned) |
| Data catalog | AWS Glue | Manages Iceberg table metadata (schema, partitions, snapshots) |
| Storage | Amazon S3 | Stores Iceberg data files (Parquet) and metadata |
| Query engine | Amazon Athena | SQL queries over Iceberg tables, including time-travel |

## Two-Table Strategy

The pipeline maintains two complementary Iceberg tables from the same Kinesis stream:

**`cdc_events`** (append-only)
- Every CDC event becomes a new row
- Full audit trail: who changed what, when
- Columns: `id`, `name`, `email`, `created_at`, `_cdc_op`, `_cdc_tx_id`, `_cdc_ts_ms`, `_cdc_source_table`, `_cdc_commit_ts_ns`

**`current_state`** (merge mode)
- Reflects the latest version of each row, updated by Amazon Data Firehose merge mode as new CDC events arrive
- Upserts on INSERT/UPDATE, tombstones on DELETE (`_is_deleted = true`), keyed by `id`
- Consumers query with `WHERE _is_deleted = false` to see only live rows
- `_cdc_commit_ts_ns` stores the transaction commit timestamp for best-effort ordering
- Columns: `id`, `name`, `email`, `created_at`, `_cdc_commit_ts_ns`, `_is_deleted`

## CDC Event Format

Amazon Aurora DSQL streams Debezium-compatible JSON:

```json
{
  "op": "c",
  "before": null,
  "after": {"id": "abc-123", "name": "Alice", "email": "alice@example.com", "created_at": "2025-03-21T..."},
  "source": {"table": "events", "schema": "public", "db": "postgres", "cluster": "abc123"},
  "txId": "tx-456",
  "ts_ms": 1771658349627
}
```

Operations: `c` = INSERT, `u` = UPDATE, `d` = DELETE

## Security

- Amazon S3 bucket: `BlockPublicAccess` enabled, SSL-only (bucket policy denies `aws:SecureTransport=false`)
- Amazon Kinesis: KMS encryption at rest
- AWS IAM roles: least-privilege, scoped to specific resource ARNs
- Amazon Aurora DSQL connections: SSL required, IAM auth tokens (15-minute expiry)
- No public endpoints, no AWS Lambda function URLs
