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

> **Ordering caveat.** Firehose merge applies upserts in the order it *processes*
> records; it does not compare `_cdc_commit_ts_ns`. Under out-of-order delivery a
> stale UPDATE can overwrite a newer one, or a late UPDATE can un-tombstone a
> deleted row. DSQL guarantees `source.ts_ns` (`_cdc_commit_ts_ns`) is a total
> order of transactions, so for a strictly correct current state, reconstruct it
> from the append-only `cdc_events` log by taking the latest event per `id` by
> `_cdc_commit_ts_ns`. See the `current_state_ordered` view in `sql/queries.sql`.

## CDC Event Format

Amazon Aurora DSQL streams each change as a JSON envelope. See
[Understanding CDC records](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html).

INSERT (`op: "c"`) and UPDATE (`op: "u"`) carry the **full** post-change row in
`after`; `before` is `null`. DELETE (`op: "d"`) sets `after` to `null` and puts
**only the primary key** columns in `before`.

```json
{
  "type": "full",
  "op": "u",
  "before": null,
  "after": {"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Alice", "email": "alice@example.com", "created_at": 1710510600123456},
  "source": {
    "version": "1.0",
    "ts_ms": 1710510600000,
    "ts_ns": 1710510600000000000,
    "txId": "qvtiesgmd55cvlfukm3dfuotji",
    "schema": "public",
    "table": "events",
    "db": "postgres",
    "cluster": "abc123"
  },
  "ts_ms": 1710510600125,
  "ts_ns": 1710510600125483291
}
```

Operations: `c` = INSERT, `u` = UPDATE, `d` = DELETE

Format details the transforms rely on:
- **Full after-image on UPDATE.** Because `after` contains every column, an
  UPDATE upserts the complete new row into `current_state` without nulling
  unchanged columns.
- **Transaction metadata is under `source`.** `source.txId`, `source.ts_ms`
  (commit time, ms), and `source.ts_ns` (commit time, ns -- the total-order
  key) live in `source`. The top-level `ts_ms`/`ts_ns` are CDC *processing*
  times. The transforms read tx id and commit timestamps from `source`.
- **`timestamptz` is an integer.** `created_at` arrives as microseconds since
  the Unix epoch (UTC), not an ISO string. The Lambda transforms convert it to
  an ISO-8601 string before writing to the `string` column.
- **`type` field.** `full` records carry inline images. Oversized rows are
  split into `chunked`/`fragment` records; the stateless transforms fail those
  to the Firehose error output rather than mis-parsing them.

## Security

- Amazon S3 bucket: `BlockPublicAccess` enabled, SSL-only (bucket policy denies `aws:SecureTransport=false`)
- Amazon Kinesis: KMS encryption at rest
- AWS IAM roles: least-privilege, scoped to specific resource ARNs
- Amazon Aurora DSQL connections: SSL required, IAM auth tokens (15-minute expiry)
- No public endpoints, no AWS Lambda function URLs
