"""Firehose Lambda transform -- append mode.

Flattens Amazon Aurora DSQL CDC events into records for the cdc_events Iceberg
table. Every CDC event becomes an insert.

DSQL CDC record format (see
https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html):
  - "op": "c" (insert), "u" (update), "d" (delete).
  - "after" holds the FULL row image for inserts and updates; null for deletes.
  - "before" is null for inserts/updates; for deletes it holds ONLY the
    primary key columns.
  - Transaction metadata (txId, ts_ms, ts_ns) lives under "source". The
    top-level ts_ms/ts_ns are CDC *processing* times, not commit times.
  - "timestamptz" columns (e.g. created_at) are serialized as integers:
    microseconds since the Unix epoch, UTC -- NOT ISO strings.
  - "type" is "full" for a complete record. Oversized rows are split into
    "chunked"/"fragment" records that require stateful reassembly; a stateless
    transform cannot handle them, so they are failed to the error output.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import base64
import datetime
import json
import os

TRANSFORM_MODE = os.environ.get("TRANSFORM_MODE", "append")
DATABASE_NAME = "dsql_cdc_iceberg"

# DSQL sentinel microsecond values for +/-infinity timestamps. These do not map
# to meaningful calendar dates, so we emit null rather than a bogus timestamp.
_TS_POS_INF = 9223372036825200000
_TS_NEG_INF = -9223372036832400000


def _to_iso_timestamp(value):
    """Convert a DSQL timestamptz (microseconds since epoch) to an ISO-8601 UTC
    string. Pass through values that are already strings or null."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value  # already a string, or None
    if value in (_TS_POS_INF, _TS_NEG_INF):
        return None
    try:
        return datetime.datetime.fromtimestamp(
            value / 1_000_000, tz=datetime.timezone.utc
        ).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def handler(event, context):
    output = []
    for record in event.get("records", []):
        try:
            payload = json.loads(base64.b64decode(record["data"]))

            # Only "full" records carry inline before/after images. Chunked main
            # records and fragments require stateful reassembly across records.
            if payload.get("type", "full") != "full":
                output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": ""})
                continue

            op = payload.get("op")
            # after = full row for c/u; before = PK-only for d.
            row = payload.get("before") if op == "d" else payload.get("after")
            if not row:
                output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": ""})
                continue

            source = payload.get("source", {})
            flattened = {
                "id": row.get("id"),
                "name": row.get("name"),
                "email": row.get("email"),
                "created_at": _to_iso_timestamp(row.get("created_at")),
                "_cdc_op": op,
                "_cdc_tx_id": source.get("txId"),
                "_cdc_ts_ms": source.get("ts_ms"),
                "_cdc_source_table": source.get("table"),
                "_cdc_commit_ts_ns": source.get("ts_ns"),
            }
            data = base64.b64encode(
                (json.dumps(flattened, separators=(",", ":")) + "\n").encode()
            ).decode()
            output.append({
                "recordId": record["recordId"],
                "result": "Ok",
                "data": data,
                "metadata": {
                    "otfMetadata": {
                        "destinationDatabaseName": DATABASE_NAME,
                        "destinationTableName": "cdc_events",
                        "operation": "insert",
                    }
                },
            })
        except Exception:
            output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": record["data"]})
    return {"records": output}
