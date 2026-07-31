"""Firehose Lambda transform -- merge mode.

Flattens Amazon Aurora DSQL CDC events into records for the current_state
Iceberg table. All operations (including deletes) are emitted as upserts keyed
on `id` (see the Firehose DestinationTableConfigurationList UniqueKeys). Deletes
set _is_deleted=true (tombstone) so downstream consumers can filter them out.

DSQL CDC record format (see
https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html):
  - "op": "c" (insert), "u" (update), "d" (delete).
  - "after" holds the FULL row image for inserts and updates -- so an UPDATE
    upserts the complete new row, never nulling unchanged columns.
  - "before" is null for inserts/updates; for deletes it holds ONLY the primary
    key columns, which is enough to tombstone the row by `id`.
  - "timestamptz" columns (e.g. created_at) are serialized as integers:
    microseconds since the Unix epoch, UTC -- NOT ISO strings.
  - "type" is "full" for a complete record. Chunked/fragment (oversized)
    records need stateful reassembly and are failed to the error output.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import base64
import datetime
import json
import os

TRANSFORM_MODE = os.environ.get("TRANSFORM_MODE", "merge")
DATABASE_NAME = "dsql_cdc_iceberg"

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

            if payload.get("type", "full") != "full":
                output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": ""})
                continue

            cdc_op = payload.get("op", "c")
            # after = full row for c/u; before = PK-only for d (enough to tombstone).
            row = payload.get("before") if cdc_op == "d" else payload.get("after")
            if not row:
                output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": ""})
                continue

            source = payload.get("source", {})
            flattened = {
                "id": row.get("id"),
                "name": row.get("name"),
                "email": row.get("email"),
                "created_at": _to_iso_timestamp(row.get("created_at")),
                "_cdc_commit_ts_ns": source.get("ts_ns"),
                "_is_deleted": cdc_op == "d",
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
                        "destinationTableName": "current_state",
                        "operation": "upsert",
                    }
                },
            })
        except Exception:
            output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": record["data"]})
    return {"records": output}
