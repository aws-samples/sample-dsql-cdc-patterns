"""Firehose record transformation Lambda for Amazon Aurora DSQL CDC events.

Flattens the nested DSQL CDC event envelope into a single-level JSON object
suitable for Athena queries.

DSQL CDC record format (see
https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html):
  - "op": "c" (insert), "u" (update), "d" (delete).
  - "after" holds the FULL row image for inserts and updates; null for deletes.
  - "before" is null for inserts/updates; for deletes it holds ONLY the
    primary key columns.
  - Transaction metadata (txId, ts_ms, ts_ns) lives under "source". The
    top-level ts_ms/ts_ns are CDC *processing* times, not commit times.
  - "timestamptz" columns (e.g. created_at) are serialized as integers:
    microseconds since the Unix epoch, UTC -- NOT ISO strings. This transform
    passes row images through unchanged, so before/after keep that encoding.
  - "type" is "full" for a complete record. Oversized rows are split into
    "chunked"/"fragment" records that require stateful reassembly; a stateless
    transform cannot handle them, so they are failed to the error output.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger(__name__)
logger.setLevel(_LOG_LEVEL)


class UnsupportedRecordType(Exception):
    """Raised for chunked/fragment records that need stateful reassembly."""


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Firehose record transformation handler."""
    output_records: list[dict[str, Any]] = []
    total = len(event.get("records", []))
    failed_count = 0

    for record in event.get("records", []):
        record_id: str = record["recordId"]
        try:
            transformed = _transform_record(record)
            output_records.append(
                {"recordId": record_id, "result": "Ok", "data": transformed}
            )
        except Exception:
            logger.exception("Failed to transform record", extra={"record_id": record_id})
            failed_count += 1
            output_records.append(
                {"recordId": record_id, "result": "ProcessingFailed", "data": record["data"]}
            )

    logger.info("Transformation complete", extra={"total": total, "failed": failed_count})
    return {"records": output_records}


def _transform_record(record: dict[str, Any]) -> str:
    """Decode, flatten, and re-encode a single Firehose record."""
    raw_data: str = record["data"]
    payload: dict[str, Any] = json.loads(base64.b64decode(raw_data))
    if payload.get("type", "full") != "full":
        raise UnsupportedRecordType(payload.get("type"))
    flattened = _flatten_cdc_event(payload)
    output_line = json.dumps(flattened, separators=(",", ":")) + "\n"
    return base64.b64encode(output_line.encode("utf-8")).decode("utf-8")


def _flatten_cdc_event(payload: dict[str, Any]) -> dict[str, Any]:
    """Flatten a nested CDC event into a single-level structure."""
    source: dict[str, Any] = payload.get("source", {})
    # Commit time of the source transaction, not CDC processing time.
    ts_ms: int = source.get("ts_ms", 0)
    ts_iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

    return {
        "op": payload.get("op"),
        "table": source.get("table"),
        "schema": source.get("schema"),
        "db": source.get("db"),
        "cluster": source.get("cluster"),
        "tx_id": source.get("txId"),
        "ts": ts_iso,
        "ts_ms": ts_ms,
        # Total order of transactions; use this to order events per row.
        "commit_ts_ns": source.get("ts_ns"),
        "before": payload.get("before"),
        "after": payload.get("after"),
    }
