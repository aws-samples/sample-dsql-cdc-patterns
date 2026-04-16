"""Firehose Lambda transform -- append mode.

Flattens Debezium-compatible CDC events from Amazon Aurora DSQL into records
for the cdc_events Iceberg table. Every CDC event becomes an insert.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import base64
import json
import os

TRANSFORM_MODE = os.environ.get("TRANSFORM_MODE", "append")
DATABASE_NAME = "dsql_cdc_iceberg"


def handler(event, context):
    output = []
    for record in event.get("records", []):
        try:
            payload = json.loads(base64.b64decode(record["data"]))
            row = payload.get("before") if payload.get("op") == "d" else payload.get("after")
            if not row:
                output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": ""})
                continue

            flattened = {
                "id": row.get("id"),
                "name": row.get("name"),
                "email": row.get("email"),
                "created_at": row.get("created_at"),
                "_cdc_op": payload.get("op"),
                "_cdc_tx_id": payload.get("txId"),
                "_cdc_ts_ms": payload.get("ts_ms"),
                "_cdc_source_table": payload.get("source", {}).get("table"),
                "_cdc_commit_ts_ns": payload.get("source", {}).get("ts_ns"),
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
