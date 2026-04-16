"""Firehose Lambda transform -- merge mode.

Flattens Debezium-compatible CDC events from Amazon Aurora DSQL into records
for the current_state Iceberg table. All operations (including deletes) are
emitted as upserts. Deletes set _is_deleted=true (tombstone) so that
out-of-order delivery does not resurrect deleted rows.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import base64
import json
import os

TRANSFORM_MODE = os.environ.get("TRANSFORM_MODE", "merge")
DATABASE_NAME = "dsql_cdc_iceberg"


def handler(event, context):
    output = []
    for record in event.get("records", []):
        try:
            payload = json.loads(base64.b64decode(record["data"]))
            cdc_op = payload.get("op", "c")
            row = payload.get("before") if cdc_op == "d" else payload.get("after")
            if not row:
                output.append({"recordId": record["recordId"], "result": "ProcessingFailed", "data": ""})
                continue

            flattened = {
                "id": row.get("id"),
                "name": row.get("name"),
                "email": row.get("email"),
                "created_at": row.get("created_at"),
                "_cdc_commit_ts_ns": payload.get("source", {}).get("ts_ns"),
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
