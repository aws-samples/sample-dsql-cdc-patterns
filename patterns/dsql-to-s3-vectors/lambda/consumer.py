"""Kinesis Lambda consumer -- DSQL CDC to Amazon S3 Vectors.

Reads Amazon Aurora DSQL CDC events for the products table, generates an
Amazon Bedrock embedding for each inserted or updated row, and keeps an S3
Vectors index in sync (one vector per product, keyed by product id).

DSQL CDC record format (see
https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html):
  - "op": "c" (insert), "u" (update), "d" (delete).
  - "after" holds the FULL row image for inserts and updates; null for deletes.
  - "before" is null for inserts/updates; for deletes it holds ONLY the
    primary key columns -- enough to delete the vector by key, nothing to embed.
  - Transaction metadata (txId, ts_ms, ts_ns) lives under "source". The
    top-level ts_ms/ts_ns are CDC *processing* times, not commit times.
    source.ts_ns is a total order of transactions.
  - "timestamptz" columns (created_at, updated_at) are integers: microseconds
    since the Unix epoch, UTC. "numeric" columns (price) are JSON strings.
  - "type" is "full" for a complete record. Oversized rows are split into
    "chunked"/"fragment" records that require stateful reassembly; this
    stateless consumer logs and skips them.

Ordering: the CDC stream is UNORDERED, so a stale event can arrive after a
newer one. Each vector stores its source commit time (cdc_commit_ts_ns) in
metadata, and writes or deletes older than the stored version are skipped.
This read-compare-write is safe because the stream has one shard and the event
source mapping uses ParallelizationFactor 1 (one invocation at a time).
A deleted vector leaves no record behind, so an UPDATE delivered after its
row's DELETE has been applied in an earlier batch re-creates the vector.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import base64
import datetime
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VECTOR_BUCKET_NAME = os.environ["VECTOR_BUCKET_NAME"]
INDEX_NAME = os.environ.get("INDEX_NAME", "products")
EMBEDDING_MODEL_ID = os.environ.get("EMBEDDING_MODEL_ID", "amazon.titan-embed-text-v2:0")
EMBEDDING_DIMENSIONS = 1024
EMBEDDING_CONCURRENCY = 10
SOURCE_TABLE = "products"

s3v = boto3.client("s3vectors")
bedrock = boto3.client("bedrock-runtime")

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


def _to_float(value):
    """Parse a DSQL numeric (a JSON string such as "29.99"). Returns None for
    null and for "NaN"/"Infinity"/"-Infinity"."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def build_embedding_text(row):
    parts = [row.get("name") or "", row.get("description") or "",
             f"Category: {row.get('category') or ''}",
             f"Brand: {row.get('brand') or ''}"]
    tags = row.get("tags")
    if tags:
        parts.append(f"Tags: {tags}")
    return ". ".join(parts) + "."


def generate_embedding(text):
    resp = bedrock.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({"inputText": text, "dimensions": EMBEDDING_DIMENSIONS, "normalize": True}),
    )
    return json.loads(resp["body"].read())["embedding"]


def extract_metadata(row, commit_ts_ns):
    meta = {
        # Filterable keys (usable in QueryVectors filters)
        "category": row.get("category"),
        "brand": row.get("brand"),
        "price": _to_float(row.get("price")),
        "in_stock": bool(row.get("in_stock")),
        # Non-filterable keys (see MetadataConfiguration in the template)
        "name": row.get("name"),
        "description": row.get("description"),
        "tags": row.get("tags"),
        "created_at": _to_iso_timestamp(row.get("created_at")),
        "updated_at": _to_iso_timestamp(row.get("updated_at")),
        # String, because nanosecond epochs exceed float64 integer precision.
        "cdc_commit_ts_ns": str(commit_ts_ns),
    }
    # S3 Vectors metadata values cannot be null.
    return {k: v for k, v in meta.items() if v is not None}


def stored_commit_ts(keys):
    """Return {key: commit ts_ns} for the keys that already have a vector."""
    stored = {}
    for i in range(0, len(keys), 100):  # GetVectors accepts up to 100 keys
        resp = s3v.get_vectors(
            vectorBucketName=VECTOR_BUCKET_NAME,
            indexName=INDEX_NAME,
            keys=keys[i:i + 100],
            returnMetadata=True,
        )
        for v in resp.get("vectors", []):
            stored[v["key"]] = int((v.get("metadata") or {}).get("cdc_commit_ts_ns") or 0)
    return stored


def handler(event, context):
    records = event.get("Records", [])
    failed_seqs = []
    latest = {}  # product id -> newest event in this batch

    for record in records:
        seq = record["kinesis"]["sequenceNumber"]
        try:
            cdc = json.loads(base64.b64decode(record["kinesis"]["data"]))

            # Chunked main records and fragments need stateful reassembly.
            if cdc.get("type", "full") != "full":
                print(f"Skipping {cdc.get('type')} record {seq} (oversized row; not supported)")
                continue

            source = cdc.get("source") or {}
            if source.get("table") != SOURCE_TABLE:
                continue

            op = cdc.get("op")
            # after = full row for c/u; before = PK-only for d.
            row = cdc.get("before") if op == "d" else cdc.get("after")
            if op not in ("c", "u", "d") or not row or not row.get("id"):
                print(f"Skipping record {seq}: op={op!r} without a row id")
                continue

            key = str(row["id"])
            ts_ns = int(source.get("ts_ns") or 0)
            prev = latest.get(key)
            if prev is None or ts_ns > prev["ts_ns"]:
                latest[key] = {"op": op, "row": row, "ts_ns": ts_ns, "seq": seq}
        except Exception as e:
            print(f"Failed to parse record {seq}: {e}")
            failed_seqs.append(seq)

    if not latest:
        return {"batchItemFailures": [{"itemIdentifier": s} for s in failed_seqs]}

    try:
        stored = stored_commit_ts(list(latest))
    except Exception as e:
        # Without the stored versions we cannot order writes; retry the batch.
        print(f"GetVectors failed, retrying batch: {e}")
        return {"batchItemFailures": [{"itemIdentifier": r["kinesis"]["sequenceNumber"]} for r in records]}

    pending_puts, deletes, stale = [], [], 0
    for key, ev in latest.items():
        current = stored.get(key)
        if ev["op"] == "d":
            if current is None:
                continue  # nothing to delete
            if current < ev["ts_ns"]:
                deletes.append((key, ev["seq"]))
            else:
                stale += 1
        elif current is None or current < ev["ts_ns"]:
            pending_puts.append((key, ev))
        else:
            stale += 1

    # Generate embeddings with concurrency
    put_vectors = []
    if pending_puts:
        def embed_item(key, ev):
            return {
                "key": key,
                "data": {"float32": generate_embedding(build_embedding_text(ev["row"]))},
                "metadata": extract_metadata(ev["row"], ev["ts_ns"]),
                "seq": ev["seq"],
            }

        with ThreadPoolExecutor(max_workers=EMBEDDING_CONCURRENCY) as pool:
            futures = {pool.submit(embed_item, key, ev): (key, ev) for key, ev in pending_puts}
            for future in as_completed(futures):
                key, ev = futures[future]
                try:
                    put_vectors.append(future.result())
                except Exception as e:
                    print(f"Embedding failed for {key}: {e}")
                    failed_seqs.append(ev["seq"])

    # Batch PutVectors (up to 500 per call)
    for i in range(0, len(put_vectors), 500):
        batch = put_vectors[i:i + 500]
        try:
            s3v.put_vectors(
                vectorBucketName=VECTOR_BUCKET_NAME,
                indexName=INDEX_NAME,
                vectors=[{k: v[k] for k in ("key", "data", "metadata")} for v in batch],
            )
        except Exception as e:
            print(f"PutVectors failed: {e}")
            failed_seqs.extend(v["seq"] for v in batch)

    # Batch DeleteVectors (up to 500 per call)
    for i in range(0, len(deletes), 500):
        batch = deletes[i:i + 500]
        try:
            s3v.delete_vectors(
                vectorBucketName=VECTOR_BUCKET_NAME,
                indexName=INDEX_NAME,
                keys=[key for key, _ in batch],
            )
        except Exception as e:
            print(f"DeleteVectors failed: {e}")
            failed_seqs.extend(seq for _, seq in batch)

    print(f"Processed {len(records)} records: {len(put_vectors)} puts, "
          f"{len(deletes)} deletes, {stale} stale skipped, {len(failed_seqs)} failed")
    return {"batchItemFailures": [{"itemIdentifier": s} for s in failed_seqs]}
