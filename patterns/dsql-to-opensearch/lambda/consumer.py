"""Kinesis CDC consumer -- syncs Amazon Aurora DSQL products to OpenSearch Serverless.

Reads DSQL CDC events from Kinesis and applies them to the `products` index
with the OpenSearch _bulk API: inserts and updates index the full document,
deletes remove it by primary key.

DSQL CDC record format (see
https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html):
  - "op": "c" (insert), "u" (update), "d" (delete).
  - "after" holds the FULL row image for inserts and updates; null for deletes.
  - "before" is null for inserts/updates; for deletes it holds ONLY the
    primary key columns, which is enough to delete the document by `id`.
  - Transaction metadata (txId, ts_ms, ts_ns) lives under "source". The
    top-level ts_ms/ts_ns are CDC *processing* times, not commit times.
    source.ts_ns is a total order of DSQL transactions.
  - "timestamptz" columns (created_at, updated_at) are serialized as integers:
    microseconds since the Unix epoch, UTC. OpenSearch would read a bare
    integer as epoch *milliseconds*, so they are converted to ISO-8601.
  - "type" is "full" for a complete record. Oversized rows are split into
    "chunked"/"fragment" records that require stateful reassembly; this
    consumer reports them as batch item failures, so after retries the event
    source mapping sends their metadata to the SQS dead-letter queue.

Ordering: the CDC stream is UNORDERED. Within a batch, only the latest event
per `id` (by source.ts_ns) is applied. Across batches, a late-arriving older
event can still overwrite a newer document -- see the README.

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import base64,json,os,urllib.request,urllib.error,datetime

ENDPOINT=os.environ['COLLECTION_ENDPOINT']
INDEX=os.environ.get('INDEX_NAME','products')
REGION=os.environ.get('AWS_REGION','us-east-1')
SERVICE='aoss'

INDEX_SETTINGS=json.dumps({
  "settings":{"analysis":{"analyzer":{"autocomplete_analyzer":{"type":"custom","tokenizer":"autocomplete_tokenizer","filter":["lowercase"]},"autocomplete_search_analyzer":{"type":"custom","tokenizer":"standard","filter":["lowercase"]}},"tokenizer":{"autocomplete_tokenizer":{"type":"edge_ngram","min_gram":2,"max_gram":20,"token_chars":["letter","digit"]}}}},
  "mappings":{"properties":{"name":{"type":"text","fields":{"autocomplete":{"type":"text","analyzer":"autocomplete_analyzer","search_analyzer":"autocomplete_search_analyzer"}}},"description":{"type":"text"},"category":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"brand":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"price":{"type":"float"},"in_stock":{"type":"boolean"},"tags":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"created_at":{"type":"date"},"updated_at":{"type":"date"},"cdc_commit_ts_ns":{"type":"long"}}}
})

# DSQL sentinel microsecond values for +/-infinity timestamps.
_TS_INF=(9223372036825200000,-9223372036832400000)

_index_ok=False

def _ts(v):
    # DSQL timestamptz (microseconds since epoch) -> ISO-8601 UTC string.
    if isinstance(v,bool) or not isinstance(v,(int,float)):
        return v
    if v in _TS_INF:
        return None
    try:
        return datetime.datetime.fromtimestamp(v/1000000,tz=datetime.timezone.utc).isoformat()
    except (OverflowError,OSError,ValueError):
        return None

def _sign(method,url,body,headers):
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.session import Session as BotoSession
    creds=BotoSession().get_credentials().get_frozen_credentials()
    req=AWSRequest(method=method,url=url,data=body,headers=headers)
    SigV4Auth(creds,SERVICE,REGION).add_auth(req)
    return dict(req.headers)

def _req(method,path,body=None):
    url=f"{ENDPOINT}/{path}"
    data=body.encode() if body else b''
    hdrs=_sign(method,url,data,{'Content-Type':'application/json'})
    r=urllib.request.Request(url,data=data if body else None,headers=hdrs,method=method)
    try:
        with urllib.request.urlopen(r) as resp:
            raw=resp.read()
            # HEAD responses have no body.
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()}")
        raise

def ensure_index():
    global _index_ok
    if _index_ok:
        return
    try:
        _req('HEAD',INDEX)
    except urllib.error.HTTPError as e:
        if e.code!=404:
            raise
        print(f'Creating index {INDEX}')
        _req('PUT',INDEX,INDEX_SETTINGS)
    _index_ok=True

def handler(event,context):
    failed=[]
    ensure_index()
    latest={}  # id -> (commit ts_ns, sequence number, bulk lines)
    for rec in event.get('Records',[]):
        seq=rec['kinesis']['sequenceNumber']
        try:
            payload=json.loads(base64.b64decode(rec['kinesis']['data']))
            if payload.get('type','full')!='full':
                print(f"Unsupported CDC record type {payload.get('type')!r} at {seq}")
                failed.append(seq)
                continue
            src=payload.get('source',{})
            if src.get('table')!='products':
                continue
            op=payload.get('op')
            # after = full row for c/u; before = PK-only for d.
            row=payload.get('before') if op=='d' else payload.get('after')
            if not row or not row.get('id'):
                continue
            pid=row['id']
            ts_ns=src.get('ts_ns') or 0
            prev=latest.get(pid)
            if prev and prev[0]>ts_ns:
                continue  # an event with a later commit is already in this batch
            if op=='d':
                lines=[json.dumps({"delete":{"_index":INDEX,"_id":pid}})]
            else:
                doc={
                    'name':row.get('name'),
                    'description':row.get('description'),
                    'category':row.get('category'),
                    'brand':row.get('brand'),
                    'price':float(row.get('price') or 0),
                    'in_stock':row.get('in_stock') in (True,'t','true'),
                    'tags':row.get('tags') or '',
                    'created_at':_ts(row.get('created_at')),
                    'updated_at':_ts(row.get('updated_at')),
                    'cdc_commit_ts_ns':ts_ns,
                }
                lines=[json.dumps({"index":{"_index":INDEX,"_id":pid}}),json.dumps(doc)]
            latest[pid]=(ts_ns,seq,lines)
        except Exception as ex:
            print(f"Record error {seq}: {ex}")
            failed.append(seq)
    ops=list(latest.values())
    if ops:
        body='\n'.join(l for o in ops for l in o[2])+'\n'
        try:
            result=_req('POST','_bulk',body)
            if result.get('errors'):
                for i,item in enumerate(result.get('items',[])):
                    act=item.get('index') or item.get('delete') or {}
                    if act.get('error'):
                        print(f"Bulk error: {json.dumps(act['error'])}")
                        if i<len(ops):
                            failed.append(ops[i][1])
            print(f"Processed {len(result.get('items',[]))} ops (errors={result.get('errors')})")
        except Exception as ex:
            print(f"Bulk failed: {ex}")
            failed.extend(rec['kinesis']['sequenceNumber'] for rec in event.get('Records',[]))
    return {'batchItemFailures':[{'itemIdentifier':fid} for fid in dict.fromkeys(failed)]}
