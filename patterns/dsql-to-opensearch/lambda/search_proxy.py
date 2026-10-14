"""Search proxy -- routes API Gateway requests to OpenSearch Serverless queries.

Endpoints (all require IAM auth at API Gateway):
  GET    /search          full-text search with category/brand/price/stock filters
  GET    /autocomplete    prefix suggestions on product names
  GET    /facets          category, brand, price-range, and stock aggregations
  GET    /stats           document count
  GET    /products        list products, newest first
  GET    /products/{id}   fetch one product
  DELETE /index           delete and recreate the products index

This is a readable copy of the inline Lambda code in the CFN template.
It is NOT deployed from here -- the template uses ZipFile inline code.
"""

import json,os,re,urllib.request,urllib.error

ENDPOINT=os.environ['COLLECTION_ENDPOINT']
INDEX=os.environ.get('INDEX_NAME','products')
REGION=os.environ.get('AWS_REGION','us-east-1')
SERVICE='aoss'

# Must match INDEX_SETTINGS in the CDC consumer.
INDEX_SETTINGS=json.dumps({
  "settings":{"analysis":{"analyzer":{"autocomplete_analyzer":{"type":"custom","tokenizer":"autocomplete_tokenizer","filter":["lowercase"]},"autocomplete_search_analyzer":{"type":"custom","tokenizer":"standard","filter":["lowercase"]}},"tokenizer":{"autocomplete_tokenizer":{"type":"edge_ngram","min_gram":2,"max_gram":20,"token_chars":["letter","digit"]}}}},
  "mappings":{"properties":{"name":{"type":"text","fields":{"autocomplete":{"type":"text","analyzer":"autocomplete_analyzer","search_analyzer":"autocomplete_search_analyzer"}}},"description":{"type":"text"},"category":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"brand":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"price":{"type":"float"},"in_stock":{"type":"boolean"},"tags":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"created_at":{"type":"date"},"updated_at":{"type":"date"},"cdc_commit_ts_ns":{"type":"long"}}}
})

_UUID=re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

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
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise Exception(f"HTTP {e.code}: {e.read().decode()}")

def resp(code,body):
    return {'statusCode':code,'headers':{'Content-Type':'application/json'},'body':json.dumps(body)}

def handle_search(event):
    p=event.get('queryStringParameters') or {}
    q=p.get('q');cat=p.get('category');br=p.get('brand')
    pmin=p.get('price_min');pmax=p.get('price_max');ins=p.get('in_stock')
    fr=int(p.get('from','0'));sz=int(p.get('size','20'))
    must=[];filt=[]
    if q:
        must.append({'multi_match':{'query':q,'fields':['name^3','description','tags','category','brand'],'fuzziness':'AUTO'}})
    else:
        must.append({'match_all':{}})
    if cat:filt.append({'term':{'category.keyword':cat}})
    if br:filt.append({'term':{'brand.keyword':br}})
    if pmin or pmax:
        rng={}
        if pmin:rng['gte']=float(pmin)
        if pmax:rng['lte']=float(pmax)
        filt.append({'range':{'price':rng}})
    if ins is not None:filt.append({'term':{'in_stock':ins=='true'}})
    body={'query':{'bool':{'must':must,'filter':filt}},'from':fr,'size':sz,'highlight':{'fields':{'name':{},'description':{}},'pre_tags':['<em>'],'post_tags':['</em>']}}
    result=_req('POST',f'{INDEX}/_search',json.dumps(body))
    hits=[{'id':h['_id'],**h['_source'],'_score':h['_score'],'highlight':h.get('highlight')} for h in result['hits']['hits']]
    return resp(200,{'hits':hits,'total':result['hits'].get('total',{}).get('value',0),'took':result.get('took')})

def handle_autocomplete(event):
    p=event.get('queryStringParameters') or {}
    q=p.get('q');sz=int(p.get('size','8'))
    if not q:return resp(200,{'suggestions':[]})
    result=_req('POST',f'{INDEX}/_search',json.dumps({'query':{'match':{'name.autocomplete':q}},'size':sz,'_source':['name','category']}))
    sugs=[{'id':h['_id'],'name':h['_source']['name'],'category':h['_source']['category']} for h in result['hits']['hits']]
    return resp(200,{'suggestions':sugs})

def handle_facets():
    body={'size':0,'aggs':{'categories':{'terms':{'field':'category.keyword','size':20}},'brands':{'terms':{'field':'brand.keyword','size':20}},'price_ranges':{'range':{'field':'price','ranges':[{'key':'$0-25','from':0,'to':25},{'key':'$25-50','from':25,'to':50},{'key':'$50-100','from':50,'to':100},{'key':'$100-250','from':100,'to':250},{'key':'$250-500','from':250,'to':500},{'key':'$500+','from':500}]}},'in_stock_count':{'terms':{'field':'in_stock'}}}}
    result=_req('POST',f'{INDEX}/_search',json.dumps(body))
    aggs=result.get('aggregations',{})
    return resp(200,{'categories':aggs.get('categories',{}).get('buckets',[]),'brands':aggs.get('brands',{}).get('buckets',[]),'price_ranges':aggs.get('price_ranges',{}).get('buckets',[]),'in_stock_count':aggs.get('in_stock_count',{}).get('buckets',[])})

def handle_stats():
    try:
        result=_req('POST',f'{INDEX}/_count',json.dumps({'query':{'match_all':{}}}))
        return resp(200,{'doc_count':result.get('count',0),'index_info':None})
    except Exception as e:
        if '404' in str(e):return resp(200,{'doc_count':0,'index_info':None})
        raise

def handle_list_products(event):
    p=event.get('queryStringParameters') or {}
    fr=int(p.get('from','0'));sz=int(p.get('size','50'))
    result=_req('POST',f'{INDEX}/_search',json.dumps({'query':{'match_all':{}},'sort':[{'created_at':{'order':'desc'}}],'from':fr,'size':sz}))
    prods=[{'id':h['_id'],**h['_source']} for h in result['hits']['hits']]
    return resp(200,{'products':prods,'total':result['hits'].get('total',{}).get('value',0)})

def handle_get_product(pid):
    # Product ids are DSQL UUIDs; reject anything else so it can't alter the URL path.
    if not _UUID.match(pid):return resp(400,{'error':'Invalid product id'})
    try:
        result=_req('GET',f'{INDEX}/_doc/{pid}')
        return resp(200,{'id':result['_id'],**result['_source']})
    except Exception as e:
        if '404' in str(e):return resp(404,{'error':'Product not found'})
        raise

def handle_delete_index():
    try:
        _req('DELETE',INDEX)
    except Exception as e:
        if '404' not in str(e):raise
    _req('PUT',INDEX,INDEX_SETTINGS)
    return resp(200,{'message':'Index deleted and recreated'})

def handler(event,context):
    path=event.get('path','')
    method=event.get('httpMethod','GET')
    print(f"{method} {path}")
    try:
        if method=='GET' and path=='/search':return handle_search(event)
        if method=='GET' and path=='/autocomplete':return handle_autocomplete(event)
        if method=='GET' and path=='/facets':return handle_facets()
        if method=='GET' and path=='/stats':return handle_stats()
        if method=='GET' and path=='/products':return handle_list_products(event)
        if method=='GET' and path.startswith('/products/'):
            return handle_get_product(path.split('/products/',1)[1])
        if method=='DELETE' and path=='/index':return handle_delete_index()
        return resp(404,{'error':f'Not found: {method} {path}'})
    except Exception as e:
        print(f"Handler error: {e}")
        return resp(500,{'error':'Internal server error'})
