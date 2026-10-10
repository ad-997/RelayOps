import asyncio,os,json,time,hmac,hashlib,secrets
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI,HTTPException,Request,Depends,Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel,Field
import httpx
from engine import Queue,Conflict,process_batch
from destinations import configured_destinations
import logging

ROOT=Path(__file__).parent
SECRET=os.environ.get('RELAY_SECRET') or secrets.token_hex(32)
API_KEY=os.environ.get('RELAY_API_KEY','')
DB=os.environ.get('RELAY_DB',str(ROOT/'relay.db'))
BASE=os.environ.get('RELAY_RECEIVER_BASE','http://127.0.0.1:8082')
PRODUCTION=os.environ.get('RELAY_ENV')=='production'
if PRODUCTION and (not API_KEY or not os.environ.get('RELAY_SECRET') or not os.environ.get('RELAY_DB')):
    raise RuntimeError('Production requires RELAY_API_KEY, RELAY_SECRET and durable RELAY_DB')
URLS=configured_destinations(BASE, PRODUCTION)
queue=Queue(DB)
async def worker():
    async with httpx.AsyncClient(timeout=5,follow_redirects=False) as client:
        while True:
            try:
                n=await process_batch(queue,client,URLS,SECRET)
                await asyncio.sleep(.05 if n else .3)
            except Exception:
                logging.exception('Delivery batch failed; expired leases will be recovered')
                await asyncio.sleep(1)
@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(worker())
    yield
    task.cancel()
    try:await task
    except asyncio.CancelledError:pass
app=FastAPI(title='RelayOps',description='Durable, authenticated webhook delivery to operator-configured destinations',lifespan=lifespan)
async def authorize(request:Request):
    if API_KEY and not hmac.compare_digest(request.headers.get('X-API-Key',''),API_KEY):
        raise HTTPException(401,'API key required')
class EventIn(BaseModel):
    key:str=Field(min_length=1,max_length=200)
    endpoint:str
    payload:dict
@app.get('/healthz')
def health():
    queue.metrics()
    return {'status':'ok'}
@app.get('/api/destinations',dependencies=[Depends(authorize)])
def destinations():return [{'name':name,'demo':name in ('healthy','flaky','offline')} for name in URLS]
@app.get('/api/metrics',dependencies=[Depends(authorize)])
def metrics():return queue.metrics()
@app.get('/api/events',dependencies=[Depends(authorize)])
def events(status:str|None=None,limit:int=Query(100,ge=1,le=500)):
    if status and status not in ('queued','sending','retrying','delivered','dead'):raise HTTPException(400,'Invalid state')
    return queue.events(status,limit)
@app.post('/api/events',dependencies=[Depends(authorize)])
def create(event:EventIn):
    if event.endpoint not in URLS:raise HTTPException(400,'Unknown endpoint')
    try:
        row,created=queue.enqueue(event.key,event.endpoint,event.payload)
        return {'event':row,'created':created}
    except Conflict as e:raise HTTPException(409,str(e))
    except ValueError as e:raise HTTPException(400,str(e))
@app.get('/api/events/{ident}/attempts',dependencies=[Depends(authorize)])
def attempts(ident:str):return queue.history(ident)
@app.post('/api/events/{ident}/replay',dependencies=[Depends(authorize)])
def replay(ident:str):
    try:queue.replay(ident);return {'queued':True}
    except KeyError:raise HTTPException(404,'Event not found')
    except Conflict as e:raise HTTPException(409,str(e))
@app.post('/receivers/{kind}')
async def receiver(kind:str,request:Request):
    if kind not in ('healthy','flaky','offline') or kind not in URLS:raise HTTPException(404)
    body=await request.body();stamp=request.headers.get('X-Relay-Timestamp','')
    try:
        if abs(time.time()-int(stamp))>300:raise ValueError()
    except ValueError:raise HTTPException(401,'Expired signature')
    expected='sha256='+hmac.new(SECRET.encode(),(stamp+'.').encode()+body,hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected,request.headers.get('X-Relay-Signature','')):raise HTTPException(401,'Bad signature')
    ident=request.headers.get('X-Relay-Event','')
    if kind=='offline':raise HTTPException(503,'Simulated receiver outage')
    if kind=='flaky' and len(queue.history(ident))<2:raise HTTPException(503,'Simulated transient failure')
    return {'accepted':True,'event':ident}
@app.get('/')
def index():return FileResponse(ROOT/'static/index.html')
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
