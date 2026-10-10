"""Separate sample fulfillment app. Run locally; do not expose its outage control."""
import os,sqlite3,time,hmac,hashlib
from fastapi import FastAPI,Request,HTTPException
app=FastAPI(title='Example fulfillment receiver')
secret=os.environ['RELAY_SECRET']
db=sqlite3.connect(os.environ.get('ORDER_DB','orders.db'),check_same_thread=False)
db.execute('CREATE TABLE IF NOT EXISTS orders(event_id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
db.commit()
@app.post('/orders')
async def order(request:Request):
    if os.environ.get('OUTAGE_FILE') and os.path.exists(os.environ['OUTAGE_FILE']):
        raise HTTPException(503,'Fulfillment temporarily unavailable')
    body=await request.body();stamp=request.headers.get('X-Relay-Timestamp','')
    try:
        if abs(time.time()-int(stamp))>300:raise ValueError()
    except ValueError:raise HTTPException(401,'Expired signature')
    expected='sha256='+hmac.new(secret.encode(),(stamp+'.').encode()+body,hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected,request.headers.get('X-Relay-Signature','')):
        raise HTTPException(401,'Invalid signature')
    ident=request.headers.get('X-Relay-Event')
    if not ident:raise HTTPException(400,'Missing event ID')
    cursor=db.execute('INSERT OR IGNORE INTO orders VALUES(?,?)',(ident,body.decode()))
    db.commit()
    return {'accepted':True,'duplicate':cursor.rowcount==0}
@app.get('/orders')
def orders():return [{'event_id':r[0],'payload':r[1]} for r in db.execute('SELECT * FROM orders')]
