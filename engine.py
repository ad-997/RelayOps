"""Transactional outbox with leases, idempotency, backoff, and attempt history."""
import sqlite3,json,time,uuid,hashlib,hmac,asyncio,random
from pathlib import Path

class Conflict(ValueError): pass
class Queue:
    """Portable transactional queue. SQLite for local runs; MySQL for compose."""
    def __init__(self,path,clock=time.time,max_attempts=4,base_delay=1):
        import sqlalchemy as sa
        self.sa=sa;self.clock=clock;self.max_attempts=max_attempts;self.base_delay=base_delay
        url=str(path) if "://" in str(path) else "sqlite:///"+str(path)
        self.engine=sa.create_engine(url,pool_pre_ping=True,connect_args={"timeout":20} if url.startswith('sqlite:') else {})
        self.sqlite=self.engine.dialect.name=='sqlite'
        m=sa.MetaData()
        self.e=sa.Table('events',m,
          sa.Column('id',sa.String(36),primary_key=True),sa.Column('idem_key',sa.String(200),unique=True,nullable=False),
          sa.Column('fingerprint',sa.String(64),nullable=False),sa.Column('endpoint',sa.String(40),nullable=False),
          sa.Column('payload',sa.Text,nullable=False),sa.Column('status',sa.String(20),nullable=False),
          sa.Column('attempts',sa.Integer,nullable=False,default=0),sa.Column('due',sa.Float,nullable=False),
          sa.Column('created',sa.Float,nullable=False),sa.Column('lease_token',sa.String(36)),
          sa.Column('lease_until',sa.Float),sa.Column('delivered',sa.Float))
        self.a=sa.Table('attempts',m,sa.Column('id',sa.Integer,primary_key=True,autoincrement=True),
          sa.Column('event_id',sa.String(36),sa.ForeignKey('events.id'),nullable=False),
          sa.Column('number',sa.Integer,nullable=False),sa.Column('code',sa.Integer),
          sa.Column('duration_ms',sa.Float,nullable=False),sa.Column('outcome',sa.String(20),nullable=False),sa.Column('created',sa.Float,nullable=False))
        sa.Index('ready_events',self.e.c.status,self.e.c.due)
        m.create_all(self.engine)
        if self.sqlite:
            with self.engine.connect() as db:db.exec_driver_sql('PRAGMA journal_mode=WAL')
    def lock(self,db):
        if self.sqlite:db.exec_driver_sql('BEGIN IMMEDIATE')
    def enqueue(self,key,endpoint,payload):
        sa=self.sa;e=self.e;raw=json.dumps(payload,sort_keys=True,separators=(',',':'))
        if not 1<=len(key)<=200:raise ValueError('Idempotency key must be 1–200 characters')
        if len(raw.encode())>65536:raise ValueError('Payload exceeds 64 KiB')
        fingerprint=hashlib.sha256((endpoint+'\n'+raw).encode()).hexdigest()
        def existing(db):return db.execute(sa.select(e).where(e.c.idem_key==key)).mappings().first()
        def check(row):
            if row['fingerprint']!=fingerprint:raise Conflict('Idempotency key reused with different data')
            return dict(row),False
        try:
            with self.engine.begin() as db:
                self.lock(db);row=existing(db)
                if row:return check(row)
                ident=str(uuid.uuid4());now=self.clock()
                db.execute(e.insert().values(id=ident,idem_key=key,fingerprint=fingerprint,endpoint=endpoint,payload=raw,status='queued',attempts=0,due=now,created=now))
                return dict(db.execute(sa.select(e).where(e.c.id==ident)).mappings().one()),True
        except sa.exc.IntegrityError:
            # Unique key resolves races between concurrent producers.
            with self.engine.connect() as db:
                row=existing(db)
                if row:return check(row)
                raise
    def claim(self,lease_seconds=30):
        sa=self.sa;e=self.e;now=self.clock()
        with self.engine.begin() as db:
            self.lock(db)
            db.execute(e.update().where((e.c.status=='sending') & (e.c.lease_until<=now)).values(status='retrying',lease_token=None))
            query=sa.select(e).where(e.c.status.in_(['queued','retrying']) & (e.c.due<=now)).order_by(e.c.due,e.c.created).limit(1)
            if not self.sqlite:query=query.with_for_update(skip_locked=True)
            row=db.execute(query).mappings().first()
            if not row:return None
            token=str(uuid.uuid4());db.execute(e.update().where(e.c.id==row['id']).values(status='sending',lease_token=token,lease_until=now+lease_seconds))
            out=dict(row);out['lease_token']=token;return out
    def complete(self,event,code,duration):
        sa=self.sa;e=self.e;now=self.clock();success=code is not None and 200<=code<300
        with self.engine.begin() as db:
            self.lock(db);q=sa.select(e).where(e.c.id==event['id'])
            if not self.sqlite:q=q.with_for_update()
            row=db.execute(q).mappings().first()
            if not row or row['status']!='sending' or row['lease_token']!=event['lease_token']:return False
            n=row['attempts']+1;retryable=code is None or code in (408,425,429) or code>=500
            state='delivered' if success else ('retrying' if retryable and n<self.max_attempts else 'dead')
            delay=min(300,self.base_delay*2**(n-1))*(1+random.random()*.1)
            db.execute(self.a.insert().values(event_id=row['id'],number=n,code=code,duration_ms=round(duration,3),outcome=state,created=now))
            db.execute(e.update().where(e.c.id==row['id']).values(status=state,attempts=n,due=now+delay,lease_token=None,lease_until=None,delivered=now if success else None))
            return True
    def replay(self,ident):
        sa=self.sa;e=self.e
        with self.engine.begin() as db:
            self.lock(db);q=sa.select(e).where(e.c.id==ident)
            if not self.sqlite:q=q.with_for_update()
            row=db.execute(q).mappings().first()
            if not row:raise KeyError(ident)
            if row['status']!='dead':raise Conflict('Only dead-letter events can be replayed')
            db.execute(e.update().where(e.c.id==ident).values(status='queued',attempts=0,due=self.clock(),lease_token=None,lease_until=None))
    def events(self,status=None,limit=100):
        q=self.sa.select(self.e).order_by(self.e.c.created.desc()).limit(limit)
        if status:q=q.where(self.e.c.status==status)
        with self.engine.connect() as db:return [dict(r) for r in db.execute(q).mappings()]
    def history(self,ident):
        with self.engine.connect() as db:
            return [dict(r) for r in db.execute(self.sa.select(self.a).where(self.a.c.event_id==ident).order_by(self.a.c.id)).mappings()]
    def metrics(self):
        sa=self.sa;e=self.e
        with self.engine.connect() as db:
            counts={s:0 for s in ('queued','sending','retrying','delivered','dead')}
            counts.update({r[0]:r[1] for r in db.execute(sa.select(e.c.status,sa.func.count()).group_by(e.c.status))})
            latencies=sorted(r[0] for r in db.execute(sa.select(self.a.c.duration_ms)))
            return {'total':sum(counts.values()),'counts':counts,'attempts':len(latencies),
                    'p95_attempt_ms':round(latencies[min(len(latencies)-1,int(len(latencies)*.95))],2) if latencies else 0}

def signed_headers(secret,event,timestamp):
    message=(str(timestamp)+'.'+event['payload']).encode()
    sig=hmac.new(secret.encode(),message,hashlib.sha256).hexdigest()
    return {'Content-Type':'application/json','X-Relay-Event':event['id'],
            'X-Relay-Timestamp':str(timestamp),'X-Relay-Signature':'sha256='+sig}

async def deliver(queue,event,client,urls,secret):
    start=time.perf_counter();code=None
    try:
        response=await client.post(urls[event['endpoint']],content=event['payload'],
                                   headers=signed_headers(secret,event,int(queue.clock())))
        code=response.status_code
    except (TimeoutError,KeyError):pass
    except Exception as exc:
        import httpx
        if not isinstance(exc,httpx.HTTPError):raise
    queue.complete(event,code,(time.perf_counter()-start)*1000)

async def process_batch(queue,client,urls,secret,concurrency=8):
    claimed=[]
    for _ in range(concurrency):
        item=queue.claim()
        if item:claimed.append(item)
    await asyncio.gather(*(deliver(queue,e,client,urls,secret) for e in claimed))
    return len(claimed)
