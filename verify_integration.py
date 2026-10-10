"""Real HTTP acceptance check against a separate fulfillment application."""
import os,sys,tempfile,subprocess,secrets,time,json
from pathlib import Path
import httpx
root=Path(__file__).parent.resolve()
processes=[]
with tempfile.TemporaryDirectory() as tmp:
    env={**os.environ,'RELAY_SECRET':secrets.token_hex(32),'RELAY_API_KEY':secrets.token_hex(32),
         'RELAY_DB':tmp+'/queue.db','ORDER_DB':tmp+'/orders.db','OUTAGE_FILE':tmp+'/outage',
         'RELAY_RECEIVER_BASE':'http://127.0.0.1:18082','RELAY_WORKER_URL':'http://127.0.0.1:18082',
         'RELAY_DESTINATIONS':'{"fulfillment":"http://127.0.0.1:18084/orders"}','PORT':'18083'}
    def start(args):
        p=subprocess.Popen(args,cwd=root,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);processes.append(p);return p
    def wait(fn,timeout=35):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            try:
                value=fn()
                if value:return value
            except httpx.HTTPError:pass
            time.sleep(.15)
        raise AssertionError('Acceptance check timed out')
    client=httpx.Client(base_url='http://127.0.0.1:18083',headers={'X-API-Key':env['RELAY_API_KEY']},timeout=5)
    try:
        start([sys.executable,'-m','uvicorn','examples.order_receiver:app','--port','18084'])
        worker=start([sys.executable,'-m','uvicorn','app:app','--port','18082'])
        start(['java','-jar','control-plane/target/control-plane-1.0.0.jar'])
        wait(lambda:client.get('/api/healthz').status_code==200)
        assert httpx.get('http://127.0.0.1:18083/api/events').status_code==401
        Path(env['OUTAGE_FILE']).touch()
        data={'key':'paid-order-42','endpoint':'fulfillment','payload':{'orderId':42,'status':'paid'}}
        first=client.post('/api/events',json=data);first.raise_for_status();ident=first.json()['event']['id']
        duplicate=client.post('/api/events',json=data).json()
        assert not duplicate['created'] and duplicate['event']['id']==ident
        wait(lambda:any(e['id']==ident and e['status']=='dead' for e in client.get('/api/events').json()))
        assert len(client.get(f'/api/events/{ident}/attempts').json())==4
        worker.terminate();worker.wait(10)
        pending=client.post('/api/events',json={'key':'after-outage','endpoint':'fulfillment','payload':{}})
        assert pending.status_code==503
        worker=start([sys.executable,'-m','uvicorn','app:app','--port','18082'])
        wait(lambda:client.get('/api/healthz').status_code==200)
        assert any(e['id']==ident and e['status']=='dead' for e in client.get('/api/events').json())
        Path(env['OUTAGE_FILE']).unlink()
        assert client.post(f'/api/events/{ident}/replay').status_code==200
        wait(lambda:any(e['id']==ident and e['status']=='delivered' for e in client.get('/api/events').json()))
        assert len(client.get(f'/api/events/{ident}/attempts').json())==5
        orders=httpx.get('http://127.0.0.1:18084/orders').json()
        assert len(orders)==1 and orders[0]['event_id']==ident
        report={'environment':'local real HTTP, SQLite, separate fulfillment receiver',
                'unauthorized_submission_rejected':True,'duplicate_submission_suppressed':True,
                'outage_attempts_retained':4,'history_survived_worker_restart':True,
                'replay_delivered_after_recovery':True,'total_attempts_preserved':5,
                'fulfillment_orders_created':1}
        print(json.dumps(report,indent=2))
    finally:
        client.close()
        for p in processes:
            if p.poll() is None:p.terminate()
        for p in processes:
            try:p.wait(10)
            except subprocess.TimeoutExpired:p.kill()
