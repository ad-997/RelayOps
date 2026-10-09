"""End-to-end HTTP experiment against a clean, running local RelayOps stack.
Run: python benchmark.py --base http://127.0.0.1:8083 --out benchmark-results.json
Uses synthetic events and controlled receivers; no production performance claim.
"""
import asyncio,json,time,platform,sys,argparse,uuid
import httpx
async def main(base):
    async with httpx.AsyncClient(base_url=base,timeout=15) as client:
        before=(await client.get('/api/metrics')).json()
        if before['total']:raise RuntimeError('Start with an empty RELAY_DB for reproducible counts')
        events=[{'key':'bench-'+str(uuid.uuid4()),'endpoint':'healthy' if i<80 else ('flaky' if i<180 else 'offline'),
                 'payload':{'type':'order.completed','sequence':i,'currency':'INR','amount':1499}} for i in range(200)]
        semaphore=asyncio.Semaphore(8);latencies=[];ids=[]
        async def dispatch(e):
            async with semaphore:
                t=time.perf_counter();r=await client.post('/api/events',json=e);r.raise_for_status()
                latencies.append((time.perf_counter()-t)*1000);ids.append(r.json()['event']['id'])
        start=time.perf_counter();await asyncio.gather(*(dispatch(e) for e in events));ingest=time.perf_counter()-start
        # Repeat the exact producer submissions to verify suppression at the API boundary.
        dedup=0
        for e in events:
            r=await client.post('/api/events',json=e);r.raise_for_status();dedup+=int(not r.json()['created'])
        deadline=time.monotonic()+40
        while time.monotonic()<deadline:
            m=(await client.get('/api/metrics')).json()
            if m['counts']['delivered']+m['counts']['dead']==200:break
            await asyncio.sleep(.25)
        assert m['counts']['delivered']==180 and m['counts']['dead']==20,m
        assert m['attempts']==460,m
        latencies.sort()
        conflict=await client.post('/api/events',json={**events[0],'payload':{'sequence':'changed'}})
        assert conflict.status_code==409
        return {'experiment':'Local synthetic HTTP fault-injection benchmark','date_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
          'host':platform.platform(),'python':sys.version.split()[0],'database':'SQLite WAL',
          'path':'React API contract → Spring Boot → FastAPI → SQL queue → signed local HTTP receiver',
          'concurrency':8,'unique_events':200,'healthy_events':80,'transient_failure_events':100,'permanent_failure_events':20,
          'delivered':180,'recovered_transient':100,'transient_recovery_percent':100,'dead_letters':20,
          'duplicate_submissions':200,'duplicates_suppressed':dedup,'conflicting_key_http_status':conflict.status_code,
          'delivery_attempts':m['attempts'],'ingest_seconds':round(ingest,3),
          'enqueue_p95_ms':round(latencies[min(len(latencies)-1,int(len(latencies)*.95))],2),
          'observed_enqueue_requests_per_second':round(200/ingest,2),
          'delivery_attempt_p95_ms':m['p95_attempt_ms'],
          'caveats':['Synthetic data and controlled local receivers.','Not production throughput or customer impact.',
           'p95 enqueue includes Java gateway, Python validation, and SQL commit.','SQLite run; MySQL deployment recipe has not been benchmarked.',
           'Backoff is real wall-clock time; worker concurrency is 8.','At-least-once delivery requires consumer deduplication.']}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',default='http://127.0.0.1:8083');p.add_argument('--out',default='benchmark-results.json');a=p.parse_args()
    result=asyncio.run(main(a.base));open(a.out,'w').write(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
