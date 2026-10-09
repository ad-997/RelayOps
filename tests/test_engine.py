import sys,time,asyncio,hmac,hashlib,json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest,httpx
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Queue,Conflict,signed_headers,process_batch
@pytest.fixture
def q(tmp_path):return Queue(tmp_path/'queue.db',base_delay=0)
def event(q,key='one',endpoint='healthy'):return q.enqueue(key,endpoint,{'order':42})[0]
def test_idempotency_returns_original(q):
    a=event(q);b,created=q.enqueue('one','healthy',{'order':42});assert not created and a['id']==b['id'] and q.metrics()['total']==1
def test_conflicting_duplicate_rejected(q):
    event(q)
    with pytest.raises(Conflict):q.enqueue('one','healthy',{'order':99})
def test_endpoint_is_part_of_fingerprint(q):
    event(q)
    with pytest.raises(Conflict):q.enqueue('one','flaky',{'order':42})
def test_success_is_terminal(q):
    event(q);e=q.claim();assert q.complete(e,204,2);assert q.metrics()['counts']['delivered']==1;assert q.claim() is None
def test_failed_delivery_retries_then_dies(q):
    event(q)
    for _ in range(4):q.complete(q.claim(),503,2)
    assert q.metrics()['counts']['dead']==1 and len(q.history(q.events()[0]['id']))==4
def test_nonretryable_4xx_dies_immediately(q):
    event(q);q.complete(q.claim(),400,2);assert q.metrics()['counts']['dead']==1
@pytest.mark.parametrize('code',[None,408,425,429,500])
def test_retryable_failures(q,code):
    event(q);q.complete(q.claim(),code,2);assert q.metrics()['counts']['retrying']==1
def test_replay_preserves_history(q):
    a=event(q);q.complete(q.claim(),400,1);q.replay(a['id']);q.complete(q.claim(),200,1)
    assert len(q.history(a['id']))==2 and q.metrics()['counts']['delivered']==1
def test_replay_only_dead_letters(q):
    a=event(q)
    with pytest.raises(Conflict):q.replay(a['id'])
def test_lease_recovery_fences_stale_worker(tmp_path):
    now=[100.];q=Queue(tmp_path/'queue.db',clock=lambda:now[0]);event(q);stale=q.claim(10)
    now[0]=111;fresh=q.claim();assert fresh['lease_token']!=stale['lease_token']
    assert q.complete(stale,200,2) is False;assert q.complete(fresh,200,2) is True
def test_restart_retains_events(tmp_path):
    path=tmp_path/'queue.db';q=Queue(path);a=event(q)
    restarted=Queue(path);assert restarted.claim()['id']==a['id']
def test_concurrent_producers_deduplicate(q):
    with ThreadPoolExecutor(max_workers=8) as pool:rows=list(pool.map(lambda _:q.enqueue('shared','healthy',{'order':42}),range(32)))
    assert sum(created for _,created in rows)==1 and q.metrics()['total']==1
def test_concurrent_workers_claim_distinct(q):
    for i in range(32):event(q,str(i))
    with ThreadPoolExecutor(max_workers=8) as pool:rows=list(pool.map(lambda _:q.claim(),range(32)))
    assert len({r['id'] for r in rows})==32
def test_signature_uses_exact_payload(q):
    a=event(q);headers=signed_headers('secret',a,100)
    expected=hmac.new(b'secret',('100.'+a['payload']).encode(),hashlib.sha256).hexdigest()
    assert headers['X-Relay-Signature']=='sha256='+expected
def test_payload_size_limit(q):
    with pytest.raises(ValueError):q.enqueue('big','healthy',{'text':'x'*65536})
def test_backoff_defers_retry(tmp_path):
    now=[100.];q=Queue(tmp_path/'queue.db',clock=lambda:now[0],base_delay=2);event(q);q.complete(q.claim(),503,2)
    assert q.claim() is None;now[0]=103;assert q.claim() is not None
def test_http_transport_recovery(q):
    calls={}
    def respond(request):
        ident=request.headers['X-Relay-Event'];calls[ident]=calls.get(ident,0)+1
        return httpx.Response(503 if calls[ident]<3 else 200)
    for i in range(20):event(q,str(i),'flaky')
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            for _ in range(10):await process_batch(q,client,{'flaky':'https://receiver.example/webhook'},'secret',8)
    asyncio.run(run());assert q.metrics()['counts']['delivered']==20 and q.metrics()['attempts']==60
