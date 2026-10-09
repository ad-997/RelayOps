import sys,os,tempfile,time
from pathlib import Path
from fastapi.testclient import TestClient
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
with tempfile.TemporaryDirectory() as d:
    os.environ['RELAY_DB']=str(Path(d)/'api.db')
    import app
# Replace initialization database with a retained fixture for this module.
import pytest
@pytest.fixture
def client(tmp_path,monkeypatch):
    from engine import Queue
    monkeypatch.setattr(app,'queue',Queue(tmp_path/'api.db'))
    return TestClient(app.app)
def test_api_idempotency(client):
    data={'key':'order-one','endpoint':'healthy','payload':{'order':1}}
    a=client.post('/api/events',json=data);b=client.post('/api/events',json=data)
    assert a.status_code==200 and a.json()['created'] and not b.json()['created']
    assert a.json()['event']['id']==b.json()['event']['id']
def test_api_conflict(client):
    client.post('/api/events',json={'key':'x','endpoint':'healthy','payload':{}})
    assert client.post('/api/events',json={'key':'x','endpoint':'offline','payload':{}}).status_code==409
def test_arbitrary_url_rejected(client):
    assert client.post('/api/events',json={'key':'x','endpoint':'http://internal','payload':{}}).status_code==400
def test_invalid_payload_rejected(client):
    assert client.post('/api/events',json={'key':'x','endpoint':'healthy','payload':[]}).status_code==422
def test_pagination_bounds(client):
    assert client.get('/api/events?limit=10000').status_code==422
def test_bad_state_rejected(client):assert client.get('/api/events?status=unknown').status_code==400
def test_missing_replay(client):assert client.post('/api/events/missing/replay').status_code==404
def test_receiver_rejects_bad_signature(client):
    assert client.post('/receivers/healthy',json={},headers={'X-Relay-Timestamp':str(int(time.time()))}).status_code==401
def test_receiver_rejects_expired_timestamp(client):
    assert client.post('/receivers/healthy',json={},headers={'X-Relay-Timestamp':'100'}).status_code==401
def test_service_key_enforced(client,monkeypatch):
    monkeypatch.setattr(app,'API_KEY','test-key')
    assert client.get('/api/metrics').status_code==401
    assert client.get('/api/metrics',headers={'X-API-Key':'test-key'}).status_code==200
