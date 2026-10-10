import pytest
from destinations import configured_destinations

def test_configured_destination(monkeypatch):
    monkeypatch.setenv('RELAY_DESTINATIONS','{"fulfillment":"http://127.0.0.1:8084/orders"}')
    assert configured_destinations('http://localhost:8082')['fulfillment'].endswith('/orders')

def test_production_rejects_plain_http(monkeypatch):
    monkeypatch.setenv('RELAY_DESTINATIONS','{"orders":"http://example.com/orders"}')
    with pytest.raises(ValueError):configured_destinations('', True)

def test_production_rejects_private_target(monkeypatch):
    monkeypatch.setenv('RELAY_DESTINATIONS','{"orders":"https://127.0.0.1/orders"}')
    with pytest.raises(ValueError):configured_destinations('', True)

def test_url_credentials_rejected(monkeypatch):
    monkeypatch.setenv('RELAY_DESTINATIONS','{"orders":"https://user:pass@example.com/orders"}')
    with pytest.raises(ValueError):configured_destinations('', False)

def test_demo_disabled_in_production(monkeypatch):
    monkeypatch.delenv('RELAY_DESTINATIONS',raising=False)
    monkeypatch.delenv('RELAY_DEMO_RECEIVERS',raising=False)
    assert configured_destinations('', True)=={}
