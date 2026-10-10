"""Operator-owned destinations; API callers never supply an outbound URL."""
import json, os, re, socket, ipaddress
from urllib.parse import urlsplit

def configured_destinations(base, production=False):
    raw = os.environ.get('RELAY_DESTINATIONS', '{}')
    configured = json.loads(raw)
    if not isinstance(configured, dict):
        raise ValueError('RELAY_DESTINATIONS must be a JSON object')
    urls = {}
    for name, url in configured.items():
        if not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_-]{0,39}', name):
            raise ValueError('Invalid destination name')
        parsed = urlsplit(url)
        if not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            raise ValueError('Invalid destination URL')
        if parsed.scheme not in ('http', 'https') or (production and parsed.scheme != 'https'):
            raise ValueError('Production destinations require HTTPS')
        if production:
            addresses = socket.getaddrinfo(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)
            if any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
                raise ValueError('Production destinations must resolve to public addresses')
        urls[name] = url
    if os.environ.get('RELAY_DEMO_RECEIVERS', 'false' if production else 'true').lower() == 'true':
        for name in ('healthy', 'flaky', 'offline'):
            if name in urls:
                raise ValueError('Destination name reserved for demo receiver')
            urls[name] = base + '/receivers/' + name
    return urls
