import hashlib
import hmac
import ipaddress
import json
import socket
from urllib.parse import urlsplit

from relay.config import settings


def key_hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def signature(secret: str, timestamp: int, event_id: str, body: bytes) -> str:
    signed = f"{timestamp}.{event_id}.".encode() + body
    return "v1=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


def canonical_body(event_id: str, event_type: str, data: object) -> bytes:
    return json.dumps({"id": event_id, "type": event_type, "data": data}, sort_keys=True, separators=(",", ":")).encode()


def validate_target(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in ({"https", "http"} if settings.allow_private_targets else {"https"}):
        raise ValueError("HTTPS target required")
    if not parts.hostname or parts.username or parts.password or parts.fragment or (
        not settings.allow_private_targets and parts.port not in (None, 80, 443)
    ):
        raise ValueError("invalid target URL")
    host = parts.hostname.lower().rstrip(".")
    if settings.allowed_hosts and host not in {h.strip().lower() for h in settings.allowed_hosts.split(",")}:
        raise ValueError("host is not allowlisted")
    addresses = socket.getaddrinfo(host, parts.port or (443 if parts.scheme == "https" else 80), type=socket.SOCK_STREAM)
    if not settings.allow_private_targets and any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise ValueError("target resolves to a non-public address")
    return url
