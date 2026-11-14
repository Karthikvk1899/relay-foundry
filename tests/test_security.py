import hashlib
import hmac
from unittest.mock import patch

import pytest

from relay.config import Settings
from relay.security import canonical_body, signature, validate_target
from relay.worker import retry_delay


def test_signature_is_deterministic_and_covers_body_and_timestamp():
    body = canonical_body("event-1", "order.created", {"id": "abc"})
    signed = signature("key", 123, "event-1", body)
    assert hmac.compare_digest(signed[3:], hmac.new(b"key", b"123.event-1." + body, hashlib.sha256).hexdigest())
    assert signed != signature("key", 124, "event-1", body)
    assert signed != signature("key", 123, "event-1", body + b"x")


@pytest.mark.parametrize("url", ["http://example.com/a", "https://user:pass@example.com/a",
                                 "https://localhost/hook", "https://127.0.0.1/hook", "file:///tmp/x"])
def test_rejects_unsafe_targets(url):
    with pytest.raises(ValueError):
        validate_target(url)


def test_blocks_private_dns_results():
    answer = [(2, 1, 6, "", ("10.0.0.5", 443))]
    with patch("relay.security.socket.getaddrinfo", return_value=answer):
        with pytest.raises(ValueError, match="non-public"):
            validate_target("https://example.com/hook")


def test_allowlist_is_exact_host_match():
    with patch("relay.security.settings.allowed_hosts", "example.com"):
        with pytest.raises(ValueError, match="allowlisted"):
            validate_target("https://example.com.evil.test/hook")


def test_production_config_requires_egress_allowlist():
    with pytest.raises(ValueError, match="ALLOWED_HOSTS"):
        Settings(api_key="x" * 32, production=True).validate_security()


def test_retry_backoff_is_bounded():
    assert retry_delay(1, 0) == 2
    assert retry_delay(20, 0) == 1024
    assert retry_delay(100, 0) == 1024
