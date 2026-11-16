"""Resolve once, connect to the checked IP, and retain HTTPS hostname verification."""
import httpx

from relay.security import resolve_target


def send_webhook(url: str, body: bytes, headers: dict[str, str]) -> int:
    host, address = resolve_target(url)
    original = httpx.URL(url)
    destination = original.copy_with(host=address)
    # A fresh pool prevents connections to a shared IP being reused across TLS names.
    # Proxies and redirects are disabled; response bodies are never buffered.
    with httpx.Client(timeout=httpx.Timeout(8, connect=3), trust_env=False) as client:
        with client.stream(
            "POST", destination, content=body,
            headers={**headers, "Host": original.netloc.decode("ascii")},
            extensions={"sni_hostname": host}, follow_redirects=False,
        ) as response:
            return response.status_code
