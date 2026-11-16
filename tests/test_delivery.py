from unittest.mock import patch

import httpx
import pytest

from relay.delivery import send_webhook
from relay.security import resolve_target


def answer(ip):
    return [(2, 1, 6, '', (ip, 443))]


@pytest.mark.parametrize('ip', ['8.8.8.8', '2606:4700:4700::1111'])
def test_connects_to_checked_address_with_original_tls_name(ip):
    requests = []

    def receive(request):
        requests.append(request)
        assert request.url.host == ip
        assert request.headers['host'] == 'hooks.example.com:8443'
        assert request.extensions['sni_hostname'] == 'hooks.example.com'
        assert request.content == b'payload'
        return httpx.Response(204)

    real_client = httpx.Client
    with patch('relay.security.settings.allow_private_targets', True), \
         patch('relay.security.socket.getaddrinfo', side_effect=[answer(ip), answer('127.0.0.1')]) as dns, \
         patch('relay.delivery.httpx.Client', side_effect=lambda **kw: real_client(
             transport=httpx.MockTransport(receive), **kw)) as factory:
        assert send_webhook('https://hooks.example.com:8443/a?q=1', b'payload', {}) == 204
        dns.assert_called_once()
        assert factory.call_args.kwargs['trust_env'] is False
    assert len(requests) == 1
    assert requests[0].url.path == '/a'
    assert requests[0].url.query == b'q=1'


def test_rejects_mixed_public_private_dns_before_sending():
    with patch('relay.security.socket.getaddrinfo', return_value=answer('8.8.8.8') + answer('10.0.0.1')), \
         patch('relay.delivery.httpx.Client') as client:
        with pytest.raises(ValueError, match='non-public'):
            send_webhook('https://hooks.example.com', b'', {})
        client.assert_not_called()


def test_empty_dns_is_rejected():
    with patch('relay.security.socket.getaddrinfo', return_value=[]):
        with pytest.raises(ValueError, match='no addresses'):
            resolve_target('https://hooks.example.com')


def test_redirect_and_response_body_are_not_consumed():
    class ForbiddenBody(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError('response body must not be read')

    real_client = httpx.Client
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(302, headers={'location': 'http://127.0.0.1'}, stream=ForbiddenBody())

    with patch('relay.security.socket.getaddrinfo', return_value=answer('8.8.8.8')), \
         patch('relay.delivery.httpx.Client', side_effect=lambda **kw: real_client(
             transport=httpx.MockTransport(receive), **kw)):
        assert send_webhook('https://hooks.example.com', b'', {}) == 302
    assert len(calls) == 1


@pytest.mark.parametrize('certificate_host, succeeds', [('hooks.example.com', True), ('wrong.example.com', False)])
def test_real_tls_verifies_original_hostname(certificate_host, succeeds):
    import socket
    import ssl
    import threading

    import trustme

    ca = trustme.CA()
    cert = ca.issue_cert(certificate_host)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(server_context)
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    listener.settimeout(5)
    port = listener.getsockname()[1]
    received = []

    def serve():
        try:
            connection, _ = listener.accept()
            with connection:
                try:
                    with server_context.wrap_socket(connection, server_side=True) as tls:
                        data = b''
                        while b'\r\n\r\n' not in data:
                            data += tls.recv(4096)
                        received.append(data)
                        tls.sendall(b'HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n')
                except ssl.SSLError:
                    pass  # Expected when the client rejects the wrong certificate name.
        finally:
            listener.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    real_client = httpx.Client
    real_dns = socket.getaddrinfo

    def resolve(host, service, *args, **kwargs):
        if host == 'hooks.example.com':
            return answer('127.0.0.1')
        return real_dns(host, service, *args, **kwargs)

    try:
        with patch('relay.security.settings.allow_private_targets', True), \
             patch('relay.security.socket.getaddrinfo', side_effect=resolve), \
             patch('relay.delivery.httpx.Client', side_effect=lambda **kw: real_client(verify=client_context, **kw)):
            url = f'https://hooks.example.com:{port}/delivery'
            if succeeds:
                assert send_webhook(url, b'', {}) == 204
            else:
                with pytest.raises(httpx.ConnectError, match='CERTIFICATE_VERIFY_FAILED'):
                    send_webhook(url, b'', {})
    finally:
        thread.join(timeout=6)
        listener.close()
    assert not thread.is_alive()
    assert bool(received) is succeeds
    if succeeds:
        assert f'Host: hooks.example.com:{port}'.encode() in received[0]
