"""The shared OTLP / ES-bulk clients are HTTP/1.1 connection pools.

One sync ``httpx.Client`` is shared by every service and generator thread. Over
HTTP/2 those threads write into one connection at the same time; httpcore does
not lock the h2 state for that, the connection is corrupted and the server
closes it (GOAWAY PROTOCOL_ERROR / COMPRESSION_ERROR), failing every request in
flight. These tests pin the pool setup and check the sender under concurrency.
"""

from __future__ import annotations

import threading
from pathlib import Path

import httpx
import pytest

from app import telemetry
from app.telemetry import ESBulkClient, OTLPClient, _make_client


def _pool(client: httpx.Client):
    return client._transport._pool


def _assert_http1_pool(client: httpx.Client) -> None:
    pool = _pool(client)
    assert pool._http1 is True
    assert pool._http2 is False


class TestMakeClient:
    def test_is_an_http1_pool(self):
        client = _make_client({"Authorization": "ApiKey k"})
        try:
            _assert_http1_pool(client)
        finally:
            client.close()

    def test_limits_and_retries_reach_the_transport(self):
        # httpx ignores the Client's own http2/limits arguments once a transport is
        # passed, so they must be set on the transport itself.
        client = _make_client({})
        try:
            pool = _pool(client)
            assert pool._max_connections == 32
            assert pool._max_keepalive_connections == 32
            assert pool._retries == 1
        finally:
            client.close()

    def test_keeps_headers_and_timeout(self):
        client = _make_client({"X-Test": "yes"})
        try:
            assert client.headers["X-Test"] == "yes"
            assert client.timeout.read == 10
        finally:
            client.close()

    def test_http2_is_not_switched_back_on_in_the_source(self):
        assert "http2=True" not in Path(telemetry.__file__).read_text()


class TestClientsUseThePool:
    def test_otlp_client(self):
        client = OTLPClient(endpoint="https://x.ingest.example", api_key="k")
        try:
            _assert_http1_pool(client.client)
        finally:
            client.client.close()

    def test_es_bulk_client(self):
        client = ESBulkClient(elastic_url="https://x.es.example", api_key="k")
        try:
            _assert_http1_pool(client.client)
        finally:
            client.client.close()

    def test_otlp_reconfigure(self):
        client = OTLPClient(endpoint="https://x.ingest.example", api_key="k")
        old = client.client
        client.reconfigure("https://y.ingest.example", "k2")
        try:
            assert client.client is not old
            assert old.is_closed
            assert not client.client.is_closed
            _assert_http1_pool(client.client)
            assert client.client.headers["Authorization"] == "ApiKey k2"
        finally:
            client.client.close()

    def test_reconfigure_swaps_in_the_new_client_before_closing_the_old_one(self, monkeypatch):
        """Threads mid-request must never find a closed client in ``self.client``."""
        client = OTLPClient(endpoint="https://x.ingest.example", api_key="k")
        old = client.client
        seen = {}
        real = telemetry._make_client

        def spy(headers):
            seen["old_closed_when_new_built"] = old.is_closed
            return real(headers)

        monkeypatch.setattr(telemetry, "_make_client", spy)
        client.reconfigure("https://y.ingest.example", "k2")
        try:
            assert seen["old_closed_when_new_built"] is False
            assert old.is_closed
        finally:
            client.client.close()


class TestSendUnderConcurrency:
    def test_many_threads_share_the_client_without_failures(self):
        calls = []
        lock = threading.Lock()

        def handler(request: httpx.Request) -> httpx.Response:
            with lock:
                calls.append(request.url.path)
            return httpx.Response(200)

        client = OTLPClient(endpoint="https://x.ingest.example", api_key="k")
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(handler))

        threads_n, per_thread = 32, 20
        barrier = threading.Barrier(threads_n)

        def worker():
            barrier.wait()
            for _ in range(per_thread):
                client._send("https://x.ingest.example/v1/logs", {"resourceLogs": []}, "logs")

        threads = [threading.Thread(target=worker) for _ in range(threads_n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(calls) == threads_n * per_thread
        assert client.consecutive_failures == 0

    def test_a_connection_error_is_counted_not_raised(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.RemoteProtocolError("Server disconnected without sending a response.")

        client = OTLPClient(endpoint="https://x.ingest.example", api_key="k")
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(handler))

        client._send("https://x.ingest.example/v1/logs", {"resourceLogs": []}, "logs")

        assert client.consecutive_failures == 1
