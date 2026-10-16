"""SSRF guard (PRD §5.4, P1 validation 1.2). No live internet unless RUN_INTEGRATION=1."""

from __future__ import annotations

import ipaddress
import os
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar
from unittest.mock import patch

import httpcore
import httpx

from simple_chat_agent.agent import net_guard
from simple_chat_agent.agent.net_guard import (
    BlockedDestination,
    DestinationUnresolvable,
    NetPolicy,
    PinnedTransport,
    TooManyRedirects,
    check_url,
    classify_address,
    guarded_get,
)

PUBLIC_V4 = "93.184.215.14"

BLOCKED_ADDRESSES = [
    # loopback
    "127.0.0.1",
    "127.255.255.254",
    "::1",
    # RFC1918
    "10.0.0.1",
    "10.255.255.255",
    "172.16.0.1",
    "172.31.255.255",
    "192.168.0.1",
    "192.168.255.255",
    # link-local and metadata
    "169.254.169.254",
    "169.254.0.1",
    "fe80::1",
    "febf:ffff::1",
    "fd00:ec2::254",
    # CGNAT
    "100.64.0.1",
    "100.127.255.255",
    # unique-local
    "fc00::1",
    "fd12:3456:789a::1",
    # multicast
    "224.0.0.1",
    "239.255.255.250",
    "ff02::1",
    # unspecified and 0.0.0.0/8
    "0.0.0.0",
    "0.1.2.3",
    "::",
    # reserved / broadcast / misc
    "240.0.0.1",
    "255.255.255.255",
    "198.18.0.1",
    "192.0.0.8",
    # IPv6 forms embedding a blocked IPv4 address
    "::ffff:127.0.0.1",
    "::ffff:169.254.169.254",
    "::ffff:10.0.0.1",
    "::ffff:100.64.0.1",
    "::127.0.0.1",
    "64:ff9b::7f00:1",
    "64:ff9b::a9fe:a9fe",
    "2002:7f00:1::1",
    "2002:a9fe:a9fe::1",
]

PUBLIC_ADDRESSES = [
    "8.8.8.8",
    "1.1.1.1",
    PUBLIC_V4,
    "100.128.0.1",  # just outside CGNAT
    "172.32.0.1",  # just outside 172.16/12
    "2606:4700:4700::1111",
    "::ffff:8.8.8.8",
]


class ClassifyAddressTests(unittest.TestCase):
    def test_every_blocked_range_is_refused(self) -> None:
        for addr in BLOCKED_ADDRESSES:
            with self.subTest(addr=addr):
                self.assertIsNotNone(classify_address(addr))

    def test_public_addresses_pass(self) -> None:
        for addr in PUBLIC_ADDRESSES:
            with self.subTest(addr=addr):
                self.assertIsNone(classify_address(addr))

    def test_metadata_endpoint_is_named(self) -> None:
        self.assertIn("metadata", classify_address("169.254.169.254") or "")
        self.assertIn("metadata", classify_address("::ffff:169.254.169.254") or "")


def _resolver(mapping: dict[str, list[str]]):
    calls: list[str] = []

    def resolve(host: str, port: int) -> list[str]:
        calls.append(host)
        if host not in mapping:
            raise socket.gaierror(socket.EAI_NONAME, "unknown host")
        return list(mapping[host])

    resolve.calls = calls  # type: ignore[attr-defined]
    return resolve


class CheckUrlTests(unittest.TestCase):
    def test_literal_blocked_addresses_in_urls(self) -> None:
        for addr in BLOCKED_ADDRESSES:
            ip = ipaddress.ip_address(addr)
            host = f"[{addr}]" if ip.version == 6 else addr
            with (
                self.subTest(addr=addr),
                self.assertRaises(BlockedDestination) as raised,
            ):
                check_url(f"http://{host}/latest/meta-data/")
            self.assertEqual(raised.exception.reason, "address")
            self.assertEqual(raised.exception.address, str(ip))

    def test_alternate_ipv4_spellings_are_resolved_then_refused(self) -> None:
        # Numeric forms that libc turns into 127.0.0.1 (no DNS involved).
        for url in ("http://2130706433/", "http://127.1/", "http://localhost/"):
            with self.subTest(url=url), self.assertRaises(BlockedDestination):
                check_url(url)

    def test_hostname_resolving_to_private_is_refused(self) -> None:
        resolve = _resolver({"intranet.example": ["192.168.1.10"]})
        with (
            patch.object(net_guard, "_resolve", resolve),
            self.assertRaises(BlockedDestination) as raised,
        ):
            check_url("https://intranet.example/")
        self.assertIn(
            "intranet.example resolves to 192.168.1.10", str(raised.exception)
        )

    def test_mixed_public_and_private_answers_are_refused(self) -> None:
        resolve = _resolver({"mixed.example": [PUBLIC_V4, "10.0.0.5"]})
        with (
            patch.object(net_guard, "_resolve", resolve),
            self.assertRaises(BlockedDestination) as raised,
        ):
            check_url("https://mixed.example/")
        self.assertEqual(raised.exception.address, "10.0.0.5")

        resolve = _resolver({"mixed6.example": ["2606:4700:4700::1111", "fe80::1"]})
        with (
            patch.object(net_guard, "_resolve", resolve),
            self.assertRaises(BlockedDestination),
        ):
            check_url("https://mixed6.example/")

    def test_public_hostname_passes_and_reports_pin(self) -> None:
        resolve = _resolver({"public.example": [PUBLIC_V4, "2606:4700:4700::1111"]})
        with patch.object(net_guard, "_resolve", resolve):
            dest = check_url("https://Public.Example./path")
        self.assertEqual(dest.host, "public.example")
        self.assertEqual(dest.port, 443)
        self.assertEqual(dest.pinned_ip, PUBLIC_V4)

    def test_non_standard_port_is_refused_before_dns(self) -> None:
        resolve = _resolver({"public.example": [PUBLIC_V4]})
        with patch.object(net_guard, "_resolve", resolve):
            for url in (
                "http://public.example:8080/",
                "https://public.example:22/",
                "http://public.example:6379/",
            ):
                with (
                    self.subTest(url=url),
                    self.assertRaises(BlockedDestination) as raised,
                ):
                    check_url(url)
                self.assertEqual(raised.exception.reason, "port")
        self.assertEqual(resolve.calls, [])

    def test_port_allowlist_is_configurable(self) -> None:
        resolve = _resolver({"public.example": [PUBLIC_V4]})
        with (
            patch.object(net_guard, "_resolve", resolve),
            patch.dict(os.environ, {net_guard.ALLOWED_PORTS_ENV: "443,8443"}),
        ):
            self.assertEqual(check_url("https://public.example:8443/").port, 8443)
            with self.assertRaises(BlockedDestination):
                check_url("http://public.example/")  # 80 no longer listed

    def test_disallowed_schemes(self) -> None:
        for url in (
            "file:///etc/passwd",
            "ftp://public.example/",
            "gopher://public.example/",
            "javascript:alert(1)",
            "public.example/no-scheme",
        ):
            with self.subTest(url=url), self.assertRaises(BlockedDestination):
                check_url(url)

    def test_unresolvable_host(self) -> None:
        with (
            patch.object(net_guard, "_resolve", _resolver({})),
            self.assertRaises(DestinationUnresolvable) as raised,
        ):
            check_url("https://nope.example/")
        self.assertFalse(raised.exception.retryable)

        def flaky(host: str, port: int) -> list[str]:
            raise socket.gaierror(socket.EAI_AGAIN, "temporary failure")

        with (
            patch.object(net_guard, "_resolve", flaky),
            self.assertRaises(DestinationUnresolvable) as raised,
        ):
            check_url("https://flaky.example/")
        self.assertTrue(raised.exception.retryable)


class _Recorder:
    """A transport factory over httpx.MockTransport that records every pinned hop."""

    def __init__(self, handler) -> None:
        self.handler = handler
        self.hops: list[tuple[str, int, str]] = []
        self.requests: list[httpx.Request] = []

    def __call__(self, host: str, port: int, ip: str) -> httpx.AsyncBaseTransport:
        self.hops.append((host, port, ip))

        def handle(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return self.handler(request)

        return httpx.MockTransport(handle)


def _redirect(location: str, status: int = 302) -> httpx.Response:
    return httpx.Response(status, headers={"Location": location})


class GuardedGetRedirectTests(unittest.IsolatedAsyncioTestCase):
    async def _get(
        self, url: str, recorder: _Recorder, mapping: dict[str, list[str]], **kw
    ):
        with patch.object(net_guard, "_resolve", _resolver(mapping)):
            return await guarded_get(
                url,
                timeout=5,
                max_bytes=kw.pop("max_bytes", 1000),
                transport_factory=recorder,
                **kw,
            )

    async def test_redirect_from_public_url_to_loopback_is_refused(self) -> None:
        recorder = _Recorder(lambda request: _redirect("http://127.0.0.1/admin"))
        with self.assertRaises(BlockedDestination) as raised:
            await self._get(
                "https://public.example/", recorder, {"public.example": [PUBLIC_V4]}
            )
        self.assertEqual(raised.exception.address, "127.0.0.1")
        # The loopback hop never got a transport, let alone a request.
        self.assertEqual(recorder.hops, [("public.example", 443, PUBLIC_V4)])
        self.assertEqual(len(recorder.requests), 1)

    async def test_redirect_to_hosts_that_resolve_private_is_refused(self) -> None:
        mapping = {
            "public.example": [PUBLIC_V4],
            "metadata.example": ["169.254.169.254"],
            "mixed.example": [PUBLIC_V4, "172.16.5.5"],
            "cgnat.example": ["100.100.100.200"],
        }
        for target in (
            "http://metadata.example/latest/meta-data/",
            "https://mixed.example/",
            "http://cgnat.example/",
            "http://[::ffff:169.254.169.254]/",
            "http://public.example:8080/",
            "file:///etc/passwd",
        ):
            recorder = _Recorder(lambda request, t=target: _redirect(t))
            with self.subTest(target=target), self.assertRaises(BlockedDestination):
                await self._get("https://public.example/", recorder, mapping)
            self.assertEqual(len(recorder.hops), 1)

    async def test_each_hop_is_resolved_and_pinned_separately(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "a.example":
                return _redirect("https://b.example/next", 301)
            if request.url.path == "/next":
                return _redirect("/final", 307)  # relative, same host
            return httpx.Response(200, text="done")

        recorder = _Recorder(handler)
        mapping = {"a.example": [PUBLIC_V4], "b.example": ["8.8.8.8"]}
        result = await self._get("https://a.example/", recorder, mapping)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.content, b"done")
        self.assertEqual(result.url, "https://b.example/final")
        self.assertEqual(result.peer_ip, "8.8.8.8")
        self.assertEqual(
            recorder.hops,
            [
                ("a.example", 443, PUBLIC_V4),
                ("b.example", 443, "8.8.8.8"),
                ("b.example", 443, "8.8.8.8"),
            ],
        )
        self.assertEqual(
            result.redirects, ["https://b.example/next", "https://b.example/final"]
        )

    async def test_redirect_limit(self) -> None:
        def chain(n: int):
            def handler(request: httpx.Request) -> httpx.Response:
                step = int(request.url.path.strip("/") or 0)
                if step < n:
                    return _redirect(f"/{step + 1}")
                return httpx.Response(200, text="end")

            return handler

        mapping = {"public.example": [PUBLIC_V4]}
        ok = await self._get("https://public.example/0", _Recorder(chain(5)), mapping)
        self.assertEqual(ok.status_code, 200)
        with self.assertRaises(TooManyRedirects):
            await self._get("https://public.example/0", _Recorder(chain(6)), mapping)

    async def test_cross_origin_redirect_drops_credentials(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "a.example":
                return _redirect("https://b.example/")
            return httpx.Response(200)

        recorder = _Recorder(handler)
        mapping = {"a.example": [PUBLIC_V4], "b.example": ["8.8.8.8"]}
        await self._get(
            "https://a.example/",
            recorder,
            mapping,
            headers={
                "Authorization": "Bearer x",
                "Cookie": "s=1",
                "Accept": "text/html",
            },
        )
        self.assertEqual(recorder.requests[0].headers.get("authorization"), "Bearer x")
        self.assertIsNone(recorder.requests[1].headers.get("authorization"))
        self.assertIsNone(recorder.requests[1].headers.get("cookie"))
        self.assertEqual(recorder.requests[1].headers.get("accept"), "text/html")

    async def test_body_is_capped_at_max_bytes(self) -> None:
        recorder = _Recorder(lambda request: httpx.Response(200, content=b"x" * 5000))
        result = await self._get(
            "https://public.example/",
            recorder,
            {"public.example": [PUBLIC_V4]},
            max_bytes=1000,
        )
        self.assertEqual(len(result.content), 1000)
        self.assertTrue(result.truncated)

    async def test_error_statuses_are_returned(self) -> None:
        recorder = _Recorder(lambda request: httpx.Response(404, text="missing"))
        result = await self._get(
            "https://public.example/", recorder, {"public.example": [PUBLIC_V4]}
        )
        self.assertEqual(result.status_code, 404)
        self.assertEqual(result.reason_phrase, "Not Found")


# -- the IP pin, against a real socket -------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    seen_hosts: ClassVar[list[str]] = []

    def do_GET(self) -> None:
        _Handler.seen_hosts.append(self.headers.get("Host", ""))
        if self.path.startswith("/redirect?to="):
            self.send_response(302)
            self.send_header("Location", self.path.split("=", 1)[1])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = b"hello from the pinned server"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


class PinnedConnectionTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _Handler.seen_hosts.clear()
        # The test server lives on loopback, so this policy (test-only) exempts
        # 127.0.0.1/32 and allows the server's port. Everything else stays blocked.
        self.policy = NetPolicy(
            allowed_ports=frozenset({self.port}),
            allow_networks=(ipaddress.ip_network("127.0.0.1/32"),),
        )

    async def test_connects_to_the_vetted_ip_and_keeps_the_host_header(self) -> None:
        # `pinned.test` does not exist in real DNS (.test is reserved), so the request can
        # only succeed if the socket goes to the IP the guard vetted, with no second lookup.
        resolve = _resolver({"pinned.test": ["127.0.0.1"]})
        with patch.object(net_guard, "_resolve", resolve):
            result = await guarded_get(
                f"http://pinned.test:{self.port}/hello",
                timeout=5,
                max_bytes=10_000,
                headers={},
                policy=self.policy,
            )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.content, b"hello from the pinned server")
        self.assertEqual(result.peer_ip, "127.0.0.1")
        self.assertEqual(_Handler.seen_hosts, [f"pinned.test:{self.port}"])
        self.assertEqual(resolve.calls, ["pinned.test"])  # resolved exactly once

    async def test_dns_rebinding_on_a_later_hop_is_caught(self) -> None:
        # First answer is the (exempted) server; any later lookup "rebinds" to RFC1918.
        answers = iter([["127.0.0.1"]])

        def rebinding(host: str, port: int) -> list[str]:
            return next(answers, ["10.0.0.7"])

        with (
            patch.object(net_guard, "_resolve", rebinding),
            self.assertRaises(BlockedDestination) as raised,
        ):
            await guarded_get(
                f"http://rebind.test:{self.port}/redirect?to=/again",
                timeout=5,
                max_bytes=10_000,
                headers={},
                policy=self.policy,
            )
        self.assertEqual(raised.exception.address, "10.0.0.7")
        self.assertEqual(len(_Handler.seen_hosts), 1)

    async def test_real_redirect_to_internal_targets_is_refused(self) -> None:
        resolve = _resolver({"pinned.test": ["127.0.0.1"]})
        for target in (
            "http://169.254.169.254/latest/meta-data/iam/",
            "http://10.0.0.1/",
            f"http://[::1]:{self.port}/",
            f"http://127.0.0.2:{self.port}/",
        ):
            _Handler.seen_hosts.clear()
            with (
                self.subTest(target=target),
                patch.object(net_guard, "_resolve", resolve),
                self.assertRaises(BlockedDestination),
            ):
                await guarded_get(
                    f"http://pinned.test:{self.port}/redirect?to={target}",
                    timeout=5,
                    max_bytes=10_000,
                    headers={},
                    policy=self.policy,
                )
            self.assertEqual(len(_Handler.seen_hosts), 1)


class _FakeStream(httpcore.AsyncNetworkStream):
    def __init__(self) -> None:
        self.server_hostname: str | None = None

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        pass

    async def aclose(self) -> None:
        pass

    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):  # type: ignore[override]
        self.server_hostname = server_hostname
        raise httpcore.ConnectError("stop after recording the TLS handshake parameters")


class _FakeBackend(httpcore.AsyncNetworkBackend):
    def __init__(self) -> None:
        self.connects: list[tuple[str, int]] = []
        self.stream = _FakeStream()

    async def connect_tcp(
        self, host, port, timeout=None, local_address=None, socket_options=None
    ):  # type: ignore[override]
        self.connects.append((host, port))
        return self.stream

    async def connect_unix_socket(self, path, timeout=None, socket_options=None):  # type: ignore[override]
        raise AssertionError("unexpected")

    async def sleep(self, seconds: float) -> None:
        pass


class PinnedTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_tls_uses_hostname_for_sni_while_socket_goes_to_vetted_ip(
        self,
    ) -> None:
        fake = _FakeBackend()
        transport = PinnedTransport(
            "www.example.org", 443, PUBLIC_V4, inner_backend=fake
        )
        async with httpx.AsyncClient(transport=transport) as client:
            with self.assertRaises(httpx.ConnectError):
                await client.get("https://www.example.org/")
        self.assertEqual(fake.connects, [(PUBLIC_V4, 443)])
        self.assertEqual(fake.stream.server_hostname, "www.example.org")

    async def test_refuses_any_other_host_or_port(self) -> None:
        fake = _FakeBackend()
        transport = PinnedTransport(
            "www.example.org", 443, PUBLIC_V4, inner_backend=fake
        )
        async with httpx.AsyncClient(transport=transport) as client:
            for url in ("https://evil.example/", "http://www.example.org/"):
                with self.subTest(url=url), self.assertRaises(httpx.ConnectError):
                    await client.get(url)
        self.assertEqual(fake.connects, [])

    def test_httpx_pool_attribute_is_ours(self) -> None:
        # PinnedTransport replaces httpx.AsyncHTTPTransport._pool; fail loudly if an httpx
        # upgrade renames it.
        transport = PinnedTransport("www.example.org", 443, PUBLIC_V4)
        self.assertIsInstance(transport._pool, httpcore.AsyncConnectionPool)
        self.assertIs(transport._pool._network_backend, transport.backend)


@unittest.skipUnless(os.environ.get("RUN_INTEGRATION") == "1", "set RUN_INTEGRATION=1")
class LiveSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_example_com(self) -> None:
        result = await guarded_get(
            "https://example.com/", timeout=20, max_bytes=100_000, headers={}
        )
        self.assertEqual(result.status_code, 200)
        self.assertIn(b"Example Domain", result.content)


if __name__ == "__main__":
    unittest.main()
