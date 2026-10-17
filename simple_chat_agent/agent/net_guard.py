"""SSRF guard for outbound HTTP from the worker (PRD §5.4, A6).

Every URL the model can steer (``fetch_url`` today, MCP server URLs in P4) goes through
this module. The rules:

1. Only ``http``/``https`` on an allowlisted port (80/443 by default).
2. Resolve the hostname and refuse if ANY resolved address is non-public: loopback,
   RFC1918, link-local (incl. 169.254.169.254 and fe80::/10), CGNAT 100.64/10, unique-local
   fc00::/7, multicast, unspecified, ``0.0.0.0/8``, reserved ranges, and any IPv6 form that
   embeds one of those (IPv4-mapped, IPv4-compatible, 6to4, NAT64, Teredo).
3. Pin the connection to the vetted IP. The check and the connect use the SAME address,
   so a DNS answer that changes between them (rebinding) cannot redirect the socket.
4. Follow redirects by hand (at most 5), re-running 1-3 on every hop.

How the pin works: each hop gets its own :class:`PinnedTransport`, an
``httpx.AsyncHTTPTransport`` whose httpcore connection pool uses :class:`_PinnedBackend`
as its network backend. httpcore asks the backend to ``connect_tcp(host, port)`` with the
URL's hostname; the backend refuses any host/port other than the vetted one and opens the
TCP socket to the vetted IP instead. Everything above the socket is untouched: httpcore
still does TLS with ``server_hostname=<hostname>`` (correct SNI and certificate
verification against the name, not the IP) and httpx still sends ``Host: <hostname>``.
No DNS lookup happens after the check. Env proxies are disabled (``trust_env=False``)
because a proxy would make its own lookup.
"""

from __future__ import annotations

import asyncio
import ipaddress
import os
import socket
import ssl
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import httpcore
import httpx

DEFAULT_ALLOWED_SCHEMES = frozenset({"http", "https"})
DEFAULT_ALLOWED_PORTS = frozenset({80, 443})
DEFAULT_MAX_REDIRECTS = 5
ALLOWED_PORTS_ENV = "AGENT_NET_ALLOWED_PORTS"

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_SENSITIVE_HEADERS = frozenset({"authorization", "cookie", "proxy-authorization"})

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


# -- errors ---------------------------------------------------------------------------


class NetGuardError(Exception):
    """Base class for guard refusals and guard-level failures."""


class BlockedDestination(NetGuardError):
    """The URL, or a redirect it led to, points somewhere the worker must not connect."""

    def __init__(
        self,
        message: str,
        *,
        url: str,
        host: str | None = None,
        address: str | None = None,
        reason: str = "blocked",
    ) -> None:
        super().__init__(message)
        self.url = url
        self.host = host
        self.address = address
        self.reason = reason


class DestinationUnresolvable(NetGuardError):
    """DNS gave no answer. ``retryable`` is True for temporary resolver failures."""

    def __init__(self, message: str, *, url: str, host: str, retryable: bool) -> None:
        super().__init__(message)
        self.url = url
        self.host = host
        self.retryable = retryable


class TooManyRedirects(NetGuardError):
    def __init__(self, message: str, *, url: str, redirects: list[str]) -> None:
        super().__init__(message)
        self.url = url
        self.redirects = redirects


# -- policy ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NetPolicy:
    allowed_schemes: frozenset[str] = DEFAULT_ALLOWED_SCHEMES
    allowed_ports: frozenset[int] = DEFAULT_ALLOWED_PORTS
    max_redirects: int = DEFAULT_MAX_REDIRECTS
    # Exceptions to the address rules. Empty in production. Tests use it to reach a
    # loopback test server; an operator could use it for one named internal service.
    allow_networks: tuple[IPNetwork, ...] = field(default_factory=tuple)
    # When set, ``allow_networks`` applies only on these ports, and these ports (if not in
    # ``allowed_ports``) are open only to ``allow_networks`` addresses. Local MCP uses it so
    # loopback is reachable on its named ports only, and public hosts keep the base ports.
    # None: the exception applies on every allowed port (tests).
    exception_ports: frozenset[int] | None = None


def default_policy() -> NetPolicy:
    """The production policy. ``AGENT_NET_ALLOWED_PORTS`` (e.g. ``"80,443,8443"``)
    replaces the port allowlist."""
    raw = os.environ.get(ALLOWED_PORTS_ENV, "").strip()
    if not raw:
        return NetPolicy()
    ports = frozenset(int(p) for p in raw.replace(" ", "").split(",") if p)
    return NetPolicy(allowed_ports=ports)


# -- address classification -----------------------------------------------------------

_V4_RULES: tuple[tuple[str, str], ...] = (
    ("169.254.169.254/32", "the cloud metadata endpoint"),
    ("0.0.0.0/8", "the 'this network' range 0.0.0.0/8"),
    ("10.0.0.0/8", "a private (RFC1918) address"),
    ("100.64.0.0/10", "a carrier-grade NAT (100.64/10) address"),
    ("127.0.0.0/8", "a loopback address"),
    ("169.254.0.0/16", "a link-local address"),
    ("172.16.0.0/12", "a private (RFC1918) address"),
    ("192.0.0.0/24", "an IETF protocol-assignment address"),
    ("192.168.0.0/16", "a private (RFC1918) address"),
    ("198.18.0.0/15", "a benchmarking address"),
    ("224.0.0.0/4", "a multicast address"),
    ("240.0.0.0/4", "a reserved address"),
)
_V6_RULES: tuple[tuple[str, str], ...] = (
    ("fd00:ec2::254/128", "the cloud metadata endpoint"),
    ("::/128", "the unspecified address"),
    ("::1/128", "a loopback address"),
    ("fc00::/7", "a unique-local (fc00::/7) address"),
    ("fe80::/10", "a link-local (fe80::/10) address"),
    ("fec0::/10", "a site-local address"),
    ("ff00::/8", "a multicast address"),
    ("100::/64", "a discard-only address"),
    ("2001:db8::/32", "a documentation address"),
)
_V4_NETS = tuple((ipaddress.IPv4Network(n), why) for n, why in _V4_RULES)
_V6_NETS = tuple((ipaddress.IPv6Network(n), why) for n, why in _V6_RULES)
_NAT64 = ipaddress.IPv6Network("64:ff9b::/96")
_V4_COMPAT = ipaddress.IPv6Network("::/96")


def classify_address(address: str | IPAddress) -> str | None:
    """Return why ``address`` is refused, or None if it is a public unicast address."""
    ip = ipaddress.ip_address(address) if isinstance(address, str) else address
    if isinstance(ip, ipaddress.IPv4Address):
        for net, why in _V4_NETS:
            if ip in net:
                return why
        if ip.is_unspecified or ip.is_multicast or not ip.is_global:
            return "a non-public address"
        return None

    for net, why in _V6_NETS:
        if ip in net:
            return why
    embedded: list[ipaddress.IPv4Address] = []
    if ip.ipv4_mapped is not None:
        embedded.append(ip.ipv4_mapped)
    if ip in _V4_COMPAT:
        embedded.append(ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF))
    if ip in _NAT64:
        embedded.append(ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF))
    if ip.sixtofour is not None:
        embedded.append(ip.sixtofour)
    if ip.teredo is not None:
        embedded.extend(ip.teredo)
    for v4 in embedded:
        why = classify_address(v4)
        if why is not None:
            return f"an IPv6 address embedding {why} ({v4})"
    if embedded:
        return None
    if ip.is_unspecified or ip.is_multicast or not ip.is_global:
        return "a non-public address"
    return None


def _allowed_by_exception(ip: IPAddress, policy: NetPolicy) -> bool:
    return any(ip.version == net.version and ip in net for net in policy.allow_networks)


# -- URL vetting ----------------------------------------------------------------------


@dataclass(frozen=True)
class VettedDestination:
    url: str
    scheme: str
    host: str  # as httpcore will see it: IDNA-encoded, IPv6 without brackets
    port: int
    addresses: tuple[str, ...]  # every resolved address; all passed the check

    @property
    def pinned_ip(self) -> str:
        return self.addresses[0]


def _resolve(host: str, port: int) -> list[str]:
    """Resolve ``host`` to its IP addresses. Patched in tests."""
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    out: list[str] = []
    for _family, _type, _proto, _canon, sockaddr in infos:
        addr = str(sockaddr[0]).split("%", 1)[0]  # drop an IPv6 zone id
        if addr not in out:
            out.append(addr)
    return out


def _parse(url: str, policy: NetPolicy) -> tuple[httpx.URL, str, str, int]:
    try:
        parsed = httpx.URL(url)
    except (httpx.InvalidURL, TypeError, ValueError) as exc:
        raise BlockedDestination(
            f"Refused {url!r}: not a valid URL ({exc}).",
            url=str(url),
            reason="invalid_url",
        ) from None
    scheme = parsed.scheme.lower()
    if scheme not in policy.allowed_schemes:
        allowed = ", ".join(sorted(policy.allowed_schemes))
        raise BlockedDestination(
            f"Refused {url!r}: the {scheme or 'missing'} scheme is not allowed (only {allowed}).",
            url=url,
            reason="scheme",
        )
    host = parsed.raw_host.decode("ascii").rstrip(".").lower()
    if not host:
        raise BlockedDestination(
            f"Refused {url!r}: no host.", url=url, reason="invalid_url"
        )
    port = parsed.port or (443 if scheme == "https" else 80)
    if port not in policy.allowed_ports and port not in (policy.exception_ports or ()):
        allowed = ", ".join(str(p) for p in sorted(policy.allowed_ports))
        raise BlockedDestination(
            f"Refused {url!r}: port {port} is not allowed (only {allowed}).",
            url=url,
            host=host,
            reason="port",
        )
    return parsed, scheme, host, port


def _literal_ip(host: str) -> IPAddress | None:
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def _vet_addresses(
    url: str,
    host: str,
    port: int,
    scheme: str,
    addresses: Iterable[str],
    policy: NetPolicy,
) -> VettedDestination:
    checked: list[str] = []
    exception_port = policy.exception_ports is None or port in policy.exception_ports
    for addr in addresses:
        ip = ipaddress.ip_address(addr)
        exempt = exception_port and _allowed_by_exception(ip, policy)
        if not exempt and port not in policy.allowed_ports:
            raise BlockedDestination(
                f"Refused {url!r}: port {port} is open only to the configured local addresses.",
                url=url,
                host=host,
                address=addr,
                reason="port",
            )
        why = None if exempt else classify_address(ip)
        if why is not None:
            via = "" if _literal_ip(host) else f" ({host} resolves to {addr})"
            raise BlockedDestination(
                f"Refused {url!r}: it points to {why}{via}. "
                "The agent can only fetch public internet addresses.",
                url=url,
                host=host,
                address=addr,
                reason="address",
            )
        checked.append(str(ip))
    if not checked:
        raise DestinationUnresolvable(
            f"Could not resolve {host}: no addresses.",
            url=url,
            host=host,
            retryable=False,
        )
    return VettedDestination(
        url=url, scheme=scheme, host=host, port=port, addresses=tuple(checked)
    )


def _unresolvable(url: str, host: str, exc: OSError) -> DestinationUnresolvable:
    retryable = isinstance(exc, socket.gaierror) and exc.errno == socket.EAI_AGAIN
    return DestinationUnresolvable(
        f"Could not resolve {host}: {exc}.", url=url, host=host, retryable=retryable
    )


def check_url(url: str, *, policy: NetPolicy | None = None) -> VettedDestination:
    """Synchronously vet ``url`` (scheme, port, every resolved address).

    For validating configured endpoints such as MCP server URLs. It does not pin; a caller
    that then connects should use :class:`PinnedTransport` with ``pinned_ip``, or re-check
    at connect time.
    """
    policy = policy or default_policy()
    _, scheme, host, port = _parse(url, policy)
    literal = _literal_ip(host)
    if literal is not None:
        addresses = [str(literal)]
    else:
        try:
            addresses = _resolve(host, port)
        except OSError as exc:
            raise _unresolvable(url, host, exc) from None
    return _vet_addresses(url, host, port, scheme, addresses, policy)


async def check_url_async(
    url: str, *, policy: NetPolicy | None = None
) -> VettedDestination:
    policy = policy or default_policy()
    _, scheme, host, port = _parse(url, policy)
    literal = _literal_ip(host)
    if literal is not None:
        addresses = [str(literal)]
    else:
        try:
            addresses = await asyncio.to_thread(_resolve, host, port)
        except OSError as exc:
            raise _unresolvable(url, host, exc) from None
    return _vet_addresses(url, host, port, scheme, addresses, policy)


# -- IP pinning -----------------------------------------------------------------------


class _PinnedBackend(httpcore.AsyncNetworkBackend):
    """Connects only to the vetted (host, port), and only to the vetted IP."""

    def __init__(
        self,
        host: str,
        port: int,
        ip: str,
        inner: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.ip = ip
        self._inner = inner or httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        if host.rstrip(".").lower() != self.host or port != self.port:
            raise httpcore.ConnectError(
                f"net_guard: connection to {host}:{port} refused; "
                f"this transport is pinned to {self.host}:{self.port}"
            )
        return await self._inner.connect_tcp(
            self.ip,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise httpcore.ConnectError("net_guard: unix sockets are not allowed")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)


class PinnedTransport(httpx.AsyncHTTPTransport):
    """An httpx transport whose every TCP connection goes to one vetted IP.

    TLS SNI, certificate verification and the Host header all keep the hostname.
    """

    def __init__(
        self,
        host: str,
        port: int,
        ip: str,
        *,
        ssl_context: ssl.SSLContext | None = None,
        inner_backend: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        super().__init__(trust_env=False)
        self.backend = _PinnedBackend(host, port, ip, inner_backend)
        # httpx's constructor has no network_backend argument, so swap in a pool built
        # with ours. Pinned in tests/test_net_guard.py (PinnedTransportTests).
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl_context or httpx.create_ssl_context(),
            max_connections=4,
            http1=True,
            http2=False,
            network_backend=self.backend,
        )


TransportFactory = Callable[[str, int, str], httpx.AsyncBaseTransport]


def _default_transport_factory(
    host: str, port: int, ip: str
) -> httpx.AsyncBaseTransport:
    return PinnedTransport(host, port, ip)


# -- guarded GET ----------------------------------------------------------------------


@dataclass
class GuardedResponse:
    url: str  # final URL after redirects
    status_code: int
    reason_phrase: str
    headers: dict[str, str]  # lower-case names
    content: bytes
    truncated: bool  # the body was longer than max_bytes
    peer_ip: str  # the vetted IP the final hop connected to
    redirects: list[str] = field(default_factory=list)

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "")

    @property
    def encoding(self) -> str | None:
        for part in self.content_type.split(";"):
            part = part.strip()
            if part.lower().startswith("charset="):
                return part.split("=", 1)[1].strip().strip('"') or None
        return None


async def _read_bounded(response: httpx.Response, max_bytes: int) -> tuple[bytes, bool]:
    parts: list[bytes] = []
    size = 0
    async for chunk in response.aiter_bytes():
        if not chunk:
            continue
        remaining = max_bytes - size
        if len(chunk) > remaining:
            parts.append(chunk[:remaining])
            return b"".join(parts), True
        parts.append(chunk)
        size += len(chunk)
    return b"".join(parts), False


def _origin(url: httpx.URL) -> tuple[str, bytes, int | None]:
    return (url.scheme, url.raw_host, url.port)


async def guarded_get(
    url: str,
    *,
    timeout: float = 20.0,
    max_bytes: int = 2_000_000,
    headers: Mapping[str, str] | None = None,
    policy: NetPolicy | None = None,
    transport_factory: TransportFactory | None = None,
) -> GuardedResponse:
    """GET ``url`` under the SSRF rules. ``timeout`` bounds the whole call, all hops.

    Raises :class:`BlockedDestination`, :class:`DestinationUnresolvable`,
    :class:`TooManyRedirects`, or an ``httpx`` transport error (``httpx.TimeoutException``
    on timeout). HTTP error statuses are returned, not raised.

    ``transport_factory(host, port, ip)`` is a test hook; production uses
    :class:`PinnedTransport`.
    """
    try:
        async with asyncio.timeout(timeout):
            return await _guarded_get(
                url,
                timeout=timeout,
                max_bytes=max_bytes,
                headers=dict(headers or {}),
                policy=policy or default_policy(),
                factory=transport_factory or _default_transport_factory,
            )
    except TimeoutError:
        raise httpx.TimeoutException(
            f"Fetching {url} took longer than {timeout:g}s."
        ) from None


async def _guarded_get(
    url: str,
    *,
    timeout: float,
    max_bytes: int,
    headers: dict[str, str],
    policy: NetPolicy,
    factory: TransportFactory,
) -> GuardedResponse:
    current = url
    redirects: list[str] = []
    for _hop in range(policy.max_redirects + 1):
        dest = await check_url_async(current, policy=policy)
        transport = factory(dest.host, dest.port, dest.pinned_ip)
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(timeout),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            request = client.build_request("GET", current, headers=headers)
            response = await client.send(request, stream=True)
            try:
                location = response.headers.get("location")
                if response.status_code in _REDIRECT_STATUSES and location:
                    next_url = request.url.join(location)
                    if _origin(next_url) != _origin(request.url):
                        headers = {
                            k: v
                            for k, v in headers.items()
                            if k.lower() not in _SENSITIVE_HEADERS
                        }
                    current = str(next_url)
                    redirects.append(current)
                    continue
                body, truncated = await _read_bounded(response, max_bytes)
                return GuardedResponse(
                    url=str(response.url),
                    status_code=response.status_code,
                    reason_phrase=response.reason_phrase,
                    headers={k.lower(): v for k, v in response.headers.items()},
                    content=body,
                    truncated=truncated,
                    peer_ip=dest.pinned_ip,
                    redirects=redirects,
                )
            finally:
                await response.aclose()
    raise TooManyRedirects(
        f"Stopped after {policy.max_redirects} redirects starting from {url}.",
        url=url,
        redirects=redirects,
    )
