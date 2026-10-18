"""The request and response objects a Python worker's handler sees.

These are the SDK's own types rather than a framework's, because the SDK must work with
every Python web stack and none of them owns the wire. :func:`volucer_worker.wsgi.environ`
turns a :class:`Request` into a WSGI environment; a framework adapter is that environment,
not these objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import parse_qsl

#: Header names that may legitimately appear more than once on a response.
#:
#: ``Set-Cookie`` is the famous one, and it only matters in the response direction: a
#: request's repeated headers mean "joined with a comma", which is what HTTP says a
#: repeated field means. Collapsing two cookies into one would silently break a session.
_REPEATABLE = frozenset({"set-cookie"})


@dataclass(slots=True)
class Request:
    """One request, as it arrived on the channel.

    `headers` is a list of pairs rather than a dict, because the same name may arrive
    twice and the order is the client's. :meth:`header` is the ordered lookup, and
    :attr:`header_map` is the collapsed form a WSGI environment needs.
    """

    method: str = "GET"
    uri: str = "/"
    protocol: str = "HTTP/1.1"
    headers: list[tuple[str, str]] = field(default_factory=list)
    remote_addr: str = ""
    script_filename: str = ""
    https: bool = False
    body: bytes = b""

    def header(self, name: str) -> str | None:
        """The first value of a header, case-insensitively, or None."""
        lowered = name.lower()
        for key, value in self.headers:
            if key.lower() == lowered:
                return value
        return None

    @property
    def header_map(self) -> dict[str, str]:
        """Every header as a name → value map, repeated names joined with a comma.

        Repeated names are joined rather than overwritten, because that is what a repeated
        HTTP field means. `Set-Cookie` is the exception and it is a *response* header, so
        the response side keeps the pair list instead.
        """
        collapsed: dict[str, str] = {}
        for key, value in self.headers:
            if key in collapsed:
                collapsed[key] = f"{collapsed[key]}, {value}"
            else:
                collapsed[key] = value
        return collapsed

    @property
    def path(self) -> str:
        """The path, without the query string."""
        return self.uri.split("?", 1)[0]

    @property
    def query_string(self) -> str:
        """Everything after the first `?`, or an empty string."""
        _, _, query = self.uri.partition("?")
        return query

    @property
    def query(self) -> dict[str, str]:
        """The query string as a dict, later values winning."""
        return dict(parse_qsl(self.query_string, keep_blank_values=True))

    @property
    def content_type(self) -> str | None:
        return self.header("content-type")

    @property
    def is_grpc(self) -> bool:
        """Whether this request is a gRPC call.

        The media type decides, on both sides of the channel — see
        `docs/worker-protocol.md`. A worker that serves both HTTP routes and gRPC services
        is the ordinary shape, so this is the question an adapter asks rather than
        something the loop decides for it.
        """
        media_type = (self.content_type or "").strip().lower()
        if not media_type.startswith("application/grpc"):
            return False
        rest = media_type.removeprefix("application/grpc")
        return rest == "" or rest[0] in ";+-"


@dataclass(slots=True)
class Response:
    """A response the SDK will send.

    Header order is preserved as given, because the server relays it verbatim and two
    `Set-Cookie` headers are two cookies.
    """

    status: int = 200
    headers: list[tuple[str, str]] = field(default_factory=list)
    body: bytes = b""

    @classmethod
    def text(cls, body: str, status: int = 200) -> "Response":
        return cls(status, [("Content-Type", "text/plain; charset=utf-8")], body.encode())

    @classmethod
    def html(cls, body: str, status: int = 200) -> "Response":
        return cls(status, [("Content-Type", "text/html; charset=utf-8")], body.encode())

    @classmethod
    def json(cls, value: object, status: int = 200) -> "Response":
        import json

        return cls(
            status,
            [("Content-Type", "application/json")],
            json.dumps(value, separators=(",", ":")).encode(),
        )

    def with_header(self, name: str, value: str) -> "Response":
        self.headers.append((name, value))
        return self

    def with_status(self, status: int) -> "Response":
        self.status = status
        return self

    def with_body(self, body: bytes) -> "Response":
        self.body = body
        return self


@dataclass(slots=True)
class Job:
    """One unit of queued work, as a consumer sees it.

    A job is a separate exchange from a request rather than a flag on one, because the two
    differ in what an application may assume: a request handler is expected to reset what
    the last one left behind, and a consumer is expected to keep its connection pool and
    warm caches across jobs.
    """

    pipeline: str = ""
    id: str = ""
    task: str = ""
    headers: list[tuple[str, str]] = field(default_factory=list)
    attempts: int = 0
    body: bytes = b""


def iter_headers(headers: Iterable[tuple[str, str]]) -> Iterable[tuple[str, str]]:
    """Validate a header list from application code, dropping malformed pairs.

    A non-string name or value is *cast* rather than dropped, because a producer that wrote
    `3` as a header name meant `"3"`, and failing a whole response over it would be a worse
    answer than sending it.
    """
    for pair in headers:
        if len(pair) != 2:
            continue
        yield str(pair[0]), str(pair[1])
