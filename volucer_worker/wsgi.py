"""A WSGI adapter: turn a :class:`~volucer_worker.message.Request` into an environment.

This is the half of a Python worker that is genuinely new work rather than a
transliteration of the PHP SDK. PHP has a SAPI that builds `$_SERVER` from a CGI
environment, so the PHP SDK already receives one; Python has WSGI, which is *also* a
CGI-derived environment but has its own opinions about what the keys mean.

The values here follow PEP 3333, with the three that are easy to get wrong called out:

* ``wsgi.url_scheme`` comes from the frame's `https` flag, because the child process has
  no socket and cannot ask. A deployment behind TLS that reported ``http`` here would
  generate ``http://`` links to pages nobody can reach.
* ``wsgi.input`` is a real file-like object over the body, not a ``BytesIO`` alone: a
  framework may call ``read()``, ``readline()``, ``readlines()`` or iterate, and each of
  those has to work.
* ``wsgi.multithread`` is ``False``. The protocol carries one request in flight per
  worker process, so a worker that told a framework otherwise would invite it into a
  concurrency model the channel cannot carry.

Sync WSGI only. ASGI is **not** a small extension: concurrency inside one worker needs
multiplexed requests on one channel, which is a protocol change rather than an adapter, so
this SDK does not promise it.
"""

from __future__ import annotations

import io
from typing import Any, Callable, Iterable

from .message import Request, Response

#: The default server name, when a `Host` header carries none.
#:
#: WSGI requires ``SERVER_NAME`` and ``SERVER_PORT``, and a request that reached the server
#: over HTTP/1.1 always has a `Host`. A missing one is a malformed request rather than a
#: case to support, so this is a value rather than a branch in the adapter.
DEFAULT_SERVER_NAME = "localhost"


def split_host(host: str | None, https: bool) -> tuple[str, str]:
    """Split a `Host` header into a name and a port, with the scheme's default filled in.

    A bracketed IPv6 literal keeps its brackets and its colons; everything else splits on
    the last colon, which is the only unambiguous rule HTTP has for a host header.
    """
    default_port = "443" if https else "80"
    if not host:
        return DEFAULT_SERVER_NAME, default_port

    host = host.strip()
    if host.startswith("["):
        closing = host.find("]")
        if closing != -1:
            name = host[: closing + 1]
            rest = host[closing + 1 :]
            if rest.startswith(":") and rest[1:]:
                return name, rest[1:]
            return name, default_port
        return host, default_port

    if ":" in host:
        name, _, port = host.rpartition(":")
        if name and port:
            return name, port
    return host, default_port


def environ(request: Request, server_port: str = "") -> dict[str, Any]:
    """Build the WSGI environment for one request.

    `server_port` overrides the port derived from the `Host` header, for a deployment whose
    listener is not the port a client sees — a container behind a published port, for
    instance. The header is the better source when it is there, so this is an override
    rather than the default.
    """
    name, port = split_host(request.header("host"), request.https)
    if server_port:
        port = server_port

    raw_path = request.path
    path_info = _decode_path(raw_path)
    query = request.query_string

    env: dict[str, Any] = {
        "REQUEST_METHOD": request.method,
        "SCRIPT_NAME": "",
        "PATH_INFO": path_info,
        "QUERY_STRING": query,
        "SERVER_NAME": name,
        "SERVER_PORT": port,
        "SERVER_PROTOCOL": request.protocol,
        "REMOTE_ADDR": request.remote_addr,
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "https" if request.https else "http",
        "wsgi.input": io.BytesIO(request.body),
        "wsgi.errors": _errors_stream(),
        "wsgi.multithread": False,
        "wsgi.multiprocess": True,
        "wsgi.run_once": False,
    }

    if request.script_filename:
        # Not a CGI variable, and deliberately not `SCRIPT_FILENAME`: the protocol's own
        # documentation says routing metadata is not a file to execute, and a Python
        # application that opened this path would be a path-traversal bug waiting to happen.
        # Named with a `volucer.` prefix so nothing mistakes it for WSGI.
        env["volucer.script_filename"] = request.script_filename

    for key, value in request.headers:
        upper = key.upper().replace("-", "_")
        if upper in ("CONTENT_TYPE", "CONTENT_LENGTH"):
            env[upper] = value
        else:
            env[f"HTTP_{upper}"] = value

    if "CONTENT_LENGTH" not in env and request.body:
        # A WSGI server is expected to supply this for a body; the request may have arrived
        # without one, and a framework that read `wsgi.input` to the end would otherwise not
        # know how much to expect.
        env["CONTENT_LENGTH"] = str(len(request.body))

    return env


def _decode_path(raw: str) -> str:
    """Percent-decode a URL path the way PEP 3333 wants it.

    PEP 3333 is specific and surprising: ``PATH_INFO`` is the decoded path, **re-encoded as
    latin-1** so that the WSGI server can round-trip arbitrary bytes. Passing the UTF-8
    bytes of a decoded path instead is a well-known bug — a path with a non-ASCII character
    arrives as mojibake, or raises when a framework decodes it.
    """
    from urllib.parse import unquote_to_bytes

    raw_bytes = unquote_to_bytes(raw)
    # `+` is a space in a query string but a literal plus in a path.
    return raw_bytes.decode("latin-1")


class _ErrorsStream:
    """``wsgi.errors``, writing to one place that is not the protocol channel.

    The SDK rebinds ``sys.stdout`` because the protocol uses it; ``sys.stderr`` is free and
    the supervisor drains it, so this is a thin pass-through rather than a buffer. An
    application's traceback is worth seeing, and losing it to a full pipe would be the
    worst of both.
    """

    def write(self, text: str) -> int:
        import sys

        sys.stderr.write(text)
        return len(text)

    def writelines(self, lines: Iterable[str]) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        import sys

        sys.stderr.flush()


def _errors_stream() -> _ErrorsStream:
    return _ErrorsStream()


def call(
    app: Callable[[dict[str, Any], Callable[[str, list[tuple[str, str]]], Any]], Iterable[bytes]],
) -> Callable[[Request, Any], Response]:
    """Wrap a WSGI application so it can be handed to :meth:`Worker.run`.

    The usual shape of a Python worker, which is why this exists as a function rather than
    as documentation:

    .. code-block:: python

        from volucer_worker import Worker, wsgi
        from myapp import application

        Worker.connect().run(wsgi.call(application))

    The result is a callable taking ``(request, worker)`` and returning a
    :class:`~volucer_worker.message.Response`, which is what ``run()`` wants.
    """

    def handler(request: Request, _worker: Any) -> Response:
        collected: list[bytes] = []
        state: dict[str, Any] = {}

        def start_response(
            status: str,
            headers: list[tuple[str, str]],
            exc_info: Any = None,
        ) -> Callable[[bytes], None]:
            if exc_info is not None:
                raise exc_info[1].with_traceback(exc_info[2])
            if "status" in state:
                raise RuntimeError(
                    "start_response was called twice; a WSGI application must call it once"
                )
            state["status"] = status
            state["headers"] = list(headers)
            return collected.append

        body = app(environ(request), start_response)

        if "status" not in state:
            raise RuntimeError(
                "the WSGI application returned without calling start_response"
            )

        try:
            for chunk in body:
                if not isinstance(chunk, bytes):
                    raise TypeError(
                        "a WSGI application must yield bytes, got " + type(chunk).__name__
                    )
                collected.append(chunk)
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

        code = int(str(state["status"]).split(" ", 1)[0])
        return Response(code, state["headers"], b"".join(collected))

    return handler
