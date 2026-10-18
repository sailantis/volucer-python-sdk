"""A WSGI application that works as a volucer worker.

The smallest useful shape: a plain WSGI callable, served by this SDK. A backend names it::

    [backends.python]
    kind = "worker"
    command = "python examples/wsgi_app.py"

The application is deliberately boring — it echoes the request back as JSON — because the
interesting part of this file is the last line, which is the whole of what makes a WSGI
application a worker.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from volucer_worker import Worker, wsgi  # noqa: E402


def application(environ, start_response):  # type: ignore[no-untyped-def]
    """A WSGI application: the ordinary interface, unchanged."""
    if environ["PATH_INFO"] == "/health":
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    body_in = environ["wsgi.input"].read()
    body = json.dumps(
        {
            "method": environ["REQUEST_METHOD"],
            "path": environ["PATH_INFO"],
            "query": environ["QUERY_STRING"],
            "scheme": environ["wsgi.url_scheme"],
            "host": environ["SERVER_NAME"],
            "body_len": len(body_in),
            "accept": environ.get("HTTP_ACCEPT", ""),
        },
        separators=(",", ":"),
    ).encode()

    start_response(
        "200 OK",
        [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
    )
    return [body]


if __name__ == "__main__":
    Worker.connect().run(wsgi.call(application))
