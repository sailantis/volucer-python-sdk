# volucer-python-sdk

A Python worker for [volucer](https://github.com/sailantis/volucer)'s worker protocol.

The specification is [`docs/worker-protocol.md`](https://github.com/sailantis/volucer/blob/main/docs/worker-protocol.md)
in the server repository. This package is the other end of it, beside the PHP one in
[`sailantis/volucer-php-sdk`](https://github.com/sailantis/volucer-php-sdk). All three
implement the same document rather than each other, and the wire-format tests in each
assert the same frozen bytes.

## What a worker is

Worker mode boots an application once and serves many requests from that process, instead
of booting it per request the way `php-cgi` and FPM-like deployments do. It is the
RoadRunner/Swoole/FrankenPHP value proposition, and it is why a real framework's bootstrap
cost stops being charged on every request.

A Python application already has servers that do this — gunicorn, uvicorn, Granian. The
reason to use this SDK instead is the channel rather than the model: it runs over the
child's stdin/stdout (or a loopback socket on Windows), so the application keeps its own
process, its own container and its own warm caches, and volucer supervises it without
wrapping it in a protocol library first.

**If you want the shortest path to "my Django or Flask app runs behind volucer", use the
`upstream` backend instead.** It supervises gunicorn over HTTP, needs no SDK, and is what
the plan recommends first. This SDK exists for the case where the worker channel itself is
what you want: the `kv`, `lock`, `jobs` and `metrics` services, recycling on RSS, and no
HTTP framing on the hot path.

## Install

```console
pip install git+https://github.com/sailantis/volucer-python-sdk
```

Python 3.10 or newer. Nothing is required at runtime beyond the standard library, and there
are no optional dependencies. A worker SDK loads before an application's own environment is
known to work, so a dependency here is a failure mode unrelated to the protocol.

## A worker, in three lines

A plain WSGI application needs no adapter beyond what this package already has:

```python
from volucer_worker import Worker, wsgi
from myapp import application

Worker.connect().run(wsgi.call(application))
```

`Worker.connect()` honours `VOLUCER_WORKER_SOCKET` and falls back to stdio. It is the call a
worker file should make; `Worker.create()` is the explicit-streams form the tests use.

## Django

Django is a WSGI application, so there is nothing to adapt — `get_wsgi_application()` is the
only Django-specific line:

```python
# worker.py
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

import django
django.setup()

from django.core.wsgi import get_wsgi_application
from volucer_worker import Worker, wsgi

Worker.connect().run(wsgi.call(get_wsgi_application()))
```

```toml
[backends.django]
kind    = "worker"
command = "python worker.py"
workers = 4

[server]
script_extensions = ["py"]   # else /worker.py is PATH_INFO, not a routed script
```

That is the whole integration. `django.setup()` belongs at import time because worker mode
boots once; `get_wsgi_application()` must be called **after** it, not inside the handler.
Database connection hygiene needs nothing from you — Django closes old connections on its own
`request_started`/`request_finished` signals, which its WSGI handler fires per request.

Verified against Django 6.1.2 and a real `volucer` binary: routing, `get_host()`,
`build_absolute_uri()`, sessions in both directions, a cookieless client correctly getting no
session, and two consecutive database queries.

## Flask, FastAPI and everything else WSGI

The same four lines. Flask, Bottle and Pyramid expose `app.wsgi_app` / `app` as WSGI callables.
**FastAPI is ASGI, not WSGI** — see "What this SDK does not do" below.

Point a backend at it:

```toml
[backends.python]
kind = "worker"
command = "python worker.py"
workers = 4
memory_limit_mb = 256
script_extensions = ["py"]
```

Two configuration keys are not optional for a non-PHP worker:

- `script_extensions` must name the extension your entry point has, or `/app.py` arrives at
  the front controller as `PATH_INFO` instead of being routed. A list naming `py` *replaces*
  the default rather than adding to it, so a site serving Python stops treating `.php` as a
  script — which is what "this tree is Python" means.
- `workers` is the concurrency, because the protocol carries **one request in flight per
  worker process**. That is a deliberate property of the channel rather than a limitation to
  work around: a server with one goroutine or greenlet per request needs a different
  protocol, not a different SDK.

## The two Python-specific answers

Everything else in this SDK is a transliteration of the specification. These two are not.

### stdout is the channel, so the SDK takes it

The protocol uses the child's stdout as its channel. PHP solves this with an output buffer
around `echo`; Python's answer is that importing this package **rebinds `sys.stdout`** to a
stream that writes to stderr, and keeps the real protocol stream private to the worker.

The consequence is worth stating plainly: `print()` still reaches a human (through stderr,
which the supervisor drains), but it will not appear on the process's own stdout. A worker
that did not do this would corrupt its framing the first time somebody left a `print` in,
and the symptom would be a killed worker rather than a stray line.

### RSS has no exact per-request answer in Python

The server recycles a worker whose reported `peak_memory_bytes` exceeds `memory_limit_mb`,
and the protocol wants that figure **per request, not per process**: a high-water mark that
never falls would turn a leak detector into a per-request application boot.

PHP has `memory_get_peak_usage()` plus `memory_reset_peak_usage()`, which is exactly the
right shape. Python does not, so this SDK offers two answers and says which you get:

| `track_memory` | Reported figure | Behaviour |
| --- | --- | --- |
| `False` (default) | The process's resident set size, from `/proc/self/statm` or `GetProcessMemoryInfo` | Cumulative by nature and **cannot fall**, so a peak that once exceeded the limit recycles the worker after every request. Cheap. |
| `True` | The peak of Python's own allocator, reset per request via `tracemalloc.reset_peak()` | The shape the protocol asks for, and it excludes memory C extensions allocate outside Python's allocator. Costs a `tracemalloc` hook on every allocation. |

```python
Worker.connect(track_memory=True).run(handler)
```

Use `track_memory=True` if you set `memory_limit_mb` and want it to mean what it says. Use
the default and **leave `memory_limit_mb` unset** if you want the cheapest worker, because a
cumulative figure against a threshold recycles constantly rather than detecting a leak.
Reporting `0` ("this runtime does not report it") is also accepted by the server, and a
worker that does not report is one the pool never recycles on memory — see
`Worker.peak_memory_bytes` for where to override it.

## The parts of the protocol a handler sees

| Frame | Handler surface |
| --- | --- |
| `REQUEST` / `RESPONSE` / `BODY` / `END` | `Worker.run(handler)`; the handler returns a `Response`, `bytes`, `str`, a `(status, headers, body)` tuple, or `None` |
| `JOB` / `ACK` | `Worker.consume(handler)`; `None` acknowledges, `False` requeues, a `str` rejects with a reason, and a raise requeues |
| `CALL` / `RETURN` | `Worker.call(name, params)` from inside a handler; `Worker.on_call(handler)` answers the server's calls |
| `LOG` / `METRIC` | `Worker.emit_log(...)`, `Worker.emit_metric(...)` |
| `CONTROL` | Answered by the SDK; `drain` and `shutdown` stop the loop |
| `PING` / `PONG` | Answered by the SDK |

`Worker.recycle()` asks to be replaced after the current request, which is reported as
`keep_alive = false` in `END`.

## gRPC

A gRPC call arrives as an ordinary `REQUEST` whose `content-type` is `application/grpc…`,
and its outcome travels in the reply's trailer section rather than in the HTTP status. The
SDK exposes that as `volucer_worker.grpc`:

```python
from volucer_worker import Worker, grpc

worker = Worker.connect()
worker.on_grpc(lambda request, worker: grpc.GrpcResponse.message(encode_reply(request)))
worker.run(handler)
```

`grpc.GrpcResponse.message(payload)` takes the **framed bytes** — the compression flag and
the big-endian length included. This SDK does not ship a protobuf encoder, because doing so
would put a schema in the transport.

## Tests

```console
python -m unittest discover -s tests
```

The wire-format tests assert the same frozen bytes as
`crates/volucer-worker/tests/wire_format.rs` and `volucer-php-sdk/tests/WireFormatTest.php`,
so all three cannot drift apart silently.

The end-to-end test drives a real `volucer` binary and is skipped unless one is found:

```console
VOLUCER_BIN=/path/to/volucer python -m unittest tests.test_end_to_end
```

It looks for `../volucer/target/debug/volucer` when `VOLUCER_BIN` is unset, so a developer
working in the sibling checkout needs no environment at all. CI sets `VOLUCER_BIN` and
fails on a skip, for the reason the server's own suites do: a skipped test and a passing one
are the same green.

## What this SDK does not do

- **ASGI.** Concurrency inside one worker needs multiplexed requests on one channel, which
  is a protocol change rather than an adapter. Do not expect it in this release.
- **Streaming responses.** `RESPONSE` declares `body_len` before the body, so a response is
  complete before it starts. That is a protocol property, not a missing feature here.
- **Per-request state reset for your framework.** WSGI's per-request environment is rebuilt
  by the SDK, so that much is reset; a framework that keeps module-level state, an open
  database connection or a cached container has to deal with that itself. **Django does not
  need anything extra**: `django/db/__init__.py` connects `close_old_connections` to both
  `request_started` and `request_finished`, and `WSGIHandler` fires both, so a transaction left
  open by one view is closed before the next one runs — verified against a real `volucer`
  binary by making a view abandon a transaction on purpose. The check that matters is two
  consecutive requests that must not see each other's state, and
  `tests/test_end_to_end.py` asserts exactly that for the SDK's own surface.
