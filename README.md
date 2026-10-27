# volucer-python-sdk

Python bindings for [volucer](https://github.com/sailantis/volucer): the `kv`, `lock` and
`jobs` services, and the worker protocol.

The specification is [`docs/worker-protocol.md`](https://github.com/sailantis/volucer/blob/main/docs/worker-protocol.md)
in the server repository. This package is the other end of it, beside the PHP one in
[`sailantis/volucer-php-sdk`](https://github.com/sailantis/volucer-php-sdk). All three
implement the same document rather than each other, and the wire-format tests in each
assert the same frozen bytes.

## Which half you want

There are two ways to reach volucer from Python, and most deployments want the first.

**`Rpc` — the services, without changing how your app is served.** You keep gunicorn,
uvicorn or Granian, keep your container, keep your process model. Your app gains volucer's
`kv`, `lock` and `jobs` by dialling a socket volucer gives every request:

```python
from volucer_worker import Rpc

def application(environ, start_response):
    rpc = Rpc.connect(environ=environ)
    rpc.kv("cache").set("greeting", "hello")
```

That is the whole setup, and it is what the rest of this package is arranged around. See
[the call plane](#the-call-plane-for-an-application-that-is-not-a-worker).

**`Worker` — let volucer serve the app instead of gunicorn.** The application boots once and
volucer sends it requests over the child's stdin/stdout, so a framework's bootstrap cost stops
being charged per request. This is worth it only if you want the channel itself: recycling on
RSS, no HTTP framing on the hot path, one process per slot. If you are happy with gunicorn,
**use the `upstream` backend instead** — volucer supervises gunicorn over HTTP, needs no SDK at
all, and is what the plan recommends first.

## The call plane, for an application that is not a worker

A worker asks for `kv`, `lock` and `jobs` with `CALL` frames on the stream that carries its
requests. A FastCGI application has no such stream — it exists for the duration of one request
and can only answer the frame it was handed — so volucer serves the same namespaces on a socket
of its own, `[rpc]`, which `Rpc` dials:

```python
from volucer_worker import Rpc

def application(environ, start_response):
    # `environ` is the WSGI request's, and passing it is not optional — see below.
    rpc = Rpc.connect(environ=environ)

    rpc.kv("cache").set("greeting", "hello")
    lock_id = rpc.lock().acquire("pdf:create")
    rpc.queue("mail").push("SendEmail", {"to": "a@b"})
```

| Handle | Methods |
| --- | --- |
| `Rpc.connect(address?, token?, timeout?, environ?)` | Build one; nothing is dialled until the first call |
| `rpc.kv(storage)` | `get`, `set`, `set_if_absent`, `has`, `delete`, `ttl`, `clear`, `get_multiple`, `get_many`, `set_multiple`, `serializer` |
| `rpc.lock()` | `acquire`, `try_acquire`, `release`, `exists`, `holders`, `update_ttl` |
| `rpc.queue(pipeline)` | `push(task, payload, headers)` |
| `rpc.call(name, params)` | One raw round trip, for a method with no handle |
| `rpc.last_error`, `rpc.last_call_was_unknown` | Why the last call failed, and whether it may have taken effect |

**Pass `environ` from a WSGI application.** volucer puts the address in the CGI *request*, and the
process the application runs in only receives `VOLUCER_FCGI_ADDRESS` — so a client reading
`os.environ` works for a script started by hand and fails for every real request, with a message
that blames the deployment. `Rpc.connect(environ=environ)` reads the right mapping; a CLI script
or a worker omits it and gets `os.environ`, which is correct for both.

Two things differ from a worker's channel and are worth knowing before relying on one. A call has
a **deadline** (`Rpc.connect(timeout=…)`, default 10 s), because this reads a socket rather than a
pipe — and a `lock.acquire` with a long `wait` needs a deadline that covers it, since
`[rpc].call_timeout_seconds` governs only how long the *server* spends writing an answer.
`last_call_was_unknown` is `True` only when the call may have been served: a refusal sets it
`False`, so retrying is a decision rather than a gamble.

`Kv` serializes with JSON by default and `serializer(encode, decode)` replaces that. The server
holds opaque bytes and never sees your objects, which is what lets a value written by a PHP worker
be read here — and `_decode_value` returns `bytes` rather than raising for anything that is not
JSON, so one storage can be shared by two languages.

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

## Install

```console
pip install volucer-worker
```

Or, from a checkout:

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
worker file should make; `Worker.create()` is the explicit-streams form, used when you are
binding streams yourself — including when the connection was opened by somebody else, which
is what the `attach` backend needs.

The variable carries either form, and `connect()` decides from its prefix:

| value | transport |
| --- | --- |
| `127.0.0.1:34567` | loopback TCP — the ephemeral port the server bound |
| `unix:/run/volucer/worker.0.sock` | a unix socket, from `socket = "unix:/run/volucer/worker"` |

`volucer_worker.unix_path()` is the whole of the decision and returns `None` for a TCP address,
so the connection code has one branch. `unix:` is the same convention the server's configuration
uses for `external`, `attach` and the worker `socket` key, so there is one spelling rather than a
fourth.

## Attaching instead of spawning

The `worker` backend starts your process. The `attach` backend does not: it *dials* a relay
that a supervisor in your own process listens on, and holds one connection per slot. That is
the shape that lets a supervisor fork its workers after booting the application once, so
every child inherits the interpreter, the imported modules and the loaded code instead of
paying for them again.

The split matters: **volucer cannot own the fork.** It has to happen in the application's
process, because the side that owns the listening socket has to be the side that owns the
fork — and because `os.fork()` in a process that already has threads is not something a
library can decide safely. So the fork lives in `volucer_worker.supervisor`, and you call it:

```python
# supervisor.py — boot once, then fork
import django
django.setup()

from django.core.wsgi import get_wsgi_application
from volucer_worker import supervisor, wsgi
from myapp import application

# Boot before the fork: whatever this closes over is inherited by every child.
handler = wsgi.call(application)

supervisor.serve(None, handler)     # or "unix:/run/app/workers.sock"
```

```toml
[backends.python]
kind    = "attach"
workers = 16
```

**The address can be written once.** With `None` as the first argument the supervisor reads
`VOLUCER_RELAY_SOCKET`, and so does the server when `relay` is omitted:

```ini
# the unit file, or a container's environment
Environment=VOLUCER_RELAY_SOCKET=unix:/run/app/workers.sock
```

That is the counterpart of `VOLUCER_WORKER_SOCKET`, for the mirror-image reason: that one is set
*by* the server on a child it spawned, so a supervisor cannot read it — no child was spawned. This
one is set by the operator, and both sides read it. A value passed to `serve()`, or a `relay` key in
the file, still wins, so a stale variable cannot move a deployment. `supervisor.relay_value()` is
that resolution on its own.

`children` caps how many workers may run at once, and the default (`0`) is **no cap**, which is
what an `attach` deployment wants: volucer dials `workers` times at startup and holds one
connection per slot, so the live children are already bounded by that number. A cap **below**
`workers` is not a queue — the surplus dials sit unaccepted in the listen backlog while the pool
waits for a handshake no child is there to send.

Each child serves exactly one connection and exits. That is what makes recycling cheap — when
`max_requests` is reached volucer closes the slot, the child sees end-of-file, exits, and the
supervisor forks a replacement for the next connection. The supervisor reaps its children, so
a long-lived one does not fill the process table with zombies.

**Unix only.** `os.fork()` does not exist on Windows, and `supervisor.serve()` raises there
rather than silently running a single process. Use `kind = "worker"` or `kind = "upstream"` on
Windows.

**Boot before the fork, connect after it.** A framework, a container and imported modules are
inherited by every child, which is the saving. An *open connection* must not be: every child
would inherit the socket and they would take turns reading from it. Open connections lazily,
per child, on the first request — Django and SQLAlchemy both do this already.

### Writing a supervisor yourself

`supervisor.serve()` is a convenience, not the contract. What `attach` needs from any
supervisor is that it serves a connection **it** opened, which is `Worker.create`:

```python
import socket

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.bind(("127.0.0.1", 9700))
listener.listen(64)

while True:
    connection, _ = listener.accept()
    child = os.fork()
    if child:
        connection.close()          # the master must drop its copy
        continue
    listener.close()
    Worker.create(                 # not connect(): the connection already exists
        connection.makefile("rb", buffering=0),
        connection.makefile("wb", buffering=0),
    ).run(handler)
    os._exit(0)
```

The `makefile` calls are not optional — a `socket` has neither `read` nor `write`, which is
what the worker calls on its streams. Passing the raw socket, as a PHP worker can with its
file-like streams, fails with `AttributeError`.

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

`tests/test_rpc.py` drives `Rpc` over a `socket.socketpair`, and asserts on the **bytes the client
writes** — a batch key is base64, a value is its JSON encoding and then base64, a push carries its
task. The server's half of that contract is
`a_batch_key_is_base64_in_both_directions` in the Rust suite, and the two disagreed for three
releases because neither side had a test for the batch methods.

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
- **Forking on Windows.** `supervisor.serve()` needs `os.fork()`. The platform without it is
  served by `kind = "worker"` or `kind = "upstream"`, and the end-to-end suite proves the
  `attach` contract on Windows with a threaded supervisor instead.

`Rpc` is the one part that does not need the worker channel at all — see
[the call plane](#the-call-plane-for-an-application-that-is-not-a-worker), which is the option to
reach for when `upstream` is the right deployment shape but the application still needs a cache
and a lock.
