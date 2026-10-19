"""A prefork supervisor for volucer's `attach` backend.

The other half of `attach`. volucer only *dials* a relay in this mode — it starts no process
at all — because the fork has to happen in the application's own process: a PHP process
cannot pass a file descriptor to another process (`ext/sockets` has no `SCM_RIGHTS`), so the
side that owns the listening socket has to be the side that owns the fork. The same is true
of the Python SDK, for a different reason: `os.fork()` after threads exist is unsafe, and a
library cannot know when that is. So the fork belongs in the supervisor, and the supervisor
belongs to the application.

Usage::

    from myapp import application
    from volucer_worker import supervisor, wsgi

    supervisor.serve(
        "unix:/run/app/workers.sock",
        wsgi.call(application),
        children=16,
    )

The relay may also be `host:port`, which is what a supervisor in another container needs;
`unix:` matters because a loopback TCP relay pays Nagle/delayed-ACK unless both ends set
`TCP_NODELAY`, and an `AF_UNIX` socket has no Nagle to disable.

**Unix only.** `os.fork()` does not exist on Windows, and `serve()` refuses there rather
than silently degrading to one process. On Windows use `kind = "worker"` (volucer spawns
the processes) or `kind = "upstream"`.

**Boot before the fork.** Whatever the handler closes over is inherited by every child — a
framework's container, imported modules, a template cache — so build it before calling
`serve()`. What must *not* be built before the fork is an open connection: every child would
inherit it and they would read from it in turns. Open those per child, lazily, on the first
request.
"""

from __future__ import annotations

import os
import signal
import socket
import sys
import time
from typing import Any, Callable

from .worker import RELAY_ENV, Worker

#: The `unix:` prefix volucer's configuration uses for a path relay.
UNIX_PREFIX = "unix:"

#: How long ``accept`` waits before the master reaps children and looks again.
#:
#: It costs a caller nothing: ``accept`` returns the instant a connection is pending, so this
#: only bounds how long the master sleeps when no traffic is arriving — which is exactly when
#: it needs to be reaping.
ACCEPT_TIMEOUT = 0.5

#: How long to wait when every child slot is busy.
SLOT_BACKOFF = 0.005


def relay_path(relay: str) -> str | None:
    """The filesystem path of a `unix:` relay, or ``None`` for a TCP one."""
    relay = relay.strip()
    if relay.startswith(UNIX_PREFIX):
        return relay[len(UNIX_PREFIX) :]
    return None


def relay_value(relay: str | None = None) -> str | None:
    """The relay to listen on: the argument, else the environment, else ``None``.

    ``None`` rather than an exception, because whether a missing relay is an error depends on
    the caller. :func:`serve` turns it into a startup failure naming both places; a test or an
    embedding application may want to decide for itself.

    The trimming rule is :func:`volucer_worker.worker.socket_address`'s, deliberately: an empty
    value is what a shell produces from ``VOLUCER_RELAY_SOCKET=``, and reporting it as an empty
    address would send the operator looking for a typo in a file that never mentions the key.

    Named ``relay_value`` rather than ``relay`` so it does not shadow the local that every
    function here already calls ``relay`` — a module-level function and a parameter of the same
    name is the kind of collision that reads fine and fails confusingly.
    """
    if relay is not None:
        relay = relay.strip()
        if relay:
            return relay

    address = os.environ.get(RELAY_ENV)
    if address is None:
        return None
    address = address.strip()
    return address or None


def listen(relay: str) -> socket.socket:
    """Bind and listen on a relay, removing a stale socket file first.

    A unix socket path is a *file*, and binding one that already exists fails with
    ``EADDRINUSE`` — so a supervisor restarted after a crash could never come up. Unlinking
    first is what every Unix server does, and it is safe here because this process is meant
    to be the only listener on that path: a live supervisor would hold the socket, and the
    bind would fail rather than two processes sharing one relay.
    """
    path = relay_path(relay)
    if path is None:
        host, _, port = relay.rpartition(":")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host or "127.0.0.1", int(port)))
        listener.listen(128)
        listener.settimeout(ACCEPT_TIMEOUT)
        return listener

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(128)
    listener.settimeout(ACCEPT_TIMEOUT)
    return listener


def serve(
    relay: str | None,
    handler: Callable[[Any, Worker], Any],
    *,
    children: int = 0,
) -> None:
    """Listen on ``relay`` and fork one child per accepted connection.

    ``handler`` is the same callable :meth:`Worker.run` takes, so a WSGI application is
    ``wsgi.call(application)``.

    ``relay`` may be ``None``, in which case :func:`relay_value` reads
    ``VOLUCER_RELAY_SOCKET``. That is
    the form a deployment should use: the server reads the same variable for its ``relay``, so
    the address is written once instead of in two files that nothing keeps in sync. A value
    passed here still wins, so a script that names its own socket keeps meaning what it says.

    ``children`` caps how many children may run at once, not a queue depth. ``0`` — the
    default — means **no cap**, which is what an ``attach`` deployment wants: volucer dials the
    relay exactly ``workers`` times at startup and holds one connection per slot, so the live
    children are already bounded by that number. A cap is only useful to reserve memory for
    *other* processes on the host, and a cap **below** ``workers`` is not a queue — it is a
    startup failure, because the surplus dials sit unaccepted in the listen backlog while the
    pool waits for a handshake no child is there to send.

    Each child serves exactly one connection and exits, which is what makes recycling cheap
    and the assignment exact: volucer closes a slot, the child sees end-of-file and exits,
    and this loop forks a replacement for the next connection.

    The master runs until it is asked to stop. ``SIGTERM``/``SIGINT`` stop it accepting,
    close the listener, remove the socket file, and wait briefly for the children already
    serving — the graceful half of what a service manager expects.
    """
    if not hasattr(os, "fork"):  # pragma: no cover - Windows
        raise RuntimeError(
            "volucer_worker.supervisor needs os.fork(), which this platform does not "
            "have; use a `worker` or `upstream` backend here, or run the supervisor on Unix"
        )

    if children < 0:
        raise ValueError(f"children must be 0 (no cap) or greater, got {children}")

    # Resolved before the fork, so a missing address is one startup error rather than one per
    # child — and so `listen` is never handed `None`.
    address = relay_value(relay)
    if address is None:
        raise ValueError(
            "no relay to listen on: pass one to supervisor.serve(), or set "
            f"{RELAY_ENV} — the same variable the server reads for its `relay`, "
            'e.g. "unix:/run/app/workers.sock"'
        )

    limit = children if children > 0 else None
    listener = listen(address)

    stopping = False

    def request_stop(*_args: Any) -> None:
        # Only a flag: the accept loop owns the teardown, because closing the listener from a
        # signal handler while `accept` is inside it is a race worth not having.
        nonlocal stopping
        stopping = True

    _on_termination(request_stop)

    _announce(address, limit)

    live: set[int] = set()
    try:
        while not stopping:
            _reap(live)

            if limit is not None and len(live) >= limit:
                time.sleep(SLOT_BACKOFF)
                continue

            try:
                connection, _ = listener.accept()
            except (TimeoutError, socket.timeout):
                # Nothing pending. The loop comes back here to reap, which is the reason the
                # timeout exists rather than blocking forever in `accept`.
                continue
            except InterruptedError:  # pragma: no cover - PEP 475 retries these
                continue

            child = os.fork()
            if child:
                # The *master* must close its copy. An inherited fd is not reference counted
                # by the protocol: leaving it open here means the child's end-of-file never
                # happens, so a recycled slot would hang instead of reconnecting.
                connection.close()
                live.add(child)
                continue

            # --- the child ---
            listener.close()
            _serve_one(connection, handler)
    finally:
        listener.close()
        path = relay_path(address)
        if path is not None:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        _wait_for(live, timeout=5.0)


def _serve_one(connection: socket.socket, handler: Callable[[Any, Worker], Any]) -> None:
    """Serve one connection in a forked child, then exit.

    ``makefile`` is not optional: the worker reads and writes *file-like* streams, and a
    ``socket`` has neither ``read`` nor ``write``. Handing the socket over directly — which
    is what the PHP supervisor does, because PHP streams *are* file-like — fails with
    ``AttributeError: 'socket' object has no attribute 'read'``.

    ``os._exit`` rather than a normal return, and that is deliberate twice over: interpreter
    shutdown would run the master's ``atexit`` handlers once per child, and it would flush
    buffers the master had already written, so every child would write them again — the
    classic duplicated-output bug of a forked server.
    """
    code = 0
    try:
        Worker.create(
            connection.makefile("rb", buffering=0),
            connection.makefile("wb", buffering=0),
        ).run(handler)
    except BaseException as error:  # noqa: BLE001 - a child must never take the master down
        try:
            sys.stderr.write(f"[volucer] worker exited: {type(error).__name__}: {error}\n")
            sys.stderr.flush()
        except (OSError, ValueError):
            pass
        code = 1
    finally:
        try:
            connection.close()
        except OSError:
            pass
        os._exit(code)


def _on_termination(handler: Callable[..., None]) -> None:
    """Install the shutdown handler, where the platform allows it.

    ``signal.signal`` only works on the main thread of the main interpreter, so a supervisor
    started from a thread — a test, or an application embedding it — would otherwise fail at
    startup with ``ValueError`` for a feature it does not need. There the caller stops it by
    other means (closing the listener, ending the process), which is why the absence of a
    signal handler is not an error.
    """
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signal_number, handler)
        except (ValueError, OSError):  # pragma: no cover - not the main thread
            return


def _reap(live: set[int]) -> None:
    """Reap every child that has exited, dropping it from ``live``.

    A child that exits stays a zombie until its parent waits for it, so a supervisor that
    only accepts and forks grows one per connection — measured at exactly 10 zombies for 10
    connections on the PHP side. ``WNOHANG`` in a loop is what keeps this from blocking in
    the middle of the accept loop.
    """
    for pid in _wait_any(nohang=True):
        live.discard(pid)


def _wait_any(*, nohang: bool) -> list[int]:
    """Every child that can be collected right now (or that exits, when ``nohang`` is False)."""
    collected: list[int] = []
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG if nohang else 0)
        except ChildProcessError:
            return collected
        except InterruptedError:  # pragma: no cover - PEP 475 retries these
            continue
        if pid == 0:
            return collected
        collected.append(pid)


def _wait_for(live: set[int], *, timeout: float) -> None:
    """Give the children already serving a bounded chance to finish.

    Bounded because a child may be mid-request and the service manager will escalate
    anyway; an unbounded wait would turn a shutdown into a hang.
    """
    deadline = time.monotonic() + timeout
    while live and time.monotonic() < deadline:
        for pid in _wait_any(nohang=True):
            live.discard(pid)
        if live:
            time.sleep(SLOT_BACKOFF)


def _announce(relay: str, children: int | None) -> None:
    """Write the one readiness line a deployment needs.

    A unit file or a container healthcheck can wait for this line rather than guessing, and
    it is on stderr because stdout belongs to whatever the application might print.
    """
    cap = "no child cap" if children is None else f"{children} children at most"
    try:
        sys.stderr.write(f"[volucer] listening on {relay} ({cap})\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass


__all__ = ["serve", "listen", "relay_path", "relay_value", "UNIX_PREFIX", "RELAY_ENV"]
