"""The worker loop: handshake, serve, and the exchanges that run beside it.

The shape is the PHP SDK's, because the two implement one specification: a handshake, a
read loop that dispatches on the frame type, and a response that is a head, zero or more
body chunks, then a terminal frame.

Two things here are genuinely language-specific rather than transliterated:

**Output.** PHP rebinds its own `echo` with an output buffer, because PHP's SAPI would
otherwise put application output on the protocol channel. Python has the same hazard from
the other direction: `print()` and a logging handler's default stream both write to
``sys.stdout``, which *is* the channel. The SDK therefore rebinds ``sys.stdout`` for the
process's lifetime and keeps the real protocol stream in a private reference. A worker
that never rebinds is a worker that corrupts its own framing the first time somebody
leaves a `print` in.

**RSS.** The protocol's `peak_memory_bytes` drives recycling on `memory_limit_mb`, and PHP
has an exact per-request answer in `memory_get_peak_usage()` plus
`memory_reset_peak_usage()`. Python has nothing equivalent, so this SDK reports the peak
of Python's own allocator (`tracemalloc`) when it is enabled, and otherwise the process's
resident set size as the platform reports it. The difference is stated in the README
because it changes how eagerly a worker is recycled, which is a deployment decision.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from typing import Any, Callable, Iterable

from . import protocol
from .message import Job, Request, Response, iter_headers
from .protocol import ProtocolError

#: The environment variable naming a socket to connect to instead of using stdio.
#:
#: Set by the server on a platform where a pipe is not the transport — Windows named
#: pipes — so a worker that supports it is reachable there. Absent, empty or whitespace
#: means stdio, because ``VOLUCER_WORKER_SOCKET=`` is what a shell produces from a stray
#: space and connecting to it would be a confusing failure rather than the obvious one.
SOCKET_ENV = "VOLUCER_WORKER_SOCKET"

#: The environment variable naming the relay an `attach` backend dials.
#:
#: The counterpart of :data:`SOCKET_ENV`, and for the same purpose: one address both halves of a
#: deployment must agree on, written once. The difference is direction. ``SOCKET_ENV`` is set *by*
#: the server, on a child it spawned, so a supervisor cannot use it — there is no spawned child,
#: and the value there would be whatever an enclosing environment happened to leave. This one is
#: set by the *operator*, in the unit file or the container environment, and both the server's
#: ``relay`` and :func:`volucer_worker.supervisor.serve` read it.
#:
#: Absent, empty or whitespace means unset, for the same reason ``SOCKET_ENV`` does.
RELAY_ENV = "VOLUCER_RELAY_SOCKET"

#: The package name this SDK is, reported in `runtime`.
PACKAGE = "volucer-worker-python"

#: This process's real stdout, captured before `volucer_worker/__init__.py` replaces it.
#:
#: That replacement is the reason for this name existing at all. Importing the package
#: rebinds ``sys.stdout`` to a stream that writes to *stderr*, so an application's ``print``
#: cannot land inside a frame. The guard is installed at the bottom of ``__init__``, *after*
#: this module is imported, so the binding below is the genuine stream — and it is the one
#: ``create()`` must use.
#:
#: Using ``sys.stdout.buffer`` in ``create()`` was a defect that only the pipe transport
#: exposed: the handshake and every response went to stderr, the server saw no ``HELLO_ACK``
#: and reported a startup timeout, with "configuration loaded" as the last thing it logged.
#: Windows defaults to the socket transport, so the channel is a socket there and
#: ``sys.stdout`` is never consulted — which is why every measurement taken on Windows passed
#: while Linux and macOS took the broken branch.
real_stdout = sys.stdout


def socket_address() -> str | None:
    """The socket to connect to, or None for stdio."""
    address = os.environ.get(SOCKET_ENV)
    if address is None:
        return None
    address = address.strip()
    return address or None


def runtime() -> str:
    """Identify this worker to the server: the SDK and the build it is running.

    The build is ``PYTHON_VERSION`` rather than a package version, because a worker is
    usually vendored rather than installed and a version constant would be one more thing
    to keep true by hand. ``runtime`` is a free-form string the server only logs, so this
    is a diagnostic rather than a contract.
    """
    return f"{PACKAGE}/{sys.version_info.major}.{sys.version_info.minor}"


def _process_rss_bytes() -> int:
    """The process's resident set size, or 0 when the platform will not say.

    Zero is the protocol's "this runtime does not report it", and the pool must not recycle
    on a value it did not get — so an honest zero is better than a guess.
    """
    try:
        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes

            class _Counters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = _Counters()
            counters.cb = ctypes.sizeof(_Counters)
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            handle = kernel32.GetCurrentProcess()
            if kernel32.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                return int(counters.WorkingSetSize)
            return 0

        with open("/proc/self/statm", "rb") as statm:
            pages = int(statm.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, AttributeError):
        return 0


class Worker:
    """The worker side of the channel.

    Construct one with :meth:`connect` (the ordinary way, which checks the socket
    environment variable) or :meth:`create` (explicit streams, which is what the tests
    use).

    ``in_`` and ``out`` are binary streams. ``err`` is where diagnostics go and is *not*
    part of the protocol: the supervisor drains it so the pipe buffer cannot fill.
    """

    def __init__(
        self,
        inp: Any,
        out: Any,
        err: Any,
        *,
        track_memory: bool = False,
    ) -> None:
        self._in = inp
        self._out = out
        self._err = err
        self._checksum = True
        self._request_id = protocol.UNSOLICITED
        self._keep_alive = True
        self._stopping = False
        self._next_call_id = 0xFFFFFFFF
        self._track_memory = track_memory
        self._call_handler: Callable[[dict[str, Any]], Any] | None = None
        self._grpc_handler: Callable[[Request, "Worker"], Any] | None = None
        self._http_handler: Callable[[Request, "Worker"], Any] | None = None
        self._job_handler: Callable[[Job, "Worker"], Any] | None = None
        self._last_call_known = True

    # ------------------------------------------------------------ construction

    @classmethod
    def create(
        cls,
        inp: Any = None,
        out: Any = None,
        err: Any = None,
        *,
        track_memory: bool = False,
    ) -> "Worker":
        """Build a worker on explicit streams, defaulting to stdio.

        The output stream is ``real_stdout.buffer``, not ``sys.stdout.buffer``: importing this
        package rebinds ``sys.stdout`` to a stream that writes to stderr (so an application's
        ``print`` cannot land inside a frame), which means ``sys.stdout`` here is the *guard*
        and using it would send every frame to stderr. See :data:`real_stdout`.
        """
        inp = inp if inp is not None else sys.stdin.buffer
        out = out if out is not None else real_stdout.buffer
        err = err if err is not None else sys.stderr
        if track_memory:
            import tracemalloc

            if not tracemalloc.is_tracing():
                tracemalloc.start()
        return cls(inp, out, err, track_memory=track_memory)

    @classmethod
    def connect(cls, *, track_memory: bool = False) -> "Worker":
        """Build a worker, using the socket transport when the server named one.

        This is what a worker file should call. With ``VOLUCER_WORKER_SOCKET`` set, the
        protocol runs over a loopback socket; with it unset, over stdin/stdout exactly as
        :meth:`create` does. Nothing else in a worker needs to know which one it got.

        The option exists for one measured reason. On Windows, PHP's stream layer takes
        ~8 ms median to return from a blocking read on a *pipe* while the same read on a
        *socket* returns in ~0.09 ms (plan hazard 46). Python's own pipe cost is smaller,
        but the option is the server's and a worker that ignored it would present as a
        startup timeout with no clue as to the cause.

        A worker that cannot reach the address exits non-zero rather than falling back to
        stdio: the server is waiting for a connection, so a silent fallback would hang.
        """
        address = socket_address()
        if address is None:
            return cls.create(track_memory=track_memory)

        import socket

        try:
            host, _, port = address.rpartition(":")
            connection = socket.create_connection((host or "127.0.0.1", int(port)), timeout=10.0)
            connection.settimeout(None)
        except (OSError, ValueError) as error:
            # The address is the server's, so a failure here is a deployment problem and it
            # has to be legible on stderr — which, on the socket transport, is the only
            # channel the server sees.
            sys.stderr.write(
                f"worker: cannot reach {address} ({SOCKET_ENV}): {error}\n"
            )
            sys.stderr.flush()
            raise SystemExit(1) from error

        return cls(
            connection.makefile("rb", buffering=0),
            connection.makefile("wb", buffering=0),
            sys.stderr,
            track_memory=track_memory,
        )

    # -------------------------------------------------------------- diagnostics

    def diagnose(self, message: str) -> None:
        """Write a diagnostic line to stderr.

        Never to the protocol stream: a line there would land inside a frame.
        """
        try:
            self._err.write(f"[volucer] {message}\n")
            self._err.flush()
        except (OSError, ValueError):
            pass

    # ------------------------------------------------------------------ reading

    def read_header(self) -> protocol.Header | None:
        """The next frame header, or None at a clean end of stream.

        A stream that ends before the first byte is a clean close — that is how the server
        asks a worker to stop. A stream that ends *part-way* through a header is an error,
        because treating it as a clean close would hide a truncated frame.
        """
        raw = self._read_exactly(protocol.HEADER_LEN, allow_eof=True)
        if raw is None:
            return None
        return protocol.Header.decode(raw)

    def _read_exactly(self, length: int, *, allow_eof: bool = False) -> bytes | None:
        chunks = []
        remaining = length
        while remaining:
            chunk = self._in.read(remaining)
            if not chunk:
                if allow_eof and remaining == length:
                    return None
                raise ProtocolError(
                    f"the stream ended after {length - remaining} of {length} bytes"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def read_payload(self, header: protocol.Header) -> bytes:
        """Read a frame's payload and verify its checksum.

        A mismatch means framing has already desynchronised, so this is fatal by
        construction: the exception propagates and the process exits, rather than trying to
        resynchronise a stream that cannot be resynchronised.

        A frame that declares ``FLAG_NO_CHECKSUM`` is read without hashing it, but only
        when this worker actually agreed to that; otherwise the claim is a protocol
        violation, because accepting it would let the server turn the check off on a
        worker that never offered to give it up.
        """
        if header.payload_len == 0:
            return b""

        # Refused *before* reading, which is the point of the limit: a corrupt
        # `payload_len` must not make the reader allocate 4 GiB. The check lives here rather
        # than in `Header.decode` so the header fixtures can carry a value no real frame
        # would, and so the answer is the same one Rust's reader gives.
        if header.payload_len > protocol.MAX_PAYLOAD_LEN:
            raise ProtocolError(
                f"a frame declares {header.payload_len} payload bytes, above the "
                f"{protocol.MAX_PAYLOAD_LEN} limit; refusing it before allocating"
            )

        payload = self._read_exactly(header.payload_len)
        assert payload is not None

        if not header.declares_no_checksum:
            computed = protocol.crc32c(payload)
            if computed != header.checksum:
                raise ProtocolError(
                    f"payload checksum mismatch on request {header.request_id}: "
                    f"expected 0x{header.checksum:08x}, computed 0x{computed:08x}"
                )
            return payload

        if self._checksum:
            raise ProtocolError(
                f"the server sent a {header.type_name} frame declaring no checksum, which "
                "this worker never agreed to"
            )

        return payload

    # ------------------------------------------------------------------ writing

    def send(self, frame: protocol.Frame) -> None:
        """Write one frame, in full."""
        self._out.write(frame.encode(self._checksum))
        self._out.flush()

    def send_json(
        self, message_type: int, request_id: int, value: Any
    ) -> None:
        self.send(
            protocol.Frame(message_type, request_id, protocol.json_bytes(value))
        )

    # ---------------------------------------------------------------- handshake

    def handshake(self) -> None:
        """Read the HELLO, answer it, and adopt the checksum agreement.

        The answer is sent *before* the agreement is adopted, so the ``HELLO_ACK`` always
        carries a real checksum: both ends are still verifying at the moment the agreement
        is exchanged, and only then does the worker stop hashing.
        """
        offer = False
        while True:
            header = self.read_header()
            if header is None:
                raise ProtocolError("the server closed the pipe before the handshake")

            payload = self.read_payload(header)

            if header.message_type == protocol.HELLO:
                if payload:
                    hello = protocol.decode_json(payload, "a HELLO")
                    version = hello.get("protocol_version", protocol.PROTOCOL_VERSION)
                    if version != protocol.PROTOCOL_VERSION:
                        raise ProtocolError(
                            f"the server speaks protocol version {version}, this SDK speaks "
                            f"{protocol.PROTOCOL_VERSION}"
                        )
                    # Only the JSON literal `true` counts: a field that switches off an
                    # integrity check must not be satisfied by a truthy value.
                    offer = hello.get("no_checksum") is True
                break

            if header.message_type in (protocol.LOG, protocol.METRIC, protocol.EVENT):
                continue

            raise ProtocolError(
                "expected a HELLO to begin the handshake, got a " + header.type_name
            )

        self.send_json(
            protocol.HELLO_ACK,
            protocol.UNSOLICITED,
            {
                "protocol_version": protocol.PROTOCOL_VERSION,
                "php_version": "",
                "sapi": "",
                "pid": os.getpid(),
                "runtime": runtime(),
                "no_checksum": offer,
            },
        )

        if offer:
            self._checksum = False

    # -------------------------------------------------------------------- loops

    def run(self, handler: Callable[[Request, "Worker"], Any]) -> None:
        """Handshake, then serve requests until the server closes the channel.

        `handler` receives the :class:`Request` and this worker, and may return:

        * a :class:`Response` — sent as given;
        * ``bytes`` or ``str`` — sent as a 200 with a text/plain or octet-stream type;
        * a ``(status, headers, body)`` tuple — the WSGI shape, which is what a framework
          adapter returns.

        Raising is allowed: the SDK catches it, reports it on stderr, and answers 500.
        """
        self._http_handler = handler
        self.handshake()

        while not self._stopping:
            header = self.read_header()
            if header is None:
                # Clean EOF: the server closed our input, which is how a worker is asked to
                # stop. Not an error, and not worth a log line.
                return

            payload = self.read_payload(header)
            self._dispatch(header, payload)

    def consume(self, handler: Callable[[Job, "Worker"], Any]) -> None:
        """Handshake, then handle jobs until the server closes the channel.

        What the handler returns decides the job's fate, and the three answers are the
        protocol's three outcomes:

        * ``None`` — **acknowledge.** Done.
        * ``False` — **requeue.** Not done, and it should be handed out again.
        * a ``str`` — **reject**, with that string as the reason. Done with, not coming back.

        A handler that *raises* requeues, and that is the safe direction: a consumer that
        crashes must not lose the job, and an application that wanted a crashing job dropped
        can catch its own exception and return a string — which is the visible, deliberate
        act this default pushes an application towards.
        """
        self._job_handler = handler
        self.handshake()

        while not self._stopping:
            header = self.read_header()
            if header is None:
                return

            payload = self.read_payload(header)
            self._dispatch(header, payload)

    def _dispatch(self, header: protocol.Header, payload: bytes) -> None:
        """One frame, on either loop. The two loops differ only in what they registered."""
        if header.message_type == protocol.REQUEST:
            if self._http_handler is None and self._grpc_handler is None:
                self.diagnose(
                    "the server sent a REQUEST to a consumer; this worker is configured for "
                    "the wrong mode, and the request is being refused"
                )
                self._refuse_request(header)
                return
            self._serve(header, payload)
            return

        if header.message_type == protocol.JOB:
            if self._job_handler is None:
                self.diagnose(
                    "the server sent a JOB to a worker that serves requests; this worker is "
                    "configured for the wrong mode, and the job is being refused"
                )
                self._refuse_job(header, payload)
                return
            self._handle_job(header, payload)
            return

        if header.message_type == protocol.PING:
            self.send(protocol.Frame(protocol.PONG, protocol.UNSOLICITED))
            return

        if header.message_type == protocol.CONTROL:
            self._handle_control(payload)
            return

        if header.message_type == protocol.CALL:
            self._answer_call(header, payload)
            return

        if header.message_type in (protocol.LOG, protocol.METRIC, protocol.EVENT):
            # The server should not send these, but a peer that echoes telemetry back must
            # not derail the loop.
            self.diagnose(
                "ignoring an unexpected " + header.type_name + " from the server"
            )
            return

        raise ProtocolError(
            f"the server sent a {header.type_name}, which a worker cannot serve"
        )

    # ---------------------------------------------------------------- requests

    def _serve(self, header: protocol.Header, payload: bytes) -> None:
        self._request_id = header.request_id
        self._keep_alive = True

        metadata = protocol.decode_json(payload, "a REQUEST header")
        body = self._read_body(header.request_id, int(metadata.get("body_len", 0)))

        request = Request(
            method=str(metadata.get("method", "GET")),
            uri=str(metadata.get("uri", "/")),
            protocol=str(metadata.get("protocol", "HTTP/1.1")),
            headers=_pairs(metadata.get("headers")),
            remote_addr=str(metadata.get("remote_addr", "")),
            script_filename=str(metadata.get("script_filename", "")),
            https=metadata.get("https") is True,
            body=body,
        )

        started = time.perf_counter()
        app_status = 0

        try:
            if request.is_grpc and self._grpc_handler is not None:
                result = self._grpc_handler(request, self)
            elif self._http_handler is not None:
                result = self._http_handler(request, self)
            else:
                # A gRPC call with no gRPC handler is UNIMPLEMENTED rather than a 500: a
                # method the application does not implement is one it is entitled to refuse.
                result = Response.text("Not Implemented\n", 501)
            response = _normalise(result)
        except Exception as error:  # noqa: BLE001 — a handler may raise anything
            self.diagnose(
                f"uncaught {type(error).__name__} while serving {request.uri}: {error}"
            )
            response = Response.text("Internal Server Error\n", 500)
            app_status = 1

        duration_us = int((time.perf_counter() - started) * 1_000_000)
        self._respond(response, app_status, duration_us)

    def _read_body(self, request_id: int, body_len: int) -> bytes:
        """Read the request body from the BODY frames that follow a REQUEST.

        Stops at `body_len` bytes, because the declared length is authoritative. A frame
        flagged LAST that arrives early means the sender and this reader disagree about the
        length, which is worth failing on rather than papering over: the rest of the stream
        cannot be trusted after that.
        """
        if body_len == 0:
            return b""

        chunks: list[bytes] = []
        received = 0
        while received < body_len:
            header = self.read_header()
            if header is None:
                raise ProtocolError(
                    f"the server closed the pipe after {received} of {body_len} body bytes"
                )

            payload = self.read_payload(header)

            if header.message_type != protocol.BODY:
                raise ProtocolError(
                    "expected a BODY while reading the request body, got a "
                    + header.type_name
                )

            if header.request_id != request_id:
                raise ProtocolError(
                    f"a BODY for request {header.request_id} arrived while reading request "
                    f"{request_id}"
                )

            chunks.append(payload)
            received += len(payload)

            if header.is_last and received != body_len:
                raise ProtocolError(
                    f"the last BODY frame arrived after {received} of {body_len} declared bytes"
                )

        return b"".join(chunks)

    def _respond(self, response: Response, app_status: int, duration_us: int) -> None:
        """Write a response: head, body chunks, then the terminal status."""
        body = response.body
        self.send_json(
            protocol.RESPONSE,
            self._request_id,
            {
                "status": response.status,
                "headers": [list(pair) for pair in iter_headers(response.headers)],
                "body_len": len(body),
            },
        )

        if body:
            last = (len(body) - 1) // protocol.MAX_BODY_CHUNK
            for index in range(last + 1):
                start = index * protocol.MAX_BODY_CHUNK
                chunk = body[start : start + protocol.MAX_BODY_CHUNK]
                self.send(
                    protocol.Frame(
                        protocol.BODY,
                        self._request_id,
                        chunk,
                        protocol.FLAG_LAST if index == last else 0,
                    )
                )

        self.send_json(
            protocol.END,
            self._request_id,
            {
                "app_status": app_status,
                "peak_memory_bytes": self.peak_memory_bytes(),
                "duration_us": duration_us,
                "keep_alive": self._keep_alive,
            },
        )

        if not self._keep_alive:
            self._stopping = True

    def peak_memory_bytes(self) -> int:
        """The peak memory this worker reached, in bytes, since the last call.

        **Per request rather than per process**, because the two behave differently and
        the difference decides how often a worker is recycled. See the module docstring for
        why Python's answer is not PHP's.
        """
        if self._track_memory:
            import tracemalloc

            _, peak = tracemalloc.get_traced_memory()
            # Re-baseline for the next request, the way PHP's
            # `memory_reset_peak_usage()` does: a figure that never falls turns a leak
            # detector into a per-request application boot.
            tracemalloc.reset_peak()
            return int(peak)

        return _process_rss_bytes()

    def _refuse_request(self, header: protocol.Header) -> None:
        """Answer a REQUEST a consumer cannot serve, rather than ignoring it.

        The server's request path has a deadline and its answer to a missed one is to kill
        the worker, so silence would turn a one-line configuration mistake into a worker
        killed once a minute.
        """
        self._request_id = header.request_id
        self._respond(
            Response.text("Not Implemented: this worker is a consumer\n", 501), 1, 0
        )

    # -------------------------------------------------------------------- jobs

    def _handle_job(self, header: protocol.Header, payload: bytes) -> None:
        metadata = protocol.decode_json(payload, "a JOB header")
        body = self._read_body(header.request_id, int(metadata.get("body_len", 0)))

        job = Job(
            pipeline=str(metadata.get("pipeline", "")),
            id=str(metadata.get("id", "")),
            task=str(metadata.get("task", "")),
            headers=_pairs(metadata.get("headers")),
            attempts=int(metadata.get("attempts", 0)),
            body=body,
        )

        started = time.perf_counter()
        outcome = "ack"
        reason: str | None = None

        try:
            assert self._job_handler is not None
            result = self._job_handler(job, self)
            if result is False:
                outcome = "requeue"
            elif isinstance(result, str):
                outcome = "reject"
                reason = result
        except Exception as error:  # noqa: BLE001 — a crashing consumer must not lose work
            self.diagnose(
                f"uncaught {type(error).__name__} while consuming job {job.id}: {error}"
            )
            outcome = "requeue"

        duration_us = int((time.perf_counter() - started) * 1_000_000)
        self._ack(header.request_id, outcome, reason, duration_us)

    def _refuse_job(self, header: protocol.Header, payload: bytes) -> None:
        """Refuse a JOB a request worker was sent, so the queue does not lose it."""
        metadata = protocol.decode_json(payload, "a JOB header")
        body_len = int(metadata.get("body_len", 0))
        if body_len:
            # The body still has to be drained, or the next frame is read as a body chunk.
            self._read_body(header.request_id, body_len)
        self._ack(
            header.request_id,
            "requeue",
            "this worker serves requests, not jobs",
            0,
        )

    def _ack(
        self,
        request_id: int,
        outcome: str,
        reason: str | None,
        duration_us: int,
    ) -> None:
        payload: dict[str, Any] = {
            # Flattened, not nested: the frame already says it is an ACK, and the server
            # reads `{"outcome":"ack","peak_memory_bytes":…}`.
            "outcome": outcome,
            "peak_memory_bytes": self.peak_memory_bytes(),
            "keep_alive": self._keep_alive,
            "duration_us": duration_us,
        }
        if reason is not None:
            payload["reason"] = reason
        self.send_json(protocol.ACK, request_id, payload)

    # ----------------------------------------------------------------- telemetry

    def emit_log(self, message: str, level: str = "info") -> None:
        """Emit a log record without touching the protocol stream."""
        self.send_json(
            protocol.LOG,
            protocol.UNSOLICITED,
            {"level": level, "message": message, "request_id": self._request_id},
        )

    def emit_metric(self, name: str, value: float, unit: str = "") -> None:
        """Emit a metric sample.

        The name must be a Prometheus metric name — ``[a-zA-Z_][a-zA-Z0-9_]*`` — because
        that is what it becomes, and the deployment must declare it in
        ``[metrics.collect]`` before the server accepts it.
        """
        self.send_json(
            protocol.METRIC,
            protocol.UNSOLICITED,
            {"name": name, "value": float(value), "unit": unit},
        )

    # --------------------------------------------------------------------- calls

    def on_call(self, handler: Callable[[dict[str, Any]], Any]) -> None:
        """Register what answers a CALL the server makes while a request is in flight.

        The callable receives the decoded call as a dict with ``name`` and ``params``, and
        may return:

        * a value, which becomes the RETURN's result;
        * ``{"ok": False, "error": "why"}``, to refuse it with a definite answer;
        * ``{"ok": False, "unknown": True, "error": "why"}``, when the call **may have
          taken effect** — a lock acquired before the reply was lost. The server treats
          that differently from a refusal, because a caller that retries a refusal is safe
          and a caller that retries an unknown state duplicates a side effect.
        """
        self._call_handler = handler

    def _answer_call(self, header: protocol.Header, payload: bytes) -> None:
        """Answer a CALL, in every case.

        A missing answer is indistinguishable from a dead worker until a deadline turns it
        into one, so a refusal is written rather than nothing.
        """
        call = protocol.decode_json(payload, "a CALL")

        try:
            if self._call_handler is None:
                self.send_json(
                    protocol.RETURN,
                    header.request_id,
                    {
                        "ok": False,
                        "error": "this worker registered no call handler, so "
                        + str(call.get("name", "the call"))
                        + " cannot be served",
                    },
                )
                return

            result = self._call_handler(call)

            # A handler that wants to say anything other than "here is the value" returns a
            # dict with `ok` in it. Checking for the key rather than for a shape means a
            # result that happens to be a dict of data is still sent as a result.
            if isinstance(result, dict) and "ok" in result:
                self.send_json(
                    protocol.RETURN,
                    header.request_id,
                    {
                        "ok": bool(result["ok"]),
                        "error": result.get("error"),
                        "unknown": bool(result.get("unknown", False)),
                        "result": result.get("result"),
                    },
                )
                return

            self.send_json(
                protocol.RETURN, header.request_id, {"ok": True, "result": result}
            )
        except Exception as error:  # noqa: BLE001 — the loop is mid-request and owes an answer
            self.diagnose(f"a call handler raised {type(error).__name__}: {error}")
            self.send_json(
                protocol.RETURN,
                header.request_id,
                {
                    "ok": False,
                    "error": f"the call handler raised {type(error).__name__}",
                    # Deliberately *not* unknown: the handler raised before returning, and
                    # whether its side effects happened is the handler's business to report.
                    "unknown": False,
                },
            )

    def call(self, name: str, params: dict[str, Any] | None = None) -> Any:
        """Ask the server for something, while a request is in flight.

        Only legal during a request: between requests the server's exchange thread is
        parked on its work channel and nothing drains the pipe, so a call made while idle
        would block until the request deadline killed the worker.
        """
        if self._request_id == protocol.UNSOLICITED:
            raise ProtocolError(
                "a call may only be made while a request is in flight; between requests "
                "nothing on the server is reading this channel"
            )

        call_id = self._next_call_id
        self._next_call_id = call_id - 1 or 0xFFFFFFFF

        self.send_json(
            protocol.CALL,
            call_id,
            {"name": name, "params": params if params is not None else {}},
        )

        while True:
            header = self.read_header()
            if header is None:
                raise ProtocolError("the server closed the channel while a call was in flight")
            payload = self.read_payload(header)

            if header.message_type == protocol.RETURN:
                if header.request_id != call_id:
                    raise ProtocolError(
                        f"a RETURN for call {header.request_id} arrived while waiting for "
                        f"call {call_id}"
                    )
                answer = protocol.decode_json(payload, "a RETURN")
                if not answer.get("ok"):
                    self._last_call_known = not answer.get("unknown", False)
                    raise ProtocolError(
                        str(answer.get("error", "the call was refused")), fatal=False
                    )
                self._last_call_known = True
                return answer.get("result")

            # Anything else arriving mid-call is not part of this exchange. Telemetry is
            # absorbed; a correlated frame is a protocol error, because it means two
            # exchanges are in flight on a channel that carries one.
            if header.message_type in (protocol.LOG, protocol.METRIC, protocol.EVENT):
                continue
            raise ProtocolError(
                "while waiting for a RETURN the server sent a "
                + header.type_name
                + ", which this worker cannot serve"
            )

    @property
    def last_call_was_known(self) -> bool:
        """Whether the last failed call's effect is known to have happened or not.

        Separate from the return value because "the call was refused" and "the call may
        have taken effect" are not the same answer, and a caller that retries the second
        one duplicates a side effect.
        """
        return self._last_call_known

    # -------------------------------------------------------------------- control

    def _handle_control(self, payload: bytes) -> None:
        control = protocol.decode_json(payload, "a CONTROL")
        kind = control.get("kind") if isinstance(control.get("kind"), str) else ""

        self.send(protocol.Frame(protocol.CONTROL_ACK, protocol.UNSOLICITED))

        if kind in ("drain", "shutdown"):
            # We are idle between requests, so this is immediate.
            self._stopping = True
        elif kind == "report":
            self.emit_metric(
                "worker_peak_memory_bytes", float(self.peak_memory_bytes()), "bytes"
            )
        elif kind == "reload":
            self.diagnose("reload acknowledged: this worker has no separate bootstrap")
        else:
            self.diagnose(f"unknown control kind {kind!r} acknowledged and ignored")

    def recycle(self) -> None:
        """Ask to be replaced after the current unit of work.

        Reported as ``keep_alive = false`` in the terminal frame, which is also how a
        worker that handled a fatal error asks to be replaced.
        """
        self._keep_alive = False

    def on_grpc(self, handler: Callable[[Request, "Worker"], Any]) -> None:
        """Register the handler for gRPC calls, which arrive as ordinary REQUESTs."""
        self._grpc_handler = handler


def _pairs(value: Any) -> list[tuple[str, str]]:
    """A ``[[name, value], …]`` field, filtered to the shape it claims to be.

    A pair that is not two elements is dropped, which is the case a malformed frame
    actually produces. A non-string member is cast rather than dropped: a producer that
    wrote ``3`` meant ``"3"``, and failing over it would be a worse answer.
    """
    if not isinstance(value, list):
        return []
    pairs = []
    for pair in value:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        pairs.append((str(pair[0]), str(pair[1])))
    return pairs


def _normalise(result: Any) -> Response:
    """Turn whatever the handler returned into a :class:`Response`.

    The tuple form is WSGI's ``(status, headers, body)``, because that is what a framework
    adapter naturally produces and converting at the boundary is cheaper than making every
    adapter build the SDK's type.
    """
    if isinstance(result, Response):
        return result

    if isinstance(result, tuple) and len(result) == 3:
        status, headers, body = result
        payload = body if isinstance(body, bytes) else str(body).encode()
        return Response(int(status), list(iter_headers(headers)), payload)

    if isinstance(result, bytes):
        return Response(200, [("Content-Type", "application/octet-stream")], result)

    if isinstance(result, str):
        return Response.html(result)

    if result is None:
        return Response.html("")

    raise ProtocolError(
        "a worker handler must return a Response, bytes, str, a (status, headers, body) "
        f"tuple or None, got {type(result).__name__}"
    )


def iter_chunks(data: bytes, size: int = protocol.MAX_BODY_CHUNK) -> Iterable[bytes]:
    """Split a body into frames' worth, for a caller that needs the boundaries itself."""
    for start in range(0, len(data), size):
        yield data[start : start + size]
