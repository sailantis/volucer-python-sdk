"""The call plane for an application that has no worker channel.

A :class:`~volucer_worker.Worker` reaches ``kv``, ``lock`` and ``jobs`` on the stream that
carries its requests. A FastCGI application has no such stream — it exists for the duration of
one request and can only answer the frame it was handed — so volucer serves the same namespaces
on a socket of its own, ``[rpc]``, which this class dials::

    from volucer_worker import Rpc

    rpc = Rpc.connect()

    rpc.kv("cache").set("greeting", "hello")
    lock_id = rpc.lock().acquire("pdf:create")
    rpc.queue("mail").push({"to": "a@b"})

Nothing here is configured, and that is the point. The address and the backend's identity are
**in the request's environment**, put there by volucer — ``VOLUCER_RPC`` and
``VOLUCER_RPC_TOKEN`` as CGI parameters, or ``X-Volucer-Rpc`` and ``X-Volucer-Rpc-Token`` as
headers for an ``upstream`` origin. One deployment states the address once, in one file, and both
sides read it; an application never has a second copy to keep in step.

A backend whose ``services`` list is empty is handed neither, which is why :meth:`Rpc.connect`
does not raise when they are missing: the application gets a plane that refuses every call with
a message naming the missing variable. A request that never touches a store therefore still
works, and the one that does gets an error pointing at the deployment rather than at this class.

The wire is the worker protocol's, minus the handshake
-------------------------------------------------------

Same :class:`~volucer_worker.protocol.Header`, same ``CALL``/``RETURN``, same CRC-32C. What a
worker establishes once in ``HELLO``/``HELLO_ACK`` — version, checksum policy, process identity
— would be paid for on every request by a CGI process, so instead **each call carries the
token**. That also means a grant withdrawn by a reload takes effect on the next call rather than
at the next reconnect.

One consequence worth knowing: there is no ``HELLO``, so there is no offer to skip the checksum,
so every frame this class writes is checksummed.

The deadline is here, unlike a worker's channel
-----------------------------------------------

``Worker.call()`` blocks with no timeout of its own, because the protocol guarantees an answer.
This class reads a **socket**, so a deadline is both possible and necessary: a call whose answer
was lost to a network hiccup, or a server that stopped reading, would otherwise hang until the
runtime killed the request — and that failure names the wrong thing.

The default matches ``[rpc].call_timeout_seconds`` in the default deployment, and **a call that
legitimately takes longer needs a longer deadline**: ``Lock.acquire(..., wait=...)`` may wait, so
a caller asking for more than the timeout must say so — ``Rpc.connect(timeout=30.0)``.
"""

from __future__ import annotations

import os
import socket as _socket
import time
from typing import Any

from . import protocol
from .protocol import ProtocolError
from .worker import unix_path

#: The CGI parameter (or upstream header) carrying the call plane's address.
ENDPOINT_PARAM = "VOLUCER_RPC"

#: The CGI parameter (or upstream header) carrying this backend's token.
TOKEN_PARAM = "VOLUCER_RPC_TOKEN"

#: The header form an ``upstream`` origin is given, as Python's CGI-style environment spells it.
ENDPOINT_HEADER = "HTTP_X_VOLUCER_RPC"

#: The header form of the token.
TOKEN_HEADER = "HTTP_X_VOLUCER_RPC_TOKEN"

#: The default deadline for one call, in seconds.
#:
#: The same number ``[rpc].call_timeout_seconds`` defaults to, so a deployment that configured
#: nothing behaves the same on both sides.
DEFAULT_TIMEOUT = 10.0

#: How long to wait for a connection, in seconds.
CONNECT_TIMEOUT = 10.0

#: The first call id. Ids descend from the top of the range, matching the SDKs and the server.
_FIRST_CALL_ID = 0xFFFFFFFF


def _environment(*names: str, environ: Any = None) -> str | None:
    """The first of ``names`` that holds a non-blank value.

    ``environ`` is the mapping searched, and it defaults to :data:`os.environ`. **A WSGI application
    must pass its own** — see :meth:`Rpc.connect`, where the reason is spelled out.

    Blank rather than merely absent, because ``VOLUCER_RPC=`` is what a shell produces while
    somebody is debugging, and an empty value is not an address to dial.
    """
    source = os.environ if environ is None else environ
    for name in names:
        value = source.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _address_of(text: str) -> tuple[str | None, tuple[str, int] | None]:
    """Split an address into a unix path or a ``(host, port)`` pair.

    One function so the two forms are decided in one place, and so a malformed address is an
    error at connect rather than an ``int()`` failure a frame later.
    """
    path = unix_path(text)
    if path is not None:
        return path, None
    host, _, port = text.rpartition(":")
    if not port.isdigit():
        raise ValueError(f"{text!r} is neither a unix: path nor a host:port address")
    return None, (host or "127.0.0.1", int(port))


class Rpc:
    """The ``[rpc]`` call plane, as a client.

    Built by :meth:`connect` from the request's environment, or constructed directly with a
    connected stream — which is what the tests do, because every interesting case here is about
    what the client does with a particular answer.
    """

    def __init__(
        self,
        stream: Any,
        token: str = "",
        address: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if timeout <= 0:
            raise ValueError(
                "a call plane needs a positive timeout; 0 would abandon every call"
            )
        self._stream = stream
        self._token = token
        self._address = address
        self._timeout = timeout
        self._next_id = _FIRST_CALL_ID
        self._last_error: str | None = None
        self._last_unknown = False
        # Why there is no stream **at construction time**, kept apart from `_last_error` because
        # that one describes the last *call* and is cleared by the next one. A plane built without
        # an address would otherwise report "the call plane ... could not be reached" — or nothing
        # at all — instead of naming the variable the deployment did not set.
        self._unavailable: str | None = None

    # ------------------------------------------------------------------ building

    @classmethod
    def connect(
        cls,
        address: str | None = None,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        environ: Any = None,
    ) -> Rpc:
        """Build a plane from the request's environment, or explicitly.

        Reads ``VOLUCER_RPC``/``VOLUCER_RPC_TOKEN``, falling back to the ``X-Volucer-Rpc*``
        header form an ``upstream`` backend is handed. A ``token`` or ``address`` passed here
        wins, which is what a test or a script started with ``VOLUCER_RPC=…`` needs.

        **``environ`` is the WSGI request's environment, and a WSGI application should pass it.**
        volucer puts the address in the *CGI request*, which is a different thing from the
        process's own environment: under ``spawn_cgi`` the values arrive in ``environ``, and the
        server only exports ``VOLUCER_FCGI_ADDRESS`` to the process itself. Reading
        :data:`os.environ` would therefore work for a script a deployment started by hand and fail
        for every real WSGI request, with a message blaming the deployment. A worker or a CLI
        script omits it and gets :data:`os.environ`, which is correct for both.

        Returns a plane even when the deployment granted nothing: the *call* is what fails, with
        the missing variable named. A request that never touches a store must still work, and the
        one that does deserves a message about the deployment rather than an exception from a
        constructor it did not think it was calling.

        **Nothing is dialled here.** The connection is made by the first call, so a request that
        never touches a store pays for neither a socket nor a failed connect — and a deployment
        whose call plane is briefly unavailable does not fail a request that would not have used
        it.
        """
        resolved = (
            address
            if address is not None
            else _environment(ENDPOINT_PARAM, ENDPOINT_HEADER, environ=environ)
        )
        resolved_token = (
            token
            if token is not None
            else (_environment(TOKEN_PARAM, TOKEN_HEADER, environ=environ) or "")
        )

        plane = cls(None, resolved_token, resolved, timeout)
        if resolved is None:
            where = "its environment" if environ is None else "the WSGI request environment"
            plane._unavailable = (
                f"this application was handed no call plane: {ENDPOINT_PARAM} is not in {where}, "
                f"which means the backend's `services` list is empty or the deployment has no "
                f"[rpc] listener"
            )
        return plane

    @staticmethod
    def _dial(address: str) -> Any:
        """Connect, or ``None`` — a connection failure is a call failure, not a constructor one.

        Bounded: a dial to an address that is up but slow holds a request open, and the whole
        reason this client has a deadline is that a hanging request names the wrong failure. The
        connect timeout is the same one ``Worker.connect`` uses.
        """
        try:
            path, tcp = _address_of(address)
            if path is not None:
                connection = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                try:
                    connection.settimeout(CONNECT_TIMEOUT)
                    connection.connect(path)
                except OSError:
                    connection.close()
                    raise
            else:
                assert tcp is not None
                connection = _socket.create_connection(tcp, timeout=CONNECT_TIMEOUT)
            # **The socket is kept, and the deadline is applied to it.** A `makefile()` result is
            # a `SocketIO`, which has neither `settimeout` nor `raw` — so an earlier version of
            # this class set the deadline on a wrapper that silently ignored it and then blocked
            # for ever on a server that had stopped answering. The whole reason this transport has
            # a deadline is that a hanging request names the wrong failure.
            connection.settimeout(None)
            return connection
        except (OSError, ValueError):
            return None

    # ------------------------------------------------------------------ the seam

    @property
    def address(self) -> str | None:
        """Which call plane this is, for a diagnostic."""
        return self._address

    @property
    def last_error(self) -> str | None:
        """Why the last :meth:`call` did not return a result, or ``None`` if it did.

        The distinction a cache miss must not collapse: ``kv.get`` on a missing key returns
        ``None`` and *succeeded*, and a caller that could not tell the two apart would treat a
        miss as a failure.
        """
        return self._last_error

    @property
    def last_call_was_unknown(self) -> bool:
        """Whether the last failed call may have taken effect.

        ``False`` means nothing was sent or the server answered a refusal, so retrying is safe.
        ``True`` means the call was written and the connection broke before the answer arrived,
        so retrying may duplicate a side effect — the same distinction ``Worker`` draws.
        """
        return self._last_unknown

    def close(self) -> None:
        """Drop the socket. The next call reconnects."""
        stream, self._stream = self._stream, None
        if stream is not None:
            try:
                stream.close()
            except (OSError, ValueError):
                pass

    def __enter__(self) -> Rpc:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------ calling

    def call(self, name: str, params: dict[str, Any] | None = None) -> Any:
        """One round trip. Returns the result, or ``None`` with :attr:`last_error` set."""
        self._last_error = None
        self._last_unknown = False

        if not self._ensure_stream():
            return None

        call_id = self._take_id()
        body: dict[str, Any] = {"name": name, "params": params or {}}
        if self._token:
            # Omitted rather than sent empty when there is no token. The server tells the two
            # apart on purpose: "carried no token" is a deployment that granted this backend
            # nothing, and "carried a token no backend was issued" is an application reading the
            # wrong variable.
            body["token"] = self._token

        try:
            self._write(protocol.Frame(protocol.CALL, call_id, protocol.json_bytes(body)))
        except OSError as error:
            # The bytes may or may not have reached the server: a write that failed part-way is
            # exactly the undecided case this flag exists for.
            self._last_unknown = True
            self._last_error = f'call "{name}" could not be written: {error}'
            self.close()
            return None

        return self._await_return(call_id, name)

    def _take_id(self) -> int:
        call_id = self._next_id
        self._next_id = call_id - 1 or 0xFFFFFFFF
        return call_id

    def _ensure_stream(self) -> bool:
        """The stream, dialling if there is not one yet.

        One path for both cases: a plane that has never connected, and a plane whose connection a
        previous call dropped. What must not be retried is a deployment that handed over no
        address at all — that is `_unavailable`, decided once, and re-dialling nothing would only
        replace its message with a vaguer one.
        """
        if self._stream is not None:
            return True
        if self._unavailable is not None:
            self._last_error = self._unavailable
            return False
        if self._address is None:
            self._last_error = "this call plane has no stream and no address to dial"
            return False
        stream = self._dial(self._address)
        if stream is None:
            self._last_error = f"the call plane at {self._address} could not be reached"
            return False
        self._stream = stream
        return True

    def _write(self, frame: protocol.Frame) -> None:
        assert self._stream is not None
        # `sendall` rather than `write`: the stream may be a socket, and a socket's `write` is not
        # guaranteed to consume the whole buffer.
        send = getattr(self._stream, "sendall", None)
        if send is not None:
            send(frame.encode())
        else:
            self._stream.write(frame.encode())

    def _await_return(self, call_id: int, name: str) -> Any:
        deadline = time.monotonic() + self._timeout
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _TimedOut
                self._set_read_deadline(remaining)
                header = self._read_header()
                if header is None:
                    # The server closed the connection after the call was written, so whether it
                    # was served is not knowable from here.
                    self._last_unknown = True
                    self._last_error = (
                        f'call "{name}" was written but the server closed the connection '
                        f"before answering"
                    )
                    self.close()
                    return None
                payload = self._read_payload(header)
                if header.message_type != protocol.RETURN:
                    # Anything else is not part of this exchange. A worker absorbs telemetry here;
                    # this transport never sees unsolicited frames, so a correlated one is a
                    # protocol error rather than something to skip.
                    raise ProtocolError(
                        f"while waiting for a RETURN the server sent a {header.type_name}"
                    )
                if header.request_id != call_id:
                    raise ProtocolError(
                        f"a RETURN for call {header.request_id} arrived while waiting for "
                        f"call {call_id}"
                    )
                answer = protocol.decode_json(payload, "a RETURN")
                if not answer.get("ok"):
                    self._last_unknown = bool(answer.get("unknown", False))
                    self._last_error = str(answer.get("error", "the call was refused"))
                    return None
                return answer.get("result")
        except _TimedOut:
            self._last_unknown = True
            self._last_error = (
                f'call "{name}" did not answer within {self._timeout}s; it may have been served, '
                f"so retrying is not necessarily safe (a longer deadline: Rpc.connect(timeout=…))"
            )
            self.close()
            return None
        # `socket.timeout` **is** an `OSError`, so this has to come after the explicit timeout
        # above and be reached only for a `TimeoutError` raised by `settimeout` on a read — which
        # is the deadline expiring, not a broken connection.
        except TimeoutError:
            self._last_unknown = True
            self._last_error = (
                f'call "{name}" did not answer within {self._timeout}s; it may have been served, '
                f"so retrying is not necessarily safe (a longer deadline: Rpc.connect(timeout=…))"
            )
            self.close()
            return None
        except (OSError, ProtocolError) as error:
            self._last_unknown = True
            self._last_error = f'call "{name}" failed: {error}'
            self.close()
            return None

    def _set_read_deadline(self, remaining: float) -> None:
        """Apply the remaining deadline to the socket.

        A plane built on something without ``settimeout`` — an in-memory stream in a test — has no
        deadline, and that is honest: the deadline is a property of the transport. What must not
        happen is the earlier behaviour, where a *socket* was wrapped in something that silently
        swallowed the call and the read then blocked for ever.
        """
        setter = getattr(self._stream, "settimeout", None)
        if setter is not None:
            setter(remaining)

    def _read_header(self) -> protocol.Header | None:
        raw = self._read_exactly(protocol.HEADER_LEN, allow_eof=True)
        if raw is None:
            return None
        return protocol.Header.decode(raw)

    def _read_payload(self, header: protocol.Header) -> bytes:
        if header.payload_len == 0:
            return b""
        payload = self._read_exactly(header.payload_len)
        assert payload is not None
        if not header.declares_no_checksum and protocol.crc32c(payload) != header.checksum:
            raise ProtocolError("a RETURN arrived with a checksum that does not match")
        return payload

    def _read_exactly(self, length: int, allow_eof: bool = False) -> bytes | None:
        assert self._stream is not None
        chunks: list[bytes] = []
        got = 0
        while got < length:
            chunk = self._read_some(length - got)
            if not chunk:
                if allow_eof and got == 0:
                    return None
                raise ProtocolError(f"the stream ended after {got} of {length} bytes")
            chunks.append(chunk)
            got += len(chunk)
        return b"".join(chunks)

    def _read_some(self, length: int) -> bytes:
        """Up to ``length`` bytes, from a socket or from a file-like object."""
        receiver = getattr(self._stream, "recv", None)
        if receiver is not None:
            return receiver(length)
        return self._stream.read(length)

    # ------------------------------------------------------------------ storage

    def kv(self, storage: str = "default") -> Kv:
        """A handle to a key-value storage."""
        return Kv(self, storage)

    def lock(self) -> Lock:
        """A handle to the resource locks."""
        return Lock(self)

    def queue(self, pipeline: str) -> Queue:
        """A handle to a queue, from the producer's side."""
        return Queue(self, pipeline)


class _TimedOut(Exception):
    """Internal: the deadline expired. Never escapes this module."""


# --------------------------------------------------------------------- base64


def _b64(value: bytes) -> str:
    import base64

    return base64.b64encode(value).decode("ascii")


def _unb64(text: str) -> bytes:
    import base64

    try:
        return base64.b64decode(text, validate=True)
    except Exception:  # noqa: BLE001 — b64decode raises binascii.Error, a ValueError subclass
        raise ProtocolError("the server sent a value that is not valid base64") from None


class Kv:
    """One key-value storage, reached through a call plane.

    The shape is PSR-16's, transposed to Python: a mapping-flavoured API where a missing key is
    ``None`` rather than an exception, and where a *failure* is an exception, because those are
    different things and a cache that returned ``None`` for a broken server would be a cache that
    silently stopped caching.

    **Keys and values are bytes on the wire and bytes in this class.** The server holds opaque
    bytes and never sees an application's objects, which is what makes a serializer possible at
    all — and the default serializer is JSON, so ``set("k", {"a": 1})`` returns the dict it was
    given.
    """

    def __init__(self, plane: Rpc, storage: str = "default") -> None:
        self._plane = plane
        self._storage = storage
        self._encode = lambda value: protocol.json_bytes(value)
        self._decode = lambda raw: _decode_value(raw)

    @property
    def storage(self) -> str:
        """The storage name this handle reaches."""
        return self._storage

    def serializer(self, encode: Any, decode: Any) -> Kv:
        """Replace the JSON serializer with a pair of callables.

        The server never sees a difference: it stores whatever bytes ``encode`` returns.
        """
        self._encode = encode
        self._decode = decode
        return self

    def get(self, key: Any) -> Any:
        """The value, or ``None``. A *failure* raises, so a miss and a broken server look different."""
        result = self._ask("get", {"key": _key(key)})
        encoded = result.get("value")
        if encoded is None:
            return None
        return self._decode(_unb64(encoded))

    def set(self, key: Any, value: Any, ttl: int | None = None) -> bool:
        """Write a value, optionally with a TTL in **seconds**."""
        params: dict[str, Any] = {
            "key": _key(key),
            "value": _b64(self._encode(value)),
        }
        if ttl is not None:
            params["ttl"] = int(ttl)
        return bool(self._ask("set", params).get("stored"))

    def set_if_absent(self, key: Any, value: Any, ttl: int | None = None) -> bool:
        """Write **only if the key does not exist**, atomically on the storage side."""
        params: dict[str, Any] = {
            "key": _key(key),
            "value": _b64(self._encode(value)),
        }
        if ttl is not None:
            params["ttl"] = int(ttl)
        return bool(self._ask("setIfAbsent", params).get("stored"))

    def has(self, key: Any) -> bool:
        """Whether the key exists."""
        return bool(self._ask("has", {"key": _key(key)}).get("present"))

    def delete(self, key: Any) -> bool:
        """Remove a key, answering whether it was there."""
        return bool(self._ask("delete", {"key": _key(key)}).get("deleted"))

    def ttl(self, key: Any) -> int | str:
        """The remaining TTL: a number of seconds, or ``"missing"`` / ``"forever"``.

        A word rather than ``-1`` because the three states are not a range, and a caller checking
        ``seconds > 0`` would read "never expires" as "expired".
        """
        return self._ask("ttl", {"key": _key(key)}).get("ttl")

    def clear(self) -> bool:
        """Empty the storage."""
        return bool(self._ask("clear", {}).get("cleared"))

    def get_multiple(self, keys: Any) -> dict[Any, Any]:
        """Several at once. Missing keys are absent from the answer, which is PSsych-16's shape."""
        ordered = list(keys)
        values = self._ask("getMultiple", {"keys": [_key(key) for key in ordered]}).get(
            "values", []
        )
        found: dict[Any, Any] = {}
        for index, key in enumerate(ordered):
            encoded = values[index] if index < len(values) else None
            if encoded is not None:
                found[key] = self._decode(_unb64(encoded))
        return found

    def get_many(self, keys: Any) -> list[Any]:
        """Several at once, keeping the *positions*: ``None`` for each key that is missing."""
        ordered = list(keys)
        values = self._ask("getMultiple", {"keys": [_key(key) for key in ordered]}).get(
            "values", []
        )
        return [
            None
            if index >= len(values) or values[index] is None
            else self._decode(_unb64(values[index]))
            for index in range(len(ordered))
        ]

    def set_multiple(self, values: dict[Any, Any], ttl: int | None = None) -> bool:
        """Store several values at once, all with the same TTL in **seconds**."""
        encoded = {_key(key): _b64(self._encode(value)) for key, value in values.items()}
        params: dict[str, Any] = {"values": encoded}
        if ttl is not None:
            params["ttl"] = int(ttl)
        stored = self._ask("setMultiple", params).get("stored", 0)
        return stored == len(encoded)

    def _ask(self, verb: str, params: dict[str, Any]) -> dict[str, Any]:
        """One call, with a failure raised as an exception naming the storage and the reason."""
        answer = self._plane.call(f"kv.{self._storage}.{verb}", params)
        if self._plane.last_error is not None:
            raise ProtocolError(
                f"kv.{self._storage}.{verb} failed: {self._plane.last_error}",
                fatal=False,
            )
        return answer if isinstance(answer, dict) else {}


class Lock:
    """The resource locks, reached through a call plane.

    A lock is for **collapsing work** — two requests rendering the same PDF — and not for
    protecting an invariant. A lease expires, and an expired lease is not a fencing token: a
    process that was paused past its TTL and then resumed will keep writing. That rule is the
    same one the worker channel's ``Lock`` documents, because it is a property of leases rather
    than of the transport.
    """

    def __init__(self, plane: Rpc) -> None:
        self._plane = plane

    def acquire(
        self,
        resource: str,
        ttl: int = 0,
        wait: int = 0,
    ) -> str | None:
        """Take the lock, returning the id that holds it or ``None`` for contention.

        ``ttl`` and ``wait`` are **microseconds**. Zero ``ttl`` means "held until released" and
        zero ``wait`` means one attempt. Contention is a successful call with
        ``acquired: false``, not an error — nothing failed, the caller just knows it is not in.

        A ``wait`` longer than the plane's timeout needs a longer timeout: the deadline is the
        client's, and a call abandoned mid-wait leaves the lock's state unknown.
        """
        answer = self._ask(
            "acquire",
            {"resource": resource, "ttl": int(ttl), "wait": int(wait)},
        )
        if not answer.get("acquired"):
            return None
        return answer.get("id")

    def try_acquire(self, resource: str, ttl: int = 0) -> str | None:
        """One attempt: the same as ``acquire`` with no wait, named for what it does."""
        return self.acquire(resource, ttl=ttl, wait=0)

    def release(self, resource: str, lock_id: str) -> bool:
        """Release a lock this caller holds."""
        return bool(self._ask("release", {"resource": resource, "id": lock_id}).get("released"))

    def exists(self, resource: str) -> bool:
        """Whether anybody holds it."""
        return bool(self._ask("exists", {"resource": resource}).get("exists"))

    def holders(self, resource: str) -> list[dict[str, Any]]:
        """Who holds it, and until when."""
        return list(self._ask("holders", {"resource": resource}).get("holders", []))

    def update_ttl(self, resource: str, lock_id: str, ttl: int) -> bool:
        """Extend a lease, in **microseconds**."""
        return bool(
            self._ask(
                "update_ttl",
                {"resource": resource, "id": lock_id, "ttl": int(ttl)},
            ).get("updated")
        )

    def _ask(self, verb: str, params: dict[str, Any]) -> dict[str, Any]:
        answer = self._plane.call(f"lock.{verb}", params)
        if self._plane.last_error is not None:
            raise ProtocolError(f"lock.{verb} failed: {self._plane.last_error}", fatal=False)
        return answer if isinstance(answer, dict) else {}


class Queue:
    """One queue pipeline, from the producer's side.

    The payload is the application's own bytes and the server never parses it — the consuming
    side is a worker with a ``JOB`` handler, and what it finds in the body is whatever this side
    put there.
    """

    def __init__(self, plane: Rpc, pipeline: str) -> None:
        self._plane = plane
        self._pipeline = pipeline

    @property
    def pipeline(self) -> str:
        """The pipeline this handle pushes to."""
        return self._pipeline

    def push(
        self,
        task: str,
        payload: Any = None,
        headers: dict[str, str] | None = None,
    ) -> str | None:
        """Enqueue one job, answering its id.

        ``task`` is the name the consuming side dispatches on — for a Python consumer it is
        whatever the application chose, since nothing in the runtime imposes a shape. It is
        **required**: the server refuses a push without one rather than guessing from the
        pipeline, because a queue whose task name was implicit would put every job on a pipeline
        into one handler.

        ``payload`` is JSON-encoded unless it is already ``bytes``, in which case it is sent as
        given — a producer that serialized its own object should not have it re-encoded.
        """
        raw = (
            b""
            if payload is None
            else (payload if isinstance(payload, bytes) else protocol.json_bytes(payload))
        )
        params: dict[str, Any] = {
            "pipeline": self._pipeline,
            "task": task,
            "payload": _b64(raw),
        }
        if headers:
            # Strings only. The server refuses a numeric header value rather than stringifying
            # it, because a header that was meant to be in the payload is a caller's bug.
            params["headers"] = {name: value for name, value in headers.items()}
        answer = self._plane.call("jobs.push", params)
        if self._plane.last_error is not None:
            raise ProtocolError(
                f"jobs.push to {self._pipeline!r} failed: {self._plane.last_error}",
                fatal=False,
            )
        return answer.get("id") if isinstance(answer, dict) else None


def _key(key: Any) -> str:
    """A key as the wire spells it: base64 of its bytes.

    ``str`` is encoded UTF-8 and ``bytes`` is used as given, which is the one rule a caller has
    to know. Everything else is refused here rather than by the server, because a key that is a
    number is a caller's bug and a message from this module names the line.
    """
    if isinstance(key, bytes):
        raw = key
    elif isinstance(key, str):
        raw = key.encode("utf-8")
    else:
        raise TypeError(f"a kv key must be str or bytes, not {type(key).__name__}")
    if not raw:
        raise ValueError(
            "a kv key must not be empty; the server refuses one rather than storing it"
        )
    return _b64(raw)


def _decode_value(raw: bytes) -> Any:
    """The default deserializer: JSON, falling back to bytes.

    A value written by ``set`` is always JSON. A value written by another application through
    the same storage — a PHP worker, say — may be anything, and returning bytes for it is the
    honest answer rather than an exception that would make one storage unusable by two languages.
    """
    import json

    try:
        return json.loads(raw)
    except ValueError:
        return raw
