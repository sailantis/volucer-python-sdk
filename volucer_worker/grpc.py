"""The outcome of one gRPC call.

A gRPC reply is not an HTTP response, and modelling it as one is the mistake this class
exists to prevent. Its **outcome travels in the trailer section** — after the body, not in
the status line — because a streaming reply has to be able to send headers before its
outcome exists. So a handler says what happened by naming a status here, and the server
moves it to where a client reads it (``docs/plugins.md`` §6.6 in `sailantis/volucer`).

``payload`` is the **body**: the raw, length-delimited framed bytes the client receives.
The server never parses or re-frames it, so a handler owns the framing entirely — write the
compression flag and the big-endian length yourself, or let whatever protobuf library you
already use write it. This SDK does not ship an encoder, because doing so would put a
schema in the transport.
"""

from __future__ import annotations

OK = 0
CANCELLED = 1
UNKNOWN = 2
INVALID_ARGUMENT = 3
DEADLINE_EXCEEDED = 4
NOT_FOUND = 5
ALREADY_EXISTS = 6
PERMISSION_DENIED = 7
RESOURCE_EXHAUSTED = 8
FAILED_PRECONDITION = 9
ABORTED = 10
OUT_OF_RANGE = 11
UNIMPLEMENTED = 12
INTERNAL = 13
UNAVAILABLE = 14
DATA_LOSS = 15
UNAUTHENTICATED = 16

_NAMES = {
    OK: "OK",
    CANCELLED: "CANCELLED",
    UNKNOWN: "UNKNOWN",
    INVALID_ARGUMENT: "INVALID_ARGUMENT",
    DEADLINE_EXCEEDED: "DEADLINE_EXCEEDED",
    NOT_FOUND: "NOT_FOUND",
    ALREADY_EXISTS: "ALREADY_EXISTS",
    PERMISSION_DENIED: "PERMISSION_DENIED",
    RESOURCE_EXHAUSTED: "RESOURCE_EXHAUSTED",
    FAILED_PRECONDITION: "FAILED_PRECONDITION",
    ABORTED: "ABORTED",
    OUT_OF_RANGE: "OUT_OF_RANGE",
    UNIMPLEMENTED: "UNIMPLEMENTED",
    INTERNAL: "INTERNAL",
    UNAVAILABLE: "UNAVAILABLE",
    DATA_LOSS: "DATA_LOSS",
    UNAUTHENTICATED: "UNAUTHENTICATED",
}


class GrpcResponse:
    """A gRPC outcome: a status, the framed body, and a reason for a developer."""

    __slots__ = ("status", "payload", "reason", "content_type")

    def __init__(
        self,
        status: int = OK,
        payload: bytes = b"",
        reason: str = "",
        content_type: str = "application/grpc+proto",
    ) -> None:
        self.status = status
        self.payload = payload
        self.reason = reason
        self.content_type = content_type

    @classmethod
    def message(
        cls, payload: bytes, content_type: str = "application/grpc+proto"
    ) -> "GrpcResponse":
        """A successful call carrying `payload` — the framed bytes a client will decode."""
        return cls(OK, payload, "", content_type)

    @classmethod
    def ok(cls, content_type: str = "application/grpc+proto") -> "GrpcResponse":
        """A successful call with no payload."""
        return cls(OK, b"", "", content_type)

    @classmethod
    def fail(cls, status: int, reason: str = "") -> "GrpcResponse":
        """A failed call, with `reason` as the sentence a developer will read.

        `reason` is **not** payload bytes, and there are none: a failed call carries no
        message. The server percent-encodes the sentence into the ``grpc-message`` trailer,
        so it may contain spaces, quotes and newlines.
        """
        return cls(status, b"", reason)

    @property
    def is_ok(self) -> bool:
        return self.status == OK

    @property
    def status_name(self) -> str:
        return _NAMES.get(self.status, "")
