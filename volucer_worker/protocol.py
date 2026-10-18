"""The volucer worker protocol, as a Python client implements it.

The specification is `docs/worker-protocol.md` in `sailantis/volucer`; this module is
the other end of it. The counterpart in PHP is `sailantis/volucer-php-sdk`, and the two
implement the same document rather than each other.

Everything here is deliberately dependency-free. A worker SDK is loaded before an
application's own dependencies are known to work, and a protocol codec that needs a
package is a codec that can fail for a reason unrelated to the protocol.
"""

from __future__ import annotations

import json
import struct
from typing import Any

#: The only protocol version that exists so far.
PROTOCOL_VERSION = 1

#: The size of a frame header on the wire.
HEADER_LEN = 16

#: Refuse absurd payloads before allocating, matching `MAX_PAYLOAD_LEN` in Rust.
MAX_PAYLOAD_LEN = 64 * 1024 * 1024

#: The largest body slice sent in one frame, matching `MAX_BODY_CHUNK` in Rust.
MAX_BODY_CHUNK = 64 * 1024

#: The request id reserved for unsolicited frames.
UNSOLICITED = 0

#: The message type table. The numbering *is* the protocol: never reorder, never reuse.
HELLO = 1
HELLO_ACK = 2
REQUEST = 3
RESPONSE = 4
BODY = 5
EVENT = 6
LOG = 7
METRIC = 8
CONTROL = 9
CONTROL_ACK = 10
END = 11
PING = 12
PONG = 13
ERROR = 14
CALL = 15
RETURN = 16
JOB = 17
ACK = 18

#: Bits in the `flags` field.
FLAG_COMPRESSED = 0x0001
FLAG_CHUNKED = 0x0002
FLAG_LAST = 0x0004
FLAG_ERROR = 0x0008
FLAG_NO_CHECKSUM = 0x0010

_NAMES = {
    HELLO: "HELLO",
    HELLO_ACK: "HELLO_ACK",
    REQUEST: "REQUEST",
    RESPONSE: "RESPONSE",
    BODY: "BODY",
    EVENT: "EVENT",
    LOG: "LOG",
    METRIC: "METRIC",
    CONTROL: "CONTROL",
    CONTROL_ACK: "CONTROL_ACK",
    END: "END",
    PING: "PING",
    PONG: "PONG",
    ERROR: "ERROR",
    CALL: "CALL",
    RETURN: "RETURN",
    JOB: "JOB",
    ACK: "ACK",
}


def type_name(message_type: int) -> str:
    """The wire name of a type, for diagnostics.

    Mirrors `MessageType::name()` in Rust so both ends describe a frame the same way.
    """
    return _NAMES.get(message_type, f"unknown ({message_type})")


class ProtocolError(Exception):
    """A framing or message failure.

    The names are the protocol's own: a *framing* failure means the reader can no longer
    trust its position in the stream and the worker must be discarded, while a *message*
    failure (a wrong request id, a remote error) leaves the stream aligned.
    """

    def __init__(self, message: str, *, fatal: bool = True) -> None:
        super().__init__(message)
        self.fatal = fatal


# --------------------------------------------------------------------- CRC-32C


def _build_crc32c_table() -> tuple[int, ...]:
    """The Castagnoli table, computed once at import.

    A pure-Python table rather than :func:`zlib.crc32`, which is CRC-32B: a different
    polynomial, so using it would make every frame fail its checksum against a correct
    peer. The check value is the standard one —
    ``crc32c(b"123456789") == 0xE3069283`` — and the test suite pins it.
    """
    polynomial = 0x82F63B78
    table = []
    for index in range(256):
        value = index
        for _ in range(8):
            value = (value >> 1) ^ polynomial if value & 1 else value >> 1
        table.append(value)
    return tuple(table)


_CRC32C_TABLE = _build_crc32c_table()


def crc32c(data: bytes) -> int:
    """CRC-32C (Castagnoli) of `data`, as the 32-bit value the header carries."""
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


# ---------------------------------------------------------------------- frames


class Header:
    """A decoded frame header: the 16 bytes that precede a payload.

    A direct transliteration of ``Header::decode`` in
    ``crates/volucer-worker/src/header.rs``. Layout, little-endian::

        offset size field
        0      4    payload_len
        4      1    protocol_version
        5      1    message_type
        6      2    flags
        8      4    request_id
        12     4    crc32c(payload)
    """

    __slots__ = ("payload_len", "message_type", "flags", "request_id", "checksum")

    def __init__(
        self,
        payload_len: int,
        message_type: int,
        flags: int,
        request_id: int,
        checksum: int,
    ) -> None:
        self.payload_len = payload_len
        self.message_type = message_type
        self.flags = flags
        self.request_id = request_id
        self.checksum = checksum

    @classmethod
    def decode(cls, raw: bytes) -> "Header":
        """Parse a header, which must be exactly 16 bytes.

        Exactly, not "at least". A stream that ended mid-header is an error rather than a
        clean close: treating it as one would hide a truncated frame.
        """
        if len(raw) != HEADER_LEN:
            raise ProtocolError(
                f"a frame header is exactly {HEADER_LEN} bytes, got {len(raw)}"
            )

        payload_len, version, message_type, flags, request_id, checksum = struct.unpack(
            "<IBBHII", raw
        )

        if version != PROTOCOL_VERSION:
            raise ProtocolError(
                f"unsupported protocol version {version} (this SDK speaks {PROTOCOL_VERSION})"
            )

        if not HELLO <= message_type <= ACK:
            raise ProtocolError(
                f"unknown message type {message_type} (this SDK defines 1..18)"
            )

        # Refused rather than misread: with no codec negotiated, a "compressed" payload
        # would be handed to json as if it were plain text.
        if flags & FLAG_COMPRESSED:
            raise ProtocolError("refusing a compressed frame: no codec is negotiated")

        return cls(payload_len, message_type, flags, request_id, checksum)

    @property
    def is_last(self) -> bool:
        return bool(self.flags & FLAG_LAST)

    @property
    def declares_no_checksum(self) -> bool:
        """The writer's *claim*, not a permission.

        Whether it may be believed is the reader's decision, and
        :meth:`Worker.read_payload` refuses one the handshake never agreed to.
        """
        return bool(self.flags & FLAG_NO_CHECKSUM)

    @property
    def type_name(self) -> str:
        return type_name(self.message_type)


class Frame:
    """A frame ready to send: a message type, a request id and a payload.

    `checksum` is passed to :meth:`encode` rather than remembered here, because the
    decision belongs to the :class:`Worker` that made the handshake agreement.
    """

    __slots__ = ("message_type", "request_id", "payload", "flags")

    def __init__(
        self,
        message_type: int,
        request_id: int,
        payload: bytes = b"",
        flags: int = 0,
    ) -> None:
        self.message_type = message_type
        self.request_id = request_id
        self.payload = payload
        self.flags = flags

    def encode(self, checksum: bool = True) -> bytes:
        """Serialize to header + payload.

        With the checksum off the field is written as zero and ``FLAG_NO_CHECKSUM`` is
        raised, which is what makes the zero mean "not a checksum" rather than "the
        checksum of an empty payload".
        """
        flags = self.flags if checksum else self.flags | FLAG_NO_CHECKSUM
        return struct.pack(
            "<IBBHII",
            len(self.payload),
            PROTOCOL_VERSION,
            self.message_type,
            flags,
            self.request_id,
            crc32c(self.payload) if checksum else 0,
        ) + self.payload


def json_bytes(value: Any) -> bytes:
    """JSON-encode a payload the way the protocol expects it.

    `separators` carries no spaces because the golden fixtures in both SDKs are written
    against that spelling, and a payload is compared byte for byte by those tests.
    """
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def decode_json(payload: bytes, what: str) -> dict[str, Any]:
    """Decode a JSON payload, turning a bad payload into a protocol error."""
    if not payload:
        return {}
    try:
        decoded = json.loads(payload)
    except ValueError as error:
        raise ProtocolError(f"{what} was not valid JSON: {error}") from None
    if not isinstance(decoded, dict):
        raise ProtocolError(f"{what} must be a JSON object")
    return decoded
