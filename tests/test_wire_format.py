"""The Python side of the protocol freeze.

Every golden value here is the *same literal* that
``crates/volucer-worker/tests/wire_format.rs`` in `sailantis/volucer` and
``tests/WireFormatTest.php`` in `sailantis/volucer-php-sdk` assert. That is the point: if
either implementation drifts, one of the three tests fails, and none can quietly become
"whatever the code currently does".

The fixtures were computed independently of all three implementations, so they are a
transcript of the specification rather than of any one of them.
"""

from __future__ import annotations

import unittest

from volucer_worker import protocol


class Crc32cTest(unittest.TestCase):
    def test_the_standard_check_value(self) -> None:
        self.assertEqual(0xE3069283, protocol.crc32c(b"123456789"))

    def test_empty_input_is_zero(self) -> None:
        self.assertEqual(0, protocol.crc32c(b""))

    def test_the_golden_response_payload(self) -> None:
        self.assertEqual(
            0x7B9F1283,
            protocol.crc32c(b'{"status":200,"headers":[],"body_len":0}'),
        )

    def test_the_golden_end_payload(self) -> None:
        self.assertEqual(
            0xB3E50F77,
            protocol.crc32c(
                b'{"app_status":0,"peak_memory_bytes":0,"duration_us":0,"keep_alive":true}'
            ),
        )

    def test_crc32c_is_not_crc32b(self) -> None:
        # zlib.crc32 is CRC-32B: a different polynomial. Using it would make every frame
        # fail its checksum against a correct peer.
        import zlib

        self.assertNotEqual(zlib.crc32(b"123456789") & 0xFFFFFFFF, 0xE3069283)


class HeaderLayoutTest(unittest.TestCase):
    def test_every_field_is_at_the_documented_offset(self) -> None:
        # A distinct value in every field, so this pins field order and endianness rather
        # than merely round-tripping.
        raw = protocol.Frame(protocol.HELLO, 0).encode()
        self.assertEqual("00000000" + "01" + "01" + "0000" + "00000000" + "00000000", raw.hex())

        header = protocol.Header.decode(raw)
        self.assertEqual(0, header.payload_len)
        self.assertEqual(protocol.HELLO, header.message_type)
        self.assertEqual(0, header.flags)
        self.assertEqual(0, header.request_id)
        self.assertEqual(0, header.checksum)

    def test_a_distinct_value_in_every_field(self) -> None:
        import struct

        raw = struct.pack("<IBBHII", 0x11223344, 1, protocol.HELLO, 0x5566, 0x778899AA, 0xBBCCDDEE)
        self.assertEqual("4433221101016655aa998877eeddccbb", raw.hex())

        header = protocol.Header.decode(raw)
        self.assertEqual(0x11223344, header.payload_len)
        self.assertEqual(protocol.HELLO, header.message_type)
        self.assertEqual(0x5566, header.flags)
        self.assertEqual(0x778899AA, header.request_id)
        self.assertEqual(0xBBCCDDEE, header.checksum)

    def test_a_header_is_exactly_sixteen_bytes(self) -> None:
        with self.assertRaises(protocol.ProtocolError):
            protocol.Header.decode(b"\x00" * 15)


class GoldenExchangeTest(unittest.TestCase):
    """RESPONSE(status 200, no headers, empty body) then END(keep_alive), byte for byte."""

    def test_the_exchange_matches_the_rust_and_php_fixtures(self) -> None:
        head = protocol.json_bytes({"status": 200, "headers": [], "body_len": 0})
        self.assertEqual(b'{"status":200,"headers":[],"body_len":0}', head)

        end = protocol.json_bytes(
            {
                "app_status": 0,
                "peak_memory_bytes": 0,
                "duration_us": 0,
                "keep_alive": True,
            }
        )
        self.assertEqual(
            b'{"app_status":0,"peak_memory_bytes":0,"duration_us":0,"keep_alive":true}', end
        )

        exchange = protocol.Frame(protocol.RESPONSE, 1, head).encode() + protocol.Frame(
            protocol.END, 1, end
        ).encode()

        expected = (
            "28000000" + "01" + "04" + "0000" + "01000000" + "83129f7b"
            + "7b22737461747573223a3230302c2268656164657273223a5b5d2c22626f64795f6c656e223a307d"
            + "48000000" + "01" + "0b" + "0000" + "01000000" + "770fe5b3"
            + "7b226170705f737461747573223a302c227065616b5f6d656d6f72795f6279746573223a302c"
            + "226475726174696f6e5f7573223a302c226b6565705f616c697665223a747275657d"
        )
        self.assertEqual(expected, exchange.hex())
        self.assertEqual(144, len(exchange))


class MessageTypeTest(unittest.TestCase):
    FROZEN = [
        (protocol.HELLO, 1),
        (protocol.HELLO_ACK, 2),
        (protocol.REQUEST, 3),
        (protocol.RESPONSE, 4),
        (protocol.BODY, 5),
        (protocol.EVENT, 6),
        (protocol.LOG, 7),
        (protocol.METRIC, 8),
        (protocol.CONTROL, 9),
        (protocol.CONTROL_ACK, 10),
        (protocol.END, 11),
        (protocol.PING, 12),
        (protocol.PONG, 13),
        (protocol.ERROR, 14),
        (protocol.CALL, 15),
        (protocol.RETURN, 16),
        (protocol.JOB, 17),
        (protocol.ACK, 18),
    ]

    def test_the_numbering_is_the_protocol(self) -> None:
        for message_type, number in self.FROZEN:
            with self.subTest(name=protocol.type_name(number)):
                encoded = protocol.Frame(message_type, 0).encode()
                self.assertEqual(number, encoded[5])
                self.assertEqual(number, message_type)

    def test_an_unknown_type_is_refused_rather_than_skipped(self) -> None:
        import struct

        raw = struct.pack("<IBBHII", 0, 1, 19, 0, 0, 0)
        with self.assertRaises(protocol.ProtocolError):
            protocol.Header.decode(raw)


class FlagsTest(unittest.TestCase):
    def test_a_compressed_frame_is_refused(self) -> None:
        import struct

        raw = struct.pack("<IBBHII", 0, 1, protocol.REQUEST, protocol.FLAG_COMPRESSED, 1, 0)
        with self.assertRaises(protocol.ProtocolError):
            protocol.Header.decode(raw)

    def test_no_checksum_writes_zero_and_raises_the_flag(self) -> None:
        encoded = protocol.Frame(protocol.BODY, 1, b"hello").encode(checksum=False)
        header = protocol.Header.decode(encoded[:16])
        self.assertTrue(header.declares_no_checksum)
        self.assertEqual(0, header.checksum)
        self.assertEqual(b"hello", encoded[16:])

    def test_a_checksummed_frame_still_carries_its_checksum(self) -> None:
        encoded = protocol.Frame(protocol.BODY, 1, b"hello").encode()
        header = protocol.Header.decode(encoded[:16])
        self.assertFalse(header.declares_no_checksum)
        self.assertEqual(protocol.crc32c(b"hello"), header.checksum)

    def test_zero_is_a_real_checksum_so_the_flag_is_not_zero(self) -> None:
        # The reason NO_CHECKSUM is a flag: `crc32c("") == 0`, which every empty-payload
        # frame carries, so using zero as the sentinel would make a correctly-checksummed
        # HELLO indistinguishable from an unchecked one.
        empty = protocol.Frame(protocol.HELLO, 0).encode()
        header = protocol.Header.decode(empty[:16])
        self.assertEqual(0, header.checksum)
        self.assertFalse(header.declares_no_checksum)

    def test_a_payload_above_the_limit_is_refused_before_allocating(self) -> None:
        # The limit is enforced when the *payload* is read, not when the header is decoded:
        # a header on its own has no body to allocate, and the Rust reader answers at the
        # same point. The test drives the reader so the enforcement is real rather than
        # asserted against a constant.
        import io
        import struct

        from volucer_worker.worker import Worker

        worker = Worker(io.BytesIO(b""), io.BytesIO(b""), io.StringIO())
        header = protocol.Header(
            protocol.MAX_PAYLOAD_LEN + 1, protocol.BODY, 0, 1, 0
        )
        self.assertEqual(
            protocol.MAX_PAYLOAD_LEN + 1,
            protocol.Header.decode(
                struct.pack(
                    "<IBBHII",
                    protocol.MAX_PAYLOAD_LEN + 1,
                    1,
                    protocol.BODY,
                    0,
                    1,
                    0,
                )
            ).payload_len,
            "the header carries the claim",
        )
        with self.assertRaises(protocol.ProtocolError):
            worker.read_payload(header)


if __name__ == "__main__":
    unittest.main()
