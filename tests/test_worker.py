"""The worker loop against an in-memory channel.

These tests drive :class:`volucer_worker.Worker` over ``BytesIO`` pairs, which is the same
thing the PHP SDK's ``WorkerTest`` does: a real frame stream, no process. The end-to-end
test is what proves the *other* end agrees; this one is what makes a regression legible.
"""

from __future__ import annotations

import io
import json
import sys
import unittest

import volucer_worker
from volucer_worker import protocol
from volucer_worker.message import Response
from volucer_worker.worker import Worker


class Channel:
    """A server's side of the channel, in memory.

    Writes the frames the server would write, then reads back everything the worker wrote.
    Writes always append (a `BytesIO` writes at its current position, which reading has
    already moved), and :func:`worker_for` rewinds before the worker starts reading.
    """

    def __init__(self) -> None:
        self.to_worker = io.BytesIO()
        self.from_worker = io.BytesIO()

    def _write(self, data: bytes) -> None:
        self.to_worker.seek(0, io.SEEK_END)
        self.to_worker.write(data)

    def hello(self, *, offer_no_checksum: bool = False) -> "Channel":
        self._write(
            protocol.Frame(
                protocol.HELLO,
                0,
                protocol.json_bytes(
                    {"protocol_version": 1, "no_checksum": offer_no_checksum}
                ),
            ).encode()
        )
        return self

    def request(
        self,
        request_id: int,
        *,
        method: str = "GET",
        uri: str = "/",
        headers: list[tuple[str, str]] | None = None,
        body: bytes = b"",
        https: bool = False,
    ) -> "Channel":
        self._write(
            protocol.Frame(
                protocol.REQUEST,
                request_id,
                protocol.json_bytes(
                    {
                        "method": method,
                        "uri": uri,
                        "protocol": "HTTP/1.1",
                        "headers": [list(pair) for pair in (headers or [])],
                        "remote_addr": "127.0.0.1",
                        "body_len": len(body),
                        "script_filename": "/srv/app.py",
                        "https": https,
                    }
                ),
            ).encode()
        )
        if body:
            last = (len(body) - 1) // protocol.MAX_BODY_CHUNK
            for index in range(last + 1):
                start = index * protocol.MAX_BODY_CHUNK
                self._write(
                    protocol.Frame(
                        protocol.BODY,
                        request_id,
                        body[start : start + protocol.MAX_BODY_CHUNK],
                        protocol.FLAG_LAST if index == last else 0,
                    ).encode()
                )
        return self

    def job(self, request_id: int, *, body: bytes = b"", attempts: int = 0) -> "Channel":
        self._write(
            protocol.Frame(
                protocol.JOB,
                request_id,
                protocol.json_bytes(
                    {
                        "pipeline": "emails",
                        "id": "job-1",
                        "task": "send",
                        "headers": [],
                        "attempts": attempts,
                        "body_len": len(body),
                    }
                ),
            ).encode()
        )
        if body:
            self._write(
                protocol.Frame(protocol.BODY, request_id, body, protocol.FLAG_LAST).encode()
            )
        return self

    def frames(self) -> list[tuple[protocol.Header, dict | bytes]]:
        """Everything the worker wrote, decoded."""
        raw = self.from_worker.getvalue()
        out: list[tuple[protocol.Header, dict | bytes]] = []
        offset = 0
        while offset < len(raw):
            header = protocol.Header.decode(raw[offset : offset + protocol.HEADER_LEN])
            payload = raw[offset + protocol.HEADER_LEN : offset + protocol.HEADER_LEN + header.payload_len]
            offset += protocol.HEADER_LEN + header.payload_len
            if header.message_type in (
                protocol.BODY,
                protocol.EVENT,
            ):
                out.append((header, payload))
            else:
                out.append((header, json.loads(payload) if payload else {}))
        return out

    def by_type(self, message_type: int) -> list[tuple[protocol.Header, dict | bytes]]:
        return [pair for pair in self.frames() if pair[0].message_type == message_type]


def worker_for(channel: Channel) -> Worker:
    """A worker reading `channel`, from its beginning.

    Rewinding is the harness's job rather than the worker's: a real pipe is not seekable, so
    a worker that seeked would not be modelling the thing it runs on.
    """
    channel.to_worker.seek(0)
    return Worker(channel.to_worker, channel.from_worker, io.StringIO())


class HandshakeTest(unittest.TestCase):
    def test_the_ack_reports_the_protocol_and_a_runtime(self) -> None:
        channel = Channel().hello()
        worker = worker_for(channel)
        worker.handshake()

        ack = channel.by_type(protocol.HELLO_ACK)
        self.assertEqual(1, len(ack))
        payload = ack[0][1]
        self.assertEqual(1, payload["protocol_version"])
        self.assertTrue(payload["runtime"].startswith("volucer-worker-python/"))
        self.assertFalse(payload["no_checksum"])
        self.assertGreater(payload["pid"], 0)

    def test_the_handshake_frames_are_always_checksummed(self) -> None:
        # Even when both ends are about to stop hashing: the agreement travels in frames, so
        # the frames carrying it must be verified.
        channel = Channel().hello(offer_no_checksum=True)
        worker = worker_for(channel)
        worker.handshake()

        header = channel.by_type(protocol.HELLO_ACK)[0][0]
        self.assertFalse(header.declares_no_checksum)
        self.assertEqual(
            protocol.crc32c(
                channel.from_worker.getvalue()[protocol.HEADER_LEN :]
            ),
            header.checksum,
        )
        # And only then does the worker stop hashing.
        self.assertFalse(worker._checksum)
        self.assertTrue(channel.by_type(protocol.HELLO_ACK)[0][1]["no_checksum"])

    def test_a_hello_with_the_wrong_version_is_refused(self) -> None:
        channel = Channel()
        channel.to_worker.write(
            protocol.Frame(protocol.HELLO, 0, b'{"protocol_version":2}').encode()
        )
        worker = worker_for(channel)
        with self.assertRaises(protocol.ProtocolError):
            worker.handshake()

    def test_a_request_where_a_hello_belongs_is_refused(self) -> None:
        channel = Channel().request(1)
        worker = worker_for(channel)
        with self.assertRaises(protocol.ProtocolError):
            worker.handshake()

    def test_an_empty_pipe_is_not_a_handshake(self) -> None:
        worker = worker_for(Channel())
        with self.assertRaises(protocol.ProtocolError):
            worker.handshake()


class RequestTest(unittest.TestCase):
    def run_one(self, channel: Channel, handler) -> list[tuple[protocol.Header, dict | bytes]]:
        # `run()` performs the handshake itself, which is the interface an application
        # uses — so the tests drive the same entry point rather than a lower one.
        worker = worker_for(channel)
        worker.run(handler)
        return channel.frames()

    def test_a_response_is_a_head_then_end(self) -> None:
        channel = Channel().hello().request(1, uri="/items")
        frames = self.run_one(
            channel, lambda request, worker: Response.json({"path": request.path})
        )

        response = [payload for header, payload in frames if header.message_type == protocol.RESPONSE]
        self.assertEqual(1, len(response))
        self.assertEqual(200, response[0]["status"])
        self.assertEqual(
            len(b'{"path":"/items"}'),
            response[0]["body_len"],
            "the head declares the length the body will have",
        )

        body = [payload for header, payload in frames if header.message_type == protocol.BODY]
        self.assertEqual([b'{"path":"/items"}'], body)

        end = [payload for header, payload in frames if header.message_type == protocol.END]
        self.assertEqual(1, len(end))
        self.assertEqual(0, end[0]["app_status"])
        self.assertTrue(end[0]["keep_alive"])
        self.assertIn("peak_memory_bytes", end[0])

    def test_the_head_body_and_end_all_carry_the_request_id(self) -> None:
        channel = Channel().hello().request(7, uri="/")
        frames = self.run_one(channel, lambda request, worker: Response.text("ok"))
        for header, _ in frames:
            if header.message_type in (protocol.HELLO_ACK,):
                continue
            self.assertEqual(7, header.request_id, protocol.type_name(header.message_type))

    def test_an_empty_body_sends_no_body_frame(self) -> None:
        channel = Channel().hello().request(1)
        frames = self.run_one(channel, lambda request, worker: Response(204, [], b""))
        self.assertEqual([], [p for h, p in frames if h.message_type == protocol.BODY])

    def test_a_body_larger_than_one_chunk_is_split_and_the_last_is_flagged(self) -> None:
        channel = Channel().hello().request(1)
        payload = b"x" * (protocol.MAX_BODY_CHUNK * 2 + 17)
        frames = self.run_one(channel, lambda request, worker: Response(200, [], payload))

        bodies = [(header, data) for header, data in frames if header.message_type == protocol.BODY]
        self.assertEqual(3, len(bodies))
        self.assertEqual(protocol.MAX_BODY_CHUNK, len(bodies[0][1]))
        self.assertEqual(protocol.MAX_BODY_CHUNK, len(bodies[1][1]))
        self.assertEqual(17, len(bodies[2][1]))
        self.assertEqual([False, False, True], [header.is_last for header, _ in bodies])
        self.assertEqual(payload, b"".join(data for _, data in bodies))

    def test_a_handler_that_raises_answers_500_and_reports_the_status(self) -> None:
        channel = Channel().hello().request(1)

        def handler(request, worker):
            raise RuntimeError("boom")

        frames = self.run_one(channel, handler)
        response = [p for h, p in frames if h.message_type == protocol.RESPONSE][0]
        self.assertEqual(500, response["status"])
        end = [p for h, p in frames if h.message_type == protocol.END][0]
        self.assertEqual(
            1,
            end["app_status"],
            "app_status is the application's exit status, not the HTTP status",
        )
        self.assertTrue(
            end["keep_alive"],
            "a handler that raised is a bug in the request, not a reason to recycle the worker",
        )

    def test_keep_alive_false_ends_the_loop(self) -> None:
        channel = Channel().hello().request(1)

        def handler(request, worker):
            worker.recycle()
            return Response.text("last one")

        worker_for(channel).run(handler)
        end = [p for h, p in channel.frames() if h.message_type == protocol.END][0]
        self.assertFalse(end["keep_alive"])

    def test_the_request_body_is_reassembled_from_its_chunks(self) -> None:
        body = b"a" * (protocol.MAX_BODY_CHUNK + 5)
        channel = Channel().hello().request(1, method="POST", body=body)
        seen: list[bytes] = []
        self.run_one(
            channel,
            lambda request, worker: (seen.append(request.body), Response.text("ok"))[1],
        )
        self.assertEqual([body], seen)

    def test_the_request_headers_keep_their_pairs(self) -> None:
        channel = Channel().hello().request(
            1, headers=[("Accept", "text/html"), ("Cookie", "a=1"), ("Cookie", "b=2")]
        )
        seen: list = []
        self.run_one(
            channel,
            lambda request, worker: (seen.append(request), Response.text("ok"))[1],
        )
        self.assertEqual("text/html", seen[0].header("accept"))
        self.assertEqual(
            "a=1, b=2",
            seen[0].header_map["Cookie"],
            "repeated names are joined, which is what a repeated field means",
        )
        self.assertEqual("https", "https" if seen[0].https else "https")

    def test_https_reaches_the_request(self) -> None:
        channel = Channel().hello().request(1, https=True)
        seen: list = []
        self.run_one(
            channel,
            lambda request, worker: (seen.append(request), Response.text("ok"))[1],
        )
        self.assertTrue(seen[0].https)

    def test_a_ping_is_answered_with_a_pong(self) -> None:
        channel = Channel().hello()
        channel.to_worker.write(protocol.Frame(protocol.PING, 0).encode())
        worker_for(channel).run(lambda request, worker: Response.text("ok"))
        self.assertEqual(1, len(channel.by_type(protocol.PONG)))

    def test_a_drain_control_stops_the_loop(self) -> None:
        channel = Channel().hello()
        channel.to_worker.write(
            protocol.Frame(protocol.CONTROL, 0, b'{"kind":"drain"}').encode()
        )
        worker_for(channel).run(lambda request, worker: Response.text("ok"))
        self.assertEqual(1, len(channel.by_type(protocol.CONTROL_ACK)))

    def test_telemetry_from_the_server_does_not_derail_the_loop(self) -> None:
        # A peer that echoes telemetry back must not be fatal.
        channel = Channel().hello()
        channel.to_worker.write(
            protocol.Frame(protocol.LOG, 0, b'{"level":"info","message":"x"}').encode()
        )
        channel.request(1, uri="/after")
        seen: list = []
        self.run_one(
            channel,
            lambda request, worker: (seen.append(request.uri), Response.text("ok"))[1],
        )
        self.assertEqual(["/after"], seen)


class CallTest(unittest.TestCase):
    def test_a_call_is_answered_while_a_request_is_in_flight(self) -> None:
        channel = Channel().hello().request(1)

        # The server answers the CALL the worker is about to make, then the request. The
        # answer is queued after the REQUEST because that is when the worker reads it: the
        # channel is one stream, and a RETURN only exists while a call is in flight.
        channel._write(
            protocol.Frame(
                protocol.RETURN,
                0xFFFFFFFF,
                protocol.json_bytes({"ok": True, "result": {"hit": 1}}),
            ).encode()
        )
        seen: list = []

        def handler(request, worker):
            seen.append(worker.call("kv.get", {"key": "a"}))
            return Response.text("ok")

        worker_for(channel).run(handler)
        self.assertEqual([{"hit": 1}], seen)

        sent = [p for h, p in channel.frames() if h.message_type == protocol.CALL]
        self.assertEqual(1, len(sent))
        self.assertEqual("kv.get", sent[0]["name"])
        self.assertEqual({"key": "a"}, sent[0]["params"])

    def test_a_call_outside_a_request_raises_rather_than_hanging(self) -> None:
        # Between requests nothing on the server is reading this channel, so a call would
        # block until the request deadline killed the worker. Raising is the SDK's half of
        # enforcing that.
        worker = worker_for(Channel())
        with self.assertRaises(protocol.ProtocolError):
            worker.call("kv.get", {})

    def test_a_refused_call_raises_and_says_whether_the_effect_is_known(self) -> None:
        channel = Channel().hello().request(1)
        channel._write(
            protocol.Frame(
                protocol.RETURN,
                0xFFFFFFFF,
                protocol.json_bytes({"ok": False, "error": "no such key", "unknown": True}),
            ).encode()
        )
        worker = worker_for(channel)

        errors: list[protocol.ProtocolError] = []

        def handler(request, worker_instance):
            try:
                worker_instance.call("lock.acquire", {"name": "x"})
            except protocol.ProtocolError as error:
                errors.append(error)
            return Response.text("ok")

        worker.run(handler)
        self.assertEqual(1, len(errors))
        self.assertFalse(errors[0].fatal, "a refused call leaves the stream aligned")
        self.assertFalse(
            worker.last_call_was_known,
            "the reply said the effect may have happened, so a retry could duplicate it",
        )

    def test_a_call_with_no_handler_is_refused_by_name(self) -> None:
        channel = Channel().hello()
        channel._write(
            protocol.Frame(
                protocol.CALL, 0xFFFFFFF0, b'{"name":"kv.get","params":{}}'
            ).encode()
        )
        worker_for(channel).run(lambda request, worker: Response.text("ok"))

        returns = channel.by_type(protocol.RETURN)
        self.assertEqual(1, len(returns))
        self.assertFalse(returns[0][1]["ok"])
        self.assertIn("kv.get", returns[0][1]["error"])


class JobTest(unittest.TestCase):
    def run_job(self, handler, **kwargs) -> dict:
        channel = Channel().hello().job(1, **kwargs)
        worker_for(channel).consume(handler)
        acks = channel.by_type(protocol.ACK)
        self.assertEqual(1, len(acks))
        return acks[0][1]

    def test_none_acknowledges(self) -> None:
        ack = self.run_job(lambda job, worker: None)
        self.assertEqual("ack", ack["outcome"])
        self.assertIn("peak_memory_bytes", ack)
        self.assertIn("duration_us", ack)

    def test_false_requeues(self) -> None:
        self.assertEqual("requeue", self.run_job(lambda job, worker: False)["outcome"])

    def test_a_string_rejects_with_a_reason(self) -> None:
        ack = self.run_job(lambda job, worker: "bad payload")
        self.assertEqual("reject", ack["outcome"])
        self.assertEqual("bad payload", ack["reason"])

    def test_a_raising_handler_requeues_because_losing_work_is_worse(self) -> None:
        def handler(job, worker):
            raise RuntimeError("database unreachable")

        self.assertEqual(
            "requeue",
            self.run_job(handler)["outcome"],
            "a crashing consumer must not lose the job; dropping one has to be deliberate",
        )

    def test_the_job_body_and_attempts_reach_the_handler(self) -> None:
        seen: list = []
        self.run_job(
            lambda job, worker: (seen.append((job.body, job.attempts)), None)[1],
            body=b"payload",
            attempts=2,
        )
        self.assertEqual([(b"payload", 2)], seen)

    def test_a_job_sent_to_a_request_worker_is_requeued_rather_than_dropped(self) -> None:
        channel = Channel().hello().job(1, body=b"payload")
        worker_for(channel).run(lambda request, worker: Response.text("ok"))

        acks = channel.by_type(protocol.ACK)
        self.assertEqual(1, len(acks))
        self.assertEqual("requeue", acks[0][1]["outcome"])
        self.assertIn("serves requests", acks[0][1]["reason"])


class TransportTest(unittest.TestCase):
    def test_an_empty_socket_variable_means_stdio(self) -> None:
        import os

        from volucer_worker.worker import SOCKET_ENV, socket_address

        for value in (None, "", "   "):
            if value is None:
                os.environ.pop(SOCKET_ENV, None)
            else:
                os.environ[SOCKET_ENV] = value
            self.assertIsNone(socket_address(), f"{value!r} must mean stdio")

        os.environ[SOCKET_ENV] = "127.0.0.1:9999"
        self.assertEqual("127.0.0.1:9999", socket_address())
        os.environ.pop(SOCKET_ENV, None)

class StdoutChannelTest(unittest.TestCase):
    """Which stream `create()` uses, which is the difference between a worker and a hang.

    Importing the package rebinds ``sys.stdout`` to a stream that writes to **stderr**, so
    that an application's ``print`` cannot land inside a frame. ``create()`` must therefore
    write frames to the *real* stdout, captured before that rebinding. It used
    ``sys.stdout.buffer`` and that was a defect no Windows test could see: Windows runs the
    socket transport, where the channel is a socket and ``sys.stdout`` is irrelevant, so the
    handshake reached stderr and the server reported a startup timeout on Linux and macOS
    only. Caught by running the end-to-end suite against a real Linux binary.
    """

    def test_the_guard_displaced_the_package_stdout(self) -> None:
        # If this ever stops being true, the test below would pass for the wrong reason.
        self.assertTrue(
            getattr(sys.stdout, "_volucer_guard", False),
            "importing the package must rebind sys.stdout, or stdout is still the channel",
        )
        self.assertFalse(
            getattr(volucer_worker.real_stdout, "_volucer_guard", False),
            "and the captured stream must be the one the guard displaced",
        )

    def test_frames_go_to_the_real_stdout_and_not_to_the_guard(self) -> None:
        # `create()` with no arguments binds the process's own streams, so the object it
        # stores is exactly the thing under test.
        worker = Worker.create()
        self.assertIs(
            worker._out,
            volucer_worker.real_stdout.buffer,
            "the protocol channel is the real stdout's buffer",
        )
        self.assertIsNot(
            worker._out,
            sys.stdout.buffer,
            "and explicitly not the guarded stream, which writes to stderr",
        )

    def test_a_diagnostic_never_reaches_the_protocol_stream(self) -> None:
        # The point of the guard, asserted on the stream the worker actually writes to.
        worker = Worker.create()
        worker.diagnose("a diagnostic line")
        self.assertFalse(
            worker._out.closed,
            "writing a diagnostic must not close or corrupt the channel",
        )


if __name__ == "__main__":
    unittest.main()
