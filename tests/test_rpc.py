"""The `[rpc]` call plane, driven without a server process.

A ``socket.socketpair`` gives this file a real socket for the transport and a script it can drive
frame by frame, which is exactly the shape a test of a *client* wants: every interesting case here
is about what the client does with a particular answer, or with no answer at all, and none of them
need a volucer process.

The frame helpers are written against :class:`protocol.Frame` rather than a request-shaped helper,
because a test that a call *carries the token* has to assert on the bytes, and a helper that went
through a decoded dict would be asserting on its own assumption.

The other end of this wire is tested in Rust — ``crates/volucer-server/tests/rpc.rs`` — and the
end-to-end suite drives a real server. What only this file can show is the client's own failures:
a refusal, a deadline, a connection that breaks after the call was written.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import unittest

from volucer_worker import protocol
from volucer_worker.protocol import ProtocolError
from volucer_worker.rpc import Rpc, _address_of, _key


def pair() -> tuple[socket.socket, socket.socket]:
    """A connected pair of stream sockets: ``(client, server)``."""
    client, server = socket.socketpair(socket.AF_INET, socket.SOCK_STREAM)
    return client, server


def stream_of(sock: socket.socket):
    """The client's transport.

    The socket itself rather than ``sock.makefile()``: a ``SocketIO`` wrapper has no
    ``settimeout``, so a deadline applied to it is silently ignored — which is exactly the bug the
    deadline test exists to catch. Passing the socket keeps ``recv``/``sendall``/``settimeout``
    available, and :class:`Rpc` prefers those when they are there.
    """
    return sock


def read_frame(sock: socket.socket) -> dict:
    """One frame the client wrote, decoded."""
    head = _read_exactly(sock, protocol.HEADER_LEN)
    header = protocol.Header.decode(head)
    payload = _read_exactly(sock, header.payload_len) if header.payload_len else b""
    return {
        "type": header.message_type,
        "id": header.request_id,
        "payload": json.loads(payload) if payload else {},
        "checksum": header.checksum,
        "raw": payload,
    }


def _read_exactly(sock: socket.socket, length: int) -> bytes:
    got = b""
    while len(got) < length:
        chunk = sock.recv(length - len(got))
        if not chunk:
            raise AssertionError(f"the client wrote only {len(got)} of {length} bytes")
        got += chunk
    return got


def write_return(sock: socket.socket, request_id: int, answer: dict) -> None:
    """Answer a call on the server end of a pair."""
    payload = json.dumps(answer, separators=(",", ":")).encode("utf-8")
    sock.sendall(protocol.Frame(protocol.RETURN, request_id, payload).encode())


def plane(client: socket.socket, token: str = "a-token", timeout: float = 0.5) -> Rpc:
    """A plane bound to one end of a pair, with a short deadline."""
    return Rpc(stream_of(client), token, "127.0.0.1:1", timeout)


class TheSeamItself(unittest.TestCase):
    def test_a_plane_is_built_without_an_address(self) -> None:
        # A backend that granted nothing is handed no address. That is an ordinary state — a
        # request which never touches a store must still work — so the plane refuses the *call*,
        # with the variable named, rather than raising from the constructor.
        unconfigured = Rpc.connect(address=None, token=None)
        for name in ("VOLUCER_RPC", "HTTP_X_VOLUCER_RPC"):
            self.assertIsNone(__import__("os").environ.get(name))

        self.assertIsNone(unconfigured.call("kv.cache.get", {"key": "a"}))
        self.assertIn("VOLUCER_RPC", unconfigured.last_error or "")
        self.assertIn("services", unconfigured.last_error or "")
        self.assertFalse(
            unconfigured.last_call_was_unknown,
            "nothing was sent, so the call certainly did not happen",
        )

    def test_a_zero_timeout_is_refused(self) -> None:
        # A deadline of zero would abandon every call, which is never what a caller meant.
        with self.assertRaises(ValueError):
            Rpc(None, "t", "127.0.0.1:1", 0)


class ReadingTheEnvironment(unittest.TestCase):
    """The address and the token are both put in the request by volucer; both are read here."""

    def setUp(self) -> None:
        import os

        self._saved = dict(os.environ)
        for name in (
            "VOLUCER_RPC",
            "VOLUCER_RPC_TOKEN",
            "HTTP_X_VOLUCER_RPC",
            "HTTP_X_VOLUCER_RPC_TOKEN",
        ):
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        import os

        os.environ.clear()
        os.environ.update(self._saved)

    def test_the_cgi_parameters_are_read(self) -> None:
        import os

        os.environ["VOLUCER_RPC"] = "127.0.0.1:1"
        os.environ["VOLUCER_RPC_TOKEN"] = "token-from-cgi"

        # Dialling is deferred to the first call, so this asserts the *reading* without a listener.
        connected = Rpc.connect()
        self.assertEqual("127.0.0.1:1", connected.address)

    def test_the_upstream_header_form_is_read(self) -> None:
        import os

        # An application behind an `upstream` backend has no CGI parameters at all.
        os.environ["HTTP_X_VOLUCER_RPC"] = "127.0.0.1:1"
        os.environ["HTTP_X_VOLUCER_RPC_TOKEN"] = "token-from-header"

        self.assertEqual("127.0.0.1:1", Rpc.connect().address)

    def test_a_blank_value_reads_as_absent(self) -> None:
        import os

        # `VOLUCER_RPC=` is what a shell produces while somebody is debugging, and it is not an
        # address to dial.
        os.environ["VOLUCER_RPC"] = "   "

        self.assertIsNone(Rpc.connect().address)

    def test_the_wsgi_request_environment_is_read_when_given(self) -> None:
        """A WSGI application has to pass its ``environ``, because the two are different things.

        volucer puts the address in the **CGI request**, and the process the application runs in
        only gets ``VOLUCER_FCGI_ADDRESS``. Reading ``os.environ`` therefore worked for a script
        started by hand and failed for every real request — with a message blaming the deployment.
        Found by running the client against a real server through ``spawn_cgi``.
        """
        environ = {
            "VOLUCER_RPC": "127.0.0.1:6001",
            "VOLUCER_RPC_TOKEN": "token-from-the-request",
        }

        connected = Rpc.connect(environ=environ)

        self.assertEqual("127.0.0.1:6001", connected.address)

    def test_the_wsgi_form_finds_the_upstream_headers(self) -> None:
        # An `upstream` backend hands the same two values as request headers, which a WSGI
        # application sees in `environ` as `HTTP_…`.
        environ = {"HTTP_X_VOLUCER_RPC": "127.0.0.1:6002"}

        self.assertEqual("127.0.0.1:6002", Rpc.connect(environ=environ).address)

    def test_a_missing_address_names_where_it_looked(self) -> None:
        # The message has to distinguish "the process had nothing" from "the request had nothing",
        # because those are different mistakes: a CLI script versus a backend with no `services`.
        from_os = Rpc.connect(address=None, token=None)
        self.assertIn("its environment", from_os.call("x", {}) or from_os.last_error or "")

        from_wsgi = Rpc.connect(environ={})
        self.assertIsNone(from_wsgi.call("x", {}))
        self.assertIn("WSGI request environment", from_wsgi.last_error or "")


class TheCall(unittest.TestCase):
    def test_a_call_carries_the_token(self) -> None:
        client, server = pair()
        try:
            written: dict = {}

            def serve() -> None:
                sent = read_frame(server)
                written.update(sent)
                write_return(server, sent["id"], {"ok": True, "result": None})

            thread = threading.Thread(target=serve)
            thread.start()
            plane(client, token="the-token").call("kv.cache.get", {"key": "YQ=="})
            thread.join(5)

            self.assertEqual(protocol.CALL, written["type"])
            self.assertEqual("the-token", written["payload"]["token"])
            self.assertEqual("kv.cache.get", written["payload"]["name"])
        finally:
            client.close()
            server.close()

    def test_a_call_with_no_token_omits_the_field(self) -> None:
        client, server = pair()
        try:
            written: dict = {}

            def serve() -> None:
                sent = read_frame(server)
                written.update(sent)
                write_return(server, sent["id"], {"ok": True, "result": None})

            thread = threading.Thread(target=serve)
            thread.start()
            plane(client, token="").call("kv.cache.get", {})
            thread.join(5)

            # Omitted rather than sent empty: the server tells "carried no token" from "carried a
            # token no backend was issued", and they are fixed in different files.
            self.assertNotIn("token", written["payload"])
        finally:
            client.close()
            server.close()

    def test_a_served_call_returns_the_result(self) -> None:
        client, server = pair()
        try:

            def serve() -> None:
                sent = read_frame(server)
                write_return(server, sent["id"], {"ok": True, "result": {"value": "aGk="}})

            thread = threading.Thread(target=serve)
            thread.start()
            answer = plane(client).call("kv.cache.get", {"key": "YQ=="})
            thread.join(5)

            self.assertEqual({"value": "aGk="}, answer)
        finally:
            client.close()
            server.close()

    def test_a_refusal_sets_the_error_and_is_not_unknown(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                write_return(
                    server,
                    sent["id"],
                    {"ok": False, "error": "this backend may not reach the cache"},
                )

            thread = threading.Thread(target=serve)
            thread.start()
            answer = connected.call("kv.cache.get", {})
            thread.join(5)

            self.assertIsNone(answer)
            self.assertIn("may not reach", connected.last_error or "")
            self.assertFalse(
                connected.last_call_was_unknown,
                "the server answered a refusal, so nothing was left undecided",
            )
        finally:
            client.close()
            server.close()

    def test_an_undecided_answer_is_reported_as_unknown(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                write_return(
                    server,
                    sent["id"],
                    {"ok": False, "unknown": True, "error": "the lock state is unknown"},
                )

            thread = threading.Thread(target=serve)
            thread.start()
            connected.call("lock.acquire", {"resource": "r"})
            thread.join(5)

            self.assertTrue(
                connected.last_call_was_unknown,
                "a refusal carrying unknown:true says the effect may have happened",
            )
        finally:
            client.close()
            server.close()

    def test_a_deadline_expires_rather_than_hanging(self) -> None:
        # A socket that accepts and never answers, which is what "the server has stopped reading"
        # looks like from here. The read deadline is the only thing that ends it.
        client, server = pair()
        try:
            connected = plane(client, timeout=0.2)
            started = time.monotonic()
            answer = connected.call("kv.cache.get", {})
            elapsed = time.monotonic() - started

            self.assertIsNone(answer)
            self.assertLess(elapsed, 3.0, "the deadline has to bound the wait")
            self.assertTrue(
                connected.last_call_was_unknown,
                "a call that was written and not answered may have been served",
            )
            self.assertIn("did not answer", connected.last_error or "")
        finally:
            client.close()
            server.close()

    def test_a_closed_connection_is_not_remembered(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)
            server.close()
            connected.call("kv.cache.get", {})

            self.assertTrue(connected.last_call_was_unknown)
            self.assertIsNotNone(connected.last_error)
        finally:
            client.close()

    def test_a_frame_that_is_not_a_return_is_a_protocol_error(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                sock = server
                sock.sendall(
                    protocol.Frame(protocol.LOG, sent["id"], b'{"message":"hi"}').encode()
                )

            thread = threading.Thread(target=serve)
            thread.start()
            answer = connected.call("kv.cache.get", {})
            thread.join(5)

            self.assertIsNone(answer)
            self.assertIn("LOG", connected.last_error or "")
        finally:
            client.close()
            server.close()


class KvOverTheWire(unittest.TestCase):
    """The storage handle, asserted on the bytes it writes.

    The server's half is `a_batch_key_is_base64_in_both_directions` in the Rust suite; this is the
    client's half, and the batch methods are here because the two sides disagreed about them for
    three releases with no test on either side.
    """

    def call_of(self, body) -> dict:
        """Run ``body`` against a scripted server and return the frame it wrote."""
        client, server = pair()
        try:
            connected = plane(client)
            written: dict = {}

            def serve() -> None:
                sent = read_frame(server)
                written.update(sent["payload"])
                # A generic answer, so the client's *own* encoding is what is under test.
                write_return(
                    server,
                    sent["id"],
                    {"ok": True, "result": {"value": None, "values": [], "stored": 0}},
                )

            thread = threading.Thread(target=serve)
            thread.start()
            try:
                body(connected)
            except ProtocolError:
                pass
            thread.join(5)
            return written
        finally:
            client.close()
            server.close()

    def test_a_key_is_base64(self) -> None:
        sent = self.call_of(lambda p: p.kv("cache").get("greeting"))

        self.assertEqual("kv.cache.get", sent["name"])
        self.assertEqual("Z3JlZXRpbmc=", sent["params"]["key"])

    def test_a_value_is_json_then_base64(self) -> None:
        sent = self.call_of(lambda p: p.kv("cache").set("k", {"a": 1}))

        self.assertEqual("kv.cache.set", sent["name"])
        self.assertEqual("eyJhIjoxfQ==", sent["params"]["value"], 'b64 of {"a":1}')

    def test_a_ttl_is_seconds(self) -> None:
        sent = self.call_of(lambda p: p.kv("cache").set("k", "v", ttl=60))

        self.assertEqual(60, sent["params"]["ttl"])

    def test_a_batch_key_is_base64_like_a_single_one(self) -> None:
        # The bug this pins: the server read the list as literal strings while decoding every
        # scalar key, so a batch read reported every key as missing.
        sent = self.call_of(lambda p: p.kv("cache").get_multiple(["greeting"]))

        self.assertEqual([base64("greeting")], sent["params"]["keys"])

    def test_a_batch_write_encodes_both_halves(self) -> None:
        sent = self.call_of(lambda p: p.kv("cache").set_multiple({"farewell": "bye"}))

        self.assertEqual(
            {base64("farewell"): base64('"bye"')},
            sent["params"]["values"],
            "the key is base64 and the value is base64 of its JSON encoding",
        )

    def test_a_bytes_key_is_used_as_given(self) -> None:
        # A caller who hashes or encrypts keys has bytes, and re-encoding them as text would be
        # a silent difference from the same key on another transport.
        self.assertEqual(base64(b"key"), _key(b"key"))

    def test_a_non_string_key_is_refused_here(self) -> None:
        with self.assertRaises(TypeError):
            _key(1)

    def test_an_empty_key_is_refused_here(self) -> None:
        with self.assertRaises(ValueError):
            _key("")


class LocksAndJobs(unittest.TestCase):
    def test_an_acquire_name_is_the_verb_the_server_serves(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                self.assertEqual("lock.acquire", sent["payload"]["name"])
                # `ttl` and `wait` are microseconds, which is the one unit a caller gets wrong.
                self.assertEqual(5000, sent["payload"]["params"]["ttl"])
                write_return(
                    server,
                    sent["id"],
                    {"ok": True, "result": {"acquired": True, "id": "holder-1"}},
                )

            thread = threading.Thread(target=serve)
            thread.start()
            holder = connected.lock().acquire("pdf:create", ttl=5000)
            thread.join(5)

            self.assertEqual("holder-1", holder)
        finally:
            client.close()
            server.close()

    def test_contention_is_none_rather_than_an_error(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                write_return(server, sent["id"], {"ok": True, "result": {"acquired": False}})

            thread = threading.Thread(target=serve)
            thread.start()
            self.assertIsNone(connected.lock().acquire("pdf:create"))
            thread.join(5)
        finally:
            client.close()
            server.close()

    def test_exists_reads_the_field_the_server_sends(self) -> None:
        # Guessed wrong once: the answer is `exists`, not `held`.
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                write_return(server, sent["id"], {"ok": True, "result": {"exists": True}})

            thread = threading.Thread(target=serve)
            thread.start()
            self.assertTrue(connected.lock().exists("pdf:create"))
            thread.join(5)
        finally:
            client.close()
            server.close()

    def test_a_push_carries_its_task_and_payload(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                self.assertEqual("jobs.push", sent["payload"]["name"])
                self.assertEqual("mail", sent["payload"]["params"]["pipeline"])
                self.assertEqual("SendEmail", sent["payload"]["params"]["task"])
                self.assertEqual(base64('{"to":"a@b"}'), sent["payload"]["params"]["payload"])
                write_return(server, sent["id"], {"ok": True, "result": {"id": "job-1"}})

            thread = threading.Thread(target=serve)
            thread.start()
            pushed = connected.queue("mail").push("SendEmail", {"to": "a@b"})
            thread.join(5)

            self.assertEqual("job-1", pushed)
        finally:
            client.close()
            server.close()

    def test_a_bytes_payload_is_not_re_encoded(self) -> None:
        client, server = pair()
        try:
            connected = plane(client)

            def serve() -> None:
                sent = read_frame(server)
                # A producer that serialized its own object should not have it wrapped in JSON.
                # Bytes here, not a str: `"\x00\xff"` as *text* is two UTF-8 bytes, which is not
                # what the client was handed.
                self.assertEqual(base64(b"\x00\xff"), sent["payload"]["params"]["payload"])
                write_return(server, sent["id"], {"ok": True, "result": {"id": "job-1"}})

            thread = threading.Thread(target=serve)
            thread.start()
            connected.queue("mail").push("t", b"\x00\xff")
            thread.join(5)
        finally:
            client.close()
            server.close()


class Addresses(unittest.TestCase):
    def test_a_unix_address_becomes_a_path(self) -> None:
        path, tcp = _address_of("unix:/run/volucer/rpc.sock")

        self.assertEqual("/run/volucer/rpc.sock", path)
        self.assertIsNone(tcp)

    def test_a_tcp_address_becomes_a_host_and_port(self) -> None:
        path, tcp = _address_of("127.0.0.1:6001")

        self.assertIsNone(path)
        self.assertEqual(("127.0.0.1", 6001), tcp)

    def test_a_malformed_address_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            _address_of("not an address")


def base64(text) -> str:
    import base64 as module

    raw = text if isinstance(text, bytes) else text.encode("utf-8")
    return module.b64encode(raw).decode("ascii")


if __name__ == "__main__":
    unittest.main()
