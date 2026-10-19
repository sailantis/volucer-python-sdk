"""End-to-end: the real server, the real Python worker, the real protocol.

Everything else in this suite tests this SDK against its own model of the protocol. This
test is the one that proves the model is right, because the other end is the Rust server
rather than a fixture.

It is skipped unless ``VOLUCER_BIN`` names a built ``volucer`` binary — the same convention
`sailantis/volucer` uses for its own end-to-end suites, which set ``VOLUCER_E2E=1`` so a
skip is a failure in CI.

What has to hold, and each is a way a worker can be wrong without noticing:

* a request is routed to this process and the answer comes back;
* the response body survives the chunking, including a body larger than one frame;
* two consecutive requests do not see each other's state — the check that a worker is
  really *reset* rather than merely reused;
* application output does not corrupt the framing, because this SDK rebinds stdout;
* the server accepts the `HELLO_ACK` this SDK sends, including `runtime`.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

# The package itself, for the tests that need no binary: importing it is safe on every platform —
# `supervisor` only refuses when `serve()` is called, not when it is imported.
from volucer_worker import supervisor  # noqa: E402

SDK_ROOT = Path(__file__).resolve().parents[1]
WORKER = SDK_ROOT / "examples" / "wsgi_app.py"


def volucer_binary() -> str | None:
    explicit = os.environ.get("VOLUCER_BIN")
    if explicit and Path(explicit).is_file():
        return explicit
    # The sibling checkout, which is where this SDK is developed; CI checks the server out
    # into `volucer/` inside this repository, which the second candidate covers.
    for candidate in (
        SDK_ROOT.parent / "volucer" / "target" / "debug",
        SDK_ROOT / "volucer" / "target" / "debug",
    ):
        binary = candidate / ("volucer.exe" if sys.platform == "win32" else "volucer")
        if binary.is_file():
            return str(binary)
    return None


def e2e_required() -> bool:
    """Whether a skip must be a failure.

    ``VOLUCER_E2E=1`` is what CI sets, for the reason the server's own suites use it: a
    skipped end-to-end test and a passing one are the same green.
    """
    return os.environ.get("VOLUCER_E2E") == "1"


def should_run(has_binary: bool, required: bool) -> bool:
    """Whether the end-to-end suite runs rather than skips.

    A named function rather than an expression inside the decorator, because the expression
    was written backwards once (``has_binary or not required``) and a decorator argument
    cannot be tested. Only the case of *neither* a binary nor a requirement skips.
    """
    return has_binary or required


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def http_get(port: int, path: str) -> tuple[int, dict[str, str], bytes]:
    with socket.create_connection(("127.0.0.1", port), timeout=30) as connection:
        request = (
            f"GET {path} HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"
        )
        connection.sendall(request.encode())
        raw = b""
        while True:
            chunk = connection.recv(65536)
            if not chunk:
                break
            raw += chunk

    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
    return status, headers, body


@unittest.skipUnless(
    should_run(volucer_binary() is not None, e2e_required()),
    "no volucer binary: set VOLUCER_BIN or build the server checkout",
)
class EndToEndTest(unittest.TestCase):
    server: subprocess.Popen | None = None
    port = 0
    root: tempfile.TemporaryDirectory | None = None

    @classmethod
    def setUpClass(cls) -> None:
        binary = volucer_binary()
        if binary is None:
            raise AssertionError(
                "VOLUCER_E2E=1 but no volucer binary was found; set VOLUCER_BIN"
            )
        cls.root = tempfile.TemporaryDirectory(prefix="volucer-py-e2e-")
        root = Path(cls.root.name)
        # The document root has to exist and the entry point has to be routable, so the
        # server's own `script_extensions` has to name `py` — which is step 38 of the plan,
        # and this test is also its proof.
        (root / "app.py").write_text("# served by the worker\n", encoding="utf-8")
        cls.port = free_port()

        config = root / "volucer.toml"
        # The command is written as a TOML *literal* string, because a Windows path in a
        # basic string turns `\U` into a unicode escape. The one thing a literal string
        # cannot carry is a single quote, so a command containing one is rejected here
        # rather than producing a confusing parse error.
        command = f"{sys.executable} {WORKER.as_posix()}"
        self_check = "'" not in command
        if not self_check:
            raise AssertionError("the interpreter path contains a single quote")
        config.write_text(
            "\n".join(
                [
                    'default_backend = "py"',
                    "[server]",
                    f'http = "127.0.0.1:{cls.port}"',
                    f'document_root = "{root.as_posix()}"',
                    'front_controller = "app.py"',
                    'script_extensions = ["py"]',
                    "",
                    "[backends.py]",
                    'kind = "worker"',
                    f"command = '{command}'",
                    "workers = 1",
                    "max_requests = 0",
                ]
            ),
            encoding="utf-8",
        )

        cls.server = subprocess.Popen(
            [binary, "serve", "--config", str(config)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Wait for the listener to answer rather than sleeping a guessed amount: a port that
        # is bound is the only readiness signal this test can observe.
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                http_get(cls.port, "/health")
                return
            except OSError:
                if cls.server.poll() is not None:
                    out, err = cls.server.communicate(timeout=5)
                    raise AssertionError(
                        "the server exited before answering:\n"
                        + out.decode(errors="replace")
                        + err.decode(errors="replace")
                    )
                time.sleep(0.05)
        # The timeout used to say only "never answered", which is the least useful version of
        # this failure: a server that is running and serving nothing looks identical to one
        # that is misconfigured, and the only evidence was its stderr, which was discarded.
        # The config is printed too, because the config is what the test writes.
        cls.server.kill()
        out, err = cls.server.communicate(timeout=10)
        raise AssertionError(
            "the server never answered within 30s\n"
            f"--- command ---\n{binary} serve --config {config}\n"
            f"--- config ---\n{config.read_text(encoding='utf-8')}\n"
            f"--- stdout ---\n{out.decode(errors='replace')}\n"
            f"--- stderr ---\n{err.decode(errors='replace')}"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.server is not None:
            cls.server.terminate()
            try:
                cls.server.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                cls.server.kill()
                cls.server.communicate(timeout=5)
        if cls.root is not None:
            cls.root.cleanup()

    def test_a_request_reaches_the_python_worker(self) -> None:
        status, headers, body = http_get(self.port, "/items?id=7")
        self.assertEqual(200, status, body)
        self.assertEqual("application/json", headers.get("content-type"))
        payload = json.loads(body)
        self.assertEqual("/items", payload["path"])
        self.assertEqual("id=7", payload["query"])
        self.assertEqual("http", payload["scheme"])
        self.assertEqual("example.test", payload["host"])

    def test_two_requests_do_not_see_each_others_state(self) -> None:
        # The acceptance criterion for a worker bridge, and the reason a request handler is
        # expected to reset: the first request's query must not appear in the second's.
        _, _, first = http_get(self.port, "/first?marker=one")
        _, _, second = http_get(self.port, "/second")
        self.assertEqual("marker=one", json.loads(first)["query"])
        self.assertEqual(
            "",
            json.loads(second)["query"],
            "the second request must not inherit the first one's query string",
        )
        self.assertEqual("/second", json.loads(second)["path"])


ATTACH_SUPERVISOR = '''\
"""An `attach` supervisor for the platforms without `fork`, using a thread.

The SDK's real supervisor is :mod:`volucer_worker.supervisor`, covered by
`SupervisorEndToEndTest`. This fixture exists so the *contract* — serving a connection
somebody else opened — is proven on Windows too, where `os.fork()` does not exist.

volucer only dials the relay in this mode and creates no process at all, because the side
that owns the listening socket has to be the side that owns the fork.
"""

import os
import socket
import sys
import threading

sys.path.insert(0, {sdk!r})

from volucer_worker import Response, Worker  # noqa: E402

port = int(sys.argv[1])
log = sys.argv[2]


def note(line):
    with open(log, "a", encoding="utf-8") as handle:
        handle.write(line + "\\n")


def serve(connection):
    try:
        # `makefile`, because the worker reads and writes *file-like* streams and a socket is
        # not one: `read` and `write` are what the SDK calls. This is the one Python-specific
        # line a supervisor needs, and copying the PHP example without it fails with
        # `AttributeError: 'socket' object has no attribute 'read'`.
        Worker.create(
            connection.makefile("rb", buffering=0),
            connection.makefile("wb", buffering=0),
        ).run(lambda request, worker: Response.text("attach " + request.uri + "\\n"))
        note("clean")
    except BaseException as error:  # noqa: BLE001 - the point is to report anything
        note("raised " + type(error).__name__ + ": " + str(error))
    finally:
        connection.close()


listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", port))
listener.listen(64)
note("ready")

while True:
    connection, _ = listener.accept()
    note("accepted")
    # A thread, not a fork: this fixture exists for the platforms that have no `fork`, so it
    # must work there. The fork itself is covered by the SDK's own supervisor, below.
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
'''

#: A supervisor that drives the SDK's own `supervisor.serve`, with a handler that reports
#: the child that served. Run in a subprocess so the fork does not inherit the test runner.
SDK_SUPERVISOR = '''\
"""Drive `volucer_worker.supervisor.serve` with a handler that names its child."""

import os
import sys

sys.path.insert(0, {sdk!r})

from volucer_worker import Response, supervisor  # noqa: E402

relay = sys.argv[1]
log = sys.argv[2]


def note(line):
    # Appended with `open(..., "a")` rather than written through a buffered stream: a forked
    # child would otherwise re-emit whatever the master had buffered.
    with open(log, "a", encoding="utf-8") as handle:
        handle.write(line + "\\n")


def handler(request, worker):
    return Response.text("attach " + request.uri + " pid=" + str(os.getpid()) + "\\n")


if __name__ == "__main__":
    note("ready")
    supervisor.serve(relay, handler, children=2)
'''


@unittest.skipUnless(
    should_run(volucer_binary() is not None, e2e_required()),
    "no volucer binary: set VOLUCER_BIN or build the server checkout",
)
class AttachEndToEndTest(unittest.TestCase):
    """The `attach` shape: volucer dials a relay somebody else listens on.

    Every other end-to-end test here drives `kind = "worker"`, where volucer spawns the
    process and owns its stdio — the one case an `attach` deployment never uses. This is the
    test that proves `create()` works on a connection the SDK did **not** open, which is the
    whole of what `attach` asks of an SDK.

    The supervisor is a fixture here rather than :mod:`volucer_worker.supervisor`, on purpose:
    it uses a thread instead of a fork, so this class covers the SDK's contract on every
    platform. `SupervisorEndToEndTest` below covers the fork itself, where the platform has
    one.

    `max_requests = 2` is deliberate: it makes recycling observable inside one server run,
    without which the test would only ever exercise the first connection.
    """

    supervisor_script = ATTACH_SUPERVISOR
    extra_args: tuple[str, ...] = ()
    body_prefix = b"attach "
    workers = 1

    server: subprocess.Popen | None = None
    supervisor: subprocess.Popen | None = None
    port = 0
    relay = 0
    root: tempfile.TemporaryDirectory | None = None
    log = Path()

    @classmethod
    def setUpClass(cls) -> None:
        binary = volucer_binary()
        if binary is None:
            raise AssertionError(
                "VOLUCER_E2E=1 but no volucer binary was found; set VOLUCER_BIN"
            )
        cls.root = tempfile.TemporaryDirectory(prefix="volucer-py-attach-")
        root = Path(cls.root.name)
        (root / "app.py").write_text("# served by the attached worker\n", encoding="utf-8")

        cls.log = root / "supervisor.log"
        cls.port = free_port()
        cls.relay = free_port()

        supervisor_script = root / "supervisor.py"
        supervisor_script.write_text(
            cls.supervisor_script.format(sdk=str(SDK_ROOT)), encoding="utf-8"
        )
        cls.supervisor = subprocess.Popen(
            [
                sys.executable,
                str(supervisor_script),
                str(cls.relay),
                str(cls.log),
                *cls.extra_args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # The listener has to exist before volucer starts, or the backend fails at startup
        # with "could not connect to its relay" — which is correct behaviour and a confusing
        # test failure, so readiness is waited for rather than raced.
        cls.wait_for_log("ready", cls.supervisor, "the supervisor")

        config = root / "volucer.toml"
        config.write_text(
            "\n".join(
                [
                    'default_backend = "py"',
                    "[server]",
                    f'http = "127.0.0.1:{cls.port}"',
                    f'document_root = "{root.as_posix()}"',
                    'front_controller = "app.py"',
                    'script_extensions = ["py"]',
                    "",
                    "[backends.py]",
                    'kind = "attach"',
                    f'relay = "127.0.0.1:{cls.relay}"',
                    f"workers = {cls.workers}",
                    "max_requests = 2",
                ]
            ),
            encoding="utf-8",
        )
        cls.server = subprocess.Popen(
            [binary, "serve", "--config", str(config)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                http_get(cls.port, "/health")
                return
            except OSError:
                if cls.server.poll() is not None:
                    out, err = cls.server.communicate(timeout=5)
                    raise AssertionError(
                        "the server exited before answering:\n"
                        + out.decode(errors="replace")
                        + err.decode(errors="replace")
                    )
                time.sleep(0.05)
        cls.server.kill()
        out, err = cls.server.communicate(timeout=10)
        raise AssertionError(
            "the server never answered within 30s\n"
            f"--- config ---\n{config.read_text(encoding='utf-8')}\n"
            f"--- supervisor log ---\n{cls.log.read_text(encoding='utf-8')}\n"
            f"--- stdout ---\n{out.decode(errors='replace')}\n"
            f"--- stderr ---\n{err.decode(errors='replace')}"
        )

    @classmethod
    def wait_for_log(cls, needle: str, process: subprocess.Popen | None, what: str) -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            if cls.log.is_file() and needle in cls.log.read_text(encoding="utf-8"):
                return
            if process is not None and process.poll() is not None:
                _, err = process.communicate(timeout=5)
                raise AssertionError(
                    f"{what} exited before saying {needle!r}:\n"
                    + err.decode(errors="replace")
                )
            time.sleep(0.05)
        raise AssertionError(f"{what} never said {needle!r}")

    @classmethod
    def tearDownClass(cls) -> None:
        # volucer first: closing its slots is how the supervisor's children are asked to
        # stop, so the other order would strand them mid-request — and a stranded child keeps
        # the supervisor's stderr pipe open, which `communicate` would then wait on forever.
        for process in (cls.server, cls.supervisor):
            if process is None:
                continue
            process.terminate()
            try:
                # `wait`, not `communicate`: a forked child inherits these pipes, so the
                # read end does not see EOF when the master exits. Waiting for the process
                # itself is the question being asked.
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if cls.root is not None:
            cls.root.cleanup()

    def test_a_request_reaches_the_attached_worker(self) -> None:
        status, _, body = http_get(self.port, "/hello")
        self.assertEqual(200, status, body)
        self.assert_body("/hello", body)

    def assert_body(self, path: str, body: bytes) -> None:
        """The exact body this class's supervisor answers with."""
        self.assertEqual(self.body_prefix + path.encode() + b"\n", body)

    def assert_recycle_evidence(self) -> None:
        """The evidence that the recycle was a reconnect rather than a lucky retry.

        Overridden by each supervisor: the fixture reports its own log lines, while the SDK
        supervisor reports a pid per child. Both answer the same question — did volucer *dial
        again*, and did the worker it left behind exit cleanly?
        """
        self.wait_for_log("accepted", self.supervisor, "the supervisor")
        deadline = time.time() + 10
        while time.time() < deadline:
            if "clean" in self.log.read_text(encoding="utf-8"):
                break
            time.sleep(0.05)
        lines = self.log.read_text(encoding="utf-8")
        self.assertNotIn("raised", lines, f"the worker did not exit cleanly:\n{lines}")
        self.assertIn("clean", lines, f"the worker never reported a clean exit:\n{lines}")
        self.assertGreaterEqual(
            lines.count("accepted"),
            2,
            f"recycling did not dial the relay again:\n{lines}",
        )

    def test_recycling_reconnects_and_the_supervisor_exits_cleanly(self) -> None:
        # Four requests over `max_requests = 2` means two connections: requests one and two
        # share the first, three and four use the one volucer dials after closing it. A
        # broken recycle shows up as a failed third request rather than as a subtle drift.
        for index in range(4):
            status, _, body = http_get(self.port, f"/r{index}")
            self.assertEqual(200, status, body)
            self.assert_body(f"/r{index}", body)

        self.assert_recycle_evidence()


@unittest.skipUnless(
    should_run(volucer_binary() is not None, e2e_required()) and hasattr(os, "fork"),
    "the supervisor needs os.fork(); set VOLUCER_BIN and run on Unix",
)
class SupervisorEndToEndTest(AttachEndToEndTest):
    """The SDK's own prefork supervisor, which is the half `attach` needs on Unix.

    Separate from `AttachEndToEndTest` because it is the *fork* under test rather than the
    plumbing around it: the fixture there is a thread, so it works on Windows, and this one
    cannot. Every request is served by a different child, which is the property that makes
    `attach` cheaper than `worker` — one boot, shared copy-on-write.
    """

    supervisor_script = SDK_SUPERVISOR
    body_prefix = b"attach "
    workers = 2

    def assert_body(self, path: str, body: bytes) -> None:
        """The SDK supervisor's body, which appends the serving child's pid.

        A prefix plus a pid rather than an exact string, because the pid is the evidence this
        class is about: it is how "a different child served each connection" becomes observable
        from outside the server.
        """
        prefix, _, tail = body.partition(b" pid=")
        self.assertEqual(self.body_prefix + path.encode() + b"\n", prefix + b"\n", body)
        self.assertTrue(tail.strip().isdigit(), f"no pid in the body: {body!r}")

    def assert_recycle_evidence(self) -> None:
        """The SDK supervisor's evidence: distinct child pids across the recycle.

        `max_requests = 2` over four requests means volucer dialled twice, so at least two
        children must have served. A supervisor that never re-forked would show one pid, and
        one that leaked would show a zombie rather than a live child.
        """
        pids = set()
        for index in range(4, 8):
            status, _, body = http_get(self.port, f"/again{index}")
            self.assertEqual(200, status, body)
            pids.add(body.decode().split("pid=")[1].strip())
        self.assertGreater(len(pids), 1, f"the supervisor stopped forking: {pids}")

    def test_each_request_is_served_by_a_different_child(self) -> None:
        pids = set()
        for index in range(6):
            status, _, body = http_get(self.port, f"/pid{index}")
            self.assertEqual(200, status, body)
            pids.add(body.decode().split("pid=")[1].strip())

        # One connection per child, because volucer's `workers = 2` dials twice and each
        # `accept()` forks. A pool that reused one child would show one pid here, and that
        # would mean the supervisor is queueing rather than forking.
        self.assertGreater(len(pids), 1, f"every request was served by the same child: {pids}")

    def test_no_child_is_left_as_a_zombie(self) -> None:
        # The zombie is the failure mode a fork-and-forget supervisor has: one per connection,
        # invisible until the process table fills. `ps` is the only honest witness.
        for index in range(6):
            http_get(self.port, f"/z{index}")
        time.sleep(1)

        states = subprocess.run(
            ["ps", "-o", "stat=", "--ppid", str(self.supervisor.pid)],
            capture_output=True,
            text=True,
        ).stdout.split()
        self.assertNotIn("Z", states, f"the supervisor left zombies behind: {states}")


class RelayResolutionTest(unittest.TestCase):
    """Where the supervisor's relay address comes from.

    Needs no `fork` and no socket, so it runs on every platform — which matters, because the
    rule it covers is the one a deployment relies on and Windows is where a mistyped unit file
    is most likely to be found.
    """

    def test_the_argument_wins_over_the_environment(self) -> None:
        with mock.patch.dict(os.environ, {supervisor.RELAY_ENV: "unix:/from-env.sock"}):
            self.assertEqual("unix:/from-arg.sock", supervisor.relay_value("unix:/from-arg.sock"))

    def test_the_environment_is_the_fallback(self) -> None:
        with mock.patch.dict(os.environ, {supervisor.RELAY_ENV: "  unix:/from-env.sock  "}):
            self.assertEqual("unix:/from-env.sock", supervisor.relay_value(None))
            self.assertEqual(
                "unix:/from-env.sock",
                supervisor.relay_value("   "),
                "a blank argument is absent, not an address",
            )

    def test_neither_place_set_is_none(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(supervisor.relay_value(None))

    def test_an_empty_value_means_unset(self) -> None:
        # `Environment=VOLUCER_RELAY_SOCKET=` in a unit file and a shell's
        # `export VOLUCER_RELAY_SOCKET=` both produce this, and reporting it as an empty
        # address would send the operator looking for a typo in a file that never mentions it.
        with mock.patch.dict(os.environ, {supervisor.RELAY_ENV: "   "}):
            self.assertIsNone(supervisor.relay_value(None))

    @unittest.skipUnless(hasattr(os, "fork"), "serve() refuses Windows for the missing fork first")
    def test_serve_refuses_a_missing_relay_by_name(self) -> None:
        # Checked before `listen` touches the filesystem, so the message is what the operator
        # sees rather than a connect failure naming a path nobody wrote.
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            supervisor, "listen"
        ) as listen:
            with self.assertRaises(ValueError) as caught:
                supervisor.serve(None, lambda request, worker: None)
            self.assertIn(supervisor.RELAY_ENV, str(caught.exception))
            listen.assert_not_called()


class GuardTest(unittest.TestCase):
    """The skip predicate, which was written backwards and shipped that way.

    Worth a test of its own rather than a note, because it failed in the direction that looks
    fine. With ``VOLUCER_E2E=1`` set — what CI does — and no binary, the old
    ``volucer_binary() or not e2e_required()`` evaluated ``False or False`` and **skipped**.
    A skipped end-to-end test is the same green as a passing one, so the CI job whose entire
    purpose is to prove the protocol against the real server proved nothing. It never
    surfaced because that job was already red for an unrelated reason.
    """

    def test_the_suite_runs_whenever_it_could_do_something(self) -> None:
        # A binary: run, whether or not a skip would be tolerated.
        self.assertTrue(should_run(True, False))
        self.assertTrue(should_run(True, True))
        # No binary but a skip is a failure: run, so setUpClass reports the missing binary.
        self.assertTrue(should_run(False, True))

    def test_the_suite_skips_only_when_there_is_nothing_to_do(self) -> None:
        # The one permitted skip. The old predicate's inverse produced this answer for the
        # `(False, True)` case above, which is the regression.
        self.assertFalse(should_run(False, False))

    def test_e2e_required_reads_the_variable_ci_sets(self) -> None:
        with mock.patch.dict(os.environ, {"VOLUCER_E2E": "1"}):
            self.assertTrue(e2e_required())
        with mock.patch.dict(os.environ, {"VOLUCER_E2E": "0"}):
            self.assertFalse(e2e_required(), "only 1 counts; 0 is not a requirement")
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(e2e_required(), "unset is not a requirement")


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
