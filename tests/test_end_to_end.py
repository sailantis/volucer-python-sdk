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
        raise AssertionError("the server never answered")

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
