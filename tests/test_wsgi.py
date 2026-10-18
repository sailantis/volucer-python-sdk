"""The WSGI adapter: the half of a Python worker that is new work.

The three values that are easy to get wrong — ``wsgi.url_scheme`` from the frame's `https`
flag, ``PATH_INFO`` re-encoded as latin-1 per PEP 3333, and ``wsgi.input`` being a real
file-like object — are asserted here rather than left to a framework to discover.
"""

from __future__ import annotations

import unittest

from volucer_worker import wsgi
from volucer_worker.message import Request, Response


class EnvironTest(unittest.TestCase):
    def test_the_cgi_core(self) -> None:
        request = Request(
            method="POST",
            uri="/submit?page=2&q=a+b",
            protocol="HTTP/1.0",
            headers=[
                ("Host", "example.test:8443"),
                ("Content-Type", "application/json"),
                ("Content-Length", "2"),
                ("Accept", "application/json"),
                ("X-Forwarded-For", "10.0.0.1"),
            ],
            remote_addr="127.0.0.1",
            body=b"{}",
        )
        env = wsgi.environ(request)

        self.assertEqual("POST", env["REQUEST_METHOD"])
        self.assertEqual("/submit", env["PATH_INFO"])
        self.assertEqual("page=2&q=a+b", env["QUERY_STRING"])
        self.assertEqual("HTTP/1.0", env["SERVER_PROTOCOL"])
        self.assertEqual("example.test", env["SERVER_NAME"])
        self.assertEqual("8443", env["SERVER_PORT"])
        self.assertEqual("127.0.0.1", env["REMOTE_ADDR"])
        self.assertEqual("application/json", env["CONTENT_TYPE"])
        self.assertEqual("2", env["CONTENT_LENGTH"])
        self.assertEqual("application/json", env["HTTP_ACCEPT"])
        self.assertEqual("10.0.0.1", env["HTTP_X_FORWARDED_FOR"])
        # `HTTP_HOST` is set as well, which is what a WSGI server does: `SERVER_NAME` and
        # `SERVER_PORT` are the split form and `HTTP_HOST` is the verbatim header. A
        # framework picks whichever it documents.
        self.assertEqual("example.test:8443", env["HTTP_HOST"])

    def test_the_scheme_comes_from_the_frame_not_from_a_header(self) -> None:
        # The child process has no socket, so this flag is the only evidence there is. A
        # deployment behind TLS that reported `http` here would generate `http://` links to
        # pages nobody can reach.
        secure = wsgi.environ(Request(uri="/", https=True))
        self.assertEqual("https", secure["wsgi.url_scheme"])
        self.assertEqual("443", secure["SERVER_PORT"], "the scheme's default port")

        plain = wsgi.environ(Request(uri="/"))
        self.assertEqual("http", plain["wsgi.url_scheme"])
        self.assertEqual("80", plain["SERVER_PORT"])

    def test_path_info_is_re_encoded_as_latin_1(self) -> None:
        # PEP 3333 is specific and surprising: PATH_INFO is the decoded path re-encoded as
        # latin-1 so arbitrary bytes survive. `%C3%A9` is the UTF-8 encoding of `é`, so the
        # bytes are `\xc3\xa9` and the latin-1 decoding of them is `Ã©` — which is not a
        # bug: `env["PATH_INFO"].encode("latin-1")` gives the original bytes back, and that
        # is what the framework's own decoding step expects.
        env = wsgi.environ(Request(uri="/caf%C3%A9/men%C3%BC"))
        self.assertEqual("/cafÃ©/menÃ¼", env["PATH_INFO"])
        self.assertEqual(
            "/café/menü".encode("utf-8").decode("latin-1"),
            env["PATH_INFO"],
            "the round trip through latin-1 preserves the client's bytes",
        )
        self.assertEqual(
            "/café/menü",
            env["PATH_INFO"].encode("latin-1").decode("utf-8"),
            "a framework that re-decodes gets the path the client asked for",
        )

    def test_a_plus_in_a_path_stays_a_plus(self) -> None:
        env = wsgi.environ(Request(uri="/a+b"))
        self.assertEqual("/a+b", env["PATH_INFO"])

    def test_wsgi_input_is_a_real_file_like_object(self) -> None:
        env = wsgi.environ(Request(uri="/", body=b"line one\nline two"))
        stream = env["wsgi.input"]
        self.assertEqual(b"line one\n", stream.readline())
        self.assertEqual(b"line two", stream.read())
        self.assertEqual(b"", stream.read())

    def test_wsgi_input_rewinds(self) -> None:
        env = wsgi.environ(Request(uri="/", body=b"abc"))
        stream = env["wsgi.input"]
        self.assertEqual(b"abc", stream.read())
        stream.seek(0)
        self.assertEqual(b"abc", stream.read())

    def test_one_request_in_flight_so_not_multithreaded(self) -> None:
        env = wsgi.environ(Request(uri="/"))
        self.assertFalse(env["wsgi.multithread"])
        self.assertFalse(env["wsgi.run_once"])
        self.assertEqual((1, 0), env["wsgi.version"])

    def test_a_body_without_a_content_length_still_reports_one(self) -> None:
        env = wsgi.environ(Request(uri="/", body=b"abc"))
        self.assertEqual("3", env["CONTENT_LENGTH"])

    def test_no_body_and_no_header_means_no_content_length(self) -> None:
        env = wsgi.environ(Request(uri="/"))
        self.assertNotIn("CONTENT_LENGTH", env)

    def test_script_filename_is_prefix_not_a_cgi_variable(self) -> None:
        # The protocol documents routing metadata as "not a file to execute", so exposing it
        # as SCRIPT_FILENAME would be exactly the name that invites an application to open it.
        env = wsgi.environ(Request(uri="/", script_filename="/srv/app/app.py"))
        self.assertEqual("/srv/app/app.py", env["volucer.script_filename"])
        self.assertNotIn("SCRIPT_FILENAME", env)


class HostSplitTest(unittest.TestCase):
    def test_an_explicit_port_wins(self) -> None:
        self.assertEqual(("example.test", "8443"), wsgi.split_host("example.test:8443", False))

    def test_a_bare_name_takes_the_schemes_default(self) -> None:
        self.assertEqual(("example.test", "80"), wsgi.split_host("example.test", False))
        self.assertEqual(("example.test", "443"), wsgi.split_host("example.test", True))

    def test_a_missing_host_falls_back(self) -> None:
        self.assertEqual((wsgi.DEFAULT_SERVER_NAME, "80"), wsgi.split_host(None, False))

    def test_an_ipv6_literal_keeps_its_brackets(self) -> None:
        self.assertEqual(("[::1]", "8080"), wsgi.split_host("[::1]:8080", False))
        self.assertEqual(("[::1]", "80"), wsgi.split_host("[::1]", False))


class CallTest(unittest.TestCase):
    def application(self, environ, start_response):  # type: ignore[no-untyped-def]
        body = f"{environ['REQUEST_METHOD']} {environ['PATH_INFO']}".encode()
        start_response(
            "201 Created",
            [
                ("Content-Type", "text/plain"),
                ("Set-Cookie", "a=1"),
                ("Set-Cookie", "b=2"),
            ],
        )
        return [body[:3], body[3:]]

    def test_a_wsgi_application_becomes_a_response(self) -> None:
        handler = wsgi.call(self.application)
        response = handler(Request(method="GET", uri="/items"), None)
        self.assertIsInstance(response, Response)
        self.assertEqual(201, response.status)
        self.assertEqual(b"GET /items", response.body)
        cookies = [value for name, value in response.headers if name == "Set-Cookie"]
        self.assertEqual(
            ["a=1", "b=2"], cookies, "two Set-Cookie headers are two cookies"
        )

    def test_an_application_that_never_calls_start_response_is_an_error(self) -> None:
        handler = wsgi.call(lambda environ, start_response: [b"x"])
        with self.assertRaises(RuntimeError):
            handler(Request(uri="/"), None)

    def test_a_second_start_response_is_an_error(self) -> None:
        def twice(environ, start_response):  # type: ignore[no-untyped-def]
            start_response("200 OK", [])
            start_response("200 OK", [])
            return []

        with self.assertRaises(RuntimeError):
            wsgi.call(twice)(Request(uri="/"), None)

    def test_a_non_bytes_chunk_is_an_error(self) -> None:
        def text(environ, start_response):  # type: ignore[no-untyped-def]
            start_response("200 OK", [])
            return ["not bytes"]

        with self.assertRaises(TypeError):
            wsgi.call(text)(Request(uri="/"), None)

    def test_the_body_is_closed(self) -> None:
        closed: list[bool] = []

        class Body:
            def __iter__(self):
                return iter([b"x"])

            def close(self):
                closed.append(True)

        def app(environ, start_response):  # type: ignore[no-untyped-def]
            start_response("200 OK", [])
            return Body()

        wsgi.call(app)(Request(uri="/"), None)
        self.assertEqual([True], closed, "a WSGI application's body must be closed")


if __name__ == "__main__":
    unittest.main()
