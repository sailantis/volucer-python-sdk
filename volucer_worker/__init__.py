"""A Python worker SDK for volucer's worker protocol.

The specification is ``docs/worker-protocol.md`` in `sailantis/volucer`; this package is
the other end of it, beside the PHP one in ``sailantis/volucer-php-sdk``.

The ordinary use is three lines::

    from volucer_worker import Worker, wsgi
    from myapp import application

    Worker.connect().run(wsgi.call(application))

Importing this package **rebinds ``sys.stdout``**, and that is deliberate rather than
surprising once the reason is known: the worker protocol uses the child process's stdout
as its channel, so anything an application prints would land inside a frame and
desynchronise the stream permanently. The PHP SDK solves the same problem with an output
buffer; Python's answer is to move stdout somewhere harmless and keep the real stream
private to the worker.
"""

from __future__ import annotations

import sys

from .message import Job, Request, Response
from .protocol import ProtocolError
from .worker import Worker, real_stdout, runtime, socket_address
from . import grpc, wsgi

__version__ = "0.1.0"

__all__ = [
    "Job",
    "ProtocolError",
    "Request",
    "Response",
    "Worker",
    "grpc",
    "runtime",
    "real_stdout",
    "socket_address",
    "wsgi",
    "__version__",
]


def _protect_stdout() -> None:
    """Rebind ``sys.stdout`` away from the protocol channel.

    Called at import, before a :class:`Worker` is constructed, because the constructor reads
    ``sys.stdout.buffer`` as the channel: rebinding first and constructing second is the
    order that makes the two impossible to confuse.

    The replacement writes to ``sys.stderr``, which is not part of the protocol and is
    drained by the supervisor. An application's ``print`` therefore still reaches a human,
    just not the wire.
    """
    if getattr(sys.stdout, "_volucer_guard", False):
        return

    class _GuardedStdout:
        """A ``sys.stdout`` that cannot corrupt the worker channel."""

        _volucer_guard = True

        def write(self, text: str) -> int:
            sys.stderr.write(text)
            return len(text)

        def writelines(self, lines) -> None:
            for line in lines:
                self.write(line)

        def flush(self) -> None:
            sys.stderr.flush()

        def isatty(self) -> bool:
            return False

        def fileno(self) -> int:
            # Deliberately the *real* stderr descriptor rather than stdout's, so a library
            # that does fd-level writes (a logging handler wrapping the stream, say) lands
            # in the same place as everything else.
            return sys.stderr.fileno()

        @property
        def encoding(self) -> str:
            return getattr(sys.stderr, "encoding", "utf-8") or "utf-8"

        @property
        def errors(self) -> str:
            return getattr(sys.stderr, "errors", "strict") or "strict"

        @property
        def buffer(self):  # type: ignore[no-untyped-def]
            return sys.stderr.buffer

    sys.stdout = _GuardedStdout()  # type: ignore[assignment]


_protect_stdout()
