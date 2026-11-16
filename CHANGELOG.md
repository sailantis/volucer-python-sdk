# Changelog

All notable changes to `volucer-worker` (the Python SDK) are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

Versions are also tagged in git, and the tag is what publishes to PyPI — a change listed here as
unreleased is not installed by anyone until the tag exists.

## [Unreleased]

## [0.2.0] - 2026-11-16

Installed by `pip install volucer-worker` from this tag. A **minor** release: the wire protocol
gains a capability the server opts into, and nothing a worker application does changes.

### Added

- **A request body whose length was not declared is read to `LAST`.** The SDK now sets
  `stream_request_body` in its `HELLO_ACK`, which lets the server forward a body as the client
  produces it instead of buffering it first. The server only omits `body_len` from a `REQUEST` for
  a worker that advertised this, because an SDK that predates the field would read an absent
  length as `0` and take the first `BODY` frame for its own response — so an older worker is never
  sent a streamed body, and this cannot break one.

  `_read_body` therefore accepts `body_len: int | None`. `None` means streamed, with `LAST` the
  only end signal; an explicit `0` stays a correctly empty body, and the two are not folded
  together. A declared length is still authoritative, so a body that ends early is still refused.

  A handler never sees the difference: `request.body` is one `bytes` object either way. The
  saving is the server's, which no longer holds every concurrent upload in memory at once.

- **A note on where a job consumer lives.** `Worker.connect().consume(handler)` is a separate
  process started through `[jobs.pool]`; the attach supervisor that `Worker.run()` serves does not
  consume jobs. The README now says so, with the configuration that starts one.

### Fixed

- **The documented job outcomes were wrong.** `consume()` said `None` acknowledges, but the code
  acknowledges on any value that is not `False` and not a `str` — so a handler returning its own
  result is done with the job either way. The old wording sent an operator looking for a bug that
  was not there. The docstring and README table now match the code, which is what the protocol
  specification already said.

## [0.1.0] - 2026-10-23

First release: the worker protocol, the request/response exchange, the call plane, and the
attach supervisor.

[Unreleased]: https://github.com/sailantis/volucer-python-sdk/compare/v0.2.0...main
[0.2.0]: https://github.com/sailantis/volucer-python-sdk/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sailantis/volucer-python-sdk/releases/tag/v0.1.0
