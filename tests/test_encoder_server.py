"""Tests for the encoder-server module.

Covers ``_handle_connection`` (per-connection synchronous loop) and
``serve_forever`` (accept loop with select-based stop signal). All
fast tests run against ``socket.socketpair`` / a local listener; one
real-subprocess smoke test exercises ``_server_main`` to validate
the multiprocessing fd-passing handshake.

Tests parametrise over both :data:`biofuse.formats.PLINK_SPEC` and
:data:`biofuse.formats.BGEN_SPEC` so the duck-typed dispatch is
exercised end-to-end for both output formats.
"""

import dataclasses
import errno
import logging
import multiprocessing as mp
import os
import pathlib
import socket
import struct
import threading
import time

import pytest
from vcztools import bgen as vcztools_bgen
from vcztools import cli as vcztools_cli
from vcztools import plink as vcztools_plink
from vcztools.cli import make_reader

from biofuse import encoder_protocol, encoder_server, formats


@pytest.fixture(params=[formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"])
def fx_spec(request):
    return request.param


@pytest.fixture
def fx_expected(fx_reader, fx_spec):
    """Spec-direct reference outputs.

    Returns ``(spec, static_files, stream_size)`` produced by calling
    ``spec.build_static_files`` / the throwaway encoder directly. This is
    the source of truth for the server's behaviour; on-disk goldens via
    ``write_plink`` / ``write_bgen`` are not used here because BGEN's
    ``write_bgen`` defaults to a compressed (non-fixed-size) layout that
    deliberately differs from the encoder's level-0 random-access layout,
    and SQLite ``.bgi`` files written in separate calls have
    non-deterministic page metadata.
    """
    static_files = fx_spec.build_static_files(fx_reader)
    with fx_spec.encoder_factory(fx_reader) as encoder:
        stream_size = int(encoder.total_size)
    return fx_spec, static_files, stream_size


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


@pytest.fixture
def fx_session(fx_reader, fx_spec):
    return encoder_server._ServerSession(fx_reader, fx_spec)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if len(chunk) == 0:
            raise EOFError(f"socket closed after {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


def _read_status_and_body(sock: socket.socket) -> tuple[int, bytes]:
    status = encoder_protocol.parse_status(
        _recv_exact(sock, encoder_protocol.REPLY_STATUS_SIZE)
    )
    if status <= 0:
        return status, b""
    return status, _recv_exact(sock, status)


def _spawn_handle_connection(
    session: encoder_server._ServerSession,
) -> tuple[socket.socket, threading.Thread]:
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    thread = threading.Thread(
        target=encoder_server._handle_connection,
        args=(child_sock, session),
        daemon=True,
    )
    thread.start()
    return parent_sock, thread


class _FakeEncoder:
    """In-memory encoder stand-in for direct unit tests of the
    per-tag reply helpers. ``read(off, size)`` returns deterministic
    bytes so callers can assert exact payloads."""

    total_size = 4096

    def __init__(self):
        self.calls: list[tuple[int, int]] = []

    def read(self, off: int, size: int) -> bytes:
        self.calls.append((off, size))
        return bytes((off + i) & 0xFF for i in range(size))


def _spawn_serve_connection(
    session: encoder_server._ServerSession,
) -> tuple[socket.socket, threading.Thread]:
    """Run ``_serve_connection`` in a daemon thread with a fresh
    encoder bound to the thread. Returns ``(parent_sock, thread)``;
    caller closes ``parent_sock`` and joins ``thread``. The child
    socket is closed by the target wrapper once the loop exits."""
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)

    def target():
        try:
            with session.spec.encoder_factory(session.reader) as encoder:
                encoder_server._serve_connection(
                    child_sock, session, encoder, "test-thread"
                )
        finally:
            try:
                child_sock.close()
            except OSError:
                pass

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return parent_sock, thread


class TestMakeErrorReply:
    def test_oserror_uses_exc_errno_without_logging(self, caplog):
        exc = OSError(errno.ENOENT, "missing")
        with caplog.at_level(logging.DEBUG, logger="biofuse.encoder_server"):
            reply = encoder_server._make_error_reply(exc, "ctx")
        assert encoder_protocol.parse_status(reply) == -errno.ENOENT
        emitted = [r for r in caplog.records if r.name == "biofuse.encoder_server"]
        assert emitted == []

    def test_non_oserror_uses_eio_and_logs_error_plus_debug_traceback(self, caplog):
        exc = ValueError("boom")
        with caplog.at_level(logging.DEBUG, logger="biofuse.encoder_server"):
            reply = encoder_server._make_error_reply(exc, "during dispatch")
        assert encoder_protocol.parse_status(reply) == -errno.EIO
        encoder_records = [
            r for r in caplog.records if r.name == "biofuse.encoder_server"
        ]
        error_records = [r for r in encoder_records if r.levelno == logging.ERROR]
        assert len(error_records) == 1
        msg = error_records[0].getMessage()
        assert "during dispatch" in msg
        assert f"errno {errno.EIO}" in msg
        assert "boom" in msg
        debug_records = [
            r
            for r in encoder_records
            if r.levelno == logging.DEBUG
            and "during dispatch traceback" in r.getMessage()
        ]
        assert len(debug_records) == 1
        assert debug_records[0].exc_info is not None


class TestMakeMetadataReply:
    def test_happy_path_matches_expected_static(self, fx_session, fx_expected):
        spec, expected_static, expected_stream_size = fx_expected
        reply = encoder_server._make_metadata_reply(fx_session)
        status = encoder_protocol.parse_status(
            reply[: encoder_protocol.REPLY_STATUS_SIZE]
        )
        body = reply[encoder_protocol.REPLY_STATUS_SIZE :]
        assert status == len(body)
        n_static, stream_size = encoder_protocol.parse_metadata_prefix(
            body[: encoder_protocol.META_PREFIX_SIZE]
        )
        assert n_static == len(spec.static_suffixes)
        assert stream_size == expected_stream_size
        sizes_size = n_static * encoder_protocol.META_SIZE_ENTRY_SIZE
        sizes = encoder_protocol.parse_static_sizes(
            body[
                encoder_protocol.META_PREFIX_SIZE : encoder_protocol.META_PREFIX_SIZE
                + sizes_size
            ],
            n_static,
        )
        for suffix, size in zip(spec.static_suffixes, sizes, strict=True):
            assert size == len(expected_static[suffix])

    def test_missing_suffix_raises_value_error(self, fx_reader, fx_spec):
        bad_spec = dataclasses.replace(fx_spec, build_static_files=lambda reader: {})
        session = encoder_server._ServerSession(fx_reader, bad_spec)
        with pytest.raises(ValueError, match="returned keys"):
            encoder_server._make_metadata_reply(session)

    def test_extra_suffix_raises_value_error(self, fx_reader, fx_spec):
        original_build = fx_spec.build_static_files
        bad_spec = dataclasses.replace(
            fx_spec,
            build_static_files=lambda r: {**original_build(r), ".unexpected": b"x"},
        )
        session = encoder_server._ServerSession(fx_reader, bad_spec)
        with pytest.raises(ValueError, match="returned keys"):
            encoder_server._make_metadata_reply(session)


class TestMakeReadReply:
    def test_full_payload_returns_packed_reply(self):
        # ``_make_read_reply`` is called after the tag has already been
        # consumed from the wire — send only the payload bytes.
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        encoder = _FakeEncoder()
        try:
            parent.sendall(struct.pack("<QQ", 100, 32))
            reply = encoder_server._make_read_reply(child, encoder, "test")
        finally:
            parent.close()
            child.close()
        assert reply is not None
        status = encoder_protocol.parse_status(
            reply[: encoder_protocol.REPLY_STATUS_SIZE]
        )
        assert status == 32
        body = reply[encoder_protocol.REPLY_STATUS_SIZE :]
        assert body == bytes((100 + i) & 0xFF for i in range(32))
        assert encoder.calls == [(100, 32)]

    def test_clean_eof_before_payload_returns_none(self):
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        encoder = _FakeEncoder()
        try:
            parent.close()
            reply = encoder_server._make_read_reply(child, encoder, "test")
        finally:
            child.close()
        assert reply is None
        assert encoder.calls == []


class TestSendReply:
    def test_success_returns_true_and_sends_bytes(self):
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            ok = encoder_server._send_reply(child, b"hello world")
            received = parent.recv(64)
        finally:
            parent.close()
            child.close()
        assert ok is True
        assert received == b"hello world"

    def test_oserror_returns_false_and_warns(self, caplog):
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        child.close()
        try:
            with caplog.at_level(logging.WARNING, logger="biofuse.encoder_server"):
                ok = encoder_server._send_reply(child, b"hello")
        finally:
            parent.close()
        assert ok is False
        warnings = [
            r.getMessage()
            for r in caplog.records
            if r.name == "biofuse.encoder_server" and r.levelno == logging.WARNING
        ]
        assert any("send failed" in m for m in warnings)


class TestServeConnection:
    """Branch tests for ``_serve_connection``. The happy paths
    (TAG_GET_METADATA, TAG_READ, unknown tag, clean EOF on tag,
    truncated TAG_READ payload) are exercised end-to-end via
    ``TestHandleConnection*`` below; this class covers the dispatch
    exception, mid-frame EOF, and send-failure branches that the
    integration tests don't reach."""

    def test_dispatch_exception_replies_errno_and_loop_continues(
        self, fx_reader, fx_spec
    ):
        bad_spec = dataclasses.replace(fx_spec, build_static_files=lambda reader: {})
        session = encoder_server._ServerSession(fx_reader, bad_spec)
        parent, thread = _spawn_serve_connection(session)
        try:
            for _ in range(2):
                parent.sendall(encoder_protocol.pack_get_metadata_request())
                status = encoder_protocol.parse_status(
                    _recv_exact(parent, encoder_protocol.REPLY_STATUS_SIZE)
                )
                assert status == -errno.EIO
        finally:
            parent.close()
            thread.join(timeout=5)
        assert not thread.is_alive()

    def test_tag_recv_eoferror_logs_and_returns(self, fx_session, monkeypatch, caplog):
        # ``_recv_exact_sync(sock, 1)`` cannot actually raise EOFError
        # (a 1-byte read either yields 1 byte or signals clean EOF
        # with n=0). The catch in ``_serve_connection`` is defensive;
        # monkeypatch the helper to fire it so we pin the behaviour.
        def fake_recv(sock, n):
            raise EOFError("simulated mid-frame EOF")

        monkeypatch.setattr(encoder_server, "_recv_exact_sync", fake_recv)
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            with caplog.at_level(logging.WARNING, logger="biofuse.encoder_server"):
                with fx_session.spec.encoder_factory(fx_session.reader) as encoder:
                    encoder_server._serve_connection(child, fx_session, encoder, "test")
        finally:
            parent.close()
            child.close()
        warnings = [
            r.getMessage()
            for r in caplog.records
            if r.name == "biofuse.encoder_server" and r.levelno == logging.WARNING
        ]
        assert any("frame error" in m for m in warnings)

    def test_read_reply_none_terminates_loop_without_reply(self, fx_session):
        parent, thread = _spawn_serve_connection(fx_session)
        try:
            parent.sendall(encoder_protocol.TAG_READ)
            parent.shutdown(socket.SHUT_WR)
            # No reply expected; the server should observe clean EOF
            # inside the payload recv and return without sending.
            trailing = parent.recv(1)
            assert trailing == b""
        finally:
            parent.close()
            thread.join(timeout=5)
        assert not thread.is_alive()

    def test_send_failure_terminates_loop(self, fx_session, monkeypatch):
        # Drive the ``not _send_reply(...)`` branch deterministically
        # by monkeypatching the helper to return False. A real send
        # failure (e.g. EPIPE) is covered as a unit by
        # ``TestSendReply.test_oserror_returns_false_and_warns``.
        monkeypatch.setattr(encoder_server, "_send_reply", lambda sock, reply: False)
        parent, thread = _spawn_serve_connection(fx_session)
        try:
            parent.sendall(encoder_protocol.pack_get_metadata_request())
            thread.join(timeout=5)
        finally:
            parent.close()
        assert not thread.is_alive()


class TestHandleConnectionMetadata:
    def test_get_metadata_returns_full_static(self, fx_session, fx_expected):
        spec, expected_static, expected_stream_size = fx_expected
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(encoder_protocol.pack_get_metadata_request())
            status, body = _read_status_and_body(parent)
            n_static = len(spec.static_suffixes)
            sizes_size = n_static * encoder_protocol.META_SIZE_ENTRY_SIZE
            static_sizes = [
                len(expected_static[suffix]) for suffix in spec.static_suffixes
            ]
            expected_body_size = (
                encoder_protocol.META_PREFIX_SIZE + sizes_size + sum(static_sizes)
            )
            assert status == expected_body_size
            got_n_static, got_stream_size = encoder_protocol.parse_metadata_prefix(
                body[: encoder_protocol.META_PREFIX_SIZE]
            )
            assert got_n_static == n_static
            assert got_stream_size == expected_stream_size
            sizes_start = encoder_protocol.META_PREFIX_SIZE
            sizes_end = sizes_start + sizes_size
            sizes = encoder_protocol.parse_static_sizes(
                body[sizes_start:sizes_end], n_static
            )
            assert sizes == tuple(static_sizes)
            offset = sizes_end
            for suffix, size in zip(spec.static_suffixes, sizes, strict=True):
                assert size == len(expected_static[suffix])
                # ``.bgi`` SQLite bytes can differ across independent writes
                # in non-payload header fields, so compare sizes only for
                # the .bgi entry; per-byte parity is covered in the apps
                # suite by reading via the live mount.
                if suffix != ".bgen.bgi":
                    assert body[offset : offset + size] == expected_static[suffix]
                offset += size
        finally:
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()


class TestHandleConnectionRead:
    def test_full_stream_read_matches_session_size(self, fx_session, fx_expected):
        """The encoder's full byte stream matches the expected
        ``total_size``.

        We do not byte-compare against the on-disk golden here: for
        BGEN the on-disk file and the encoder both emit level-0 blocks
        with the same fixed layout, but a parametric byte-compare
        belongs in the apps suite. This test pins that the read loop
        reaches EOF exactly at the expected stream size.
        """
        _, _, expected_stream_size = fx_expected
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(encoder_protocol.pack_read_request(0, expected_stream_size))
            status, body = _read_status_and_body(parent)
            assert status == expected_stream_size
            assert len(body) == expected_stream_size
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_chunked_read(self, fx_session, fx_expected):
        _, _, expected_stream_size = fx_expected
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            chunks = []
            offset = 0
            block = 4096
            while True:
                parent.sendall(encoder_protocol.pack_read_request(offset, block))
                _, body = _read_status_and_body(parent)
                if len(body) == 0:
                    break
                chunks.append(body)
                offset += len(body)
            assert sum(len(c) for c in chunks) == expected_stream_size
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_unknown_tag_terminates_loop(self, fx_session):
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(b"Z")
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            parent.close()

    def test_eof_terminates_loop(self, fx_session):
        parent, thread = _spawn_handle_connection(fx_session)
        parent.close()
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_partial_read_payload_terminates_loop(self, fx_session):
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(b"R" + struct.pack("<Q", 1))  # truncated payload
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            try:
                parent.close()
            except OSError:
                pass


class TestHandleConnectionEncoderConstructionFailure:
    """Errors raised while constructing the per-connection encoder
    must surface as an errno reply on the wire — the parent layer relies
    on this to translate failures into a real ``OSError`` rather than an
    unexplained EOF."""

    def test_plink_oserror_returns_matching_errno(self, fx_reader, monkeypatch):
        class _BoomEncoder:
            def __init__(self, *args, **kwargs):
                raise OSError(errno.EACCES, "boom")

        session = encoder_server._ServerSession(fx_reader, formats.PLINK_SPEC)
        monkeypatch.setattr(vcztools_plink, "BedEncoder", _BoomEncoder)
        parent, thread = _spawn_handle_connection(session)
        try:
            status, body = _read_status_and_body(parent)
            assert status == -errno.EACCES
            assert body == b""
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            parent.close()

    def test_bgen_oserror_returns_matching_errno(self, fx_reader, monkeypatch):
        class _BoomEncoder:
            def __init__(self, *args, **kwargs):
                raise OSError(errno.EACCES, "boom")

        session = encoder_server._ServerSession(fx_reader, formats.BGEN_SPEC)
        monkeypatch.setattr(vcztools_bgen, "BgenEncoder", _BoomEncoder)
        parent, thread = _spawn_handle_connection(session)
        try:
            status, body = _read_status_and_body(parent)
            assert status == -errno.EACCES
            assert body == b""
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            parent.close()

    def test_plink_other_exception_returns_eio(self, fx_reader, monkeypatch):
        class _BoomEncoder:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("not an OSError")

        session = encoder_server._ServerSession(fx_reader, formats.PLINK_SPEC)
        monkeypatch.setattr(vcztools_plink, "BedEncoder", _BoomEncoder)
        parent, thread = _spawn_handle_connection(session)
        try:
            status, body = _read_status_and_body(parent)
            assert status == -errno.EIO
            assert body == b""
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            parent.close()


class TestServeForever:
    def _bind_listener(
        self, tmp_path: pathlib.Path
    ) -> tuple[socket.socket, pathlib.Path]:
        sock_path = tmp_path / "encoder.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(8)
        return listener, sock_path

    def _start_server_thread(
        self, listener: socket.socket, session: encoder_server._ServerSession
    ) -> tuple[socket.socket, threading.Thread]:
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        thread = threading.Thread(
            target=encoder_server.serve_forever,
            args=(listener, child_stop, session),
            daemon=True,
        )
        thread.start()
        return parent_stop, thread

    def test_concurrent_connections_each_get_metadata(self, fx_session, tmp_path):
        listener, sock_path = self._bind_listener(tmp_path)
        parent_stop, server_thread = self._start_server_thread(listener, fx_session)
        try:
            replies: list[bytes] = []
            errors: list[BaseException] = []
            lock = threading.Lock()

            def client_worker():
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.connect(str(sock_path))
                    try:
                        s.sendall(encoder_protocol.pack_get_metadata_request())
                        _, body = _read_status_and_body(s)
                        with lock:
                            replies.append(body)
                    finally:
                        s.close()
                except BaseException as exc:  # noqa: BLE001
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=client_worker) for _ in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            assert errors == []
            assert len(replies) == 4
            assert all(r == replies[0] for r in replies)
        finally:
            parent_stop.close()
            server_thread.join(timeout=5)
            assert not server_thread.is_alive()

    def test_concurrent_stream_reads_independent_state(
        self, fx_session, fx_expected, tmp_path
    ):
        """Each connection runs in its own server thread with its own
        encoder; full reads on distinct connections see byte-identical
        streams regardless of interleaving."""
        _, _, expected_stream_size = fx_expected
        listener, sock_path = self._bind_listener(tmp_path)
        parent_stop, server_thread = self._start_server_thread(listener, fx_session)
        try:
            n = 4
            results: dict[int, bytes] = {}
            errors: list[BaseException] = []
            lock = threading.Lock()
            barrier = threading.Barrier(n)

            def worker(tid):
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.connect(str(sock_path))
                    try:
                        barrier.wait(timeout=5)
                        s.sendall(
                            encoder_protocol.pack_read_request(0, expected_stream_size)
                        )
                        _, body = _read_status_and_body(s)
                        with lock:
                            results[tid] = body
                    finally:
                        s.close()
                except BaseException as exc:  # noqa: BLE001
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
            assert errors == []
            assert len(results) == n
            reference = results[0]
            assert len(reference) == expected_stream_size
            for body in results.values():
                assert body == reference
        finally:
            parent_stop.close()
            server_thread.join(timeout=5)
            assert not server_thread.is_alive()

    def test_stop_signal_exits_accept_loop(self, fx_session, tmp_path):
        listener, sock_path = self._bind_listener(tmp_path)
        parent_stop, server_thread = self._start_server_thread(listener, fx_session)
        # Server is in select(); close parent's stop end → child wakes.
        parent_stop.close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()
        # The listener socket should now be closed; new connects fail.
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(1)
        with pytest.raises(ConnectionRefusedError):
            s.connect(str(sock_path))


class TestServerMainHandshakeFailure:
    """Static-file build failures (e.g. multi-allelic input) surface at
    handshake time as errno replies on the wire. The subprocess must
    keep running through a failed handshake and exit cleanly when
    stop-signalled."""

    @pytest.mark.parametrize(
        "spec", [formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"]
    )
    def test_multiallelic_handshake_returns_errno(
        self, fx_multiallelic_vcz, tmp_path, spec
    ):
        sock_path = tmp_path / "encoder.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(8)
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=encoder_server._server_main,
            args=(
                listener,
                child_stop,
                str(fx_multiallelic_vcz.path),
                spec,
                vcztools_cli.ViewPlinkOptions(),
                vcztools_cli.LogConfig(),
            ),
        )
        proc.start()
        listener.close()
        child_stop.close()
        parent_stop_closed = False
        try:
            client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_sock.settimeout(30)
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    client_sock.connect(str(sock_path))
                    break
                except (FileNotFoundError, ConnectionRefusedError):
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)
            client_sock.sendall(encoder_protocol.pack_get_metadata_request())
            status_buf = _recv_exact(client_sock, encoder_protocol.REPLY_STATUS_SIZE)
            status = encoder_protocol.parse_status(status_buf)
            assert status < 0, f"expected errno reply, got status={status}"
            assert -status == errno.EIO
            client_sock.close()
            parent_stop.close()
            parent_stop_closed = True
            proc.join(timeout=10)
            assert not proc.is_alive(), "subprocess did not exit after stop signal"
            assert proc.exitcode == 0, (
                f"subprocess exited with {proc.exitcode}; expected 0"
            )
        finally:
            if not parent_stop_closed:
                parent_stop.close()
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
            try:
                os.unlink(sock_path)
            except OSError:
                pass


class TestServerMainSmoke:
    """End-to-end check that ``_server_main`` runs in a real subprocess."""

    @pytest.mark.parametrize(
        "spec", [formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"]
    )
    def test_spawn_metadata_handshake(self, fx_small_vcz, tmp_path, spec):
        sock_path = tmp_path / "encoder.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(8)
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=encoder_server._server_main,
            args=(
                listener,
                child_stop,
                str(fx_small_vcz.path),
                spec,
                vcztools_cli.ViewPlinkOptions(),
                vcztools_cli.LogConfig(),
            ),
        )
        proc.start()
        listener.close()
        child_stop.close()
        try:
            # Connect with a brief retry while the server's accept loop
            # spins up.
            deadline = time.monotonic() + 10
            client_sock = None
            while time.monotonic() < deadline:
                try:
                    client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    client_sock.connect(str(sock_path))
                    break
                except OSError:
                    if client_sock is not None:
                        client_sock.close()
                    client_sock = None
                    time.sleep(0.05)
            assert client_sock is not None, "could not connect to spawned server"
            try:
                client_sock.sendall(encoder_protocol.pack_get_metadata_request())
                status, body = _read_status_and_body(client_sock)
                assert status > 0
                n_static, stream_size = encoder_protocol.parse_metadata_prefix(
                    body[: encoder_protocol.META_PREFIX_SIZE]
                )
                assert n_static == len(spec.static_suffixes)
                assert stream_size > 0
            finally:
                client_sock.close()
        finally:
            parent_stop.close()
            proc.join(timeout=10)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
            assert not proc.is_alive()
            assert proc.exitcode == 0
            try:
                os.unlink(sock_path)
            except OSError:
                pass
