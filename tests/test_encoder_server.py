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

import errno
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


class TestServerSession:
    def test_stream_size_matches_encoder_total_size(self, fx_session, fx_expected):
        _, _, expected_stream_size = fx_expected
        assert fx_session.stream_size == expected_stream_size


class TestHandleConnectionMetadata:
    def test_get_metadata_returns_full_static(self, fx_session, fx_expected):
        spec, expected_static, _ = fx_expected
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
            assert got_stream_size == fx_session.stream_size
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
    def test_full_stream_read_matches_session_size(self, fx_session):
        """The encoder's full byte stream matches ``stream_size``.

        We do not byte-compare against the on-disk golden here: for
        BGEN the on-disk file and the encoder both emit level-0 blocks
        with the same fixed layout, but a parametric byte-compare
        belongs in the apps suite. This test pins that the read loop
        reaches EOF exactly at ``stream_size``.
        """
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(
                encoder_protocol.pack_read_request(0, fx_session.stream_size)
            )
            status, body = _read_status_and_body(parent)
            assert status == fx_session.stream_size
            assert len(body) == fx_session.stream_size
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_chunked_read(self, fx_session):
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
            assert sum(len(c) for c in chunks) == fx_session.stream_size
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

    def test_concurrent_stream_reads_independent_state(self, fx_session, tmp_path):
        """Each connection runs in its own server thread with its own
        encoder; full reads on distinct connections see byte-identical
        streams regardless of interleaving."""
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
                            encoder_protocol.pack_read_request(
                                0, fx_session.stream_size
                            )
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
            assert len(reference) == fx_session.stream_size
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
