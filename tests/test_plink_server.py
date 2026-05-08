"""Tests for the plink-server module.

Covers ``_handle_connection`` (per-connection synchronous loop) and
``serve_forever`` (accept loop with select-based stop signal). All
fast tests run against ``socket.socketpair`` / a local listener; one
real-subprocess smoke test exercises ``_server_main`` to validate
the multiprocessing fd-passing handshake.
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
from vcztools import cli as vcztools_cli
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import plink_protocol, plink_server


@pytest.fixture
def fx_golden_dir(tmp_path, fx_small_vcz):
    """Materialised PLINK fileset for fx_small_vcz, used as byte-identity
    reference."""
    golden = tmp_path / "golden"
    golden.mkdir()
    write_plink(make_reader(str(fx_small_vcz.path)), golden / "small")
    return golden, "small"


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


@pytest.fixture
def fx_session(fx_reader):
    return plink_server._ServerSession(fx_reader)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if len(chunk) == 0:
            raise EOFError(f"socket closed after {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


def _read_status_and_body(sock: socket.socket) -> tuple[int, bytes]:
    status = plink_protocol.parse_status(
        _recv_exact(sock, plink_protocol.REPLY_STATUS_SIZE)
    )
    if status <= 0:
        return status, b""
    return status, _recv_exact(sock, status)


def _spawn_handle_connection(
    session: plink_server._ServerSession,
) -> tuple[socket.socket, threading.Thread]:
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    thread = threading.Thread(
        target=plink_server._handle_connection,
        args=(child_sock, session),
        daemon=True,
    )
    thread.start()
    return parent_sock, thread


class TestServerSession:
    def test_metadata_sizes_match(self, fx_session, fx_golden_dir, fx_small_vcz):
        golden, basename = fx_golden_dir
        assert len(fx_session.bim_bytes) == (golden / f"{basename}.bim").stat().st_size
        assert len(fx_session.fam_bytes) == (golden / f"{basename}.fam").stat().st_size
        bytes_per_variant = (fx_small_vcz.num_samples + 3) // 4
        assert fx_session.bed_size == 3 + fx_small_vcz.num_variants * bytes_per_variant


class TestHandleConnectionMetadata:
    def test_get_metadata_returns_full_static(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(plink_protocol.pack_get_metadata_request())
            status, body = _read_status_and_body(parent)
            expected_body_size = (
                plink_protocol.META_HEADER_SIZE
                + len(fx_session.bim_bytes)
                + len(fx_session.fam_bytes)
            )
            assert status == expected_body_size
            bim_size, fam_size, bed_size = plink_protocol.parse_metadata_header(
                body[: plink_protocol.META_HEADER_SIZE]
            )
            assert bim_size == (golden / f"{basename}.bim").stat().st_size
            assert fam_size == (golden / f"{basename}.fam").stat().st_size
            assert bed_size == fx_session.bed_size
            offset = plink_protocol.META_HEADER_SIZE
            assert (
                body[offset : offset + bim_size]
                == (golden / f"{basename}.bim").read_bytes()
            )
            offset += bim_size
            assert (
                body[offset : offset + fam_size]
                == (golden / f"{basename}.fam").read_bytes()
            )
        finally:
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()


class TestHandleConnectionRead:
    def test_full_bed_read_matches_golden(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            parent.sendall(plink_protocol.pack_read_request(0, len(expected)))
            status, body = _read_status_and_body(parent)
            assert status == len(expected)
            assert body == expected
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_chunked_bed_read(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            chunks = []
            offset = 0
            block = 4096
            while True:
                parent.sendall(plink_protocol.pack_read_request(offset, block))
                _, body = _read_status_and_body(parent)
                if len(body) == 0:
                    break
                chunks.append(body)
                offset += len(body)
            assert b"".join(chunks) == expected
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
    """Errors raised while constructing the per-connection ``BedEncoder``
    must surface as an errno reply on the wire — the parent layer relies
    on this to translate failures into a real ``OSError`` rather than an
    unexplained EOF."""

    def test_oserror_returns_matching_errno(self, fx_session, monkeypatch):
        class _BoomEncoder:
            def __init__(self, *args, **kwargs):
                raise OSError(errno.EACCES, "boom")

        monkeypatch.setattr(plink_server.vcztools_plink, "BedEncoder", _BoomEncoder)
        parent, thread = _spawn_handle_connection(fx_session)
        try:
            status, body = _read_status_and_body(parent)
            assert status == -errno.EACCES
            assert body == b""
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            parent.close()

    def test_other_exception_returns_eio(self, fx_session, monkeypatch):
        class _BoomEncoder:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("not an OSError")

        monkeypatch.setattr(plink_server.vcztools_plink, "BedEncoder", _BoomEncoder)
        parent, thread = _spawn_handle_connection(fx_session)
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
        sock_path = tmp_path / "plink.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(8)
        return listener, sock_path

    def _start_server_thread(
        self, listener: socket.socket, session: plink_server._ServerSession
    ) -> tuple[socket.socket, threading.Thread]:
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        thread = threading.Thread(
            target=plink_server.serve_forever,
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
                        s.sendall(plink_protocol.pack_get_metadata_request())
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

    def test_concurrent_bed_reads_independent_state(
        self, fx_session, fx_golden_dir, tmp_path
    ):
        """Each ``BedConnection`` runs in its own server thread with its
        own encoder; reads on distinct connections see byte-identical
        bed bytes regardless of interleaving."""
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()

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
                        # All threads start their reads at roughly the
                        # same time so the server has multiple in-flight
                        # connections, demonstrating concurrent threads.
                        barrier.wait(timeout=5)
                        s.sendall(plink_protocol.pack_read_request(0, len(expected)))
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
            for body in results.values():
                assert body == expected
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


class TestServerMainStartupFailure:
    """``_server_main`` must catch its own startup exceptions so the
    multiprocessing subprocess exits cleanly (no Python traceback printed
    to stderr) when the VCZ cannot be served — e.g. multi-allelic
    input."""

    def test_multiallelic_subprocess_exits_cleanly(self, fx_multiallelic_vcz, tmp_path):
        sock_path = tmp_path / "plink.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(8)
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=plink_server._server_main,
            args=(
                listener,
                child_stop,
                str(fx_multiallelic_vcz.path),
                vcztools_cli.ViewPlinkOptions(),
                vcztools_cli.LogConfig(),
            ),
        )
        proc.start()
        listener.close()
        child_stop.close()
        try:
            proc.join(timeout=30)
            assert not proc.is_alive(), "subprocess did not exit"
            assert proc.exitcode == 0, (
                f"subprocess exited with {proc.exitcode}; expected 0 "
                "(_server_main should catch startup errors and return)"
            )
            client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_sock.settimeout(1)
            with pytest.raises((ConnectionRefusedError, FileNotFoundError)):
                client_sock.connect(str(sock_path))
            client_sock.close()
        finally:
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

    def test_spawn_metadata_handshake(self, fx_small_vcz, tmp_path):
        sock_path = tmp_path / "plink.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(8)
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=plink_server._server_main,
            args=(
                listener,
                child_stop,
                str(fx_small_vcz.path),
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
                client_sock.sendall(plink_protocol.pack_get_metadata_request())
                status, body = _read_status_and_body(client_sock)
                assert status > 0
                bim_size, fam_size, bed_size = plink_protocol.parse_metadata_header(
                    body[: plink_protocol.META_HEADER_SIZE]
                )
                assert bim_size > 0
                assert fam_size > 0
                assert bed_size > 0
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
