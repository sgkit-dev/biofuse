"""Tests for the bed-worker module.

Covers three layers without ever spawning a subprocess:

- ``_StaticBytesFile`` — the in-memory backend for ``.bim``/``.fam``.
- ``WorkerSession`` — the in-process logic that owns ``VczReader`` and
  the open-handle table. Most of the parity tests live here.
- ``serve()`` — the blocking request/reply loop, exercised against an
  in-process ``socket.socketpair`` running on a thread.
"""

import errno
import multiprocessing as mp
import random
import socket
import struct
import threading

import pytest
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import bed_protocol, bed_worker


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
    return bed_worker.WorkerSession(fx_reader, "small")


class TestStaticBytesFile:
    def test_full_read(self):
        f = bed_worker._StaticBytesFile("x.bim", b"hello world")
        assert f.read(0, 100) == b"hello world"

    def test_partial_read(self):
        f = bed_worker._StaticBytesFile("x.bim", b"hello world")
        assert f.read(6, 5) == b"world"

    def test_size_property(self):
        f = bed_worker._StaticBytesFile("x.bim", b"hello world")
        assert f.size == 11
        assert f.name == "x.bim"

    def test_read_past_eof_returns_empty(self):
        f = bed_worker._StaticBytesFile("x.bim", b"abc")
        assert f.read(3, 10) == b""
        assert f.read(100, 10) == b""

    def test_read_zero_size_returns_empty(self):
        f = bed_worker._StaticBytesFile("x.bim", b"abc")
        assert f.read(0, 0) == b""

    def test_negative_offset_raises(self):
        f = bed_worker._StaticBytesFile("x.bim", b"abc")
        with pytest.raises(ValueError, match="off must be >= 0"):
            f.read(-1, 10)

    def test_negative_size_raises(self):
        f = bed_worker._StaticBytesFile("x.bim", b"abc")
        with pytest.raises(ValueError, match="size must be >= 0"):
            f.read(0, -1)

    def test_close_is_idempotent(self):
        f = bed_worker._StaticBytesFile("x.bim", b"abc")
        f.close()
        f.close()

    def test_read_after_close_raises(self):
        f = bed_worker._StaticBytesFile("x.bim", b"abc")
        f.close()
        with pytest.raises(RuntimeError):
            f.read(0, 1)


class TestWorkerSessionListFiles:
    def test_three_entries_sorted(self, fx_session):
        names = [spec.name for spec in fx_session.list_files()]
        assert names == ["small.bed", "small.bim", "small.fam"]

    def test_basename_propagates(self, fx_reader):
        session = bed_worker.WorkerSession(fx_reader, "alt_name")
        names = [spec.name for spec in session.list_files()]
        assert names == ["alt_name.bed", "alt_name.bim", "alt_name.fam"]

    def test_bed_size_matches_formula(self, fx_session, fx_small_vcz):
        bytes_per_variant = (fx_small_vcz.num_samples + 3) // 4
        expected = 3 + fx_small_vcz.num_variants * bytes_per_variant
        bed = next(s for s in fx_session.list_files() if s.name == "small.bed")
        assert bed.size == expected

    def test_bim_fam_sizes_match_golden(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        for ext in (".bim", ".fam"):
            spec = next(
                s for s in fx_session.list_files() if s.name == f"{basename}{ext}"
            )
            assert spec.size == (golden / f"{basename}{ext}").stat().st_size


class TestWorkerSessionOpen:
    def test_open_unknown_raises(self, fx_session):
        with pytest.raises(FileNotFoundError):
            fx_session.open("nope.bed")

    def test_open_returns_distinct_handles(self, fx_session):
        h1, _, _ = fx_session.open("small.bed")
        h2, _, _ = fx_session.open("small.bed")
        assert h1 != h2

    def test_open_size_matches_list_files(self, fx_session):
        sizes_from_list = {s.name: s.size for s in fx_session.list_files()}
        for name in ("small.bed", "small.bim", "small.fam"):
            _, size, _ = fx_session.open(name)
            assert size == sizes_from_list[name]


class TestWorkerSessionRead:
    def test_full_bed_matches_golden(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        handle, _, _ = fx_session.open(f"{basename}.bed")
        try:
            assert fx_session.read(handle, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(handle)

    @pytest.mark.parametrize("block_size", [1, 7, 13, 4096, 65536])
    def test_chunked_bed_matches_golden(
        self, fx_session, fx_golden_dir, block_size
    ):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        handle, _, _ = fx_session.open(f"{basename}.bed")
        try:
            chunks = []
            offset = 0
            while True:
                data = fx_session.read(handle, offset, block_size)
                if len(data) == 0:
                    break
                chunks.append(data)
                offset += len(data)
            assert b"".join(chunks) == expected
        finally:
            fx_session.release(handle)

    def test_random_pread(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        handle, _, _ = fx_session.open(f"{basename}.bed")
        rng = random.Random(11)
        try:
            for _ in range(50):
                offset = rng.randrange(len(expected))
                size = rng.randrange(1, 64)
                got = fx_session.read(handle, offset, size)
                assert got == expected[offset : offset + size]
        finally:
            fx_session.release(handle)

    def test_read_past_eof_returns_empty(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        bed_size = (golden / f"{basename}.bed").stat().st_size
        handle, _, _ = fx_session.open(f"{basename}.bed")
        try:
            assert fx_session.read(handle, bed_size, 100) == b""
            assert fx_session.read(handle, bed_size + 10_000, 100) == b""
        finally:
            fx_session.release(handle)

    def test_bim_full_match(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bim").read_bytes()
        handle, _, _ = fx_session.open(f"{basename}.bim")
        try:
            assert fx_session.read(handle, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(handle)

    def test_fam_full_match(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.fam").read_bytes()
        handle, _, _ = fx_session.open(f"{basename}.fam")
        try:
            assert fx_session.read(handle, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(handle)

    def test_unknown_handle_raises_ebadf(self, fx_session):
        with pytest.raises(OSError, match="unknown handle") as excinfo:
            fx_session.read(9999, 0, 10)
        assert excinfo.value.errno == errno.EBADF


class TestWorkerSessionConcurrentHandles:
    def test_two_bed_encoders_independent(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        h1, _, _ = fx_session.open(f"{basename}.bed")
        h2, _, _ = fx_session.open(f"{basename}.bed")
        try:
            assert h1 != h2
            half = len(expected) // 2
            assert fx_session.read(h1, 0, half) == expected[:half]
            assert fx_session.read(h2, half, half) == expected[half : 2 * half]
            assert fx_session.read(h1, half, half) == expected[half : 2 * half]
            assert fx_session.read(h2, 0, half) == expected[:half]
        finally:
            fx_session.release(h1)
            fx_session.release(h2)


class TestWorkerSessionRelease:
    def test_reopen_after_release(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        handle, _, _ = fx_session.open(f"{basename}.bed")
        fx_session.read(handle, 0, 100)
        fx_session.release(handle)
        handle2, _, _ = fx_session.open(f"{basename}.bed")
        try:
            assert fx_session.read(handle2, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(handle2)

    def test_release_unknown_is_silent(self, fx_session):
        fx_session.release(9999)

    def test_release_is_idempotent(self, fx_session):
        handle, _, _ = fx_session.open("small.bed")
        fx_session.release(handle)
        fx_session.release(handle)


class TestWorkerSessionClose:
    def test_close_releases_open_handles(self, fx_session):
        h1, _, _ = fx_session.open("small.bed")
        h2, _, _ = fx_session.open("small.bim")
        fx_session.close()
        # Subsequent reads on either handle now hit unknown-handle.
        with pytest.raises(OSError, match="unknown handle") as excinfo:
            fx_session.read(h1, 0, 1)
        assert excinfo.value.errno == errno.EBADF
        with pytest.raises(OSError, match="unknown handle") as excinfo:
            fx_session.read(h2, 0, 1)
        assert excinfo.value.errno == errno.EBADF


# -- serve loop tests ------------------------------------------------------

# Helpers for sending raw frames to a serve() running in a thread on the
# other end of an in-process socketpair. The tests deliberately use the
# wire protocol directly rather than the BedEncoderClient wrapper so a
# regression in either layer cannot be masked by the other.


def _spawn_serve_thread(session: bed_worker.WorkerSession) -> tuple[
    socket.socket, threading.Thread
]:
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    thread = threading.Thread(
        target=bed_worker.serve, args=(child_sock, session), daemon=True
    )
    thread.start()
    return parent_sock, thread


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if len(chunk) == 0:
            raise EOFError(f"socket closed after {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


class TestServe:
    def test_list_request(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_list_request())
            status = bed_protocol.parse_status(
                _recv_exact(parent, bed_protocol.REPLY_STATUS_SIZE)
            )
            assert status == 3
            names = []
            for _ in range(status):
                hdr = _recv_exact(
                    parent, bed_protocol.REPLY_LIST_ENTRY_HDR_SIZE
                )
                name_len, _size, _mode = bed_protocol.parse_list_entry_header(hdr)
                names.append(_recv_exact(parent, name_len).decode("utf-8"))
            assert names == ["small.bed", "small.bim", "small.fam"]
        finally:
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()

    def test_open_read_close_roundtrip(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_open_request(f"{basename}.bed"))
            assert bed_protocol.parse_status(
                _recv_exact(parent, bed_protocol.REPLY_STATUS_SIZE)
            ) == 0
            handle, size, _mode = bed_protocol.parse_open_body(
                _recv_exact(parent, bed_protocol.REPLY_OPEN_BODY_SIZE)
            )
            assert size == len(expected)

            parent.sendall(bed_protocol.pack_read_request(handle, 0, len(expected)))
            n = bed_protocol.parse_status(
                _recv_exact(parent, bed_protocol.REPLY_STATUS_SIZE)
            )
            assert n == len(expected)
            assert _recv_exact(parent, n) == expected

            parent.sendall(bed_protocol.pack_close_request(handle))
            assert bed_protocol.parse_status(
                _recv_exact(parent, bed_protocol.REPLY_STATUS_SIZE)
            ) == 0
        finally:
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()

    def test_open_unknown_returns_enoent(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_open_request("nope.bed"))
            status = bed_protocol.parse_status(
                _recv_exact(parent, bed_protocol.REPLY_STATUS_SIZE)
            )
            assert status == -errno.ENOENT
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_read_unknown_handle_returns_ebadf(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_read_request(9999, 0, 10))
            status = bed_protocol.parse_status(
                _recv_exact(parent, bed_protocol.REPLY_STATUS_SIZE)
            )
            assert status == -errno.EBADF
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_unknown_tag_terminates_loop(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(b"Z")  # unknown tag
            # Loop should exit. Reading from the socket after the worker
            # exits returns 0 bytes (EOF). Allow time for thread to exit.
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            parent.close()

    def test_eof_terminates_loop_cleanly(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        parent.close()
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_partial_request_terminates_loop(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(b"R" + struct.pack("<Q", 1))  # truncated READ
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            try:
                parent.close()
            except OSError:
                pass


# -- subprocess smoke test -------------------------------------------------


class TestWorkerMainSmoke:
    """End-to-end check that ``_worker_main`` runs in a real subprocess.

    Most worker logic is covered above without ``multiprocessing``; this
    test only validates the spawn handshake.
    """

    def test_spawn_and_list(self, fx_small_vcz):
        ctx = mp.get_context("spawn")
        parent_sock, child_sock = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        proc = ctx.Process(
            target=bed_worker._worker_main,
            args=(child_sock, str(fx_small_vcz.path), "small", None),
        )
        proc.start()
        child_sock.close()
        try:
            parent_sock.sendall(bed_protocol.pack_list_request())
            status = bed_protocol.parse_status(
                _recv_exact(parent_sock, bed_protocol.REPLY_STATUS_SIZE)
            )
            assert status == 3
            for _ in range(status):
                hdr = _recv_exact(
                    parent_sock, bed_protocol.REPLY_LIST_ENTRY_HDR_SIZE
                )
                name_len, _, _ = bed_protocol.parse_list_entry_header(hdr)
                _recv_exact(parent_sock, name_len)
        finally:
            parent_sock.close()
            proc.join(timeout=10)
            assert not proc.is_alive()
            assert proc.exitcode == 0
