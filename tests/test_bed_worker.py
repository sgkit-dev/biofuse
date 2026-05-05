"""Tests for the bed-worker module.

Covers three layers without ever spawning a subprocess (except for one
smoke test that exercises the real spawn handshake):

- ``_StaticBytesFile`` — the in-memory backend for ``.bim``/``.fam``.
- ``WorkerSession`` — the in-process logic that owns ``VczReader``
  and the open-handle table. Most of the parity tests live here.
- ``serve()`` — the multi-threaded request/reply loop, exercised
  against an in-process ``socket.socketpair`` running on a thread.
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

BED = bed_protocol.FileType.BED
BIM = bed_protocol.FileType.BIM
FAM = bed_protocol.FileType.FAM


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
    return bed_worker.WorkerSession(fx_reader)


class TestStaticBytesFile:
    def test_full_read(self):
        f = bed_worker._StaticBytesFile(b"hello world")
        assert f.read(0, 100) == b"hello world"

    def test_partial_read(self):
        f = bed_worker._StaticBytesFile(b"hello world")
        assert f.read(6, 5) == b"world"

    def test_size_property(self):
        f = bed_worker._StaticBytesFile(b"hello world")
        assert f.size == 11

    def test_read_past_eof_returns_empty(self):
        f = bed_worker._StaticBytesFile(b"abc")
        assert f.read(3, 10) == b""
        assert f.read(100, 10) == b""

    def test_read_zero_size_returns_empty(self):
        f = bed_worker._StaticBytesFile(b"abc")
        assert f.read(0, 0) == b""

    def test_negative_offset_raises(self):
        f = bed_worker._StaticBytesFile(b"abc")
        with pytest.raises(ValueError, match="off must be >= 0"):
            f.read(-1, 10)

    def test_negative_size_raises(self):
        f = bed_worker._StaticBytesFile(b"abc")
        with pytest.raises(ValueError, match="size must be >= 0"):
            f.read(0, -1)

    def test_close_is_idempotent(self):
        f = bed_worker._StaticBytesFile(b"abc")
        f.close()
        f.close()

    def test_read_after_close_raises(self):
        f = bed_worker._StaticBytesFile(b"abc")
        f.close()
        with pytest.raises(RuntimeError):
            f.read(0, 1)


class TestWorkerSessionListFiles:
    def test_three_entries_in_canonical_order(self, fx_session):
        types = [t for t, _ in fx_session.list_files()]
        assert types == [BED, BIM, FAM]

    def test_bed_size_matches_formula(self, fx_session, fx_small_vcz):
        bytes_per_variant = (fx_small_vcz.num_samples + 3) // 4
        expected = 3 + fx_small_vcz.num_variants * bytes_per_variant
        sizes = dict(fx_session.list_files())
        assert sizes[BED] == expected

    def test_bim_fam_sizes_match_golden(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        sizes = dict(fx_session.list_files())
        assert sizes[BIM] == (golden / f"{basename}.bim").stat().st_size
        assert sizes[FAM] == (golden / f"{basename}.fam").stat().st_size


class TestWorkerSessionOpen:
    def test_open_duplicate_fh_raises(self, fx_session):
        fx_session.open(1, BED)
        with pytest.raises(OSError, match="already open") as excinfo:
            fx_session.open(1, BED)
        assert excinfo.value.errno == errno.EEXIST

    def test_open_distinct_fhs_allowed(self, fx_session):
        fx_session.open(1, BED)
        fx_session.open(2, BED)  # both BedEncoders; independent state

    def test_open_each_file_type(self, fx_session):
        fx_session.open(1, BED)
        fx_session.open(2, BIM)
        fx_session.open(3, FAM)


class TestWorkerSessionRead:
    def test_full_bed_matches_golden(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        fx_session.open(7, BED)
        try:
            assert fx_session.read(7, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(7)

    @pytest.mark.parametrize("block_size", [1, 7, 13, 4096, 65536])
    def test_chunked_bed_matches_golden(self, fx_session, fx_golden_dir, block_size):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        fx_session.open(7, BED)
        try:
            chunks = []
            offset = 0
            while True:
                data = fx_session.read(7, offset, block_size)
                if len(data) == 0:
                    break
                chunks.append(data)
                offset += len(data)
            assert b"".join(chunks) == expected
        finally:
            fx_session.release(7)

    def test_random_pread(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        fx_session.open(7, BED)
        rng = random.Random(11)
        try:
            for _ in range(50):
                offset = rng.randrange(len(expected))
                size = rng.randrange(1, 64)
                got = fx_session.read(7, offset, size)
                assert got == expected[offset : offset + size]
        finally:
            fx_session.release(7)

    def test_read_past_eof_returns_empty(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        bed_size = (golden / f"{basename}.bed").stat().st_size
        fx_session.open(7, BED)
        try:
            assert fx_session.read(7, bed_size, 100) == b""
            assert fx_session.read(7, bed_size + 10_000, 100) == b""
        finally:
            fx_session.release(7)

    def test_bim_full_match(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bim").read_bytes()
        fx_session.open(7, BIM)
        try:
            assert fx_session.read(7, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(7)

    def test_fam_full_match(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.fam").read_bytes()
        fx_session.open(7, FAM)
        try:
            assert fx_session.read(7, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(7)

    def test_unknown_handle_raises_ebadf(self, fx_session):
        with pytest.raises(OSError, match="unknown handle") as excinfo:
            fx_session.read(9999, 0, 10)
        assert excinfo.value.errno == errno.EBADF


class TestWorkerSessionConcurrentHandles:
    def test_two_bed_encoders_independent(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        fx_session.open(1, BED)
        fx_session.open(2, BED)
        try:
            half = len(expected) // 2
            assert fx_session.read(1, 0, half) == expected[:half]
            assert fx_session.read(2, half, half) == expected[half : 2 * half]
            assert fx_session.read(1, half, half) == expected[half : 2 * half]
            assert fx_session.read(2, 0, half) == expected[:half]
        finally:
            fx_session.release(1)
            fx_session.release(2)

    def test_threaded_reads_on_distinct_fhs(self, fx_session, fx_golden_dir):
        """Reads on distinct fhs from concurrent threads each get correct
        bytes."""
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        n_threads = 4
        for fh in range(1, n_threads + 1):
            fx_session.open(fh, BED)
        results: dict[int, bytes] = {}
        errors: list[BaseException] = []

        def worker(fh):
            try:
                # Each thread streams the whole file end-to-end on its own fh.
                got = fx_session.read(fh, 0, len(expected) * 2)
                results[fh] = got
            except BaseException as exc:  # noqa: BLE001 - propagate to assert
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(fh,))
            for fh in range(1, n_threads + 1)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        try:
            assert errors == []
            assert all(results[fh] == expected for fh in range(1, n_threads + 1))
        finally:
            for fh in range(1, n_threads + 1):
                fx_session.release(fh)


class TestWorkerSessionRelease:
    def test_reopen_after_release(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        fx_session.open(1, BED)
        fx_session.read(1, 0, 100)
        fx_session.release(1)
        fx_session.open(1, BED)
        try:
            assert fx_session.read(1, 0, len(expected) * 2) == expected
        finally:
            fx_session.release(1)

    def test_release_unknown_is_silent(self, fx_session):
        fx_session.release(9999)

    def test_release_is_idempotent(self, fx_session):
        fx_session.open(1, BED)
        fx_session.release(1)
        fx_session.release(1)


class TestWorkerSessionClose:
    def test_close_releases_open_handles(self, fx_session):
        fx_session.open(1, BED)
        fx_session.open(2, BIM)
        fx_session.close()
        # Subsequent reads on either handle now hit unknown-handle.
        with pytest.raises(OSError, match="unknown handle") as excinfo:
            fx_session.read(1, 0, 1)
        assert excinfo.value.errno == errno.EBADF
        with pytest.raises(OSError, match="unknown handle") as excinfo:
            fx_session.read(2, 0, 1)
        assert excinfo.value.errno == errno.EBADF


# -- serve loop tests ------------------------------------------------------

# Helpers for sending raw frames to a serve() running in a thread on the
# other end of an in-process socketpair. The tests deliberately use the
# wire protocol directly rather than the BedEncoderClient wrapper so a
# regression in either layer cannot be masked by the other.


def _spawn_serve_thread(
    session: bed_worker.WorkerSession, *, max_workers: int = 4
) -> tuple[socket.socket, threading.Thread]:
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    thread = threading.Thread(
        target=bed_worker.serve,
        args=(child_sock, session),
        kwargs={"max_workers": max_workers},
        daemon=True,
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


def _recv_reply(sock: socket.socket) -> tuple[int, int, bytes]:
    """Read one reply frame; returns (seq, status, body)."""
    header = _recv_exact(sock, bed_protocol.REPLY_HEADER_SIZE)
    seq, status = bed_protocol.parse_reply_header(header)
    body = b""
    if status > 0:
        body = _recv_exact(sock, status)
    return seq, status, body


class TestServe:
    def test_list_request(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_list_request(1))
            seq, status, body = _recv_reply(parent)
            assert seq == 1
            assert status == 3 * bed_protocol.REPLY_LIST_ENTRY_SIZE
            entries = []
            for offset in range(0, len(body), bed_protocol.REPLY_LIST_ENTRY_SIZE):
                entries.append(
                    bed_protocol.parse_list_entry(
                        body[offset : offset + bed_protocol.REPLY_LIST_ENTRY_SIZE]
                    )
                )
            assert [t for t, _ in entries] == [BED, BIM, FAM]
        finally:
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()

    def test_open_read_close_roundtrip(self, fx_session, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_open_request(10, 42, BED))
            seq, status, _ = _recv_reply(parent)
            assert (seq, status) == (10, 0)

            parent.sendall(bed_protocol.pack_read_request(11, 42, 0, len(expected)))
            seq, status, body = _recv_reply(parent)
            assert seq == 11
            assert status == len(expected)
            assert body == expected

            parent.sendall(bed_protocol.pack_close_request(12, 42))
            seq, status, _ = _recv_reply(parent)
            assert (seq, status) == (12, 0)
        finally:
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()

    def test_open_duplicate_fh_returns_eexist(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_open_request(1, 1, BED))
            seq, status, _ = _recv_reply(parent)
            assert (seq, status) == (1, 0)
            parent.sendall(bed_protocol.pack_open_request(2, 1, BED))
            seq, status, _ = _recv_reply(parent)
            assert seq == 2
            assert status == -errno.EEXIST
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_read_unknown_handle_returns_ebadf(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            parent.sendall(bed_protocol.pack_read_request(7, 9999, 0, 10))
            seq, status, _ = _recv_reply(parent)
            assert seq == 7
            assert status == -errno.EBADF
        finally:
            parent.close()
            thread.join(timeout=5)

    def test_unknown_tag_terminates_loop(self, fx_session):
        parent, thread = _spawn_serve_thread(fx_session)
        try:
            # Valid 9-byte header but with an unknown tag byte.
            parent.sendall(struct.pack("<Q", 0) + b"Z")
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
            # Truncated READ payload: full 9-byte header + only 8 bytes
            # of the 24-byte READ payload.
            parent.sendall(struct.pack("<Q", 0) + b"R" + struct.pack("<Q", 1))
            parent.close()
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            try:
                parent.close()
            except OSError:
                pass

    def test_concurrent_reads_complete(self, fx_session, fx_golden_dir):
        """Pipelined reads on distinct fhs all return correctly even when
        sent back-to-back without waiting for individual replies."""
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        parent, thread = _spawn_serve_thread(fx_session, max_workers=4)
        try:
            n = 4
            # Open 4 fhs (sequentially is fine; opens are cheap).
            for i in range(n):
                parent.sendall(bed_protocol.pack_open_request(100 + i, i + 1, BED))
                seq, status, _ = _recv_reply(parent)
                assert (seq, status) == (100 + i, 0)

            # Pipeline 4 full-file READs without awaiting between them.
            for i in range(n):
                parent.sendall(
                    bed_protocol.pack_read_request(200 + i, i + 1, 0, len(expected))
                )

            # Drain replies; they may arrive in any order.
            got: dict[int, bytes] = {}
            for _ in range(n):
                seq, status, body = _recv_reply(parent)
                assert 200 <= seq < 200 + n
                assert status == len(expected)
                got[seq] = body
            assert all(b == expected for b in got.values())

            for i in range(n):
                parent.sendall(bed_protocol.pack_close_request(300 + i, i + 1))
            for _ in range(n):
                seq, status, _ = _recv_reply(parent)
                assert 300 <= seq < 300 + n
                assert status == 0
        finally:
            parent.close()
            thread.join(timeout=10)
            assert not thread.is_alive()


# -- subprocess smoke test -------------------------------------------------


class TestWorkerMainSmoke:
    """End-to-end check that ``_worker_main`` runs in a real subprocess.

    Most worker logic is covered above without ``multiprocessing``;
    this test only validates the spawn handshake.
    """

    def test_spawn_and_list(self, fx_small_vcz):
        ctx = mp.get_context("spawn")
        parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        proc = ctx.Process(
            target=bed_worker._worker_main,
            args=(child_sock, str(fx_small_vcz.path), None),
        )
        proc.start()
        child_sock.close()
        try:
            parent_sock.sendall(bed_protocol.pack_list_request(7))
            header = _recv_exact(parent_sock, bed_protocol.REPLY_HEADER_SIZE)
            seq, status = bed_protocol.parse_reply_header(header)
            assert seq == 7
            assert status == 3 * bed_protocol.REPLY_LIST_ENTRY_SIZE
            _recv_exact(parent_sock, status)
        finally:
            parent_sock.close()
            proc.join(timeout=10)
            assert not proc.is_alive()
            assert proc.exitcode == 0
