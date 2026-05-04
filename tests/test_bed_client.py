"""Tests for BedEncoderClient.

The fast tests pair the client with a ``serve()`` thread running on the
other end of an in-process ``socket.socketpair`` — the trio side of
the protocol is exercised without paying spawn-startup cost. A single
``TestRealSubprocess`` test runs the same flow over a real
``multiprocessing`` subprocess to validate the spawn handshake and
clean shutdown.
"""

import errno
import multiprocessing as mp
import random
import socket
import threading

import pytest
import trio
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import bed_client, bed_worker


@pytest.fixture
def fx_golden_dir(tmp_path, fx_small_vcz):
    golden = tmp_path / "golden"
    golden.mkdir()
    write_plink(make_reader(str(fx_small_vcz.path)), golden / "small")
    return golden, "small"


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


class _FakeProc:
    """Stand-in for a multiprocessing.Process when serve() runs in a thread."""

    def __init__(self, thread: threading.Thread) -> None:
        self._thread = thread

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)

    def terminate(self) -> None:
        # Threads can't be terminated; the loop exits on socket EOF, which
        # the client triggers via send_eof on its end of the socketpair.
        pass

    def kill(self) -> None:
        pass


async def _client_with_thread_worker(
    reader, basename: str
) -> bed_client.BedEncoderClient:
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    session = bed_worker.WorkerSession(reader, basename)
    thread = threading.Thread(
        target=bed_worker.serve, args=(child_sock, session), daemon=True
    )
    thread.start()
    stream = trio.SocketStream(trio.socket.from_stdlib_socket(parent_sock))
    return await bed_client.BedEncoderClient._from_stream(stream, _FakeProc(thread))


@pytest.fixture
async def fx_client(fx_reader):
    client = await _client_with_thread_worker(fx_reader, "small")
    try:
        yield client
    finally:
        await client.aclose()


class TestHandshake:
    async def test_file_entries_populated(self, fx_client):
        names = sorted(spec.name for spec in fx_client.file_entries)
        assert names == ["small.bed", "small.bim", "small.fam"]

    async def test_file_entries_match_golden_sizes(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        sizes = {spec.name: spec.size for spec in fx_client.file_entries}
        for ext in (".bed", ".bim", ".fam"):
            assert sizes[f"{basename}{ext}"] == (
                golden / f"{basename}{ext}"
            ).stat().st_size

    async def test_file_entries_returns_a_copy(self, fx_client):
        a = fx_client.file_entries
        a.clear()
        assert len(fx_client.file_entries) == 3


class TestOpenReadRelease:
    async def test_full_bed_read_matches_golden(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        handle, size, _ = await fx_client.open(f"{basename}.bed")
        assert size == len(expected)
        data = await fx_client.read(handle, 0, len(expected) * 2)
        await fx_client.release(handle)
        assert data == expected

    async def test_chunked_bed_read_matches_golden(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        handle, _, _ = await fx_client.open(f"{basename}.bed")
        chunks = []
        offset = 0
        while True:
            data = await fx_client.read(handle, offset, 4096)
            if len(data) == 0:
                break
            chunks.append(data)
            offset += len(data)
        await fx_client.release(handle)
        assert b"".join(chunks) == expected

    async def test_random_pread(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        rng = random.Random(7)
        handle, _, _ = await fx_client.open(f"{basename}.bed")
        try:
            for _ in range(20):
                offset = rng.randrange(len(expected))
                size = rng.randrange(1, 256)
                got = await fx_client.read(handle, offset, size)
                assert got == expected[offset : offset + size]
        finally:
            await fx_client.release(handle)

    async def test_bim_full_read(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bim").read_bytes()
        handle, _, _ = await fx_client.open(f"{basename}.bim")
        data = await fx_client.read(handle, 0, len(expected) * 2)
        await fx_client.release(handle)
        assert data == expected


class TestErrors:
    async def test_open_unknown_raises_oserror_enoent(self, fx_client):
        with pytest.raises(OSError, match="errno") as excinfo:
            await fx_client.open("nope.bed")
        assert excinfo.value.errno == errno.ENOENT

    async def test_read_unknown_handle_raises_oserror_ebadf(self, fx_client):
        with pytest.raises(OSError, match="errno") as excinfo:
            await fx_client.read(9999, 0, 1)
        assert excinfo.value.errno == errno.EBADF

    async def test_release_unknown_handle_returns_silently(self, fx_client):
        # Worker session treats unknown release as a no-op.
        await fx_client.release(9999)


class TestConcurrentHandles:
    async def test_two_handles_independent(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        h1, _, _ = await fx_client.open(f"{basename}.bed")
        h2, _, _ = await fx_client.open(f"{basename}.bed")
        try:
            half = len(expected) // 2
            a = await fx_client.read(h1, 0, half)
            b = await fx_client.read(h2, half, half)
            assert a == expected[:half]
            assert b == expected[half : 2 * half]
            # Now reverse the order on the same handles.
            c = await fx_client.read(h2, 0, half)
            d = await fx_client.read(h1, half, half)
            assert c == expected[:half]
            assert d == expected[half : 2 * half]
        finally:
            await fx_client.release(h1)
            await fx_client.release(h2)


class TestClose:
    async def test_aclose_is_idempotent(self, fx_reader):
        client = await _client_with_thread_worker(fx_reader, "small")
        await client.aclose()
        await client.aclose()

    async def test_aclose_terminates_worker_thread(self, fx_reader):
        client = await _client_with_thread_worker(fx_reader, "small")
        proc = client._proc
        await client.aclose()
        assert not proc.is_alive()


class TestRealSubprocess:
    """End-to-end with a real ``multiprocessing.Process`` worker.

    Validates the spawn handshake and clean shutdown on top of the same
    request flow exercised in fast tests above.
    """

    async def test_spawn_open_read_close(self, fx_small_vcz, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        client = await bed_client.BedEncoderClient.connect(
            str(fx_small_vcz.path), basename
        )
        try:
            assert sorted(s.name for s in client.file_entries) == [
                f"{basename}.bed",
                f"{basename}.bim",
                f"{basename}.fam",
            ]
            handle, _, _ = await client.open(f"{basename}.bed")
            data = await client.read(handle, 0, len(expected) * 2)
            await client.release(handle)
            assert data == expected
        finally:
            await client.aclose()
        assert isinstance(client._proc, mp.process.BaseProcess)
        assert not client._proc.is_alive()
        assert client._proc.exitcode == 0
