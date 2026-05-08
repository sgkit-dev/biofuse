"""Tests for PlinkClient and BedConnection.

Most tests pair the trio client with a thread-based ``serve_forever``
running on a real ``AF_UNIX`` listener under tmp_path. A single
``TestRealSubprocess`` test runs the same flow over a real
``multiprocessing`` subprocess to validate the spawn handshake and
clean shutdown.
"""

import errno
import multiprocessing as mp
import pathlib
import random
import socket
import threading

import pytest
import trio
from vcztools import cli as vcztools_cli
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import plink_client, plink_server


@pytest.fixture
def fx_golden_dir(tmp_path, fx_small_vcz):
    golden = tmp_path / "golden"
    golden.mkdir()
    write_plink(make_reader(str(fx_small_vcz.path)), golden / "small")
    return golden, "small"


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


class _ThreadServer:
    """Drop-in stand-in for the ``multiprocessing.Process`` worker.

    Runs ``plink_server.serve_forever`` on a thread bound to a real
    ``AF_UNIX`` listener at ``socket_path``. Exposes the small subset
    of the ``mp.Process`` API that ``PlinkClient.aclose`` needs.
    """

    def __init__(
        self,
        reader,
        socket_path: pathlib.Path,
    ) -> None:
        self.exitcode: int | None = None
        self.session = plink_server._ServerSession(reader)
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(socket_path))
        self._listener.listen(16)
        self._parent_stop, self._child_stop = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self._thread = threading.Thread(
            target=plink_server.serve_forever,
            args=(self._listener, self._child_stop, self.session),
            daemon=True,
        )
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)
        if not self._thread.is_alive():
            self.exitcode = 0

    def terminate(self) -> None:
        try:
            self._parent_stop.close()
        except OSError:
            pass

    def kill(self) -> None:
        self.terminate()

    @property
    def parent_stop(self) -> socket.socket:
        return self._parent_stop


async def _client_with_thread_server(
    reader, socket_path: pathlib.Path
) -> plink_client.PlinkClient:
    server = _ThreadServer(reader, socket_path)
    self = plink_client.PlinkClient.__new__(plink_client.PlinkClient)
    self.bim_bytes = b""
    self.fam_bytes = b""
    self.bed_size = 0
    self._proc = server
    self._socket_path = socket_path
    self._stop_sock = server.parent_stop
    self._closed = False
    try:
        await self._handshake()
    except BaseException:
        await self.aclose()
        raise
    return self


@pytest.fixture
async def fx_client(fx_reader, tmp_path):
    socket_path = tmp_path / "plink.sock"
    client = await _client_with_thread_server(fx_reader, socket_path)
    try:
        yield client
    finally:
        await client.aclose()


class TestHandshake:
    async def test_metadata_populated(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        assert fx_client.bim_bytes == (golden / f"{basename}.bim").read_bytes()
        assert fx_client.fam_bytes == (golden / f"{basename}.fam").read_bytes()
        assert fx_client.bed_size == (golden / f"{basename}.bed").stat().st_size


class TestBedConnection:
    async def test_full_bed_read_matches_golden(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        conn = await fx_client.open_bed()
        try:
            data = await conn.read(0, len(expected) * 2)
            assert data == expected
        finally:
            await conn.aclose()

    async def test_chunked_bed_read(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        conn = await fx_client.open_bed()
        try:
            chunks = []
            offset = 0
            while True:
                data = await conn.read(offset, 4096)
                if len(data) == 0:
                    break
                chunks.append(data)
                offset += len(data)
            assert b"".join(chunks) == expected
        finally:
            await conn.aclose()

    async def test_random_pread(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        rng = random.Random(7)
        conn = await fx_client.open_bed()
        try:
            for _ in range(20):
                offset = rng.randrange(len(expected))
                size = rng.randrange(1, 256)
                got = await conn.read(offset, size)
                assert got == expected[offset : offset + size]
        finally:
            await conn.aclose()

    async def test_two_connections_independent(self, fx_client, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        conn_a = await fx_client.open_bed()
        conn_b = await fx_client.open_bed()
        try:
            half = len(expected) // 2
            a = await conn_a.read(0, half)
            b = await conn_b.read(half, half)
            assert a == expected[:half]
            assert b == expected[half : 2 * half]
            # Cross-encoder backward seeks: each connection has its own
            # encoder, so a backward read on one doesn't disturb the
            # other.
            c = await conn_b.read(0, half)
            d = await conn_a.read(half, half)
            assert c == expected[:half]
            assert d == expected[half : 2 * half]
        finally:
            await conn_a.aclose()
            await conn_b.aclose()

    async def test_concurrent_connections_run_in_parallel(
        self, fx_client, fx_golden_dir
    ):
        """Open four bed connections and run all four reads concurrently
        in a trio nursery. Each one must see a byte-identical copy of
        the bed file, regardless of interleaving on the server."""
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        n = 4
        results: dict[int, bytes] = {}

        async def runner(idx: int) -> None:
            conn = await fx_client.open_bed()
            try:
                results[idx] = await conn.read(0, len(expected) * 2)
            finally:
                await conn.aclose()

        async with trio.open_nursery() as nursery:
            for i in range(n):
                nursery.start_soon(runner, i)
        assert len(results) == n
        for body in results.values():
            assert body == expected

    async def test_read_after_close_raises(self, fx_client):
        conn = await fx_client.open_bed()
        await conn.aclose()
        with pytest.raises(OSError, match="bed connection is closed") as excinfo:
            await conn.read(0, 1)
        assert excinfo.value.errno == errno.EIO

    async def test_aclose_is_idempotent(self, fx_client):
        conn = await fx_client.open_bed()
        await conn.aclose()
        await conn.aclose()


class TestClientClose:
    async def test_aclose_is_idempotent(self, fx_reader, tmp_path):
        socket_path = tmp_path / "plink.sock"
        client = await _client_with_thread_server(fx_reader, socket_path)
        await client.aclose()
        await client.aclose()

    async def test_aclose_terminates_server_thread(self, fx_reader, tmp_path):
        socket_path = tmp_path / "plink.sock"
        client = await _client_with_thread_server(fx_reader, socket_path)
        proc = client._proc
        await client.aclose()
        assert not proc.is_alive()


class TestTimeouts:
    """The FUSE handler must never block forever on the worker. These
    tests pin that property by pointing the client at a deliberately
    unresponsive server and asserting that ``read`` and ``aclose``
    surface ``OSError(EIO)`` within a deadline."""

    @staticmethod
    def _bind_stall_listener(sock_path):
        """Bind+listen a UNIX socket that will accept exactly one
        connection inside the test's nursery and then go silent."""
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(1)
        listener.setblocking(False)
        return listener

    async def _stall_server(self, listener):
        trio_listener = trio.socket.from_stdlib_socket(listener)
        conn, _ = await trio_listener.accept()
        # Hold the connection open with no reads or writes. The test
        # nursery cancels us at teardown.
        try:
            await trio.sleep_forever()
        finally:
            conn.close()

    async def _make_stalled_connection(
        self, sock_path, nursery
    ) -> plink_client.BedConnection:
        listener = self._bind_stall_listener(sock_path)
        nursery.start_soon(self._stall_server, listener)
        stream = await trio.open_unix_socket(str(sock_path))
        return plink_client.BedConnection(stream)

    async def test_read_times_out_to_eio(self, monkeypatch, tmp_path):
        monkeypatch.setattr(plink_client, "_REQUEST_TIMEOUT_S", 0.2)
        sock_path = tmp_path / "stall.sock"
        async with trio.open_nursery() as nursery:
            conn = await self._make_stalled_connection(sock_path, nursery)
            t0 = trio.current_time()
            with pytest.raises(OSError, match="plink-server") as excinfo:
                await conn.read(0, 1024)
            elapsed = trio.current_time() - t0
            assert excinfo.value.errno == errno.EIO
            assert elapsed < 1.0, f"read should fail fast, took {elapsed:.2f}s"
            nursery.cancel_scope.cancel()

    async def test_read_after_timeout_is_immediate(self, monkeypatch, tmp_path):
        monkeypatch.setattr(plink_client, "_REQUEST_TIMEOUT_S", 0.2)
        sock_path = tmp_path / "stall.sock"
        async with trio.open_nursery() as nursery:
            conn = await self._make_stalled_connection(sock_path, nursery)
            with pytest.raises(OSError, match="plink-server"):
                await conn.read(0, 1024)
            t0 = trio.current_time()
            with pytest.raises(OSError, match="bed connection is closed") as excinfo:
                await conn.read(0, 1024)
            elapsed = trio.current_time() - t0
            assert excinfo.value.errno == errno.EIO
            assert elapsed < 0.05, (
                f"second read should be immediate, took {elapsed:.3f}s"
            )
            nursery.cancel_scope.cancel()

    async def test_aclose_does_not_hang_on_unresponsive_peer(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(plink_client, "_ACLOSE_TIMEOUT_S", 0.2)
        sock_path = tmp_path / "stall.sock"
        async with trio.open_nursery() as nursery:
            conn = await self._make_stalled_connection(sock_path, nursery)
            t0 = trio.current_time()
            await conn.aclose()
            elapsed = trio.current_time() - t0
            assert elapsed < 1.0, f"aclose should not hang, took {elapsed:.2f}s"
            nursery.cancel_scope.cancel()


class TestRealSubprocess:
    """End-to-end test against a real ``multiprocessing.Process`` worker."""

    async def test_start_fast_fails_on_subprocess_startup_error(
        self, fx_multiallelic_vcz, tmp_path
    ):
        """``PlinkClient.start()`` must surface a clean ``OSError`` and
        return well under the 10 s connect deadline when the subprocess
        catches a startup error (here: multi-allelic VCZ) and exits."""
        socket_path = tmp_path / "plink.sock"
        t0 = trio.current_time()
        with pytest.raises(OSError, match="exited during startup") as excinfo:
            await plink_client.PlinkClient.start(
                str(fx_multiallelic_vcz.path), socket_path
            )
        elapsed = trio.current_time() - t0
        assert excinfo.value.errno == errno.EIO
        assert elapsed < plink_client._CONNECT_DEADLINE_S, (
            f"start() should fast-fail when child dies, took {elapsed:.2f}s"
        )

    async def test_max_alleles_filter_through_start(
        self, fx_multiallelic_vcz, tmp_path
    ):
        """``ViewPlinkOptions(max_alleles=2)`` flows through the
        multiprocessing spawn into the worker's ``make_reader_from_options``
        and drops multi-allelic variants, so the handshake succeeds and
        the metadata reflects the filtered variant count."""
        # Sanity: the fixture really is multi-allelic, otherwise the
        # filter has nothing to drop and the test isn't testing anything.
        assert fx_multiallelic_vcz.num_biallelic_sites < (
            fx_multiallelic_vcz.num_variants
        )
        socket_path = tmp_path / "plink.sock"
        client = await plink_client.PlinkClient.start(
            str(fx_multiallelic_vcz.path),
            socket_path,
            reader_options=vcztools_cli.ViewPlinkOptions(max_alleles=2),
        )
        try:
            bim_lines = client.bim_bytes.decode("utf-8").splitlines()
            assert len(bim_lines) == fx_multiallelic_vcz.num_biallelic_sites
            fam_lines = client.fam_bytes.decode("utf-8").splitlines()
            assert len(fam_lines) == fx_multiallelic_vcz.num_samples
            bytes_per_variant = (fx_multiallelic_vcz.num_samples + 3) // 4
            expected_bed_size = (
                3 + fx_multiallelic_vcz.num_biallelic_sites * bytes_per_variant
            )
            assert client.bed_size == expected_bed_size
            conn = await client.open_bed()
            try:
                data = await conn.read(0, expected_bed_size)
                assert len(data) == expected_bed_size
            finally:
                await conn.aclose()
        finally:
            await client.aclose()
        assert isinstance(client._proc, mp.process.BaseProcess)
        assert not client._proc.is_alive()
        assert client._proc.exitcode == 0

    async def test_spawn_handshake_open_read_close(
        self, fx_small_vcz, fx_golden_dir, tmp_path
    ):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        socket_path = tmp_path / "plink.sock"
        client = await plink_client.PlinkClient.start(
            str(fx_small_vcz.path), socket_path
        )
        try:
            assert client.bim_bytes == (golden / f"{basename}.bim").read_bytes()
            assert client.fam_bytes == (golden / f"{basename}.fam").read_bytes()
            assert client.bed_size == len(expected)
            conn = await client.open_bed()
            try:
                data = await conn.read(0, len(expected))
                assert data == expected
            finally:
                await conn.aclose()
        finally:
            await client.aclose()
        assert isinstance(client._proc, mp.process.BaseProcess)
        assert not client._proc.is_alive()
        assert client._proc.exitcode == 0
