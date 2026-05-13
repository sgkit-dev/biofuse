"""Tests for EncoderClient and StreamConnection.

Most tests pair the trio client with a thread-based ``serve_forever``
running on a real ``AF_UNIX`` listener under tmp_path. Real-subprocess
tests use the parametrised :data:`fx_spec` to exercise both PLINK and
BGEN through the spawn handshake.
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

from biofuse import encoder_client, encoder_server, formats


@pytest.fixture(params=[formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"])
def fx_spec(request):
    return request.param


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


@pytest.fixture
def fx_expected(fx_reader, fx_spec):
    """Static-body / stream-size reference for the active spec.

    Built directly from the spec to avoid the BGEN / SQLite ``.bgi``
    non-determinism caveats documented in :mod:`test_encoder_server`.
    """
    static = fx_spec.build_static_bytes(fx_reader)
    with fx_spec.encoder_factory(fx_reader) as encoder:
        stream_size = int(encoder.total_size)
    return fx_spec, static, stream_size


class _ThreadServer:
    """Drop-in stand-in for the ``multiprocessing.Process`` worker.

    Runs ``encoder_server.serve_forever`` on a thread bound to a real
    ``AF_UNIX`` listener at ``socket_path``. Exposes the small subset
    of the ``mp.Process`` API that ``EncoderClient.aclose`` needs.
    """

    def __init__(
        self,
        reader,
        spec: formats.FormatSpec,
        socket_path: pathlib.Path,
    ) -> None:
        self.exitcode: int | None = None
        self.session = encoder_server._ServerSession(reader, spec)
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(socket_path))
        self._listener.listen(16)
        self._parent_stop, self._child_stop = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self._thread = threading.Thread(
            target=encoder_server.serve_forever,
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
    reader, spec: formats.FormatSpec, socket_path: pathlib.Path
) -> encoder_client.EncoderClient:
    server = _ThreadServer(reader, spec, socket_path)
    self = encoder_client.EncoderClient.__new__(encoder_client.EncoderClient)
    self.spec = spec
    self.static_bytes = []
    self.stream_size = 0
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
async def fx_client(fx_reader, fx_spec, tmp_path):
    socket_path = tmp_path / "encoder.sock"
    client = await _client_with_thread_server(fx_reader, fx_spec, socket_path)
    try:
        yield client
    finally:
        await client.aclose()


class TestHandshake:
    async def test_metadata_populated(self, fx_client, fx_expected):
        spec, expected_static, expected_stream_size = fx_expected
        assert fx_client.spec is spec
        assert fx_client.stream_size == expected_stream_size
        assert len(fx_client.static_bytes) == len(expected_static)
        for got, expected in zip(fx_client.static_bytes, expected_static, strict=True):
            assert len(got) == len(expected)


class TestStreamConnection:
    async def test_full_stream_read_matches_stream_size(self, fx_client):
        conn = await fx_client.open_stream()
        try:
            data = await conn.read(0, fx_client.stream_size * 2)
            assert len(data) == fx_client.stream_size
        finally:
            await conn.aclose()

    async def test_chunked_stream_read(self, fx_client):
        conn = await fx_client.open_stream()
        try:
            chunks = []
            offset = 0
            while True:
                data = await conn.read(offset, 4096)
                if len(data) == 0:
                    break
                chunks.append(data)
                offset += len(data)
            assert sum(len(c) for c in chunks) == fx_client.stream_size
        finally:
            await conn.aclose()

    async def test_random_pread_matches_full_read(self, fx_client):
        """A random-offset pread on a fresh connection reads the same
        bytes as the same window of a full sequential read on another
        connection. This pins the encoder's random-access contract
        without comparing against an on-disk golden."""
        full_conn = await fx_client.open_stream()
        try:
            full = await full_conn.read(0, fx_client.stream_size)
        finally:
            await full_conn.aclose()

        rng = random.Random(7)
        conn = await fx_client.open_stream()
        try:
            for _ in range(20):
                offset = rng.randrange(fx_client.stream_size)
                size = rng.randrange(1, 256)
                got = await conn.read(offset, size)
                assert got == full[offset : offset + size]
        finally:
            await conn.aclose()

    async def test_two_connections_independent(self, fx_client):
        full_conn = await fx_client.open_stream()
        try:
            expected = await full_conn.read(0, fx_client.stream_size)
        finally:
            await full_conn.aclose()
        conn_a = await fx_client.open_stream()
        conn_b = await fx_client.open_stream()
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

    async def test_concurrent_connections_run_in_parallel(self, fx_client):
        """Open four stream connections and run all four reads
        concurrently in a trio nursery. Each one must see a byte-identical
        copy of the stream, regardless of interleaving on the server."""
        full_conn = await fx_client.open_stream()
        try:
            expected = await full_conn.read(0, fx_client.stream_size)
        finally:
            await full_conn.aclose()
        n = 4
        results: dict[int, bytes] = {}

        async def runner(idx: int) -> None:
            conn = await fx_client.open_stream()
            try:
                results[idx] = await conn.read(0, fx_client.stream_size * 2)
            finally:
                await conn.aclose()

        async with trio.open_nursery() as nursery:
            for i in range(n):
                nursery.start_soon(runner, i)
        assert len(results) == n
        for body in results.values():
            assert body == expected

    async def test_read_after_close_raises(self, fx_client):
        conn = await fx_client.open_stream()
        await conn.aclose()
        with pytest.raises(OSError, match="stream connection is closed") as excinfo:
            await conn.read(0, 1)
        assert excinfo.value.errno == errno.EIO

    async def test_aclose_is_idempotent(self, fx_client):
        conn = await fx_client.open_stream()
        await conn.aclose()
        await conn.aclose()


class TestClientClose:
    async def test_aclose_is_idempotent(self, fx_reader, fx_spec, tmp_path):
        socket_path = tmp_path / "encoder.sock"
        client = await _client_with_thread_server(fx_reader, fx_spec, socket_path)
        await client.aclose()
        await client.aclose()

    async def test_aclose_terminates_server_thread(self, fx_reader, fx_spec, tmp_path):
        socket_path = tmp_path / "encoder.sock"
        client = await _client_with_thread_server(fx_reader, fx_spec, socket_path)
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
    ) -> encoder_client.StreamConnection:
        listener = self._bind_stall_listener(sock_path)
        nursery.start_soon(self._stall_server, listener)
        stream = await trio.open_unix_socket(str(sock_path))
        return encoder_client.StreamConnection(stream)

    async def test_read_times_out_to_eio(self, monkeypatch, tmp_path):
        monkeypatch.setattr(encoder_client, "_REQUEST_TIMEOUT_S", 0.2)
        sock_path = tmp_path / "stall.sock"
        async with trio.open_nursery() as nursery:
            conn = await self._make_stalled_connection(sock_path, nursery)
            t0 = trio.current_time()
            with pytest.raises(OSError, match="encoder-server") as excinfo:
                await conn.read(0, 1024)
            elapsed = trio.current_time() - t0
            assert excinfo.value.errno == errno.EIO
            assert elapsed < 1.0, f"read should fail fast, took {elapsed:.2f}s"
            nursery.cancel_scope.cancel()

    async def test_read_after_timeout_is_immediate(self, monkeypatch, tmp_path):
        monkeypatch.setattr(encoder_client, "_REQUEST_TIMEOUT_S", 0.2)
        sock_path = tmp_path / "stall.sock"
        async with trio.open_nursery() as nursery:
            conn = await self._make_stalled_connection(sock_path, nursery)
            with pytest.raises(OSError, match="encoder-server"):
                await conn.read(0, 1024)
            t0 = trio.current_time()
            with pytest.raises(OSError, match="stream connection is closed") as excinfo:
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
        monkeypatch.setattr(encoder_client, "_ACLOSE_TIMEOUT_S", 0.2)
        sock_path = tmp_path / "stall.sock"
        async with trio.open_nursery() as nursery:
            conn = await self._make_stalled_connection(sock_path, nursery)
            t0 = trio.current_time()
            await conn.aclose()
            elapsed = trio.current_time() - t0
            assert elapsed < 1.0, f"aclose should not hang, took {elapsed:.2f}s"
            nursery.cancel_scope.cancel()


class TestRealSubprocess:
    """End-to-end tests against a real ``multiprocessing.Process`` worker."""

    @pytest.mark.parametrize(
        "spec", [formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"]
    )
    async def test_start_fast_fails_on_subprocess_startup_error(
        self, fx_multiallelic_vcz, tmp_path, spec
    ):
        """``EncoderClient.start()`` must surface a clean ``OSError`` and
        return well under the 10 s connect deadline when the subprocess
        catches a startup error (here: multi-allelic VCZ) and exits."""
        socket_path = tmp_path / "encoder.sock"
        t0 = trio.current_time()
        with pytest.raises(OSError, match="exited during startup") as excinfo:
            await encoder_client.EncoderClient.start(
                str(fx_multiallelic_vcz.path), socket_path, spec
            )
        elapsed = trio.current_time() - t0
        assert excinfo.value.errno == errno.EIO
        assert elapsed < encoder_client._CONNECT_DEADLINE_S, (
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
        socket_path = tmp_path / "encoder.sock"
        client = await encoder_client.EncoderClient.start(
            str(fx_multiallelic_vcz.path),
            socket_path,
            formats.PLINK_SPEC,
            reader_options=vcztools_cli.ViewPlinkOptions(max_alleles=2),
        )
        try:
            bim_bytes, fam_bytes = client.static_bytes
            bim_lines = bim_bytes.decode("utf-8").splitlines()
            assert len(bim_lines) == fx_multiallelic_vcz.num_biallelic_sites
            fam_lines = fam_bytes.decode("utf-8").splitlines()
            assert len(fam_lines) == fx_multiallelic_vcz.num_samples
            bytes_per_variant = (fx_multiallelic_vcz.num_samples + 3) // 4
            expected_bed_size = (
                3 + fx_multiallelic_vcz.num_biallelic_sites * bytes_per_variant
            )
            assert client.stream_size == expected_bed_size
            conn = await client.open_stream()
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

    @pytest.mark.parametrize(
        "spec", [formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"]
    )
    async def test_spawn_handshake_open_read_close(self, fx_small_vcz, tmp_path, spec):
        # The expected stream_size and static-body sizes are computed
        # via a fresh in-process reader so the test does not depend on
        # external goldens (write_plink / write_bgen). See the
        # encoder-server test module for the rationale.
        ref_reader = make_reader(str(fx_small_vcz.path))
        with spec.encoder_factory(ref_reader) as enc:
            expected_stream_size = int(enc.total_size)
        socket_path = tmp_path / "encoder.sock"
        client = await encoder_client.EncoderClient.start(
            str(fx_small_vcz.path), socket_path, spec
        )
        try:
            assert client.stream_size == expected_stream_size
            assert len(client.static_bytes) == len(spec.static_suffixes)
            conn = await client.open_stream()
            try:
                data = await conn.read(0, client.stream_size)
                assert len(data) == client.stream_size
            finally:
                await conn.aclose()
        finally:
            await client.aclose()
        assert isinstance(client._proc, mp.process.BaseProcess)
        assert not client._proc.is_alive()
        assert client._proc.exitcode == 0
