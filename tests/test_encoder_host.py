"""Tests for the in-process encoder host.

Two layers:

- ``StreamHandle`` exercised directly against a hand-built fake encoder
  so the timeout / drain / leak paths can be triggered deterministically.
- ``EncoderHost`` exercised against the real spec factories (PLINK / BGEN)
  on the ``fx_small_vcz`` fixture, parity-checked against an in-process
  reference encoder built with the same ``(reader, opts)``.

End-to-end FUSE behaviour against real ``plink`` / ``bgenix`` binaries
lives in ``test_plink_apps.py`` / ``test_bgen_apps.py``.
"""

import dataclasses
import errno
import threading

import pytest
import trio
import vcztools

from biofuse import encoder_host, formats


@pytest.fixture(params=[formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"])
def fx_spec(request):
    return request.param


@pytest.fixture
def fx_opts(fx_spec):
    if fx_spec.name == "plink":
        return vcztools.ViewPlinkOptions()
    return vcztools.ViewBgenOptions()


@pytest.fixture
def fx_reference(fx_small_vcz, fx_spec, fx_opts):
    """In-process reference: ``(static_files, stream_size, full_bytes)``.

    Built directly off the spec from a fresh reader so tests don't need
    on-disk goldens. ``.bgen.bgi`` SQLite payloads have non-deterministic
    header bytes across independent writes; tests using this fixture
    therefore compare sizes / suffix sets for that entry rather than
    full bodies.
    """
    reader = fx_opts.make_reader(str(fx_small_vcz.path))
    try:
        reader.materialise_variant_filter()
        static_files = fx_spec.build_static_files(reader, fx_opts)
        with fx_spec.encoder_factory(reader, fx_opts) as enc:
            stream_size = int(enc.total_size)
            full_bytes = enc.read(0, stream_size)
    finally:
        reader.__exit__(None, None, None)
    return static_files, stream_size, full_bytes


# -----------------------------------------------------------------------------
# Fake encoder for unit-testing StreamHandle in isolation.
# -----------------------------------------------------------------------------


class _FakeEncoder:
    """Hand-built encoder for StreamHandle timeout/drain tests.

    The ``read`` method blocks on a per-call ``threading.Event`` so a
    test can release the worker thread at will. ``close`` records the
    invocation.
    """

    def __init__(self) -> None:
        self.release_read = threading.Event()
        self.entered_read = threading.Event()
        self.read_calls: list[tuple[int, int]] = []
        self.try_cached_read_calls: list[tuple[int, int]] = []
        self.close_calls = 0
        self._payload = b"X"
        # When set, ``try_cached_read`` returns a slice of this buffer
        # so the StreamHandle fast path can be exercised. Default ``None``
        # means cache miss — every call falls back to ``read``.
        self.cached_payload: bytes | None = None

    def set_payload(self, body: bytes) -> None:
        self._payload = body

    def read(self, off: int, size: int) -> bytes:
        self.read_calls.append((off, size))
        self.entered_read.set()
        self.release_read.wait()
        return self._payload

    def try_cached_read(self, off: int, size: int) -> bytes | None:
        self.try_cached_read_calls.append((off, size))
        if self.cached_payload is None:
            return None
        return self.cached_payload[off : off + size]

    def close(self) -> None:
        self.close_calls += 1


# -----------------------------------------------------------------------------
# StreamHandle direct unit tests.
# -----------------------------------------------------------------------------


class TestStreamHandleHappyPath:
    async def test_read_returns_encoder_bytes(self):
        encoder = _FakeEncoder()
        encoder.set_payload(b"hello")
        encoder.release_read.set()
        handle = encoder_host.StreamHandle(encoder)
        try:
            got = await handle.read(0, 5)
            assert got == b"hello"
            assert encoder.read_calls == [(0, 5)]
        finally:
            await handle.aclose()
        assert encoder.close_calls == 1

    async def test_aclose_is_idempotent(self):
        encoder = _FakeEncoder()
        encoder.release_read.set()
        handle = encoder_host.StreamHandle(encoder)
        await handle.aclose()
        await handle.aclose()
        assert encoder.close_calls == 1

    async def test_on_aclose_hook_fires_once(self):
        encoder = _FakeEncoder()
        encoder.release_read.set()
        hook_calls: list[tuple[float, float]] = []
        handle = encoder_host.StreamHandle(
            encoder, on_aclose=lambda t0, t1: hook_calls.append((t0, t1))
        )
        await handle.aclose()
        await handle.aclose()  # idempotent: hook only fires once.
        assert len(hook_calls) == 1
        t0, t1 = hook_calls[0]
        assert t1 >= t0

    async def test_read_after_close_raises_eio(self):
        encoder = _FakeEncoder()
        encoder.release_read.set()
        handle = encoder_host.StreamHandle(encoder)
        await handle.aclose()
        with pytest.raises(OSError, match="stream handle is closed") as excinfo:
            await handle.read(0, 1)
        assert excinfo.value.errno == errno.EIO


class TestStreamHandleCachedFastPath:
    async def test_cache_hit_returns_bytes_without_thread_dispatch(self):
        encoder = _FakeEncoder()
        encoder.cached_payload = b"hello"
        # release_read deliberately not set: if the fast path doesn't
        # short-circuit, the slow-path worker thread will block forever
        # and the test will hang.
        handle = encoder_host.StreamHandle(encoder)
        try:
            got = await handle.read(0, 5)
            assert got == b"hello"
            assert encoder.try_cached_read_calls == [(0, 5)]
            assert encoder.read_calls == []
            assert not encoder.entered_read.is_set()
        finally:
            encoder.release_read.set()
            await handle.aclose()

    async def test_cache_miss_falls_back_to_slow_path(self):
        encoder = _FakeEncoder()
        encoder.set_payload(b"world")
        encoder.release_read.set()
        handle = encoder_host.StreamHandle(encoder)
        try:
            got = await handle.read(0, 5)
            assert got == b"world"
            assert encoder.try_cached_read_calls == [(0, 5)]
            assert encoder.read_calls == [(0, 5)]
        finally:
            await handle.aclose()

    async def test_cache_hit_does_not_enter_encoder_read(self):
        encoder = _FakeEncoder()
        encoder.cached_payload = b"ABCDEFGH"
        handle = encoder_host.StreamHandle(encoder)
        try:
            async with trio.open_nursery() as nursery:
                for off in range(4):
                    nursery.start_soon(handle.read, off, 2)
            assert len(encoder.try_cached_read_calls) == 4
            assert encoder.read_calls == []
        finally:
            encoder.release_read.set()
            await handle.aclose()

    async def test_cache_hit_after_close_raises_eio_without_calling_encoder(self):
        encoder = _FakeEncoder()
        encoder.cached_payload = b"never seen"
        encoder.release_read.set()
        handle = encoder_host.StreamHandle(encoder)
        await handle.aclose()
        with pytest.raises(OSError, match="stream handle is closed") as excinfo:
            await handle.read(0, 1)
        assert excinfo.value.errno == errno.EIO
        assert encoder.try_cached_read_calls == []


class TestStreamHandleSerialisation:
    async def test_concurrent_reads_serialise_on_one_handle(self):
        """Two concurrent ``handle.read`` calls must not enter the
        encoder at the same time — ``FormatEncoder`` mutates iterator
        state in place."""
        encoder = _FakeEncoder()
        encoder.set_payload(b"A")
        handle = encoder_host.StreamHandle(encoder)
        in_flight = 0
        peak = 0
        lock = threading.Lock()

        original_read = encoder.read

        def watched_read(off: int, size: int) -> bytes:
            nonlocal in_flight, peak
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            try:
                return original_read(off, size)
            finally:
                with lock:
                    in_flight -= 1

        encoder.read = watched_read

        try:
            async with trio.open_nursery() as nursery:

                async def runner():
                    # Release per-call so each read can complete.
                    encoder.release_read.set()
                    await handle.read(0, 1)
                    encoder.release_read.clear()

                # Sequentially start tasks that will serialise on the lock.
                for _ in range(3):
                    encoder.release_read.set()
                    nursery.start_soon(handle.read, 0, 1)
                    await trio.sleep(0)
        finally:
            encoder.release_read.set()
            await handle.aclose()

        assert peak == 1


class TestStreamHandleTimeout:
    async def test_read_times_out_to_eio(self, monkeypatch):
        monkeypatch.setattr(encoder_host, "_REQUEST_TIMEOUT_S", 0.1)
        encoder = _FakeEncoder()  # release_read intentionally not set
        handle = encoder_host.StreamHandle(encoder)
        try:
            t0 = trio.current_time()
            with pytest.raises(OSError, match="encoder read timed out") as excinfo:
                await handle.read(0, 1024)
            elapsed = trio.current_time() - t0
            assert excinfo.value.errno == errno.EIO
            assert elapsed < 1.0, f"read should fail fast, took {elapsed:.2f}s"
        finally:
            # Release the abandoned worker so aclose can drain.
            encoder.release_read.set()
            await handle.aclose()

    async def test_read_after_timeout_is_immediate(self, monkeypatch):
        monkeypatch.setattr(encoder_host, "_REQUEST_TIMEOUT_S", 0.1)
        encoder = _FakeEncoder()
        handle = encoder_host.StreamHandle(encoder)
        try:
            with pytest.raises(OSError, match="encoder read timed out"):
                await handle.read(0, 1024)
            t0 = trio.current_time()
            with pytest.raises(OSError, match="stream handle is closed") as excinfo:
                await handle.read(0, 1024)
            elapsed = trio.current_time() - t0
            assert excinfo.value.errno == errno.EIO
            assert elapsed < 0.05, (
                f"second read should be immediate, took {elapsed:.3f}s"
            )
        finally:
            encoder.release_read.set()
            await handle.aclose()

    async def test_aclose_drains_abandoned_thread_then_closes(self, monkeypatch):
        """After a timeout the worker thread is still running. ``aclose``
        must wait for it to return before calling ``encoder.close`` —
        the encoder's own thread pool cannot tolerate concurrent
        ``read``/``close``."""
        monkeypatch.setattr(encoder_host, "_REQUEST_TIMEOUT_S", 0.1)
        encoder = _FakeEncoder()
        handle = encoder_host.StreamHandle(encoder)
        with pytest.raises(OSError, match="encoder read timed out"):
            await handle.read(0, 1024)
        assert encoder.close_calls == 0  # not yet drained

        # Release the abandoned worker; aclose should then close the encoder.
        encoder.release_read.set()
        await handle.aclose()
        assert encoder.close_calls == 1


class TestStreamHandleAcloseLeak:
    async def test_aclose_does_not_hang_on_wedged_encoder(self, monkeypatch):
        """If the abandoned worker thread never returns, ``aclose`` must
        log + leak rather than hang the unmount path. We pin the
        elapsed-time bound here; the warning text is best-effort logging."""
        monkeypatch.setattr(encoder_host, "_REQUEST_TIMEOUT_S", 0.05)
        monkeypatch.setattr(encoder_host, "_ACLOSE_TIMEOUT_S", 0.1)
        encoder = _FakeEncoder()
        handle = encoder_host.StreamHandle(encoder)
        try:
            with pytest.raises(OSError, match="encoder read timed out"):
                await handle.read(0, 1024)
            t0 = trio.current_time()
            await handle.aclose()
            elapsed = trio.current_time() - t0
            assert elapsed < 1.0, f"aclose should not hang, took {elapsed:.2f}s"
            # encoder.close was not called because we never drained.
            assert encoder.close_calls == 0
        finally:
            encoder.release_read.set()


# -----------------------------------------------------------------------------
# EncoderHost end-to-end tests against real spec factories.
# -----------------------------------------------------------------------------


class TestEncoderHostStart:
    async def test_static_files_keys_match_spec(
        self, fx_small_vcz, fx_spec, fx_opts, fx_reference
    ):
        ref_static, ref_stream_size, _ = fx_reference
        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        ) as host:
            assert tuple(host.static_files) == fx_spec.static_suffixes(fx_opts)
            assert host.stream_size == ref_stream_size
            for suffix in ref_static:
                # ``.bgen.bgi`` SQLite headers vary across writes; size match
                # is the strongest portable assertion.
                if suffix == ".bgen.bgi":
                    assert len(host.static_files[suffix]) == len(ref_static[suffix])
                else:
                    assert host.static_files[suffix] == ref_static[suffix]

    async def test_start_failure_on_multiallelic_plink(
        self, fx_multiallelic_vcz, tmp_path
    ):
        """PLINK static-file build refuses multi-allelic VCZ. The
        failure must propagate cleanly from ``start`` rather than
        leaving a half-initialised host behind."""
        with pytest.raises((ValueError, OSError)):
            await encoder_host.EncoderHost.start(
                str(fx_multiallelic_vcz.path),
                formats.PLINK_SPEC,
                opts=vcztools.ViewPlinkOptions(),
            )

    async def test_max_alleles_filter_drops_multiallelic_sites(
        self, fx_multiallelic_vcz
    ):
        """``ViewPlinkOptions(max_alleles=2)`` must reach ``opts.make_reader``
        and drop multi-allelic variants so the static-file build succeeds."""
        # Sanity: the fixture really is multi-allelic.
        assert fx_multiallelic_vcz.num_biallelic_sites < (
            fx_multiallelic_vcz.num_variants
        )
        default_opts = vcztools.ViewPlinkOptions()
        selection = dataclasses.replace(default_opts.selection, max_alleles=2)
        opts = dataclasses.replace(default_opts, selection=selection)
        async with await encoder_host.EncoderHost.start(
            str(fx_multiallelic_vcz.path), formats.PLINK_SPEC, opts=opts
        ) as host:
            bim_lines = host.static_files[".bim"].decode("utf-8").splitlines()
            assert len(bim_lines) == fx_multiallelic_vcz.num_biallelic_sites
            fam_lines = host.static_files[".fam"].decode("utf-8").splitlines()
            assert len(fam_lines) == fx_multiallelic_vcz.num_samples


class TestEncoderHostStreaming:
    async def test_full_stream_read_matches_reference(
        self, fx_small_vcz, fx_spec, fx_opts, fx_reference
    ):
        _, ref_stream_size, ref_bytes = fx_reference
        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        ) as host:
            handle = await host.open_stream()
            try:
                got = await handle.read(0, ref_stream_size * 2)
                assert got == ref_bytes
            finally:
                await handle.aclose()

    async def test_chunked_stream_read(self, fx_small_vcz, fx_spec, fx_opts):
        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        ) as host:
            handle = await host.open_stream()
            try:
                chunks = []
                offset = 0
                while True:
                    data = await handle.read(offset, 4096)
                    if len(data) == 0:
                        break
                    chunks.append(data)
                    offset += len(data)
                assert sum(len(c) for c in chunks) == host.stream_size
            finally:
                await handle.aclose()

    async def test_two_handles_independent(
        self, fx_small_vcz, fx_spec, fx_opts, fx_reference
    ):
        _, _, ref_bytes = fx_reference
        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        ) as host:
            handle_a = await host.open_stream()
            handle_b = await host.open_stream()
            try:
                half = len(ref_bytes) // 2
                a = await handle_a.read(0, half)
                b = await handle_b.read(half, half)
                assert a == ref_bytes[:half]
                assert b == ref_bytes[half : 2 * half]
                # Each handle has its own encoder; a backward read on one
                # does not disturb the other.
                c = await handle_b.read(0, half)
                d = await handle_a.read(half, half)
                assert c == ref_bytes[:half]
                assert d == ref_bytes[half : 2 * half]
            finally:
                await handle_a.aclose()
                await handle_b.aclose()

    async def test_concurrent_handles_run_in_parallel(
        self, fx_small_vcz, fx_spec, fx_opts, fx_reference
    ):
        _, _, ref_bytes = fx_reference
        results: dict[int, bytes] = {}

        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        ) as host:

            async def runner(idx: int) -> None:
                handle = await host.open_stream()
                try:
                    results[idx] = await handle.read(0, host.stream_size * 2)
                finally:
                    await handle.aclose()

            async with trio.open_nursery() as nursery:
                for i in range(4):
                    nursery.start_soon(runner, i)

        assert len(results) == 4
        for body in results.values():
            assert body == ref_bytes


class TestEncoderHostClose:
    async def test_aclose_is_idempotent(self, fx_small_vcz, fx_spec, fx_opts):
        host = await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        )
        await host.aclose()
        await host.aclose()

    async def test_open_stream_after_close_raises(self, fx_small_vcz, fx_spec, fx_opts):
        host = await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), fx_spec, opts=fx_opts
        )
        await host.aclose()
        with pytest.raises(OSError, match="encoder host is closed") as excinfo:
            await host.open_stream()
        assert excinfo.value.errno == errno.EIO


class TestEncoderHostBgenOptions:
    @pytest.mark.parametrize("no_header_samples", [True, False])
    async def test_no_header_samples_stable_across_opens(
        self, fx_small_vcz, no_header_samples
    ):
        """``--no-header-samples`` flows into every per-handle
        ``BgenEncoder``. Three sequential open/read/close cycles must
        all return the same bytes — and those bytes must match an
        in-process reference ``BgenEncoder`` with the matching
        ``embed_header_samples`` flag."""
        opts = dataclasses.replace(
            vcztools.ViewBgenOptions(), no_header_samples=no_header_samples
        )
        ref_reader = opts.make_reader(str(fx_small_vcz.path))
        try:
            with vcztools.BgenEncoder(
                ref_reader, embed_header_samples=not no_header_samples
            ) as ref:
                expected_size = int(ref.total_size)
                expected = ref.read(0, expected_size)
        finally:
            ref_reader.__exit__(None, None, None)

        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), formats.BGEN_SPEC, opts=opts
        ) as host:
            assert host.stream_size == expected_size
            for cycle in range(3):
                handle = await host.open_stream()
                try:
                    data = await handle.read(0, host.stream_size)
                finally:
                    await handle.aclose()
                assert data == expected, f"cycle {cycle} differed from reference"

    @pytest.mark.parametrize(
        ("no_sample_file", "no_bgi"),
        [(True, False), (False, True), (True, True), (False, False)],
    )
    async def test_sidecar_toggles_honour_spec(
        self, fx_small_vcz, no_sample_file, no_bgi
    ):
        opts = dataclasses.replace(
            vcztools.ViewBgenOptions(),
            no_sample_file=no_sample_file,
            no_bgi=no_bgi,
        )
        expected_suffixes = formats.BGEN_SPEC.static_suffixes(opts)
        async with await encoder_host.EncoderHost.start(
            str(fx_small_vcz.path), formats.BGEN_SPEC, opts=opts
        ) as host:
            assert tuple(host.static_files) == expected_suffixes
