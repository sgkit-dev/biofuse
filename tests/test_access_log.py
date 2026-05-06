"""Tests for the AccessLogger."""

import json
import threading
import time

import pytest

from biofuse import access_log


def _record(logger: access_log.AccessLogger, path: str, fh: int, off: int, size: int):
    """Helper: capture t_start, then record."""
    t0 = time.monotonic()
    logger.record(path, fh, off, size, t0)


class TestInMemory:
    def test_empty_initially(self):
        logger = access_log.AccessLogger()
        assert logger.records == []

    def test_records_in_order(self):
        logger = access_log.AccessLogger()
        _record(logger, "a.bed", 1, 0, 100)
        _record(logger, "a.bed", 1, 100, 200)
        _record(logger, "a.bim", 2, 0, 50)
        recs = logger.records
        assert [(r.path, r.fh, r.offset, r.size) for r in recs] == [
            ("a.bed", 1, 0, 100),
            ("a.bed", 1, 100, 200),
            ("a.bim", 2, 0, 50),
        ]

    def test_records_property_returns_copy(self):
        logger = access_log.AccessLogger()
        _record(logger, "a.bed", 1, 0, 1)
        snapshot = logger.records
        _record(logger, "a.bed", 1, 1, 1)
        assert len(snapshot) == 1
        assert len(logger.records) == 2

    def test_t_end_is_after_t_start(self):
        logger = access_log.AccessLogger()
        for offset in range(20):
            _record(logger, "a.bed", 1, offset, 1)
        for r in logger.records:
            assert r.t_end >= r.t_start


class TestJsonlFile:
    def test_writes_one_line_per_record(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            _record(logger, "a.bed", 7, 0, 64)
            _record(logger, "a.bim", 8, 100, 32)

        lines = out.read_text().splitlines()
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["path"] == "a.bed"
        assert first["fh"] == 7
        assert first["offset"] == 0
        assert first["size"] == 64
        assert "t_start" in first
        assert "t_end" in first
        assert first["t_end"] >= first["t_start"]

    def test_in_memory_records_skipped_in_file_mode(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            _record(logger, "a.bed", 1, 0, 64)
            assert logger.records == []

    def test_appends_when_reopened(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            _record(logger, "a.bed", 1, 0, 1)
        with access_log.AccessLogger(out) as logger:
            _record(logger, "a.bed", 1, 1, 1)
        assert len(out.read_text().splitlines()) == 2

    def test_close_flushes_buffered_lines(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        logger = access_log.AccessLogger(out)
        for offset in range(50):
            _record(logger, "a.bed", 1, offset, 1)
        logger.close()
        assert len(out.read_text().splitlines()) == 50

    def test_double_close_is_idempotent(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        logger = access_log.AccessLogger(out)
        _record(logger, "a.bed", 1, 0, 1)
        logger.close()
        logger.close()


class TestConcurrent:
    def test_no_lost_records_under_threads(self):
        logger = access_log.AccessLogger()
        n_threads = 8
        per_thread = 200

        def worker(tid: int):
            for i in range(per_thread):
                _record(logger, f"t{tid}.bed", tid, i, 1)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        recs = logger.records
        assert len(recs) == n_threads * per_thread

    def test_jsonl_lines_are_well_formed_under_threads(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            n_threads = 4
            per_thread = 100

            def worker(tid: int):
                for i in range(per_thread):
                    _record(logger, f"t{tid}.bed", tid, i, 1)

            threads = [
                threading.Thread(target=worker, args=(t,)) for t in range(n_threads)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        lines = out.read_text().splitlines()
        assert len(lines) == n_threads * per_thread
        for line in lines:
            json.loads(line)


class TestEventKinds:
    """Lifecycle events (open / release / aclose / limiter_wait) round-trip
    through the same JSONL writer with a ``kind`` field; existing reads
    keep ``kind="read"``."""

    def test_read_default_kind(self):
        logger = access_log.AccessLogger()
        _record(logger, "a.bed", 1, 0, 100)
        recs = logger.records
        assert len(recs) == 1
        assert recs[0].kind == "read"

    def test_record_event_in_memory(self):
        logger = access_log.AccessLogger()
        t0 = time.monotonic()
        logger.record_event("aclose", "a.bed", 7, t0)
        recs = logger.records
        assert len(recs) == 1
        assert recs[0].kind == "aclose"
        assert recs[0].path == "a.bed"
        assert recs[0].fh == 7
        assert recs[0].offset == 0
        assert recs[0].size == 0
        assert recs[0].t_end >= recs[0].t_start

    def test_record_event_jsonl(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            t0 = time.monotonic()
            logger.record_event("open", "a.bed", 1, t0)
            logger.record_event("release", "a.bed", 1, t0, t_end=t0 + 0.01)
            _record(logger, "a.bed", 1, 0, 100)
        lines = out.read_text().splitlines()
        assert len(lines) == 3
        kinds = [json.loads(line)["kind"] for line in lines]
        assert kinds == ["open", "release", "read"]
        release = json.loads(lines[1])
        assert release["t_end"] - release["t_start"] == pytest.approx(0.01)


class TestRecordValidation:
    @pytest.mark.parametrize(
        ("offset", "size"), [(0, 0), (0, 1), (10, 5), (1 << 40, 4096)]
    )
    def test_round_trips_through_jsonl(self, tmp_path, offset, size):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            _record(logger, "x.bed", 42, offset, size)
        rec = json.loads(out.read_text().splitlines()[0])
        assert rec["offset"] == offset
        assert rec["size"] == size
        assert rec["fh"] == 42
