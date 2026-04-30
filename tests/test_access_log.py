"""Tests for the AccessLogger."""

import json
import threading

import pytest

from biofuse import access_log


class TestInMemory:
    def test_empty_initially(self):
        logger = access_log.AccessLogger()
        assert logger.records == []

    def test_records_in_order(self):
        logger = access_log.AccessLogger()
        logger.record("a.bed", 0, 100)
        logger.record("a.bed", 100, 200)
        logger.record("a.bim", 0, 50)
        recs = logger.records
        assert [(r.path, r.offset, r.size) for r in recs] == [
            ("a.bed", 0, 100),
            ("a.bed", 100, 200),
            ("a.bim", 0, 50),
        ]

    def test_records_property_returns_copy(self):
        logger = access_log.AccessLogger()
        logger.record("a.bed", 0, 1)
        snapshot = logger.records
        logger.record("a.bed", 1, 1)
        assert len(snapshot) == 1
        assert len(logger.records) == 2

    def test_t_monotonic_is_nondecreasing(self):
        logger = access_log.AccessLogger()
        for offset in range(20):
            logger.record("a.bed", offset, 1)
        ts = [r.t_monotonic for r in logger.records]
        assert ts == sorted(ts)


class TestJsonlFile:
    def test_writes_one_line_per_record(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            logger.record("a.bed", 0, 64)
            logger.record("a.bim", 100, 32)

        lines = out.read_text().splitlines()
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["path"] == "a.bed"
        assert first["offset"] == 0
        assert first["size"] == 64
        assert "t_monotonic" in first

    def test_in_memory_records_skipped_in_file_mode(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            logger.record("a.bed", 0, 64)
            assert logger.records == []

    def test_appends_when_reopened(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            logger.record("a.bed", 0, 1)
        with access_log.AccessLogger(out) as logger:
            logger.record("a.bed", 1, 1)
        assert len(out.read_text().splitlines()) == 2

    def test_close_flushes_buffered_lines(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        logger = access_log.AccessLogger(out)
        for offset in range(50):
            logger.record("a.bed", offset, 1)
        logger.close()
        assert len(out.read_text().splitlines()) == 50

    def test_double_close_is_idempotent(self, tmp_path):
        out = tmp_path / "trace.jsonl"
        logger = access_log.AccessLogger(out)
        logger.record("a.bed", 0, 1)
        logger.close()
        logger.close()


class TestConcurrent:
    def test_no_lost_records_under_threads(self):
        logger = access_log.AccessLogger()
        n_threads = 8
        per_thread = 200

        def worker(tid: int):
            for i in range(per_thread):
                logger.record(f"t{tid}.bed", i, 1)

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
                    logger.record(f"t{tid}.bed", i, 1)

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


class TestRecordValidation:
    @pytest.mark.parametrize(
        ("offset", "size"), [(0, 0), (0, 1), (10, 5), (1 << 40, 4096)]
    )
    def test_round_trips_through_jsonl(self, tmp_path, offset, size):
        out = tmp_path / "trace.jsonl"
        with access_log.AccessLogger(out) as logger:
            logger.record("x.bed", offset, size)
        rec = json.loads(out.read_text().splitlines()[0])
        assert rec["offset"] == offset
        assert rec["size"] == size
