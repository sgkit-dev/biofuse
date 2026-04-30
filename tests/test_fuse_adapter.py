"""Unit tests for BiofuseOperations.

Exercises the adapter via direct async-method calls — no kernel mount.
End-to-end FUSE behaviour is covered by tests/test_mount.py.
"""

import errno
import os
import stat

import pyfuse3
import pytest
import trio

from biofuse import fuse_adapter, passthrough_view


def run(coro):
    """Run a coroutine to completion under trio."""
    return trio.run(lambda: coro)


@pytest.fixture
def fx_dir_with_files(tmp_path):
    (tmp_path / "alpha.bed").write_bytes(bytes(range(64)))
    (tmp_path / "alpha.bim").write_text("rec\n")
    (tmp_path / "alpha.fam").write_text("sample\n")
    return tmp_path


@pytest.fixture
def fx_view(fx_dir_with_files):
    v = passthrough_view.PassthroughDirectoryView(fx_dir_with_files)
    yield v
    v.close()


@pytest.fixture
def fx_ops(fx_view):
    return fuse_adapter.BiofuseOperations(fx_view)


def _expect_fuse_error(coro, expected_errno):
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        run(coro)
    assert excinfo.value.errno == expected_errno


class TestInodeAllocation:
    def test_root_inode_known(self, fx_ops):
        attrs = run(fx_ops.getattr(pyfuse3.ROOT_INODE))
        assert stat.S_ISDIR(attrs.st_mode)

    def test_each_file_has_distinct_inode(self, fx_ops):
        inodes = set(fx_ops._name_to_inode.values())
        assert len(inodes) == 3
        assert all(i > pyfuse3.ROOT_INODE for i in inodes)

    def test_inodes_are_assigned_in_sorted_order(self, fx_ops):
        names_in_order = [
            fx_ops._inode_to_name[i] for i in sorted(fx_ops._inode_to_name)
        ]
        assert names_in_order == sorted(names_in_order)


class TestGetattr:
    def test_root(self, fx_ops):
        attrs = run(fx_ops.getattr(pyfuse3.ROOT_INODE))
        assert stat.S_ISDIR(attrs.st_mode)
        assert attrs.st_size == 0

    def test_regular_file(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        attrs = run(fx_ops.getattr(inode))
        assert stat.S_ISREG(attrs.st_mode)
        assert attrs.st_size == 64

    def test_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.getattr(9999), errno.ENOENT)


class TestLookup:
    def test_known_name(self, fx_ops):
        attrs = run(fx_ops.lookup(pyfuse3.ROOT_INODE, b"alpha.bed"))
        assert attrs.st_size == 64

    def test_unknown_name(self, fx_ops):
        _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"missing.bed"), errno.ENOENT
        )

    def test_lookup_in_non_root(self, fx_ops):
        _expect_fuse_error(fx_ops.lookup(2, b"alpha.bed"), errno.ENOENT)

    def test_invalid_utf8_name(self, fx_ops):
        _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"\xff\xfe.bed"), errno.ENOENT
        )


class TestOpenReadRelease:
    def test_open_and_read_full(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            data = run(fx_ops.read(info.fh, 0, 1024))
            assert data == bytes(range(64))
        finally:
            run(fx_ops.release(info.fh))

    def test_open_with_write_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_WRONLY), errno.EROFS)

    def test_open_with_rdwr_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_RDWR), errno.EROFS)

    def test_open_with_append_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_RDONLY | os.O_APPEND), errno.EROFS)

    def test_open_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.open(9999, os.O_RDONLY), errno.ENOENT)

    def test_read_unknown_handle(self, fx_ops):
        _expect_fuse_error(fx_ops.read(9999, 0, 10), errno.EBADF)

    def test_read_at_offset(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert run(fx_ops.read(info.fh, 32, 16)) == bytes(range(32, 48))
        finally:
            run(fx_ops.release(info.fh))

    def test_read_past_eof_returns_empty(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert run(fx_ops.read(info.fh, 1024, 16)) == b""
        finally:
            run(fx_ops.release(info.fh))

    def test_release_unknown_handle_silent(self, fx_ops):
        run(fx_ops.release(9999))

    def test_release_is_idempotent(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        run(fx_ops.release(info.fh))
        run(fx_ops.release(info.fh))

    def test_concurrent_handles(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        info1 = run(fx_ops.open(inode, os.O_RDONLY))
        info2 = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert info1.fh != info2.fh
            assert run(fx_ops.read(info1.fh, 0, 10)) == bytes(range(10))
            assert run(fx_ops.read(info2.fh, 50, 10)) == bytes(range(50, 60))
        finally:
            run(fx_ops.release(info1.fh))
            run(fx_ops.release(info2.fh))


class TestOpendir:
    def test_root(self, fx_ops):
        fh = run(fx_ops.opendir(pyfuse3.ROOT_INODE))
        assert fh == pyfuse3.ROOT_INODE
        run(fx_ops.releasedir(fh))

    def test_non_root_is_notdir(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        _expect_fuse_error(fx_ops.opendir(inode), errno.ENOTDIR)


class TestAccess:
    def test_read_access_root(self, fx_ops):
        run(fx_ops.access(pyfuse3.ROOT_INODE, os.R_OK))

    def test_read_access_file(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        run(fx_ops.access(inode, os.R_OK))

    def test_write_access_denied(self, fx_ops):
        inode = fx_ops._name_to_inode["alpha.bed"]
        _expect_fuse_error(fx_ops.access(inode, os.W_OK), errno.EROFS)

    def test_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.access(9999, os.R_OK), errno.ENOENT)


class TestForget:
    def test_forget_is_noop(self, fx_ops):
        run(fx_ops.forget([(2, 1), (3, 1)]))
