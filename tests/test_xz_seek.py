"""Tests for the seekable-xz layer (python-xz over SeekableS3Object)."""

import contextlib
import io
import shutil
import subprocess

import pytest

from s3_archive.exceptions import ResumeUnsupportedError
from s3_archive.seekable import _BUFFER_SIZE, SeekableS3Object
from s3_archive.xz_seek import open_tar_xz_seekable

from .conftest import build_tar, build_tar_xz_multiblock, build_zip, incompressible_bytes


def _buffered_source(s3, bucket, key):
    raw = SeekableS3Object(s3, bucket, key)
    return io.BufferedReader(raw, buffer_size=_BUFFER_SIZE)


def _multiblock_xz_bytes(payload: bytes, *, block_size: str = "1MiB") -> bytes:
    """Multi-block .xz of arbitrary (non-tar) *payload* via the xz CLI."""
    if shutil.which("xz") is None:
        pytest.skip("xz CLI not installed")
    # cmd is built from constants; not user input.
    cmd = ["xz", "-z", f"--block-size={block_size}", "-c", "-"]
    return subprocess.run(cmd, input=payload, capture_output=True, check=True).stdout  # noqa: S603


class TestOpenTarXzSeekable:
    def test_iterates_multiblock_members_byte_for_byte(self, s3_client):
        files = {f"m{i}.bin": incompressible_bytes(512 * 1024, seed=i) for i in range(4)}
        s3_client.put_object(
            Bucket="src-bucket", Key="a.tar.xz", Body=build_tar_xz_multiblock(files)
        )

        xzf, members = open_tar_xz_seekable(_buffered_source(s3_client, "src-bucket", "a.tar.xz"))
        try:
            got = {m.name: m.read_all() for m in members}
        finally:
            with contextlib.suppress(Exception):
                xzf.close()
        assert got == files

    def test_single_block_xz_refuses(self, s3_client):
        # stdlib lzma (tar -J style) emits one block → no interior seek point.
        s3_client.put_object(
            Bucket="src-bucket", Key="a.tar.xz", Body=build_tar({"a.txt": b"x"}, mode="w:xz")
        )
        with pytest.raises(ResumeUnsupportedError, match="single xz block"):
            open_tar_xz_seekable(_buffered_source(s3_client, "src-bucket", "a.tar.xz"))

    def test_non_xz_body_refuses(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="a.tar.xz", Body=build_zip({"a.txt": b"x"}))
        with pytest.raises(ResumeUnsupportedError):
            open_tar_xz_seekable(_buffered_source(s3_client, "src-bucket", "a.tar.xz"))

    def test_multiblock_xz_but_not_tar_refuses(self, s3_client):
        # A valid multi-block .xz whose decompressed bytes aren't a tar: it
        # clears the block-count gate but fails the tar open.
        body = _multiblock_xz_bytes(incompressible_bytes(2 * 1024 * 1024, seed=7))
        s3_client.put_object(Bucket="src-bucket", Key="a.tar.xz", Body=body)
        with pytest.raises(ResumeUnsupportedError):
            open_tar_xz_seekable(_buffered_source(s3_client, "src-bucket", "a.tar.xz"))
