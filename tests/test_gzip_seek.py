"""Tests for the seekable-gzip layer (indexed_gzip over SeekableS3Object)."""

import gzip as gzip_mod
import io

import pytest

from s3_archive.exceptions import ResumeUnsupportedError
from s3_archive.gzip_seek import export_index_bytes, open_tar_gz_seekable
from s3_archive.seekable import _BUFFER_SIZE, SeekableS3Object

from .conftest import build_tar_gz, build_zip, incompressible_bytes

# Comfortably above indexed_gzip's ~32 KB window floor, but tiny enough that
# a few-MB fixture accrues several seek points.
_TINY_SPACING = 131072


def _buffered_source(s3, bucket, key):
    raw = SeekableS3Object(s3, bucket, key)
    return io.BufferedReader(raw, buffer_size=_BUFFER_SIZE)


class TestOpenTarGzSeekable:
    def test_iterates_members_byte_for_byte(self, s3_client):
        files = {f"m{i}.bin": incompressible_bytes(256 * 1024, seed=i) for i in range(4)}
        s3_client.put_object(Bucket="src-bucket", Key="a.tar.gz", Body=build_tar_gz(files))

        _igzf, members = open_tar_gz_seekable(
            _buffered_source(s3_client, "src-bucket", "a.tar.gz"), spacing=_TINY_SPACING
        )
        got = {m.name: m.read_all() for m in members}
        assert got == files

    def test_export_then_import_seeks_to_late_member(self, s3_client):
        # Build the index by a full forward pass (draining every member in
        # small chunks accrues seek points), export it, then reopen with the
        # imported index and confirm the *last* member reads correctly — the
        # seek jumped past the earlier ones.
        files = {f"m{i}.bin": incompressible_bytes(512 * 1024, seed=100 + i) for i in range(4)}
        s3_client.put_object(Bucket="src-bucket", Key="a.tar.gz", Body=build_tar_gz(files))

        igzf, members = open_tar_gz_seekable(
            _buffered_source(s3_client, "src-bucket", "a.tar.gz"), spacing=_TINY_SPACING
        )
        for m in members:
            m.drain()
        index_bytes = export_index_bytes(igzf)
        # A real, populated index — a forward read that accrued no points
        # would export only a tiny header.
        assert len(index_bytes) > 1024

        _igzf2, members2 = open_tar_gz_seekable(
            _buffered_source(s3_client, "src-bucket", "a.tar.gz"),
            index_bytes=index_bytes,
            spacing=_TINY_SPACING,
        )
        seen = {}
        for m in members2:
            seen[m.name] = m.read_all()
        assert seen["m3.bin"] == files["m3.bin"]
        assert seen == files

    def test_corrupt_index_falls_back_to_forward_decode(self, s3_client, caplog):
        # A garbage .idx must not break extraction — it's a pure
        # optimization, so import fails, we warn, and iterate correctly.
        files = {"a.txt": b"hello\n", "b.txt": b"world\n"}
        s3_client.put_object(Bucket="src-bucket", Key="a.tar.gz", Body=build_tar_gz(files))

        _igzf, members = open_tar_gz_seekable(
            _buffered_source(s3_client, "src-bucket", "a.tar.gz"),
            index_bytes=b"this is not a real indexed_gzip index",
            spacing=_TINY_SPACING,
        )
        got = {m.name: m.read_all() for m in members}
        assert got == files
        assert any("failed to import" in r.message for r in caplog.records)

    def test_non_gzip_body_refuses(self, s3_client):
        # A .tar.gz whose bytes aren't gzip (here: a zip) can't be opened as
        # a seekable gzip → ResumeUnsupportedError (surfaced before any marker
        # is written by the caller).
        s3_client.put_object(Bucket="src-bucket", Key="a.tar.gz", Body=build_zip({"a.txt": b"x"}))

        with pytest.raises(ResumeUnsupportedError):
            open_tar_gz_seekable(
                _buffered_source(s3_client, "src-bucket", "a.tar.gz"), spacing=_TINY_SPACING
            )

    def test_gzip_but_not_tar_refuses(self, s3_client):
        s3_client.put_object(
            Bucket="src-bucket", Key="a.tar.gz", Body=gzip_mod.compress(b"not a tar stream")
        )

        with pytest.raises(ResumeUnsupportedError):
            open_tar_gz_seekable(
                _buffered_source(s3_client, "src-bucket", "a.tar.gz"), spacing=_TINY_SPACING
            )
