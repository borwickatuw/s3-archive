"""Tests for the streaming `s3-archive ls` subcommand."""

import pytest
import zstandard

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.ls import _format_size, list_archive

from .conftest import build_tar, build_zip


@pytest.fixture
def sample_files():
    return {"a.txt": b"hello\n", "sub/b.txt": b"world\n"}


class TestListTar:
    @pytest.mark.parametrize(
        "fmt,mode,suffix",
        [
            ("tar", "w", "tar"),
            ("tar.gz", "w:gz", "tar.gz"),
            ("tar.bz2", "w:bz2", "tar.bz2"),
        ],
    )
    def test_streams_member_names(self, s3_client, sample_files, capsys, fmt, mode, suffix):
        archive = build_tar(sample_files, mode=mode)
        s3_client.put_object(Bucket="src-bucket", Key=f"in/archive.{suffix}", Body=archive)

        count, total = list_archive(s3_client, "src-bucket", f"in/archive.{suffix}", fmt)

        out = capsys.readouterr().out
        assert "a.txt" in out
        assert "sub/b.txt" in out
        assert count == len(sample_files)
        assert total >= sum(len(c) for c in sample_files.values())
        assert " files, " in out


class TestListTarZst:
    def test_streams_member_names(self, s3_client, sample_files, capsys):
        inner = build_tar(sample_files, mode="w")
        archive = zstandard.ZstdCompressor().compress(inner)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.zst", Body=archive)

        count, total = list_archive(s3_client, "src-bucket", "in/archive.tar.zst", "tar.zst")

        out = capsys.readouterr().out
        assert "a.txt" in out
        assert "sub/b.txt" in out
        assert count == len(sample_files)
        assert total >= sum(len(c) for c in sample_files.values())


class TestListZip:
    def test_streams_member_names(self, s3_client, sample_files, capsys):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)

        count, total = list_archive(s3_client, "src-bucket", "in/archive.zip", "zip")

        out = capsys.readouterr().out
        assert "a.txt" in out
        assert count == len(sample_files)
        assert total > 0


class TestUnsupportedFormat:
    def test_raises(self, s3_client):
        with pytest.raises(UnsupportedArchiveFormatError, match="Unsupported format"):
            list_archive(s3_client, "src-bucket", "x", "7z")


class TestFormatSize:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (0, "0 B"),
            (512, "512 B"),
            (1024, "1.0 KB"),
            (1536, "1.5 KB"),
            (1024 * 1024 + 500_000, "1.5 MB"),
        ],
    )
    def test_format_size(self, value, expected):
        assert _format_size(value) == expected
