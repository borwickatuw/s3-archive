"""Tests for streaming extract from S3 to S3."""

import pytest
import zstandard

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.extract import extract

from .conftest import build_tar, build_tar_gz, build_zip


@pytest.fixture
def sample_files():
    return {"a.txt": b"hello\n", "sub/b.txt": b"world\n"}


def _extracted_keys(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


def _body(s3, bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


class TestExtractTarGz:
    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        members = extract(
            s3_client, "src-bucket", "in/archive.tar.gz", "dest-bucket", "out/", "tar.gz"
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert "out/sub/b.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"

    def test_dry_run_uploads_nothing(self, s3_client, sample_files):
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        members = extract(
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            dry_run=True,
        )

        assert set(members) == set(sample_files)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []

    def test_empty_prefix(self, s3_client, sample_files):
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="archive.tar.gz", Body=archive)

        extract(s3_client, "src-bucket", "archive.tar.gz", "dest-bucket", "", "tar.gz")
        keys = _extracted_keys(s3_client, "dest-bucket", "")
        assert "a.txt" in keys


class TestExtractTar:
    """Plain (uncompressed) tar — tarfile.open mode 'r|'."""

    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar", Body=archive)

        members = extract(s3_client, "src-bucket", "in/archive.tar", "dest-bucket", "out/", "tar")

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"


class TestExtractTarBz2:
    """bzip2-compressed tar — exercises the dispatch into mode 'r|bz2'."""

    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w:bz2")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.bz2", Body=archive)

        members = extract(
            s3_client,
            "src-bucket",
            "in/archive.tar.bz2",
            "dest-bucket",
            "out/",
            "tar.bz2",
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"


class TestExtractTarZst:
    """zstandard-compressed tar — wired via members.py's ZstdDecompressor path."""

    def test_round_trip(self, s3_client, sample_files):
        inner = build_tar(sample_files, mode="w")
        archive = zstandard.ZstdCompressor().compress(inner)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.zst", Body=archive)

        members = extract(
            s3_client,
            "src-bucket",
            "in/archive.tar.zst",
            "dest-bucket",
            "out/",
            "tar.zst",
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"


class TestExtractZip:
    def test_round_trip(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)

        members = extract(s3_client, "src-bucket", "in/archive.zip", "dest-bucket", "out/", "zip")

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert "out/sub/b.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"

    def test_dry_run_uploads_nothing(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        members = extract(
            s3_client,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            dry_run=True,
        )
        assert set(members) == set(sample_files)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []


def test_unsupported_format_raises(s3_client):
    with pytest.raises(UnsupportedArchiveFormatError, match="Unsupported format"):
        extract(s3_client, "src-bucket", "x", "dest-bucket", "", "7z")
