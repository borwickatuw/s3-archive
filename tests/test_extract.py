"""Tests for streaming extract from S3 to S3."""

import pytest
import zstandard

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.extract import ExtractEvent, extract

from .conftest import SEVEN_Z_FLAVORS, build_7z, build_tar, build_tar_gz, build_zip


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
            s3_client, s3_client, "src-bucket", "in/archive.tar.gz", "dest-bucket", "out/", "tar.gz"
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

        extract(s3_client, s3_client, "src-bucket", "archive.tar.gz", "dest-bucket", "", "tar.gz")
        keys = _extracted_keys(s3_client, "dest-bucket", "")
        assert "a.txt" in keys


class TestExtractTar:
    """Plain (uncompressed) tar — tarfile.open mode 'r|'."""

    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.tar", "dest-bucket", "out/", "tar"
        )

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

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.zip", "dest-bucket", "out/", "zip"
        )

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


class TestExtract7z:
    """py7zr-backed extract — see :mod:`s3_archive.seven_z`."""

    @pytest.mark.parametrize("flavor", sorted(SEVEN_Z_FLAVORS))
    def test_round_trip(self, s3_client, sample_files, flavor):
        archive = build_7z(sample_files, flavor=flavor)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.7z", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.7z", "dest-bucket", "out/", "7z"
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert "out/sub/b.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"
        assert _body(s3_client, "dest-bucket", "out/sub/b.txt") == b"world\n"


def test_unsupported_format_raises(s3_client):
    with pytest.raises(UnsupportedArchiveFormatError, match="Unsupported format"):
        extract(s3_client, s3_client, "src-bucket", "x", "dest-bucket", "", "rar")


class TestExtractProgressCallback:
    """The on_progress callback is invoked with structured events."""

    def test_emits_boundary_event_per_member_with_member_metadata(self, s3_client, sample_files):
        # Use a tar.gz so members come from the tar path (not 7z's
        # pipe-thread model) — gives a simple deterministic sequence.
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        events: list[ExtractEvent] = []
        extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            on_progress=events.append,
        )

        # One boundary event per member, in archive order.
        boundary = [e for e in events if e.bytes_transferred == 0]
        assert [e.member for e in boundary] == list(sample_files)
        # member_index counts up from 0
        assert [e.member_index for e in boundary] == list(range(len(sample_files)))
        # member_size carries the known uncompressed size when the archive
        # exposes it (tar does)
        for ev in boundary:
            assert ev.member_size == len(sample_files[ev.member])

    def test_byte_events_sum_to_each_member_size(self, s3_client):
        # Pick a member large enough that boto3's multipart machinery
        # emits at least one Callback invocation per upload.
        files = {"big.bin": b"x" * (8 * 1024 * 1024)}
        archive = build_tar_gz(files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        events: list[ExtractEvent] = []
        extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            on_progress=events.append,
        )

        # Sum of byte-transfer events per member equals that member's size.
        for member, content in files.items():
            transferred = sum(e.bytes_transferred for e in events if e.member == member)
            assert transferred == len(content)

    def test_dry_run_still_emits_boundary_events(self, s3_client, sample_files):
        # In dry_run mode no upload happens, but operators still benefit
        # from seeing what *would* be written; boundary events let the
        # UI render that list incrementally without buffering.
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        events: list[ExtractEvent] = []
        extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            dry_run=True,
            on_progress=events.append,
        )

        # Only boundary events — no byte-transfer events when nothing is uploaded.
        assert all(e.bytes_transferred == 0 for e in events)
        assert {e.member for e in events} == set(sample_files)


class TestExtractDualEndpoint:
    """Real two-endpoint wiring via `cross_env_real_endpoints` (moto-server)."""

    def test_extracts_across_endpoints(self, cross_env_real_endpoints):
        src = cross_env_real_endpoints["src"]
        dst = cross_env_real_endpoints["dst"]

        files = {"a.txt": b"alpha\n", "sub/b.txt": b"beta\n"}
        src["client"].put_object(
            Bucket=src["bucket"], Key="in/archive.tar.gz", Body=build_tar_gz(files)
        )

        members = extract(
            src["client"],
            dst["client"],
            src["bucket"],
            "in/archive.tar.gz",
            dst["bucket"],
            "out/",
            "tar.gz",
        )

        assert set(members) == set(files)
        # Members land in the *destination* endpoint's bucket and not
        # the source endpoint — verify both ways to catch cross-talk.
        dst_keys = _extracted_keys(dst["client"], dst["bucket"], "out/")
        assert "out/a.txt" in dst_keys
        assert "out/sub/b.txt" in dst_keys
        src_keys = _extracted_keys(src["client"], src["bucket"], "out/")
        assert src_keys == []
