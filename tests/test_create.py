"""Tests for streaming archive create (S3 prefix → archive in S3)."""

import botocore.exceptions
import pytest

from s3_archive.create import create, create_tar_gz, create_zip
from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.extract import extract


def _put_source(client, bucket: str, prefix: str, files: dict[str, bytes]) -> None:
    for rel, content in files.items():
        client.put_object(Bucket=bucket, Key=prefix + rel, Body=content)


def _list_keys(client, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


def _body(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


class TestCreateTarGz:
    def test_round_trip(self, s3_client):
        """Create a tar.gz from a prefix, re-extract, verify contents match."""
        source = {"a.txt": b"alpha\n", "sub/b.txt": b"beta\n", "sub/deep/c.txt": b"gamma\n"}
        _put_source(s3_client, "src-bucket", "src/", source)

        create_tar_gz(s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.tar.gz")

        # Re-extract and verify byte-for-byte.
        members = extract(
            s3_client, s3_client, "dest-bucket", "archive.tar.gz", "dest-bucket", "out/", "tar.gz"
        )
        assert set(members) == set(source)
        for rel, expected in source.items():
            assert _body(s3_client, "dest-bucket", "out/" + rel) == expected

    def test_dry_run_uploads_nothing(self, s3_client):
        _put_source(s3_client, "src-bucket", "src/", {"a.txt": b"alpha"})
        create_tar_gz(
            s3_client,
            s3_client,
            "src-bucket",
            "src/",
            "dest-bucket",
            "archive.tar.gz",
            dry_run=True,
        )
        assert _list_keys(s3_client, "dest-bucket", "") == []

    def test_empty_source_emits_warning_and_skips_upload(self, s3_client):
        # Nothing under src/; create should be a no-op.
        create_tar_gz(s3_client, s3_client, "src-bucket", "empty/", "dest-bucket", "archive.tar.gz")
        assert _list_keys(s3_client, "dest-bucket", "") == []

    def test_skips_directory_markers(self, s3_client):
        _put_source(s3_client, "src-bucket", "src/", {"a.txt": b"hello"})
        s3_client.put_object(Bucket="src-bucket", Key="src/empty-dir/", Body=b"")

        create_tar_gz(s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.tar.gz")
        members = extract(
            s3_client, s3_client, "dest-bucket", "archive.tar.gz", "dest-bucket", "out/", "tar.gz"
        )
        # The dir marker (src/empty-dir/) must not appear as a member.
        assert "empty-dir/" not in members
        assert members == ["a.txt"]


class TestCreateZip:
    def test_round_trip(self, s3_client):
        source = {"a.txt": b"alpha\n", "sub/b.txt": b"beta\n"}
        _put_source(s3_client, "src-bucket", "src/", source)

        create_zip(s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.zip")

        members = extract(
            s3_client, s3_client, "dest-bucket", "archive.zip", "dest-bucket", "out/", "zip"
        )
        assert set(members) == set(source)
        for rel, expected in source.items():
            assert _body(s3_client, "dest-bucket", "out/" + rel) == expected

    def test_dry_run_uploads_nothing(self, s3_client):
        _put_source(s3_client, "src-bucket", "src/", {"a.txt": b"alpha"})
        create_zip(
            s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.zip", dry_run=True
        )
        assert _list_keys(s3_client, "dest-bucket", "") == []


class TestCreateDispatch:
    def test_tar_gz(self, s3_client):
        _put_source(s3_client, "src-bucket", "src/", {"a.txt": b"alpha"})
        create(
            s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.tar.gz", "tar.gz"
        )
        assert "archive.tar.gz" in _list_keys(s3_client, "dest-bucket", "")

    def test_zip(self, s3_client):
        _put_source(s3_client, "src-bucket", "src/", {"a.txt": b"alpha"})
        create(s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.zip", "zip")
        assert "archive.zip" in _list_keys(s3_client, "dest-bucket", "")

    def test_unsupported_format_raises(self, s3_client):
        with pytest.raises(UnsupportedArchiveFormatError, match="not supported"):
            create(s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.tar", "tar")

    def test_rejects_7z(self, s3_client):
        with pytest.raises(UnsupportedArchiveFormatError, match=r"\.7z create is not supported"):
            create(s3_client, s3_client, "src-bucket", "src/", "dest-bucket", "archive.7z", "7z")


class _DropAfterBody:
    """Wrap a real StreamingBody, dropping mid-stream after *drop_after* bytes.

    Serves at most *chunk_cap* bytes per read (so a modest object still
    spans several reads and the drop lands *before* EOF, leaving a
    non-empty tail for the ranged resume). Once *drop_after* bytes have
    been served, the next read raises a transient error.
    """

    def __init__(self, body, *, drop_after, chunk_cap=64):
        self._body = body
        self._served = 0
        self._drop_after = drop_after
        self._chunk_cap = chunk_cap

    def read(self, size=-1):
        if self._served >= self._drop_after:
            raise botocore.exceptions.ResponseStreamingError(
                error=ValueError("Connection broken: IncompleteRead")
            )
        want = self._chunk_cap if size is None or size < 0 else min(size, self._chunk_cap)
        chunk = self._body.read(want)
        self._served += len(chunk)
        return chunk


class _DropOnceClient:
    """Delegate to a real client, dropping the first read of one source key.

    The initial (unranged) GET of *drop_key* returns a body that serves
    part of the object then raises a transient error; the resume (ranged)
    GET delegates straight to the real client, which serves the tail via
    a normal ``Range`` response. Everything else (list_objects, uploads)
    passes through untouched.
    """

    def __init__(self, real, *, drop_key, drop_after):
        self._real = real
        self._drop_key = drop_key
        self._drop_after = drop_after
        self._dropped = False
        self.ranged_resume_seen = False

    def __getattr__(self, name):
        return getattr(self._real, name)

    def get_object(self, **kwargs):
        if kwargs.get("Key") == self._drop_key and "Range" in kwargs:
            self.ranged_resume_seen = True
        resp = self._real.get_object(**kwargs)
        if kwargs.get("Key") == self._drop_key and "Range" not in kwargs and not self._dropped:
            self._dropped = True
            resp["Body"] = _DropAfterBody(resp["Body"], drop_after=self._drop_after)
        return resp


class TestCreateResumable:
    """Per-source-object reads survive a transient mid-object drop.

    A connection drop while reading one source object mid-archive should
    resume via a ranged GET rather than aborting the whole create.
    """

    def test_tar_gz_resumes_after_source_object_drop(self, s3_client, monkeypatch):
        monkeypatch.setattr("s3_archive.retry.time.sleep", lambda _s: None)
        content = b"abcdefghij" * 100  # 1000 bytes, spans several capped reads
        _put_source(s3_client, "src-bucket", "src/", {"big.bin": content})
        flaky = _DropOnceClient(s3_client, drop_key="src/big.bin", drop_after=100)

        create_tar_gz(flaky, s3_client, "src-bucket", "src/", "dest-bucket", "archive.tar.gz")

        assert flaky.ranged_resume_seen
        members = extract(
            s3_client, s3_client, "dest-bucket", "archive.tar.gz", "dest-bucket", "out/", "tar.gz"
        )
        assert members == ["big.bin"]
        assert _body(s3_client, "dest-bucket", "out/big.bin") == content

    def test_zip_resumes_after_source_object_drop(self, s3_client, monkeypatch):
        monkeypatch.setattr("s3_archive.retry.time.sleep", lambda _s: None)
        content = b"0123456789" * 100
        _put_source(s3_client, "src-bucket", "src/", {"big.bin": content})
        flaky = _DropOnceClient(s3_client, drop_key="src/big.bin", drop_after=100)

        create_zip(flaky, s3_client, "src-bucket", "src/", "dest-bucket", "archive.zip")

        assert flaky.ranged_resume_seen
        members = extract(
            s3_client, s3_client, "dest-bucket", "archive.zip", "dest-bucket", "out/", "zip"
        )
        assert members == ["big.bin"]
        assert _body(s3_client, "dest-bucket", "out/big.bin") == content


class TestCreateDualEndpoint:
    """Real two-endpoint wiring via `cross_env_real_endpoints` (moto-server)."""

    def test_creates_across_endpoints(self, cross_env_real_endpoints):
        src = cross_env_real_endpoints["src"]
        dst = cross_env_real_endpoints["dst"]

        _put_source(src["client"], src["bucket"], "src/", {"a.txt": b"alpha\n"})

        create_tar_gz(
            src["client"],
            dst["client"],
            src["bucket"],
            "src/",
            dst["bucket"],
            "archive.tar.gz",
        )

        # Archive lands in destination bucket and not source.
        head = dst["client"].head_object(Bucket=dst["bucket"], Key="archive.tar.gz")
        assert head["ContentLength"] > 0
        assert _list_keys(src["client"], src["bucket"], "archive.tar.gz") == []
