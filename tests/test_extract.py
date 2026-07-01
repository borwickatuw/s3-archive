"""Tests for streaming extract from S3 to S3."""

import pytest
import zstandard

from s3_archive import resume
from s3_archive.exceptions import (
    ResumeUnsupportedError,
    UnsafeArchiveMemberError,
    UnsupportedArchiveFormatError,
)
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


class TestExtractPathNormalization:
    """End-to-end: normalized member names drive the destination S3 keys."""

    def test_windows_zip_dest_keys_are_forward_slashed(self, s3_client):
        archive = build_zip({"Image repository\\UW26509z.tif": b"tiff"})
        s3_client.put_object(Bucket="src-bucket", Key="in/win.zip", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/win.zip", "dest-bucket", "out/", "zip"
        )

        assert members == ["Image repository/UW26509z.tif"]
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert keys == ["out/Image repository/UW26509z.tif"]
        # No literal backslash survives into the destination key.
        assert not any("\\" in k for k in keys)

    def test_dotdot_member_raises(self, s3_client):
        archive = build_tar({"../evil.txt": b"x"}, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/evil.tar", Body=archive)

        with pytest.raises(UnsafeArchiveMemberError):
            extract(s3_client, s3_client, "src-bucket", "in/evil.tar", "dest-bucket", "out/", "tar")

    def test_dotdot_member_collapses_with_fix_unsafe_paths(self, s3_client):
        archive = build_tar({"a/../safe.txt": b"payload"}, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/fix.tar", Body=archive)

        members = extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/fix.tar",
            "dest-bucket",
            "out/",
            "tar",
            fix_unsafe_paths=True,
        )

        assert members == ["safe.txt"]
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/safe.txt"]


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


class _UploadSpyClient:
    """Delegate to a real client, recording the Key of each upload_fileobj call.

    Lets a resume test assert *which* members were actually re-transferred
    (vs. skipped as already-present) without inspecting S3 timing.
    """

    def __init__(self, real):
        self._real = real
        self.uploaded_keys: list[str] = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def upload_fileobj(self, fileobj, bucket, key, **kwargs):
        self.uploaded_keys.append(key)
        return self._real.upload_fileobj(fileobj, bucket, key, **kwargs)


def _src_etag(s3, bucket, key):
    return s3.head_object(Bucket=bucket, Key=key)["ETag"]


def _write_matching_control(s3, *, src_bucket, src_key, dest_bucket, dest_prefix, fmt):
    """Write a control marker for the *actual* source ETag (a resumable run)."""
    etag = _src_etag(s3, src_bucket, src_key)
    ckey = resume.control_key(dest_prefix, etag)
    resume.write_control_file(
        s3,
        dest_bucket,
        ckey,
        source_etag=etag,
        source_size=1,
        fmt=fmt,
        now_iso="2026-07-01T00:00:00+00:00",
    )
    return ckey


class TestExtractResumeZip:
    """--resume over a zip: skip already-written members, transfer the rest."""

    def test_skip_present_reuploads_only_the_missing_member(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        # Full extract, then knock out one destination object to simulate a
        # run that died just before finishing it.
        extract(s3_client, s3_client, "src-bucket", "in/archive.zip", "dest-bucket", "out/", "zip")
        s3_client.delete_object(Bucket="dest-bucket", Key="out/a.txt")
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/archive.zip",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="zip",
        )

        spy = _UploadSpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        # Full member list is still reported, but only the dropped member
        # was actually re-uploaded.
        assert set(members) == set(sample_files)
        assert spy.uploaded_keys == ["out/a.txt"]
        # Both members present; the control marker is gone on clean finish.
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/a.txt", "out/sub/b.txt"]
        assert resume.control_file_exists(s3_client, "dest-bucket", ckey) is False

    def test_interrupt_then_resume_transfers_only_missing(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        # Pre-populate the destination with a subset (as a prior run would
        # have) at the correct size, plus the control marker.
        s3_client.put_object(
            Bucket="dest-bucket", Key="out/sub/b.txt", Body=sample_files["sub/b.txt"]
        )
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/archive.zip",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="zip",
        )

        spy = _UploadSpyClient(s3_client)
        extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        assert spy.uploaded_keys == ["out/a.txt"]
        assert _body(s3_client, "dest-bucket", "out/a.txt") == sample_files["a.txt"]
        assert resume.control_file_exists(s3_client, "dest-bucket", ckey) is False

    def test_no_control_marker_means_fresh_run_writes_everything(self, s3_client, sample_files):
        # A pre-existing object with no matching control marker is NOT
        # vouched for: a fresh --resume run must not trust it, so it writes
        # every member (and the marker it creates is cleaned up at the end).
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        s3_client.put_object(Bucket="dest-bucket", Key="out/a.txt", Body=b"stale")

        spy = _UploadSpyClient(s3_client)
        extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        assert sorted(spy.uploaded_keys) == ["out/a.txt", "out/sub/b.txt"]
        # The stale object was overwritten with the real member bytes.
        assert _body(s3_client, "dest-bucket", "out/a.txt") == sample_files["a.txt"]
        etag = _src_etag(s3_client, "src-bucket", "in/archive.zip")
        assert (
            resume.control_file_exists(s3_client, "dest-bucket", resume.control_key("out/", etag))
            is False
        )

    def test_identity_guard_wrong_etag_marker_skips_nothing(self, s3_client, sample_files):
        # A marker left by a DIFFERENT source (wrong ETag) must not make us
        # treat this source's destination objects as done.
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        # Destination already fully populated, but only under a foreign marker.
        for name, body in sample_files.items():
            s3_client.put_object(Bucket="dest-bucket", Key=f"out/{name}", Body=body)
        foreign_key = resume.control_key("out/", '"some-other-archive-etag"')
        resume.write_control_file(
            s3_client,
            "dest-bucket",
            foreign_key,
            source_etag='"some-other-archive-etag"',
            source_size=1,
            fmt="zip",
            now_iso="2026-07-01T00:00:00+00:00",
        )

        spy = _UploadSpyClient(s3_client)
        extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        # Nothing was skipped — every member re-uploaded despite being present.
        assert sorted(spy.uploaded_keys) == ["out/a.txt", "out/sub/b.txt"]
        # The foreign marker is untouched (we only ever manage our own).
        assert resume.control_file_exists(s3_client, "dest-bucket", foreign_key) is True

    def test_no_central_directory_refuses(self, s3_client):
        # A .zip that isn't a real zip (no usable central directory) can't
        # be walked per-member → refuse, without leaving a control marker.
        s3_client.put_object(Bucket="src-bucket", Key="in/bad.zip", Body=b"not a zip at all")

        with pytest.raises(ResumeUnsupportedError):
            extract(
                s3_client,
                s3_client,
                "src-bucket",
                "in/bad.zip",
                "dest-bucket",
                "out/",
                "zip",
                resume=True,
            )
        assert _no_control_markers(s3_client, "dest-bucket")


class TestExtractResumeTar:
    """--resume over an uncompressed tar mirrors the zip skip behavior."""

    def test_skip_present_reuploads_only_the_missing_member(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar", Body=archive)
        extract(s3_client, s3_client, "src-bucket", "in/archive.tar", "dest-bucket", "out/", "tar")
        s3_client.delete_object(Bucket="dest-bucket", Key="out/sub/b.txt")
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/archive.tar",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="tar",
        )

        spy = _UploadSpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.tar",
            "dest-bucket",
            "out/",
            "tar",
            resume=True,
        )

        assert set(members) == set(sample_files)
        assert spy.uploaded_keys == ["out/sub/b.txt"]
        assert _body(s3_client, "dest-bucket", "out/sub/b.txt") == sample_files["sub/b.txt"]
        assert resume.control_file_exists(s3_client, "dest-bucket", ckey) is False


class TestExtractResumeRefuse:
    """Non-seekable formats refuse up front and write no control marker."""

    @pytest.mark.parametrize(
        ("fmt", "key", "body_factory"),
        [
            ("tar.gz", "in/a.tar.gz", lambda: build_tar_gz({"a.txt": b"x"})),
            ("tar.zst", "in/a.tar.zst", lambda: zstandard.ZstdCompressor().compress(b"x")),
            ("7z", "in/a.7z", lambda: b"7z placeholder"),
        ],
    )
    def test_refuse_writes_no_control_file(self, s3_client, fmt, key, body_factory):
        s3_client.put_object(Bucket="src-bucket", Key=key, Body=body_factory())

        with pytest.raises(ResumeUnsupportedError):
            extract(
                s3_client, s3_client, "src-bucket", key, "dest-bucket", "out/", fmt, resume=True
            )

        # Fail-fast contract: nothing extracted, no marker left behind.
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []
        assert _no_control_markers(s3_client, "dest-bucket")


def _no_control_markers(s3, bucket):
    return not any(resume.is_control_key(k) for k in _extracted_keys(s3, bucket, ""))
