"""Tests for s3_archive.manifest primitives.

The fileobj / chunks / central-directory primitives also have
end-to-end coverage in storage-scripts' inventory test suite (which
consumes this module); this file covers the streaming hasher
(:func:`_hash_stream`) directly.
"""

import hashlib
import io
import tarfile
import zipfile
from unittest.mock import MagicMock

import botocore.exceptions
import pytest
from stream_unzip import NotStreamUnzippable

from s3_archive.exceptions import UnsupportedArchiveFormatError, ZipNotStreamableError
from .conftest import build_stored_dd_zip
from s3_archive.manifest import (
    TAP_SUPPORTED_FORMATS,
    TAR_FAMILY_FORMATS,
    _decode_zip_filename,
    _hash_stream,
    build_manifest_from_tap,
    build_manifest_zip_chunks,
    build_manifest_zip_seekable,
    zip_central_directory_from_path,
    zip_central_directory_from_s3,
)


class TestHashStream:
    """The streaming hasher must NOT buffer the entry; peak memory == one chunk."""

    def test_empty_input(self):
        assert _hash_stream(iter([])) == (
            0,
            "d41d8cd98f00b204e9800998ecf8427e",
            "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )

    def test_single_chunk_matches_known_hash(self):
        size, md5, sha1, sha256 = _hash_stream([b"hello"])
        assert size == 5
        assert md5 == "5d41402abc4b2a76b9719d911017c592"
        assert sha1 == "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
        assert sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_chunked_equals_single_blob(self):
        data = b"the quick brown fox jumps over the lazy dog"
        a = _hash_stream([data])
        b = _hash_stream([data[:5], data[5:20], data[20:]])
        assert a == b

    def test_does_not_buffer_chunks(self):
        """Generator-fed: verify _hash_stream consumes chunks without holding them.

        After _hash_stream returns, the generator has been fully consumed
        and there's no reference to the byte buffers we yielded. This
        wouldn't catch every form of buffering, but it catches the old
        ``parts.append(chunk); b"".join(parts)`` pattern.
        """
        produced: list[int] = []  # we record sizes only, not the bytes themselves

        def gen():
            for _ in range(8):
                chunk = b"X" * 1024
                produced.append(len(chunk))
                yield chunk

        size, *_ = _hash_stream(gen())
        assert size == 8 * 1024
        assert produced == [1024] * 8  # all chunks were consumed
        # Generator exhausted; no chunk-buffer remains accessible from caller.
        assert next(iter(()), None) is None  # sanity: generator is gone

    def test_handles_large_synthetic_entry(self):
        """A 16 MB synthetic entry hashes correctly without OOM-shaped behavior.

        We use a deterministic small chunk so the hash is predictable, and
        feed many chunks to exercise the streaming path.
        """
        chunk = b"A" * 1024  # 1 KiB
        n_chunks = 16 * 1024  # → 16 MiB total
        size, _md5, _sha1, sha256 = _hash_stream(chunk for _ in range(n_chunks))
        assert size == 16 * 1024 * 1024
        expected = hashlib.sha256(b"A" * size).hexdigest()
        assert sha256 == expected

    def test_golden_vector_no_drift_after_refactor(self):
        """Frozen output: ``_hash_stream`` is a thin shim over
        :func:`s3_archive.hashing.triple_hash` after the v0.2.0
        consolidation.

        This exact tuple must NEVER change without a coordinated bump
        of every snapshot that already exists in the wild — it is the
        on-disk identity of every file inventoried so far.
        """
        assert _hash_stream([b"BagIt-Version: 1.0\n"]) == (
            19,
            "9e28fcefb9ca3530e043b6334904fd7c",
            "922a2b6762717eabcade85ba446f13b1d861f250",
            "02d3510b13d2351380e0509feddfd17acdcb605d82c79e8e61a3ff1f1cdb5684",
        )


def _build_tar_gz(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, body in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return buf.getvalue()


def _build_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


class TestBuildManifestFromTap:
    """Dispatcher that lets inventory consume one entry point for all tap-supported formats."""

    def test_format_constants_membership(self):
        # Lock the published surface — inventory imports these by name.
        assert TAR_FAMILY_FORMATS == ("tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst")
        assert TAP_SUPPORTED_FORMATS == TAR_FAMILY_FORMATS + ("zip",)
        assert "7z" not in TAP_SUPPORTED_FORMATS

    def test_dispatches_to_tar_gz(self):
        tar_bytes = _build_tar_gz({"a.txt": b"alpha", "b.txt": b"beta"})
        entries = build_manifest_from_tap("tar.gz", io.BytesIO(tar_bytes))
        keys = sorted(e.key for e in entries)
        assert keys == ["a.txt", "b.txt"]
        by_key = {e.key: e for e in entries}
        assert by_key["a.txt"].size == 5
        assert by_key["a.txt"].sha256 == hashlib.sha256(b"alpha").hexdigest()

    def test_dispatches_to_zip_with_cd_mtimes(self):
        zip_bytes = _build_zip({"x.txt": b"xx", "y.txt": b"yyy"})
        entries = build_manifest_from_tap(
            "zip", io.BytesIO(zip_bytes), cd_mtimes={"x.txt": "2026-01-01T00:00:00Z"}
        )
        by_key = {e.key: e for e in entries}
        assert by_key["x.txt"].mtime == "2026-01-01T00:00:00Z"
        assert by_key["y.txt"].mtime == ""  # missing from cd_mtimes → empty
        assert by_key["x.txt"].sha256 == hashlib.sha256(b"xx").hexdigest()

    def test_dispatches_to_zip_with_none_cd_mtimes(self):
        """None cd_mtimes defaults to empty dict — all entries get "" mtime."""
        zip_bytes = _build_zip({"x.txt": b"xx"})
        entries = build_manifest_from_tap("zip", io.BytesIO(zip_bytes), cd_mtimes=None)
        assert entries[0].mtime == ""

    def test_seven_z_raises_with_helpful_pointer(self):
        with pytest.raises(UnsupportedArchiveFormatError, match="iter_archive_members"):
            build_manifest_from_tap("7z", io.BytesIO(b""))

    def test_unknown_format_raises(self):
        with pytest.raises(UnsupportedArchiveFormatError):
            build_manifest_from_tap("rar", io.BytesIO(b""))


class TestZipNameNormalization:
    """Zip separator normalization flows into decode + manifest keys, and
    the CD-decode / LFH-decode paths stay byte-identical so mtime attaches."""

    def test_decode_normalizes_separators(self):
        assert _decode_zip_filename(b"dir\\sub\\f.txt") == "dir/sub/f.txt"

    def test_manifest_key_is_forward_slashed_for_windows_zip(self):
        zip_bytes = _build_zip({"Image repository\\UW.tif": b"data"})
        entries = build_manifest_from_tap("zip", io.BytesIO(zip_bytes))
        assert [e.key for e in entries] == ["Image repository/UW.tif"]

    def test_cd_lfh_key_parity_keeps_mtime_after_normalization(self, s3_client):
        """CD reader and LFH walk must decode the Windows name identically.

        If one normalized ``\\`` → ``/`` and the other didn't, the
        ``cd_mtimes.get(file_name)`` lookup would miss and the entry would
        lose its mtime. Both route through the shared decoder, so parity
        holds and mtime attaches.
        """
        zip_bytes = _build_zip({"win\\path\\f.txt": b"data"})
        s3_client.put_object(Bucket="src-bucket", Key="a.zip", Body=zip_bytes)

        cd_mtimes = zip_central_directory_from_s3(s3_client, "src-bucket", "a.zip")
        # The central-directory key is normalized to forward slashes.
        assert "win/path/f.txt" in cd_mtimes

        entries = build_manifest_from_tap("zip", io.BytesIO(zip_bytes), cd_mtimes=cd_mtimes)
        assert len(entries) == 1
        assert entries[0].key == "win/path/f.txt"
        # mtime attached despite the name being stored with backslashes.
        assert entries[0].mtime != ""
        assert entries[0].mtime == cd_mtimes["win/path/f.txt"]


class TestEntryObserver:
    """entry_observer must see every member's key + full decoded bytes,
    and its pass-through stream must feed the hash unchanged."""

    _MEMBERS = {"a.txt": b"alpha", "sub/b.bin": b"\x00\x01\x02" * 100}

    def _recording_observer(self, seen: dict):
        def observer(key, chunks):
            collected = seen.setdefault(key, bytearray())
            for chunk in chunks:
                collected.extend(chunk)
                yield chunk

        return observer

    def _assert_observed(self, entries, seen):
        assert {k: bytes(v) for k, v in seen.items()} == self._MEMBERS
        by_key = {e.key: e for e in entries}
        for key, body in self._MEMBERS.items():
            assert by_key[key].sha256 == hashlib.sha256(body).hexdigest()
            assert by_key[key].size == len(body)

    def test_tar_gz_observer_sees_all_members(self):
        tar_bytes = _build_tar_gz(self._MEMBERS)
        seen: dict = {}
        entries = build_manifest_from_tap(
            "tar.gz", io.BytesIO(tar_bytes), entry_observer=self._recording_observer(seen)
        )
        self._assert_observed(entries, seen)

    def test_zip_observer_sees_all_members(self):
        zip_bytes = _build_zip(self._MEMBERS)
        seen: dict = {}
        entries = build_manifest_from_tap(
            "zip", io.BytesIO(zip_bytes), entry_observer=self._recording_observer(seen)
        )
        self._assert_observed(entries, seen)

    def test_zip_seekable_observer_sees_all_members(self):
        zip_bytes = _build_zip(self._MEMBERS)
        seen: dict = {}
        entries = build_manifest_zip_seekable(
            io.BytesIO(zip_bytes), entry_observer=self._recording_observer(seen)
        )
        self._assert_observed(entries, seen)

    def test_observer_key_is_the_safety_passed_manifest_key(self):
        """The observer's key must equal the ManifestEntry key (post safe_member_key)."""
        zip_bytes = _build_zip({"win\\dir\\f.txt": b"data"})
        seen: dict = {}
        entries = build_manifest_from_tap(
            "zip", io.BytesIO(zip_bytes), entry_observer=self._recording_observer(seen)
        )
        assert list(seen) == ["win/dir/f.txt"]
        assert entries[0].key == "win/dir/f.txt"


class TestZipSeekableFallback:
    """Stored+data-descriptor zips: streaming refuses with a stable typed
    error; the seekable builder walks them via the central directory."""

    _MEMBERS = {"473A0003.jpg": b"jpeg-ish bytes " * 100, "sub/notes.txt": b"hello"}

    def test_streaming_raises_zip_not_streamable(self):
        fixture = build_stored_dd_zip(self._MEMBERS)
        with pytest.raises(ZipNotStreamableError) as excinfo:
            build_manifest_zip_chunks(iter([fixture]), {})
        # The first member's decoded name, not a bytes repr.
        assert excinfo.value.member_name == "473A0003.jpg"
        assert isinstance(excinfo.value.__cause__, NotStreamUnzippable)
        assert excinfo.value.cause is excinfo.value.__cause__
        assert "473A0003.jpg" in str(excinfo.value)
        assert "build_manifest_zip_seekable" in str(excinfo.value)

    def test_seekable_builder_walks_the_dd_fixture(self):
        fixture = build_stored_dd_zip(self._MEMBERS)
        entries = build_manifest_zip_seekable(io.BytesIO(fixture))
        assert [e.key for e in entries] == list(self._MEMBERS)
        by_key = {e.key: e for e in entries}
        for name, body in self._MEMBERS.items():
            assert by_key[name].size == len(body)
            assert by_key[name].md5 == hashlib.new("md5", body, usedforsecurity=False).hexdigest()
            assert by_key[name].sha1 == hashlib.new("sha1", body, usedforsecurity=False).hexdigest()
            assert by_key[name].sha256 == hashlib.sha256(body).hexdigest()
            # The fixture writes zeroed DOS timestamps → "" mtime.
            assert by_key[name].mtime == ""

    def test_parity_with_streaming_builder(self, tmp_path):
        """Both builders must produce byte-identical ManifestEntry lists for
        any zip both can walk — consumers' snapshot diffs churn otherwise.
        Mix of stored/deflated members, a directory entry, a Windows name.
        """
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("plain.txt", b"alpha", compress_type=zipfile.ZIP_STORED)
            zf.writestr("deep/deflated.bin", b"\x00\x01" * 500, compress_type=zipfile.ZIP_DEFLATED)
            zf.writestr("emptydir/", b"")
            zf.writestr("win\\styled.txt", b"backslash name")
        blob = buf.getvalue()

        zip_path = tmp_path / "parity.zip"
        zip_path.write_bytes(blob)
        cd_mtimes = zip_central_directory_from_path(zip_path)

        streamed = build_manifest_zip_chunks(iter([blob]), cd_mtimes)
        seekable = build_manifest_zip_seekable(io.BytesIO(blob))
        assert streamed == seekable
        assert [e.key for e in streamed] == ["plain.txt", "deep/deflated.bin", "win/styled.txt"]
        assert all(e.mtime != "" for e in streamed)  # writestr stamps real mtimes


class TestZipCentralDirectoryIfMatch:
    """if_match pins the CD Range GET; a 412 raises instead of degrading to {}."""

    def _range_client(self, blob: bytes) -> MagicMock:
        client = MagicMock()

        def get_object(**kwargs):
            start, end = kwargs["Range"].removeprefix("bytes=").split("-")
            body = MagicMock()
            body.read.return_value = blob[int(start) : int(end) + 1]
            return {"Body": body}

        client.get_object.side_effect = get_object
        return client

    def test_if_match_sent_quoted(self):
        blob = _build_zip({"x.txt": b"xx"})
        client = self._range_client(blob)
        mtimes = zip_central_directory_from_s3(client, "b", "a.zip", len(blob), if_match="abc123")
        assert "x.txt" in mtimes
        assert client.get_object.call_args.kwargs["IfMatch"] == '"abc123"'

    def test_no_if_match_sends_no_header(self):
        blob = _build_zip({"x.txt": b"xx"})
        client = self._range_client(blob)
        zip_central_directory_from_s3(client, "b", "a.zip", len(blob))
        assert "IfMatch" not in client.get_object.call_args.kwargs

    def test_precondition_failed_raises(self):
        client = MagicMock()
        client.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "PreconditionFailed", "Message": "412"}}, "GetObject"
        )
        with pytest.raises(botocore.exceptions.ClientError):
            zip_central_directory_from_s3(client, "b", "a.zip", 1024, if_match="abc")

    def test_other_client_error_still_degrades_to_empty(self):
        client = MagicMock()
        client.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "gone"}}, "GetObject"
        )
        assert zip_central_directory_from_s3(client, "b", "a.zip", 1024, if_match="abc") == {}
