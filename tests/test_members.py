"""Tests for s3_archive.members.iter_archive_members.

Round-trip every supported format through moto: build the archive
bytes locally, upload, iter_archive_members back, and confirm the
member names + bodies match. Also exercise the read_all / drain /
auto-drain semantics.
"""

import io
import tarfile
import zipfile
from unittest.mock import MagicMock

import botocore.exceptions
import pytest
import zstandard
from stream_unzip import UnzipError

from s3_archive.exceptions import (
    ArchiveReadError,
    UnsafeArchiveMemberError,
    UnsupportedArchiveFormatError,
)
from s3_archive.extract import extract
from s3_archive.members import ArchiveMember, iter_archive_members

from .conftest import build_tar, build_zip


_FILES = {
    "alpha.txt": b"alpha contents\n",
    "nested/beta.bin": b"\x00\x01\x02\x03\x04" * 100,
    "gamma.txt": b"gamma " * 50,
}


def _upload(client, key, body):
    client.put_object(Bucket="src-bucket", Key=key, Body=body)


@pytest.mark.parametrize(
    "fmt,tar_mode",
    [
        ("tar", "w"),
        ("tar.gz", "w:gz"),
        ("tar.bz2", "w:bz2"),
        ("tar.xz", "w:xz"),
    ],
)
def test_iter_tar_family_round_trip(s3_client, fmt, tar_mode):
    _upload(s3_client, "archive", build_tar(_FILES, mode=tar_mode))

    seen: dict[str, bytes] = {}
    for member in iter_archive_members(s3_client, "src-bucket", "archive", fmt):
        assert isinstance(member, ArchiveMember)
        seen[member.name] = member.read_all()
    assert seen == _FILES


def test_iter_zip_round_trip(s3_client):
    _upload(s3_client, "archive.zip", build_zip(_FILES))

    seen: dict[str, bytes] = {}
    for member in iter_archive_members(s3_client, "src-bucket", "archive.zip", "zip"):
        seen[member.name] = member.read_all()
    assert seen == _FILES


def test_iter_tar_zst_round_trip(s3_client):
    inner = build_tar(_FILES, mode="w")
    compressed = zstandard.ZstdCompressor().compress(inner)
    _upload(s3_client, "archive.tar.zst", compressed)

    seen: dict[str, bytes] = {}
    for member in iter_archive_members(s3_client, "src-bucket", "archive.tar.zst", "tar.zst"):
        seen[member.name] = member.read_all()
    assert seen == _FILES


def test_drain_skips_payload_without_buffering(s3_client):
    """The verify-against use case: capture one file, drain the rest."""
    _upload(s3_client, "archive", build_tar(_FILES, mode="w"))

    captured: dict[str, bytes] = {}
    for member in iter_archive_members(s3_client, "src-bucket", "archive", "tar"):
        if member.name == "alpha.txt":
            captured[member.name] = member.read_all()
        else:
            member.drain()
    assert captured == {"alpha.txt": _FILES["alpha.txt"]}


def test_auto_drain_on_next_yield(s3_client):
    """A caller that yields without consuming should not corrupt the next member.

    Without auto-drain, advancing the iterator before reading the
    previous member's bytes would leave tarfile pointing into the
    middle of a member body and the next member would be garbled or
    raise.
    """
    _upload(s3_client, "archive", build_tar(_FILES, mode="w"))

    names: list[str] = []
    for member in iter_archive_members(s3_client, "src-bucket", "archive", "tar"):
        names.append(member.name)
        # NOTE: deliberately do NOT consume member's bytes.
    assert names == list(_FILES)


def test_zip_directory_entries_are_skipped(s3_client):
    """Directory entries (size 0 + trailing slash) should not appear in iteration."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("subdir/", b"")
        zf.writestr("subdir/file.txt", b"hello")
    _upload(s3_client, "archive.zip", buf.getvalue())

    names = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive.zip", "zip")]
    assert names == ["subdir/file.txt"]


def test_tar_skips_non_regular_members(s3_client):
    """Tar dir entries, symlinks, etc. are skipped (only ``isfile()`` members are yielded)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        # Directory entry
        dir_info = tarfile.TarInfo(name="adir")
        dir_info.type = tarfile.DIRTYPE
        tar.addfile(dir_info)
        # Regular file
        info = tarfile.TarInfo(name="adir/file.txt")
        info.size = 5
        tar.addfile(info, io.BytesIO(b"hello"))
    _upload(s3_client, "archive", buf.getvalue())

    members = list(iter_archive_members(s3_client, "src-bucket", "archive", "tar"))
    assert [m.name for m in members] == ["adir/file.txt"]


def test_unsupported_format_raises(s3_client):
    with pytest.raises(UnsupportedArchiveFormatError, match="Unsupported format"):
        list(iter_archive_members(s3_client, "src-bucket", "archive", "rar"))


def test_corrupted_tar_raises_archive_read_error(s3_client):
    """A garbled tar.gz body wraps tarfile.TarError as ArchiveReadError."""
    _upload(s3_client, "archive.tar.gz", b"not a real gzip header\x00" * 10)
    with pytest.raises(ArchiveReadError) as exc_info:
        list(iter_archive_members(s3_client, "src-bucket", "archive.tar.gz", "tar.gz"))
    assert isinstance(exc_info.value.__cause__, tarfile.TarError)
    assert exc_info.value.cause is exc_info.value.__cause__


def test_corrupted_zip_raises_archive_read_error(s3_client):
    """A garbled zip body wraps stream_unzip's UnzipError as ArchiveReadError."""
    _upload(s3_client, "archive.zip", b"PK\x03\x04 garbage that is not a real zip body")
    with pytest.raises(ArchiveReadError) as exc_info:
        list(iter_archive_members(s3_client, "src-bucket", "archive.zip", "zip"))
    assert isinstance(exc_info.value.__cause__, UnzipError)


@pytest.mark.parametrize(
    "fmt,builder",
    [
        ("tar", lambda f: build_tar(f, mode="w")),
        ("tar.gz", lambda f: build_tar(f, mode="w:gz")),
        ("tar.bz2", lambda f: build_tar(f, mode="w:bz2")),
        ("tar.xz", lambda f: build_tar(f, mode="w:xz")),
        ("zip", build_zip),
    ],
)
def test_extract_member_set_matches_iter_archive_members(s3_client, fmt, builder):
    """Drift guard: extract() must surface the same names iter_archive_members yields."""
    _upload(s3_client, "archive", builder(_FILES))
    via_extract = extract(s3_client, s3_client, "src-bucket", "archive", "dest-bucket", "out/", fmt)
    via_iter = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive", fmt)]
    assert via_extract == via_iter


class TestMemberNameNormalization:
    """Yielded member names are normalized to safe relative S3 keys."""

    def test_zip_backslash_names_become_forward_slashes(self, s3_client):
        """A Windows-authored zip stores ``\\``; every extractor rewrites it to ``/``."""
        _upload(
            s3_client,
            "archive.zip",
            build_zip({"Image repository\\UW26509z.tif": b"tiff-bytes"}),
        )

        names = [
            m.name for m in iter_archive_members(s3_client, "src-bucket", "archive.zip", "zip")
        ]
        assert names == ["Image repository/UW26509z.tif"]

    def test_zip_drive_prefix_stripped(self, s3_client):
        _upload(s3_client, "archive.zip", build_zip({"C:\\docs\\readme.txt": b"x"}))

        names = [
            m.name for m in iter_archive_members(s3_client, "src-bucket", "archive.zip", "zip")
        ]
        assert names == ["docs/readme.txt"]

    def test_tar_literal_backslash_is_preserved(self, s3_client):
        """In tar a backslash is a legal filename byte — must NOT be rewritten."""
        _upload(s3_client, "archive", build_tar({"a\\b": b"literal"}, mode="w"))

        # Read inside the loop: the streaming tar auto-drains each member
        # when the iterator advances, so a post-hoc read_all() would be empty.
        seen: dict[str, bytes] = {}
        for member in iter_archive_members(s3_client, "src-bucket", "archive", "tar"):
            seen[member.name] = member.read_all()
        assert seen == {"a\\b": b"literal"}

    def test_tar_leading_slash_is_stripped(self, s3_client):
        """A leading ``/`` is never a valid relative member (matches GNU tar)."""
        _upload(s3_client, "archive", build_tar({"/abs/path.txt": b"x"}, mode="w"))

        names = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive", "tar")]
        assert names == ["abs/path.txt"]

    def test_dotdot_member_raises_by_default(self, s3_client):
        _upload(s3_client, "archive", build_tar({"../evil.txt": b"x"}, mode="w"))

        with pytest.raises(UnsafeArchiveMemberError) as exc_info:
            list(iter_archive_members(s3_client, "src-bucket", "archive", "tar"))
        assert exc_info.value.member_name == "../evil.txt"

    def test_dotdot_member_collapses_with_fix_unsafe_paths(self, s3_client):
        _upload(s3_client, "archive", build_tar({"a/../b.txt": b"x"}, mode="w"))

        names = [
            m.name
            for m in iter_archive_members(
                s3_client, "src-bucket", "archive", "tar", fix_unsafe_paths=True
            )
        ]
        assert names == ["b.txt"]

    def test_dotdot_in_zip_raises_by_default(self, s3_client):
        _upload(s3_client, "archive.zip", build_zip({"../evil.txt": b"x"}))

        with pytest.raises(UnsafeArchiveMemberError):
            list(iter_archive_members(s3_client, "src-bucket", "archive.zip", "zip"))


class _FlakyBody:
    """A fake S3 ``Body`` that serves bytes and can drop mid-stream.

    Serves *data* in slices of at most *chunk_cap* bytes per ``read()``
    (so a small archive still spans several reads, letting a drop land
    mid-stream). Once *drop_after* bytes have been served, the next
    ``read()`` raises the transient error from *exc_factory* instead of
    returning bytes — modeling a connection broken part-way through the
    HTTP response. ``drop_after=None`` never drops.
    """

    def __init__(self, data, *, drop_after=None, chunk_cap=64, exc_factory=None):
        self._data = data
        self._pos = 0
        self._served = 0
        self._drop_after = drop_after
        self._chunk_cap = chunk_cap
        self._exc_factory = exc_factory

    def read(self, size=-1):
        if self._drop_after is not None and self._served >= self._drop_after:
            # Only raise once per body: null it so a (defensive) re-read
            # can't spin. In practice the generator discards a dropped
            # body and reopens, so this read is never called again.
            self._drop_after = None
            raise self._exc_factory()
        if size is None or size < 0:
            size = len(self._data) - self._pos
        size = min(size, self._chunk_cap)
        chunk = self._data[self._pos : self._pos + size]
        self._pos += len(chunk)
        self._served += len(chunk)
        return chunk


class TestResumableStreaming:
    """The sequential tar/zip byte stream resumes across transient drops.

    A mid-stream connection drop (``ResponseStreamingError`` /
    ``IncompleteRead``) should be survived by re-issuing
    ``get_object(Range="bytes=<pos>-")`` from the offset already
    consumed, so an isolated hiccup doesn't abort a long extract.
    """

    @staticmethod
    def _stream_drop():
        # Same shape botocore raises when urllib3 drops the response
        # body part-way through (the crash reported from Kopah/RGW).
        return botocore.exceptions.ResponseStreamingError(
            error=ValueError("Connection broken: IncompleteRead")
        )

    def _flaky_client(self, body_bytes, *, drops, chunk_cap=64):
        """A MagicMock client whose get_object follows a per-GET drop schedule.

        *drops* is indexed by get_object call number; each entry is the
        number of bytes to serve on that GET before raising a transient
        error, or ``None`` to serve to completion. A ranged GET resumes
        from the offset parsed out of ``Range``.

        Returns ``(client, ranges_seen)`` where *ranges_seen* records the
        ``Range`` kwarg of every get_object call (``None`` for the
        initial unranged open).
        """
        client = MagicMock()
        ranges_seen: list[str | None] = []
        call_count = [0]

        def get_object(*, Bucket, Key, Range=None):  # noqa: ARG001, N803
            idx = call_count[0]
            call_count[0] += 1
            ranges_seen.append(Range)
            start = 0 if Range is None else int(Range.removeprefix("bytes=").split("-")[0])
            drop_after = drops[idx] if idx < len(drops) else None
            body = _FlakyBody(
                body_bytes[start:],
                drop_after=drop_after,
                chunk_cap=chunk_cap,
                exc_factory=self._stream_drop,
            )
            return {"Body": body}

        client.get_object.side_effect = get_object
        return client, ranges_seen

    def test_zip_resumes_after_transient_stream_drop(self, monkeypatch):
        monkeypatch.setattr("s3_archive.retry.time.sleep", lambda _s: None)
        body_bytes = build_zip(_FILES)
        # Drop partway through the first GET; the resume GET delivers the rest.
        client, ranges = self._flaky_client(body_bytes, drops=[100])

        seen: dict[str, bytes] = {}
        for member in iter_archive_members(client, "b", "k", "zip", retry_delay_s=0):
            seen[member.name] = member.read_all()

        assert seen == _FILES
        # First GET is the unranged open; the resume carries Range=bytes=<pos>-.
        assert ranges[0] is None
        assert ranges[1] is not None and ranges[1].startswith("bytes=")
        resume_pos = int(ranges[1].removeprefix("bytes=").split("-")[0])
        assert 0 < resume_pos < len(body_bytes)

    def test_tar_resumes_after_transient_stream_drop(self, monkeypatch):
        monkeypatch.setattr("s3_archive.retry.time.sleep", lambda _s: None)
        body_bytes = build_tar(_FILES, mode="w")
        # Exercises the IterableFileobj tar wiring on top of the resumable stream.
        client, ranges = self._flaky_client(body_bytes, drops=[100])

        seen: dict[str, bytes] = {}
        for member in iter_archive_members(client, "b", "k", "tar", retry_delay_s=0):
            seen[member.name] = member.read_all()

        assert seen == _FILES
        assert ranges[0] is None
        assert ranges[1] is not None and ranges[1].startswith("bytes=")
        resume_pos = int(ranges[1].removeprefix("bytes=").split("-")[0])
        assert 0 < resume_pos < len(body_bytes)

    def test_stream_gives_up_after_max_consecutive_failures(self, monkeypatch):
        monkeypatch.setattr("s3_archive.retry.time.sleep", lambda _s: None)
        body_bytes = build_zip(_FILES)
        # Every GET drops before serving a byte → no forward progress ever,
        # so the consecutive-failure cap is reached and the error propagates.
        client, _ = self._flaky_client(body_bytes, drops=[0, 0, 0, 0])

        with pytest.raises(botocore.exceptions.ResponseStreamingError):
            list(
                iter_archive_members(client, "b", "k", "zip", retry_delay_s=0, retry_max_attempts=3)
            )

    def test_backoff_delays_grow_between_consecutive_failures(self, monkeypatch):
        # Capture the actual sleeps: with base 5 and consecutive no-progress
        # drops, the resumable path must back off 5 s then 15 s before giving
        # up at the 3rd attempt.
        slept: list[float] = []
        monkeypatch.setattr("s3_archive.retry.time.sleep", slept.append)
        body_bytes = build_zip(_FILES)
        client, _ = self._flaky_client(body_bytes, drops=[0, 0, 0])

        with pytest.raises(botocore.exceptions.ResponseStreamingError):
            list(
                iter_archive_members(client, "b", "k", "zip", retry_delay_s=5, retry_max_attempts=3)
            )
        assert slept == [5, 15]

    def test_retry_budget_resets_on_progress(self, monkeypatch):
        monkeypatch.setattr("s3_archive.retry.time.sleep", lambda _s: None)
        body_bytes = build_zip(_FILES)
        # Two drops, each after a chunk of forward progress, with a cap of
        # 2 *consecutive* failures. Total drops (2) would trip a naive
        # total-attempt cap, but each resume delivers bytes and zeroes the
        # counter, so consecutive failures never reach 2 → extraction
        # completes. This is the key semantic guard.
        client, ranges = self._flaky_client(body_bytes, drops=[64, 64, None], chunk_cap=64)

        seen: dict[str, bytes] = {}
        for member in iter_archive_members(
            client, "b", "k", "zip", retry_delay_s=0, retry_max_attempts=2
        ):
            seen[member.name] = member.read_all()

        assert seen == _FILES
        # Three GETs total: the initial open plus two resumes.
        assert len(ranges) == 3
        assert ranges[0] is None
        assert all(r is not None and r.startswith("bytes=") for r in ranges[1:])
