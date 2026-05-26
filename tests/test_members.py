"""Tests for s3_archive.members.iter_archive_members.

Round-trip every supported format through moto: build the archive
bytes locally, upload, iter_archive_members back, and confirm the
member names + bodies match. Also exercise the read_all / drain /
auto-drain semantics.
"""

import io
import tarfile
import zipfile

import pytest
import zstandard

from s3_archive.exceptions import UnsupportedArchiveFormatError
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
    _upload(s3_client, "archive.7z", b"\x37\x7a\xbc\xaf\x27\x1c")
    with pytest.raises(UnsupportedArchiveFormatError):
        list(iter_archive_members(s3_client, "src-bucket", "archive.7z", "7z"))


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
    via_extract = extract(s3_client, "src-bucket", "archive", "dest-bucket", "out/", fmt)
    via_iter = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive", fmt)]
    assert via_extract == via_iter
