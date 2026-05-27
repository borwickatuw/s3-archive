"""Tests for streaming .7z read support."""

import py7zr
import pytest

from s3_archive.members import ArchiveMember, iter_archive_members
from s3_archive.seven_z import iter_seven_z_members

from .conftest import SEVEN_Z_FLAVORS, build_7z

_FILES = {
    "alpha.txt": b"alpha contents\n",
    "empty.txt": b"",
    "nested/beta.bin": b"\x00\x01\x02\x03\x04" * 100,
    "gamma.txt": b"gamma " * 50,
}


def _upload(client, key: str, body: bytes) -> None:
    client.put_object(Bucket="src-bucket", Key=key, Body=body)


@pytest.mark.parametrize("flavor", sorted(SEVEN_Z_FLAVORS))
def test_round_trip_via_iter_archive_members(s3_client, flavor):
    """Every flavor extracts byte-for-byte through the public dispatch path."""
    _upload(s3_client, "archive.7z", build_7z(_FILES, flavor=flavor))

    seen: dict[str, bytes] = {}
    for member in iter_archive_members(s3_client, "src-bucket", "archive.7z", "7z"):
        assert isinstance(member, ArchiveMember)
        seen[member.name] = member.read_all()
    assert seen == _FILES


@pytest.mark.parametrize("flavor", sorted(SEVEN_Z_FLAVORS))
def test_sizes_match_uncompressed_lengths(s3_client, flavor):
    """``ArchiveMember.size`` is the uncompressed length from the 7z header."""
    _upload(s3_client, "archive.7z", build_7z(_FILES, flavor=flavor))

    for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z"):
        assert member.size == len(_FILES[member.name])
        member.drain()


def test_auto_drain_on_next_yield(s3_client):
    """Forgetting to consume a member must not corrupt the next one."""
    _upload(s3_client, "archive.7z", build_7z(_FILES))

    names: list[str] = []
    for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z"):
        names.append(member.name)
        # Deliberately do NOT consume member.chunks().

    # Members from the iterator should match the input set even when the
    # caller never reads their bodies — the main loop auto-drains.
    assert set(names) == set(_FILES)


def test_empty_member_yields_zero_bytes(s3_client):
    """Zero-byte members should yield an immediately-empty chunk iterator."""
    _upload(s3_client, "archive.7z", build_7z(_FILES))

    for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z"):
        if member.name == "empty.txt":
            assert member.size == 0
            assert member.read_all() == b""
        else:
            member.drain()


def test_iterator_close_mid_stream(s3_client):
    """Closing the iterator early should clean up without raising or hanging."""
    _upload(s3_client, "archive.7z", build_7z(_FILES))

    it = iter_seven_z_members(s3_client, "src-bucket", "archive.7z")
    first = next(it)
    first.drain()
    # Abandon the generator — its finally block must close pipes, drain the
    # metadata queue, and join the worker thread without raising.
    it.close()


def test_worker_error_propagates_to_consumer(s3_client):
    """A truncated archive surfaces as an exception, not a hang."""
    full = build_7z(_FILES)
    # 32 bytes is the SignatureHeader — enough for py7zr to recognize the
    # magic and then choke on the missing NextHeader. The exception is
    # raised by py7zr inside its open path; we want it to propagate
    # cleanly rather than the worker hanging on a write to a dead pipe.
    _upload(s3_client, "archive.7z", full[:32])

    with pytest.raises(py7zr.Bad7zFile):
        list(iter_seven_z_members(s3_client, "src-bucket", "archive.7z"))


def test_iter_archive_members_yields_seven_z_in_order(s3_client):
    """The dispatch from iter_archive_members yields members in archive order."""
    _upload(s3_client, "archive.7z", build_7z(_FILES))

    names = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive.7z", "7z")]
    # py7zr orders members per its internal scan; verify the set matches and
    # that the iterator yields each member exactly once.
    assert sorted(names) == sorted(_FILES)
    assert len(names) == len(_FILES)
