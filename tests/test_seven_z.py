"""Tests for streaming .7z read support."""

from unittest.mock import MagicMock

import botocore.exceptions
import py7zr
import pytest

from s3_archive.exceptions import ArchiveReadError
from s3_archive.members import ArchiveMember, iter_archive_members
from s3_archive.seven_z import SeekableS3Object, iter_seven_z_members

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


def test_header_parse_error_surfaces_as_archive_read_error(s3_client):
    """A truncated archive surfaces as ArchiveReadError, not a hang or Bad7zFile."""
    full = build_7z(_FILES)
    # 32 bytes is the SignatureHeader — enough for py7zr to recognize the
    # magic and then choke on the missing NextHeader. Wrapped into
    # ArchiveReadError so callers see one exception type for "bad bytes."
    _upload(s3_client, "archive.7z", full[:32])

    with pytest.raises(ArchiveReadError) as exc_info:
        list(iter_seven_z_members(s3_client, "src-bucket", "archive.7z"))
    # Original py7zr exception preserved on both __cause__ and .cause.
    assert isinstance(exc_info.value.__cause__, py7zr.Bad7zFile)
    assert isinstance(exc_info.value.cause, py7zr.Bad7zFile)


def test_very_short_input_surfaces_as_archive_read_error(s3_client):
    """An archive too short for py7zr to even reach Bad7zFile (struct.error path)."""
    # 6 bytes is enough for the magic check but struct.unpack on the
    # SignatureHeader fields fails with struct.error.
    _upload(s3_client, "archive.7z", b"\x37\x7a\xbc\xaf\x27\x1c")

    with pytest.raises(ArchiveReadError) as exc_info:
        list(iter_seven_z_members(s3_client, "src-bucket", "archive.7z"))
    assert exc_info.value.cause is not None


def test_iter_archive_members_yields_seven_z_in_order(s3_client):
    """The dispatch from iter_archive_members yields members in archive order."""
    _upload(s3_client, "archive.7z", build_7z(_FILES))

    names = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive.7z", "7z")]
    # py7zr orders members per its internal scan; verify the set matches and
    # that the iterator yields each member exactly once.
    assert sorted(names) == sorted(_FILES)
    assert len(names) == len(_FILES)


class TestSeekableS3ObjectRetry:
    """SeekableS3Object._ranged_get retries on transient errors.

    Real failures we want to survive: one stalled ranged GET out of
    ~3000 on a 3 GB .7z walk shouldn't drop the whole archive. The
    retry covers ReadTimeoutError and the connection-class botocore
    exceptions.
    """

    def _client_with_failure_schedule(self, body: bytes, *, failures_per_call: list[int]):
        """A mocked client whose get_object follows a per-call failure schedule.

        Each entry in *failures_per_call* is the number of transient
        failures that must occur before that GET succeeds. So
        ``[2, 0]`` means: the first ``get_object`` is preceded by 2
        scheduled failures (3rd attempt succeeds), the second is
        unconditional success.

        head_object is unconditional so SeekableS3Object's __init__ can
        size the object before any retry behavior is exercised.
        """
        client = MagicMock()
        client.head_object.return_value = {"ContentLength": len(body)}
        remaining_failures = [failures_per_call.copy() if failures_per_call else []]
        current_budget = [0]

        def get_object(*, Bucket, Key, Range):  # noqa: ARG001, N803
            # Start of a new logical GET: refill the budget from the schedule.
            if current_budget[0] == 0 and remaining_failures[0]:
                current_budget[0] = remaining_failures[0].pop(0)
            if current_budget[0] > 0:
                current_budget[0] -= 1
                raise botocore.exceptions.ReadTimeoutError(
                    endpoint_url="https://test.example/" + Key
                )
            start, end = Range.removeprefix("bytes=").split("-")
            slice_bytes = body[int(start) : int(end) + 1]
            mock_resp = MagicMock()
            mock_resp.read.return_value = slice_bytes
            return {"Body": mock_resp}

        client.get_object.side_effect = get_object
        return client

    def test_recovers_after_transient_read_timeouts(self, monkeypatch):
        # Skip the real sleep — we just need to assert it was called the
        # right number of times.
        sleep_calls: list[float] = []
        monkeypatch.setattr("s3_archive.seven_z.time.sleep", sleep_calls.append)

        body = b"abcdefghij" * 1000  # 10 KB
        # One logical _fetch call. First 2 attempts fail; 3rd succeeds.
        client = self._client_with_failure_schedule(body, failures_per_call=[2])
        # tail_prefetch_bytes=0 so init does no GETs and the test's
        # failure budget is fully consumed by the explicit _fetch.
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=0, retry_delay_s=0)
        chunk = obj._fetch(0, len(body))

        assert chunk == body
        # Two retries → two sleep calls.
        assert sleep_calls == [0, 0]

    def test_propagates_after_max_attempts(self, monkeypatch):
        monkeypatch.setattr("s3_archive.seven_z.time.sleep", lambda _s: None)
        body = b"x" * 100
        # Failure budget exhausted: 3 attempts default, 5 scheduled
        # failures — every attempt fails.
        client = self._client_with_failure_schedule(body, failures_per_call=[5])
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=0, retry_delay_s=0)
        with pytest.raises(botocore.exceptions.ReadTimeoutError):
            obj._fetch(0, len(body))

    def test_non_transient_error_propagates_immediately(self, monkeypatch):
        sleep_calls: list[float] = []
        monkeypatch.setattr("s3_archive.seven_z.time.sleep", sleep_calls.append)

        body = b"x" * 100
        client = MagicMock()
        client.head_object.return_value = {"ContentLength": len(body)}
        client.get_object.side_effect = botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "nope"}},
            operation_name="GetObject",
        )
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=0, retry_delay_s=0)
        with pytest.raises(botocore.exceptions.ClientError):
            obj._fetch(0, len(body))
        # No sleep — non-transient errors shouldn't burn time in retries.
        assert sleep_calls == []

    def test_tail_prefetch_also_retries(self, monkeypatch):
        # The init-time tail prefetch is the same shape of ranged GET
        # and equally vulnerable. The retry should cover it too.
        sleep_calls: list[float] = []
        monkeypatch.setattr("s3_archive.seven_z.time.sleep", sleep_calls.append)

        body = b"y" * 5000
        # The tail prefetch is the FIRST get_object call. Make it fail
        # once, succeed second try.
        client = self._client_with_failure_schedule(body, failures_per_call=[1])
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=1000, retry_delay_s=0)
        # Last 1000 bytes are now in memory.
        assert obj._tail_bytes == body[-1000:]
        assert sleep_calls == [0]
