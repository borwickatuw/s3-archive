"""Unit tests for the format-agnostic resume core (:mod:`s3_archive.resume`)."""

import json

import pytest

from s3_archive import resume


class TestControlKey:
    """Control-object key construction + ETag sanitization."""

    def test_strips_surrounding_quotes(self):
        # S3/RGW hand back ETags wrapped in literal double quotes.
        assert (
            resume.control_key("out/", '"d41d8cd98f00b204e9800998ecf8427e"')
            == "out/.s3-archive-resume.d41d8cd98f00b204e9800998ecf8427e.json"
        )

    def test_keeps_multipart_etag_suffix(self):
        # Multipart ETags look like "<hex>-<partcount>"; the hyphen is
        # allowed so the suffix survives sanitization.
        assert (
            resume.control_key("out/", '"abc123def456-42"')
            == "out/.s3-archive-resume.abc123def456-42.json"
        )

    def test_drops_disallowed_characters(self):
        # Anything outside [A-Za-z0-9._-] is dropped, never substituted, so
        # the result is always a single well-formed path segment.
        assert resume.control_key("out/", 'a/b c:d"e') == "out/.s3-archive-resume.abcde.json"

    def test_empty_prefix(self):
        assert resume.control_key("", '"tag"') == ".s3-archive-resume.tag.json"

    def test_prefix_without_trailing_slash(self):
        assert resume.control_key("out", '"tag"') == "out/.s3-archive-resume.tag.json"


class TestIsControlKey:
    def test_matches_full_key(self):
        assert resume.is_control_key("out/.s3-archive-resume.tag.json")

    def test_matches_bare_relative_path(self):
        # build_done_set sees prefix-stripped RelativePaths, so a bare name
        # (no leading slash) must match too.
        assert resume.is_control_key(".s3-archive-resume.tag.json")

    def test_rejects_ordinary_member(self):
        assert not resume.is_control_key("out/a.txt")
        assert not resume.is_control_key("out/notes.s3-archive-resume.txt")


class TestControlFileRoundTrip:
    """write → exists → delete against moto, plus the body contract."""

    def test_write_then_exists_then_delete(self, s3_client):
        key = resume.control_key("out/", '"etag-1"')
        assert resume.control_file_exists(s3_client, "dest-bucket", key) is False

        resume.write_control_file(
            s3_client,
            "dest-bucket",
            key,
            source_etag='"etag-1"',
            source_size=123,
            fmt="zip",
            now_iso="2026-07-01T00:00:00+00:00",
        )
        assert resume.control_file_exists(s3_client, "dest-bucket", key) is True

        resume.delete_control_file(s3_client, "dest-bucket", key)
        assert resume.control_file_exists(s3_client, "dest-bucket", key) is False

    def test_body_records_provenance_but_no_bucket_or_key(self, s3_client):
        key = resume.control_key("out/", '"etag-1"')
        resume.write_control_file(
            s3_client,
            "dest-bucket",
            key,
            source_etag='"etag-1"',
            source_size=456,
            fmt="tar",
            now_iso="2026-07-01T12:00:00+00:00",
        )
        body = json.loads(s3_client.get_object(Bucket="dest-bucket", Key=key)["Body"].read())
        assert body == {
            "schema_version": resume.SCHEMA_VERSION,
            "source_etag": '"etag-1"',
            "source_size": 456,
            "format": "tar",
            "created_at": "2026-07-01T12:00:00+00:00",
        }
        # Deliberately no bucket/key anywhere in the marker (no leak).
        assert "bucket" not in body
        assert "key" not in body


class TestBuildDoneSet:
    def test_maps_relative_path_to_size(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="out/a.txt", Body=b"hello")
        s3_client.put_object(Bucket="dest-bucket", Key="out/sub/b.txt", Body=b"worldish")

        done = resume.build_done_set(s3_client, "dest-bucket", "out/")
        assert done == {"a.txt": 5, "sub/b.txt": 8}

    def test_excludes_the_control_object(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="out/a.txt", Body=b"hello")
        key = resume.control_key("out/", '"etag-1"')
        resume.write_control_file(
            s3_client,
            "dest-bucket",
            key,
            source_etag='"etag-1"',
            source_size=1,
            fmt="zip",
            now_iso="2026-07-01T00:00:00+00:00",
        )

        done = resume.build_done_set(s3_client, "dest-bucket", "out/")
        # Only the real member — the marker is filtered out.
        assert done == {"a.txt": 5}

    def test_empty_prefix(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="a.txt", Body=b"hi")
        done = resume.build_done_set(s3_client, "dest-bucket", "")
        assert done == {"a.txt": 2}


@pytest.mark.parametrize(
    ("done_size", "member_size", "should_skip"),
    [
        (5, 5, True),  # present at expected size → done
        (3, 5, False),  # present but wrong size → not done (partial/mismatch)
        (0, 0, True),  # empty member present as empty → done
    ],
)
def test_skip_rule(done_size, member_size, should_skip):
    """The skip predicate used by extract: done iff size matches exactly."""
    done = {"a.txt": done_size}
    assert (done.get("a.txt") == member_size) is should_skip


def test_skip_rule_absent_member_never_skips():
    done: dict[str, int] = {}
    assert (done.get("a.txt") == 0) is False
