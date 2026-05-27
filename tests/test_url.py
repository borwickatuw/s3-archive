"""Tests for s3_archive.url."""

import pytest

from s3_archive.exceptions import ConfigError, UnsupportedArchiveFormatError
from s3_archive.url import (
    ParsedS3Url,
    detect_format,
    looks_like_archive_url,
    parse_s3_prefix,
    parse_s3_url,
)


class TestParseS3Url:
    def test_basic(self):
        assert parse_s3_url("s3://my-bucket/path/file.tar.gz") == ParsedS3Url(
            None,
            "my-bucket",
            "path/file.tar.gz",
        )

    def test_bucket_only(self):
        assert parse_s3_url("s3://my-bucket") == ParsedS3Url(None, "my-bucket", "")

    def test_bucket_with_trailing_slash(self):
        assert parse_s3_url("s3://my-bucket/") == ParsedS3Url(None, "my-bucket", "")

    def test_rejects_missing_scheme(self):
        with pytest.raises(ConfigError, match="must start with s3"):
            parse_s3_url("my-bucket/path")

    def test_rejects_empty_bucket(self):
        with pytest.raises(ConfigError, match="empty bucket"):
            parse_s3_url("s3:///path")


class TestParseS3UrlWithProfile:
    """profile:s3:// prefix support."""

    def test_simple_profile(self):
        assert parse_s3_url("aws-prsv:s3://bucket/key") == ParsedS3Url("aws-prsv", "bucket", "key")

    def test_profile_with_underscore_and_digits(self):
        assert parse_s3_url("team_1:s3://b/k") == ParsedS3Url("team_1", "b", "k")

    def test_bare_url_profile_is_none(self):
        assert parse_s3_url("s3://b/k").profile is None

    def test_profile_with_bucket_only(self):
        assert parse_s3_url("kopah:s3://b") == ParsedS3Url("kopah", "b", "")

    def test_profile_with_trailing_slash(self):
        assert parse_s3_url("kopah:s3://b/") == ParsedS3Url("kopah", "b", "")

    @pytest.mark.parametrize(
        "url",
        [
            "bad name:s3://b/k",  # space
            "with/slash:s3://b/k",
            "with.dot:s3://b/k",
        ],
    )
    def test_rejects_invalid_profile_grammar(self, url):
        with pytest.raises(ConfigError, match="Invalid profile name"):
            parse_s3_url(url)

    def test_colon_with_s3_in_key_is_not_profile(self):
        # The split rule looks for ":s3://" specifically, not any ":s3" —
        # so a key that happens to contain s3 doesn't get misparsed.
        assert parse_s3_url("s3://b/k/with:s3/foo") == ParsedS3Url(None, "b", "k/with:s3/foo")


class TestParseS3Prefix:
    def test_appends_slash(self):
        assert parse_s3_prefix("s3://b/path") == ParsedS3Url(None, "b", "path/")

    def test_preserves_slash(self):
        assert parse_s3_prefix("s3://b/path/") == ParsedS3Url(None, "b", "path/")

    def test_empty_prefix(self):
        assert parse_s3_prefix("s3://b") == ParsedS3Url(None, "b", "")

    def test_with_profile(self):
        assert parse_s3_prefix("kopah:s3://b/path") == ParsedS3Url("kopah", "b", "path/")

    def test_with_profile_and_trailing_slash(self):
        assert parse_s3_prefix("kopah:s3://b/path/") == ParsedS3Url("kopah", "b", "path/")


class TestDetectFormat:
    @pytest.mark.parametrize("url", ["s3://b/x.tar.gz", "s3://b/X.TAR.GZ", "s3://b/x.tgz"])
    def test_tar_gz(self, url):
        assert detect_format(url) == "tar.gz"

    @pytest.mark.parametrize("url", ["s3://b/x.tar.bz2", "s3://b/x.tbz2", "s3://b/X.TBZ2"])
    def test_tar_bz2(self, url):
        assert detect_format(url) == "tar.bz2"

    @pytest.mark.parametrize("url", ["s3://b/x.tar.xz", "s3://b/x.txz", "s3://b/X.TAR.XZ"])
    def test_tar_xz(self, url):
        assert detect_format(url) == "tar.xz"

    @pytest.mark.parametrize("url", ["s3://b/x.tar.zst", "s3://b/X.TAR.ZST"])
    def test_tar_zst(self, url):
        assert detect_format(url) == "tar.zst"

    @pytest.mark.parametrize("url", ["s3://b/x.tar", "s3://b/X.TAR"])
    def test_tar(self, url):
        assert detect_format(url) == "tar"

    @pytest.mark.parametrize("url", ["s3://b/x.zip", "s3://b/X.ZIP"])
    def test_zip(self, url):
        assert detect_format(url) == "zip"

    @pytest.mark.parametrize("url", ["s3://b/x.7z", "s3://b/X.7Z"])
    def test_seven_z(self, url):
        assert detect_format(url) == "7z"

    def test_rejects_unknown(self):
        with pytest.raises(UnsupportedArchiveFormatError, match="Cannot detect archive format"):
            detect_format("s3://b/x.rar")

    def test_error_lists_all_extensions(self):
        with pytest.raises(UnsupportedArchiveFormatError) as exc_info:
            detect_format("s3://b/x.rar")
        msg = str(exc_info.value)
        for token in (".tar", ".tar.gz", ".tar.bz2", ".tar.xz", ".zip", ".7z"):
            assert token in msg

    def test_strips_profile_prefix(self):
        # Profile prefix must not influence extension detection.
        assert detect_format("kopah:s3://b/x.tar.gz") == "tar.gz"


class TestLooksLikeArchiveUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "s3://b/x.tar",
            "s3://b/x.tar.gz",
            "s3://b/X.TGZ",
            "s3://b/x.tar.bz2",
            "s3://b/x.tar.xz",
            "s3://b/x.tar.zst",
            "s3://b/x.zip",
            "s3://b/x.7z",
            "s3://b/x.7z/",  # trailing slash tolerated
            "kopah:s3://b/x.tar.gz",  # profile prefix tolerated
        ],
    )
    def test_recognized(self, url):
        assert looks_like_archive_url(url) is True

    @pytest.mark.parametrize(
        "url",
        [
            "s3://b/x.rar",
            "s3://b/prefix/",
            "s3://b/file.txt",
            "s3://b/",
            "",
        ],
    )
    def test_not_recognized(self, url):
        assert looks_like_archive_url(url) is False

    def test_invalid_profile_grammar_returns_false(self):
        # Defensive: an invalid profile name in the URL is bad input,
        # but the predicate must not raise — it should just say "no".
        assert looks_like_archive_url("bad name:s3://b/x.tar.gz") is False

    def test_stays_in_sync_with_detect_format(self):
        """Both functions are backed by _EXTENSION_FORMATS — any URL one
        recognizes the other should too."""
        for sample in (
            "s3://b/a.tar",
            "s3://b/a.tar.gz",
            "s3://b/a.tgz",
            "s3://b/a.tar.bz2",
            "s3://b/a.tar.xz",
            "s3://b/a.tar.zst",
            "s3://b/a.zip",
            "s3://b/a.7z",
        ):
            assert looks_like_archive_url(sample)
            detect_format(sample)  # should not raise
