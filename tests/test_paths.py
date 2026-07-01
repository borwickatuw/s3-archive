"""Unit tests for the pure path-normalization helpers in s3_archive.paths."""

import pytest

from s3_archive.exceptions import UnsafeArchiveMemberError
from s3_archive.paths import (
    decode_zip_filename,
    normalize_zip_separators,
    safe_member_key,
)


class TestNormalizeZipSeparators:
    """Zip-only Windows-ism rewriting: ``\\`` → ``/`` and drop a drive prefix."""

    def test_backslashes_become_forward_slashes(self):
        assert normalize_zip_separators("a\\b\\c") == "a/b/c"

    def test_real_world_windows_name(self):
        assert normalize_zip_separators("Image repository\\UW26509z.tif") == (
            "Image repository/UW26509z.tif"
        )

    def test_strips_leading_drive_letter(self):
        assert normalize_zip_separators("C:\\x\\y") == "x/y"

    def test_strips_drive_letter_with_forward_slashes(self):
        assert normalize_zip_separators("D:/foo/bar") == "foo/bar"

    def test_lowercase_drive_letter(self):
        assert normalize_zip_separators("z:\\only") == "only"

    def test_leaves_plain_relative_name_untouched(self):
        assert normalize_zip_separators("a/b/c") == "a/b/c"

    def test_colon_not_at_position_one_is_not_a_drive(self):
        # A colon deeper in the name is a legal (if unusual) filename byte,
        # not a drive spec — don't strip it.
        assert normalize_zip_separators("ab:c/d") == "ab:c/d"

    def test_idempotent(self):
        once = normalize_zip_separators("C:\\a\\b")
        assert normalize_zip_separators(once) == once


class TestDecodeZipFilename:
    """UTF-8 → CP437 decode, then separator normalization."""

    def test_utf8_bytes(self):
        assert decode_zip_filename("naïve\\dir\\f.txt".encode()) == "naïve/dir/f.txt"

    def test_cp437_fallback_for_non_utf8_bytes(self):
        # 0x82 is 'é' in CP437; as a lone continuation byte it is invalid
        # UTF-8, so the decoder falls back to CP437.
        raw = b"caf\x82\\x.txt"
        assert decode_zip_filename(raw) == "café/x.txt"

    def test_accepts_str_and_normalizes(self):
        assert decode_zip_filename("C:\\a\\b") == "a/b"


class TestSafeMemberKey:
    """Format-agnostic S3-key safety: leading slash, ``.``, ``..``."""

    def test_plain_name_untouched(self):
        assert safe_member_key("a/b/c") == "a/b/c"

    def test_strips_leading_slash(self):
        assert safe_member_key("/foo") == "foo"

    def test_strips_multiple_leading_slashes(self):
        assert safe_member_key("///foo/bar") == "foo/bar"

    def test_drops_dot_segments(self):
        assert safe_member_key("a/./b") == "a/b"

    def test_collapses_interior_double_slash(self):
        assert safe_member_key("a//b") == "a/b"

    def test_dotdot_raises_by_default(self):
        with pytest.raises(UnsafeArchiveMemberError) as exc_info:
            safe_member_key("a/../b")
        assert exc_info.value.member_name == "a/../b"
        assert "--fix-unsafe-paths" in str(exc_info.value)

    def test_dotdot_collapses_with_fix_unsafe(self):
        assert safe_member_key("a/../b", fix_unsafe=True) == "b"

    def test_leading_dotdot_collapses_without_escaping_root(self):
        assert safe_member_key("../../etc", fix_unsafe=True) == "etc"

    def test_leading_dotdot_raises_by_default(self):
        with pytest.raises(UnsafeArchiveMemberError):
            safe_member_key("../../etc")

    def test_deep_traversal_never_escapes(self):
        assert safe_member_key("a/b/../../../../c", fix_unsafe=True) == "c"
