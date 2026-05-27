"""S3 URL parsing and archive-format detection.

URLs may carry an optional profile prefix: ``profile:s3://bucket/key``.
The profile name selects which credentials/endpoint to use (see
:mod:`s3_archive.s3_client`); a bare ``s3://…`` URL parses with
``profile=None`` (resolver canonicalises to the ``default`` profile).
"""

import re
from typing import NamedTuple

from s3_archive.exceptions import ConfigError, UnsupportedArchiveFormatError

# Same grammar as `s3_archive.config_cmd.validate_profile_name`.
# We re-define rather than import to avoid an import cycle between
# url.py (lower-level) and config_cmd.py (higher-level, depends on
# s3_client which depends on... nothing here). The grammar is
# trivial enough that keeping the duplicate doesn't risk drift.
_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class ParsedS3Url(NamedTuple):
    """A parsed ``[profile:]s3://bucket/key`` URL.

    *profile* is ``None`` for bare ``s3://…`` URLs (the resolver
    canonicalises that to the ``"default"`` profile). *key* is empty
    for bucket-only URLs and is normalised by :func:`parse_s3_prefix`
    to end with ``/``.
    """

    profile: str | None
    bucket: str
    key: str


def _split_profile(url: str) -> tuple[str | None, str]:
    """Return ``(profile, remainder)`` where remainder is an ``s3://…`` URL.

    Split rule (per design doc): find the first ``:s3://``; if it is at
    index > 0, everything before it is the profile name and we validate
    against ``[A-Za-z0-9_-]+``. Otherwise the URL is bare and the
    profile is ``None``.
    """
    needle = ":s3://"
    idx = url.find(needle)
    if idx <= 0:
        # idx == -1 (no s3:// at all — let parse_s3_url raise its own
        # clear error) or idx == 0 (the URL starts with :s3:// — same).
        return None, url
    profile = url[:idx]
    if not _PROFILE_NAME_RE.match(profile):
        raise ConfigError(
            f"Invalid profile name {profile!r} in URL {url!r}: "
            f"must match [A-Za-z0-9_-]+ (letters, digits, underscore, hyphen)."
        )
    return profile, url[idx + 1 :]  # skip the colon, keep "s3://…"


def parse_s3_url(url: str) -> ParsedS3Url:
    """Parse ``[profile:]s3://bucket/key`` into a :class:`ParsedS3Url`.

    The key may be empty for prefix-style URLs that end in ``/`` —
    callers that need a non-empty key should validate separately.
    """
    profile, rest_url = _split_profile(url)
    if not rest_url.startswith("s3://"):
        raise ConfigError(f"Not an S3 URL: {url!r} (must start with s3://)")
    rest = rest_url[5:]
    if "/" not in rest:
        # Bucket-only URL like s3://bucket — treat as empty key/prefix.
        bucket, key = rest, ""
    else:
        bucket, key = rest.split("/", 1)
    if not bucket:
        raise ConfigError(f"S3 URL has empty bucket: {url!r}")
    return ParsedS3Url(profile, bucket, key)


def parse_s3_prefix(url: str) -> ParsedS3Url:
    """Like :func:`parse_s3_url` but normalizes the prefix to end with ``/``.

    Empty prefix is allowed (whole-bucket). Used for ``extract`` and
    ``create`` source/destination prefixes.
    """
    parsed = parse_s3_url(url)
    if parsed.key and not parsed.key.endswith("/"):
        return ParsedS3Url(parsed.profile, parsed.bucket, parsed.key + "/")
    return parsed


# Order matters: longer suffixes (e.g. ".tar.gz") must be checked before
# their bare-extension counterparts (".tar") to avoid misclassifying a
# compressed tar as an uncompressed one.
_EXTENSION_FORMATS: tuple[tuple[tuple[str, ...], str], ...] = (
    ((".tar.gz", ".tgz"), "tar.gz"),
    ((".tar.bz2", ".tbz2"), "tar.bz2"),
    ((".tar.xz", ".txz"), "tar.xz"),
    ((".tar.zst",), "tar.zst"),
    ((".tar",), "tar"),
    ((".zip",), "zip"),
    ((".7z",), "7z"),
)


def _strip_profile_prefix(url: str) -> str:
    """Return *url* without its ``profile:`` prefix, for suffix-based checks.

    Used by :func:`looks_like_archive_url` and :func:`detect_format` so
    a profile name never accidentally influences extension detection.
    """
    _profile, rest = _split_profile(url)
    return rest


def looks_like_archive_url(url: str) -> bool:
    """True if *url* ends with any extension :func:`detect_format` recognizes.

    Useful to callers that want to check "is this an archive?" without
    catching :class:`UnsupportedArchiveFormatError` (e.g. s3-bagit's
    ``verify`` guard, which rejects archive URLs in favor of
    extracted-bag prefixes). Backed by the same :data:`_EXTENSION_FORMATS`
    table as :func:`detect_format`, so the two never drift.

    Trailing slashes are stripped before the suffix check so that
    URLs like ``s3://b/archive.7z/`` are still recognized. A
    ``profile:`` prefix on the URL is also stripped first so it can't
    influence the suffix match.
    """
    try:
        stripped = _strip_profile_prefix(url)
    except ConfigError:
        # An invalid profile-grammar means we can't safely judge the
        # tail — be conservative and say "no".
        return False
    lower = stripped.lower().rstrip("/")
    return any(lower.endswith(suffixes) for suffixes, _fmt in _EXTENSION_FORMATS)


def detect_format(url: str) -> str:
    """Detect archive format from URL extension.

    Returns one of ``"tar"``, ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``,
    ``"tar.zst"``, ``"zip"``, or ``"7z"``. Raises
    :class:`UnsupportedArchiveFormatError` for unrecognized extensions.
    A leading ``profile:`` is stripped before the suffix check.
    """
    stripped = _strip_profile_prefix(url)
    lower = stripped.lower()
    for suffixes, fmt in _EXTENSION_FORMATS:
        if lower.endswith(suffixes):
            return fmt
    raise UnsupportedArchiveFormatError(
        f"Cannot detect archive format from: {url!r} "
        f"(expected .tar, .tar.gz/.tgz, .tar.bz2/.tbz2, .tar.xz/.txz, "
        f".tar.zst, .zip, or .7z)"
    )
