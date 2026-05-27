"""S3 URL parsing and archive-format detection."""

from s3_archive.exceptions import ConfigError, UnsupportedArchiveFormatError


def parse_s3_url(url: str) -> tuple[str, str]:
    """Parse ``s3://bucket/key`` into ``(bucket, key)``.

    The key may be empty for prefix-style URLs that end in ``/`` — callers
    that need a non-empty key should validate separately.
    """
    if not url.startswith("s3://"):
        raise ConfigError(f"Not an S3 URL: {url!r} (must start with s3://)")
    rest = url[5:]
    if "/" not in rest:
        # Bucket-only URL like s3://bucket — treat as empty key/prefix.
        bucket, key = rest, ""
    else:
        bucket, key = rest.split("/", 1)
    if not bucket:
        raise ConfigError(f"S3 URL has empty bucket: {url!r}")
    return bucket, key


def parse_s3_prefix(url: str) -> tuple[str, str]:
    """Like :func:`parse_s3_url` but normalizes the prefix to end with ``/``.

    Empty prefix is allowed (whole-bucket). Used for ``extract`` and
    ``create`` source/destination prefixes.
    """
    bucket, prefix = parse_s3_url(url)
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    return bucket, prefix


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


def looks_like_archive_url(url: str) -> bool:
    """True if *url* ends with any extension :func:`detect_format` recognizes.

    Useful to callers that want to check "is this an archive?" without
    catching :class:`UnsupportedArchiveFormatError` (e.g. s3-bagit's
    ``verify`` guard, which rejects archive URLs in favor of
    extracted-bag prefixes). Backed by the same :data:`_EXTENSION_FORMATS`
    table as :func:`detect_format`, so the two never drift.

    Trailing slashes are stripped before the suffix check so that
    URLs like ``s3://b/archive.7z/`` are still recognized.
    """
    lower = url.lower().rstrip("/")
    return any(lower.endswith(suffixes) for suffixes, _fmt in _EXTENSION_FORMATS)


def detect_format(url: str) -> str:
    """Detect archive format from URL extension.

    Returns one of ``"tar"``, ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``,
    ``"tar.zst"``, ``"zip"``, or ``"7z"``. Raises
    :class:`UnsupportedArchiveFormatError` for unrecognized extensions.
    """
    lower = url.lower()
    for suffixes, fmt in _EXTENSION_FORMATS:
        if lower.endswith(suffixes):
            return fmt
    raise UnsupportedArchiveFormatError(
        f"Cannot detect archive format from: {url!r} "
        f"(expected .tar, .tar.gz/.tgz, .tar.bz2/.tbz2, .tar.xz/.txz, "
        f".tar.zst, .zip, or .7z)"
    )
