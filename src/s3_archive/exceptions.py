"""Exceptions raised by s3-archive.

Library code raises these instead of calling sys.exit(). The CLI entry
point catches them and maps to a non-zero exit code with a clean error
message on stderr.
"""


class ConfigError(Exception):
    """Missing or invalid configuration (env vars, paths, S3 URLs)."""


class UnsupportedArchiveFormatError(Exception):
    """Archive format isn't recognized or supported (unknown extension)."""


class UnsafeArchiveMemberError(Exception):
    """An archive member name would escape the destination prefix.

    Raised by :func:`s3_archive.paths.safe_member_key` when a member
    name contains a ``..`` path-traversal segment and the caller did not
    opt into collapsing it. Because the extract model is single-pass
    streaming, this is detected when the offending member is reached —
    earlier members are already written. Re-running with
    ``--fix-unsafe-paths`` safely collapses ``..`` instead of raising.

    The offending name is available as :attr:`member_name`.
    """

    def __init__(self, member_name: str) -> None:
        self.member_name = member_name
        super().__init__(
            f"Archive member {member_name!r} contains a '..' path-traversal segment "
            f"that would escape the destination prefix. Re-run with --fix-unsafe-paths "
            f"to safely collapse it instead."
        )


class ResumeUnsupportedError(Exception):
    """``--resume`` was requested for an archive it can't be applied to.

    Resume relies on per-member random access into the source archive. In
    v1 that means only the natively seekable formats — ``zip`` and
    uncompressed ``tar``. Every other format (compressed tar variants,
    ``7z``) currently refuses up front, *before* any object is written and
    without creating a control file, so an operator is never left with a
    half-run under the false impression it is resumable. The same
    exception is raised when a nominally-supported archive turns out to
    lack the per-member index resume needs (e.g. a zip with no usable
    central directory).

    The CLI maps this to a clean stderr message and a config-style exit
    code; the caller should re-run without ``--resume``.
    """


class ETagMismatchError(Exception):
    """The object's current ETag no longer matches the caller-pinned one.

    Raised when a caller passes ``if_match=<etag>`` (typically the ETag
    observed at LIST time) and the object has since been overwritten.
    Reads that would mix bytes from two object generations fail fast
    instead — the caller should re-list and retry against the new
    generation.
    """

    def __init__(self, key: str, expected: str, actual: str) -> None:
        self.key = key
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"ETag mismatch for {key!r}: expected {expected}, object now has {actual} "
            f"(object changed since it was listed)"
        )


class ArchiveReadError(Exception):
    """The archive bytes themselves are bad — wrong magic, truncated, CRC failure, etc.

    Raised by :func:`s3_archive.members.iter_archive_members` (and its
    callers, ``extract`` / ``list_archive``) when the underlying decoder
    — ``tarfile`` / ``stream_unzip`` / ``py7zr`` — signals corruption.

    The original decoder exception is available in two places:
    ``__cause__`` (the PEP 3134 exception chain, used by traceback
    formatting) and the explicit :attr:`cause` attribute, for callers
    that want to dispatch on it without reaching for dunders.
    """

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
