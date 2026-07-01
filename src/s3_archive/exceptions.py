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
