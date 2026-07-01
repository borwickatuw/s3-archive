"""s3-archive: streaming S3 archive operations (extract / create / list / hash)."""

from s3_archive.create import create, create_tar_gz, create_zip
from s3_archive.exceptions import ConfigError, ETagMismatchError, UnsupportedArchiveFormatError
from s3_archive.extract import extract
from s3_archive.hashing import (
    HashingTap,
    TripleHash,
    body_chunks,
    multi_hash,
    stream_hash_object,
    triple_hash,
)
from s3_archive.iter import IterableFileobj, NonSeekableReader, PipeReader
from s3_archive.list import list_objects
from s3_archive.ls import list_archive
from s3_archive.members import ArchiveMember, iter_archive_members
from s3_archive.s3_client import client_for
from s3_archive.url import ParsedS3Url, detect_format, parse_s3_prefix, parse_s3_url

try:
    # Written by hatch-vcs on every build from `git describe --tags`.
    from s3_archive._version import __version__
except ImportError:
    # Working from a source tree where the build hook hasn't run yet.
    __version__ = "0.0.0+unknown"

REPO_URL = "https://github.com/borwickatuw/s3-archive"

__all__ = [
    "REPO_URL",
    "ArchiveMember",
    "ConfigError",
    "ETagMismatchError",
    "HashingTap",
    "IterableFileobj",
    "NonSeekableReader",
    "ParsedS3Url",
    "PipeReader",
    "TripleHash",
    "UnsupportedArchiveFormatError",
    "__version__",
    "body_chunks",
    "client_for",
    "create",
    "create_tar_gz",
    "create_zip",
    "detect_format",
    "extract",
    "iter_archive_members",
    "list_archive",
    "list_objects",
    "multi_hash",
    "parse_s3_prefix",
    "parse_s3_url",
    "stream_hash_object",
    "triple_hash",
]
