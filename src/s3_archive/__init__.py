"""s3-archive: streaming S3 archive operations (extract / create / list)."""

from s3_archive.exceptions import ConfigError, UnsupportedArchiveFormatError
from s3_archive.extract import extract, extract_tar, extract_zip
from s3_archive.iter import IterableFileobj, NonSeekableReader
from s3_archive.list import list_objects
from s3_archive.ls import list_archive
from s3_archive.url import detect_format, parse_s3_prefix, parse_s3_url

try:
    # Written by hatch-vcs on every build from `git describe --tags`.
    from s3_archive._version import __version__
except ImportError:
    # Working from a source tree where the build hook hasn't run yet.
    __version__ = "0.0.0+unknown"

REPO_URL = "https://github.com/borwickatuw/s3-archive"

__all__ = [
    "REPO_URL",
    "ConfigError",
    "IterableFileobj",
    "NonSeekableReader",
    "UnsupportedArchiveFormatError",
    "__version__",
    "detect_format",
    "extract",
    "extract_tar",
    "extract_zip",
    "list_archive",
    "list_objects",
    "parse_s3_prefix",
    "parse_s3_url",
]
