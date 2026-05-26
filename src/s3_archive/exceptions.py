"""Exceptions raised by s3-archive.

Library code raises these instead of calling sys.exit(). The CLI entry
point catches them and maps to a non-zero exit code with a clean error
message on stderr.
"""


class ConfigError(Exception):
    """Missing or invalid configuration (env vars, paths, S3 URLs)."""


class UnsupportedArchiveFormatError(Exception):
    """Archive format isn't recognized or supported (e.g. ``.7z``)."""
