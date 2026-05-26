"""Streaming archive ``ls`` subcommand.

Lists archive members without extracting — useful for a sanity check
before a multi-GB extract job ("what's the top-level directory called?
how many files?"). Streams in the same single-pass model as
:mod:`s3_archive.extract`; nothing is staged on disk.
"""

import tarfile

from stream_unzip import stream_unzip

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.extract import _CHUNK_SIZE, _TAR_MODES
from s3_archive.log_config import get_logger

log = get_logger(__name__)


def _format_size(num_bytes: int) -> str:
    """Return a short, human-readable size like ``"12.3 MB"`` or ``"723 B"``."""
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{num_bytes} B"  # pragma: no cover


def _print_entry(size: int, name: str) -> None:
    print(f"{size:>12d}  {name}")


def _list_tar(client, archive_bucket: str, archive_key: str, tar_mode: str) -> tuple[int, int]:
    """Stream a tar (any supported compression) and print one line per file member."""
    resp = client.get_object(Bucket=archive_bucket, Key=archive_key)
    count = 0
    total = 0
    with tarfile.open(fileobj=resp["Body"], mode=tar_mode) as tar:
        for member in tar:
            if not member.isfile():
                continue
            _print_entry(member.size, member.name)
            count += 1
            total += member.size
    return count, total


def _list_zip(client, archive_bucket: str, archive_key: str) -> tuple[int, int]:
    """Stream a zip and print one line per file member.

    Each member's chunks must be drained (read but discarded) before
    ``stream_unzip`` advances to the next member — same dry-run pattern
    used by :func:`s3_archive.extract.extract_zip`.
    """
    resp = client.get_object(Bucket=archive_bucket, Key=archive_key)

    def _archive_chunks():
        while True:
            chunk = resp["Body"].read(_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    count = 0
    total = 0
    for name, size, chunks in stream_unzip(_archive_chunks()):
        file_name = name.decode("utf-8") if isinstance(name, bytes) else name
        observed = 0
        for chunk in chunks:
            observed += len(chunk)
        if file_name.endswith("/"):
            continue
        # `size` is None when the central directory hasn't disclosed it;
        # fall back to the byte count we actually streamed.
        member_size = size if size is not None else observed
        _print_entry(member_size, file_name)
        count += 1
        total += member_size
    return count, total


def list_archive(
    client,
    archive_bucket: str,
    archive_key: str,
    fmt: str,
) -> tuple[int, int]:
    """Dispatch on archive format. Prints to stdout; returns ``(count, total_bytes)``."""
    if fmt in _TAR_MODES:
        count, total = _list_tar(client, archive_bucket, archive_key, _TAR_MODES[fmt])
    elif fmt == "zip":
        count, total = _list_zip(client, archive_bucket, archive_key)
    else:
        raise UnsupportedArchiveFormatError(f"Unsupported format: {fmt!r}")
    print(f"{count} files, {_format_size(total)}")
    return count, total
