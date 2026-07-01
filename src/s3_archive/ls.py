"""Streaming archive ``ls`` subcommand.

Lists archive members without extracting — useful for a sanity check
before a multi-GB extract job ("what's the top-level directory called?
how many files?"). Streams in the same single-pass model as
:mod:`s3_archive.extract`; nothing is staged on disk.
"""

from s3_archive.log_config import get_logger
from s3_archive.members import iter_archive_members

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


def list_archive(
    client,
    archive_bucket: str,
    archive_key: str,
    fmt: str,
    *,
    fix_unsafe_paths: bool = False,
) -> tuple[int, int]:
    """Stream an archive and print one line per file member.

    Prints to stdout; returns ``(count, total_bytes)``.

    *fix_unsafe_paths* mirrors :func:`s3_archive.extract.extract` so
    ``ls`` is a faithful preview of what ``extract`` will write: member
    names are normalized the same way, and a ``..`` traversal segment
    raises :class:`s3_archive.exceptions.UnsafeArchiveMemberError`
    unless *fix_unsafe_paths* collapses it.
    """
    count = 0
    total = 0
    for member in iter_archive_members(
        client, archive_bucket, archive_key, fmt, fix_unsafe_paths=fix_unsafe_paths
    ):
        observed = 0
        for chunk in member.chunks():
            observed += len(chunk)
        # Streaming zip's local-header size can be 0; fall back to drained bytes.
        member_size = member.size if member.size else observed
        _print_entry(member_size, member.name)
        count += 1
        total += member_size
    print(f"{count} files, {_format_size(total)}")
    return count, total
