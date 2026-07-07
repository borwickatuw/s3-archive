"""Archive ``ls`` subcommand — reads as little of the archive as it can.

Lists archive members without extracting — useful as a sanity check
before a multi-TB extract job ("what's the top-level directory called?
how many files?"). How much is read depends on what the format's
layout allows:

- **zip** — the central directory at the tail of the file: a HEAD plus
  a few ranged GETs, regardless of archive size. A 1 TB zip lists in
  seconds; no member bytes are ever fetched.
- **7z** — the trailing header, same story: names and sizes come from
  the tail prefetch, no member bytes.
- **tar family** — streamed front-to-back. Tar has no index; reading
  through the (decompressed) stream is the only way to find the
  headers. Still single-pass and disk-free, but it does read the whole
  archive — inherent to the format, not a choice.

Nothing is ever staged on disk in any of these modes.
"""

import zipfile

from s3_archive.exceptions import ArchiveReadError
from s3_archive.log_config import get_logger
from s3_archive.members import _apply_safe_keys, iter_archive_members
from s3_archive.paths import safe_member_key
from s3_archive.seekable import iter_zip_members_seekable, open_seekable
from s3_archive.seven_z import list_seven_z_entries

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


def _zip_entries(client, bucket: str, key: str, *, fix_unsafe_paths: bool):
    """Yield ``(size, name)`` from the zip central directory only.

    The member chunk generators are never pulled, so ``zf.open`` never
    fires — total bytes fetched is the CD read, independent of archive
    size. Also immune to the stored-with-data-descriptor shape that
    defeats forward-only streaming, and reports real sizes for it (the
    local headers say 0; the CD knows).
    """
    fileobj = open_seekable(client, bucket, key)
    try:
        try:
            members = iter_zip_members_seekable(fileobj)
        except zipfile.BadZipFile as exc:
            raise ArchiveReadError(f"zip central directory read failed: {exc}", cause=exc) from exc
        for member in _apply_safe_keys(members, fix_unsafe_paths=fix_unsafe_paths):
            yield member.size, member.name
    finally:
        fileobj.close()


def _seven_z_entries(client, bucket: str, key: str, *, fix_unsafe_paths: bool):
    """Yield ``(size, name)`` from the 7z trailing header only."""
    for raw_name, size in list_seven_z_entries(client, bucket, key):
        yield size, safe_member_key(raw_name, fix_unsafe=fix_unsafe_paths)


def _streamed_entries(client, bucket: str, key: str, fmt: str, *, fix_unsafe_paths: bool):
    """Yield ``(size, name)`` by streaming the archive (tar family)."""
    for member in iter_archive_members(client, bucket, key, fmt, fix_unsafe_paths=fix_unsafe_paths):
        observed = 0
        for chunk in member.chunks():
            observed += len(chunk)
        # Belt-and-suspenders: fall back to drained bytes if the header
        # size is 0 (tar headers always carry one in practice).
        yield member.size if member.size else observed, member.name


def list_archive(
    client,
    archive_bucket: str,
    archive_key: str,
    fmt: str,
    *,
    fix_unsafe_paths: bool = False,
) -> tuple[int, int]:
    """List an archive, printing one line per file member.

    Prints to stdout; returns ``(count, total_bytes)``. Zip and 7z read
    only the archive's index (central directory / trailing header) via
    ranged GETs; the tar family is streamed front-to-back because tar
    has no index — see the module docstring.

    *fix_unsafe_paths* mirrors :func:`s3_archive.extract.extract` so
    ``ls`` is a faithful preview of what ``extract`` will write: member
    names are normalized the same way, and a ``..`` traversal segment
    raises :class:`s3_archive.exceptions.UnsafeArchiveMemberError`
    unless *fix_unsafe_paths* collapses it.
    """
    if fmt == "zip":
        entries = _zip_entries(
            client, archive_bucket, archive_key, fix_unsafe_paths=fix_unsafe_paths
        )
    elif fmt == "7z":
        entries = _seven_z_entries(
            client, archive_bucket, archive_key, fix_unsafe_paths=fix_unsafe_paths
        )
    else:
        entries = _streamed_entries(
            client, archive_bucket, archive_key, fmt, fix_unsafe_paths=fix_unsafe_paths
        )

    count = 0
    total = 0
    for size, name in entries:
        _print_entry(size, name)
        count += 1
        total += size
    print(f"{count} files, {_format_size(total)}")
    return count, total
