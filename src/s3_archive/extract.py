"""Streaming archive extract from S3 to S3.

``extract`` streams the archive object out of S3 (via *src_client*),
decodes it via :func:`s3_archive.members.iter_archive_members`, and
``upload_fileobj`` each member back to S3 (via *dst_client*) — nothing
is staged on local disk. A 500 GB archive does not need 500 GB of free
space anywhere.

The two clients may be the same boto3 client (single-endpoint workflows)
or two clients pointed at different endpoints (e.g. archive at AWS,
extracted tree at Kopah/RGW). The streaming model is identical either
way — see docs/ARCHITECTURE.md.
"""

from collections.abc import Callable
from dataclasses import dataclass

from s3_archive.iter import IterableFileobj
from s3_archive.log_config import get_logger
from s3_archive.members import iter_archive_members

log = get_logger(__name__)


@dataclass(frozen=True)
class ExtractEvent:
    """One progress event emitted during :func:`extract`.

    Designed to map cleanly onto ``tqdm.update(bytes_transferred)`` plus
    ``tqdm.set_postfix(file=member)`` without the callback needing to
    track state itself. Events come in two shapes:

    - **Byte-transfer events** during an upload (``bytes_transferred > 0``):
      ``member`` and ``member_index`` identify the file in flight,
      ``bytes_transferred`` is the delta since the last event (raw from
      boto3's per-chunk ``Callback=``), and ``member_size`` is the file's
      known uncompressed size (``None`` if the archive format doesn't
      expose it before extraction, e.g. for streamed tar entries).

    - **Member-boundary events** (``bytes_transferred == 0``): emitted
      once before each upload begins, so a UI can switch its "current
      file" indicator without waiting for the first chunk.

    Frozen dataclass: the event is shared by reference into a callback
    that may run on boto3's transfer threadpool, so it must not mutate
    after construction.
    """

    member: str
    bytes_transferred: int
    member_index: int
    member_size: int | None = None


ProgressCallback = Callable[[ExtractEvent], None]


def _dest_key(prefix: str, member_name: str) -> str:
    """Join *prefix* (may be ``""`` or end-with-``/``) and *member_name* into an S3 key."""
    if not prefix:
        return member_name
    if prefix.endswith("/"):
        return prefix + member_name
    return prefix + "/" + member_name


def _make_upload_callback(
    on_progress: ProgressCallback,
    member_name: str,
    member_index: int,
    member_size: int | None,
) -> Callable[[int], None]:
    """Build a one-arg ``Callback=`` for ``boto3.upload_fileobj``.

    Extracted out of the extract loop so the closure doesn't capture
    loop variables (ruff B023). boto3 calls this with a single int:
    bytes transferred since the previous invocation.
    """

    def _cb(bytes_transferred: int) -> None:
        on_progress(
            ExtractEvent(
                member=member_name,
                bytes_transferred=bytes_transferred,
                member_index=member_index,
                member_size=member_size,
            )
        )

    return _cb


def extract(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    on_progress: ProgressCallback | None = None,
    on_read: Callable[[int], None] | None = None,
) -> list[str]:
    """Stream an archive from S3 and upload each member back to S3.

    *src_client* reads the archive object; *dst_client* writes the
    extracted members. They may be the same boto3 client. *fmt* is one
    of the strings returned by :func:`s3_archive.url.detect_format`.
    Returns the list of member names that were (or would be) written,
    relative to *dest_prefix*.

    *on_progress*, if supplied, is invoked for byte-level *upload*
    progress events (uncompressed member bytes written to the
    destination). See :class:`ExtractEvent`. The callback may run on
    boto3's transfer threadpool, so it must be thread-safe (tqdm.update
    is).

    *on_read*, if supplied, is called with the length of each archive
    chunk *read* from the source (compressed bytes). Sized against the
    archive object's ``ContentLength`` it drives a "how far through the
    archive" progress bar. Not reported for ``7z`` (random-access
    decode). Distinct from *on_progress*, which counts uncompressed
    upload bytes and has no clean total for streamed tar entries.
    """
    log.info(
        "Extracting %s s3://%s/%s -> s3://%s/%s",
        fmt,
        archive_bucket,
        archive_key,
        dest_bucket,
        dest_prefix,
    )

    member_names: list[str] = []
    members = iter_archive_members(src_client, archive_bucket, archive_key, fmt, on_bytes=on_read)
    for idx, member in enumerate(members):
        member_names.append(member.name)
        if on_progress is not None:
            # Boundary event so the UI can switch "current file" before
            # any bytes arrive.
            on_progress(
                ExtractEvent(
                    member=member.name,
                    bytes_transferred=0,
                    member_index=idx,
                    member_size=member.size if member.size else None,
                )
            )
        if dry_run:
            member.drain()
            if verbose:
                if member.size:
                    log.info("  would write %s (%d bytes)", member.name, member.size)
                else:
                    log.info("  would write %s", member.name)
            continue
        dest_key = _dest_key(dest_prefix, member.name)
        if verbose:
            log.info("  %s -> s3://%s/%s", member.name, dest_bucket, dest_key)

        if on_progress is None:
            dst_client.upload_fileobj(
                IterableFileobj(member.chunks()),
                dest_bucket,
                dest_key,
            )
        else:
            dst_client.upload_fileobj(
                IterableFileobj(member.chunks()),
                dest_bucket,
                dest_key,
                Callback=_make_upload_callback(
                    on_progress,
                    member.name,
                    idx,
                    member.size if member.size else None,
                ),
            )

    log.info(
        "extract %s: %d files",
        "(dry-run)" if dry_run else "complete",
        len(member_names),
    )
    return member_names
