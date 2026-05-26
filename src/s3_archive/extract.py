"""Streaming archive extract from S3 to S3.

``extract`` streams the archive object out of S3, decodes it via
:func:`s3_archive.members.iter_archive_members`, and
``upload_fileobj`` each member back to S3 — nothing is staged on
local disk. A 500 GB archive does not need 500 GB of free space
anywhere.

See docs/ARCHITECTURE.md for the streaming model.
"""

from s3_archive.iter import IterableFileobj
from s3_archive.log_config import get_logger
from s3_archive.members import iter_archive_members

log = get_logger(__name__)


def _dest_key(prefix: str, member_name: str) -> str:
    """Join *prefix* (may be ``""`` or end-with-``/``) and *member_name* into an S3 key."""
    if not prefix:
        return member_name
    if prefix.endswith("/"):
        return prefix + member_name
    return prefix + "/" + member_name


def extract(
    client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> list[str]:
    """Stream an archive from S3 and upload each member back to S3.

    *fmt* is one of the strings returned by
    :func:`s3_archive.url.detect_format`. Returns the list of member
    names that were (or would be) written, relative to *dest_prefix*.
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
    for member in iter_archive_members(client, archive_bucket, archive_key, fmt):
        member_names.append(member.name)
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
        client.upload_fileobj(
            IterableFileobj(member.chunks()),
            dest_bucket,
            dest_key,
        )

    log.info(
        "extract %s: %d files",
        "(dry-run)" if dry_run else "complete",
        len(member_names),
    )
    return member_names
