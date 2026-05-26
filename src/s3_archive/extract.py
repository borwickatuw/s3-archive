"""Streaming archive extract from S3 to S3.

Both ``extract_tar`` and ``extract_zip`` stream the archive object out
of S3, decompress on the fly, and ``upload_fileobj`` each member back to
S3 — nothing is staged on local disk. A 500 GB archive does not need
500 GB of free space anywhere.

See docs/ARCHITECTURE.md for the streaming model.
"""

import tarfile

from stream_unzip import stream_unzip

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.iter import IterableFileobj, NonSeekableReader
from s3_archive.log_config import get_logger

log = get_logger(__name__)

_CHUNK_SIZE = 65536

# Maps the format string from :func:`s3_archive.url.detect_format` to the
# ``tarfile.open`` streaming mode that decodes it.
_TAR_MODES: dict[str, str] = {
    "tar": "r|",
    "tar.gz": "r|gz",
    "tar.bz2": "r|bz2",
    "tar.xz": "r|xz",
}


def _dest_key(prefix: str, member_name: str) -> str:
    """Join *prefix* (may be ``""`` or end-with-``/``) and *member_name* into an S3 key."""
    if not prefix:
        return member_name
    if prefix.endswith("/"):
        return prefix + member_name
    return prefix + "/" + member_name


def extract_tar(
    client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    tar_mode: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> list[str]:
    """Stream a tar (optionally compressed) from S3 and upload each member back to S3.

    *tar_mode* is one of ``"r|"``, ``"r|gz"``, ``"r|bz2"``, ``"r|xz"`` —
    all of which ``tarfile.open`` handles in a single non-seeking pass.

    Returns the list of member names that were (or would be) written,
    relative to *dest_prefix*.
    """
    log.info(
        "Extracting tar (mode=%s) s3://%s/%s -> s3://%s/%s",
        tar_mode,
        archive_bucket,
        archive_key,
        dest_bucket,
        dest_prefix,
    )

    resp = client.get_object(Bucket=archive_bucket, Key=archive_key)
    member_names: list[str] = []

    with tarfile.open(fileobj=resp["Body"], mode=tar_mode) as tar:
        for member in tar:
            if not member.isfile():
                continue
            member_names.append(member.name)
            if dry_run:
                if verbose:
                    log.info("  would write %s (%d bytes)", member.name, member.size)
                continue

            fileobj = tar.extractfile(member)
            if fileobj is None:
                continue
            dest_key = _dest_key(dest_prefix, member.name)
            if verbose:
                log.info("  %s -> s3://%s/%s", member.name, dest_bucket, dest_key)
            client.upload_fileobj(NonSeekableReader(fileobj), dest_bucket, dest_key)

    log.info(
        "tar extract %s: %d files",
        "(dry-run)" if dry_run else "complete",
        len(member_names),
    )
    return member_names


def extract_zip(
    client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> list[str]:
    """Stream a zip from S3 and upload each member back to S3.

    Returns the list of member names that were (or would be) written.
    """
    log.info(
        "Extracting zip s3://%s/%s -> s3://%s/%s",
        archive_bucket,
        archive_key,
        dest_bucket,
        dest_prefix,
    )

    resp = client.get_object(Bucket=archive_bucket, Key=archive_key)

    def _archive_chunks():
        while True:
            chunk = resp["Body"].read(_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    member_names: list[str] = []
    for name, _size, chunks in stream_unzip(_archive_chunks()):
        file_name = name.decode("utf-8") if isinstance(name, bytes) else name
        if file_name.endswith("/"):
            # Directory entry — drain its (empty) chunks and skip.
            for _ in chunks:
                pass
            continue

        member_names.append(file_name)
        if dry_run:
            for _ in chunks:
                pass
            if verbose:
                log.info("  would write %s", file_name)
            continue

        dest_key = _dest_key(dest_prefix, file_name)
        if verbose:
            log.info("  %s -> s3://%s/%s", file_name, dest_bucket, dest_key)
        fileobj = IterableFileobj(chunks)
        client.upload_fileobj(fileobj, dest_bucket, dest_key)

    log.info(
        "zip extract %s: %d files",
        "(dry-run)" if dry_run else "complete",
        len(member_names),
    )
    return member_names


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
    """Dispatch on archive format. Returns the list of extracted member names."""
    if fmt in _TAR_MODES:
        return extract_tar(
            client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            _TAR_MODES[fmt],
            dry_run=dry_run,
            verbose=verbose,
        )
    if fmt == "zip":
        return extract_zip(
            client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            dry_run=dry_run,
            verbose=verbose,
        )
    raise UnsupportedArchiveFormatError(f"Unsupported format: {fmt!r}")
