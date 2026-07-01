"""Streaming S3-prefix → archive create.

Both ``create_tar_gz`` and ``create_zip`` walk an S3 prefix (via
*src_client*) and emit a serialized archive at another S3 key (via
*dst_client*). The two clients may be the same boto3 client or two
clients pointed at different endpoints. Nothing is staged on local
disk — see docs/ARCHITECTURE.md.

The tar.gz path uses ``os.pipe()`` + a writer thread because stdlib
``tarfile`` writes to a file-like sink; we point it at the pipe's
write-end and feed the read-end to ``upload_fileobj`` in parallel.
The zip path uses ``stream_zip`` directly — it produces a bytes
iterable, which we wrap in :class:`IterableFileobj`.
"""

import os
import tarfile
import threading
from collections.abc import Callable
from datetime import datetime, timezone

from stream_zip import ZIP_64, stream_zip

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.iter import IterableFileobj, PipeReader
from s3_archive.list import list_objects
from s3_archive.log_config import get_logger
from s3_archive.retry import resumable_body_chunks

log = get_logger(__name__)


def create_tar_gz(
    src_client,
    dst_client,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_key: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    on_bytes: Callable[[int], None] | None = None,
) -> None:
    """Create a ``.tar.gz`` archive from S3 objects and upload it to S3.

    Uses ``os.pipe()`` + a writer thread so ``tarfile.open(mode="w|gz")``
    can stream into ``boto3.upload_fileobj`` without staging on disk.
    *src_client* reads the per-object source bodies; *dst_client* writes
    the single archive object.

    Each source object is read via
    :func:`s3_archive.retry.resumable_body_chunks`, so a transient
    mid-object connection drop resumes from the byte offset already read
    rather than failing the whole archive. The body streams member-by-
    member (sized from the object listing) — no source object is buffered
    whole in memory. *on_bytes*, if supplied, is called with the length
    of each source chunk read (for a byte-progress bar).
    """
    objects = list_objects(src_client, source_bucket, source_prefix, sort=True)
    if not objects:
        log.warning("No objects found under s3://%s/%s", source_bucket, source_prefix)
        return

    if dry_run:
        log.info(
            "Would archive %d objects to s3://%s/%s",
            len(objects),
            dest_bucket,
            dest_key,
        )
        if verbose:
            for obj in objects:
                log.info("  %s (%d bytes)", obj["Key"], obj["Size"])
        return

    log.info(
        "Creating tar.gz from %d objects in s3://%s/%s",
        len(objects),
        source_bucket,
        source_prefix,
    )

    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb")
    write_file = os.fdopen(write_fd, "wb")

    writer_error: list[BaseException] = []

    def _writer() -> None:
        try:
            with tarfile.open(fileobj=write_file, mode="w|gz") as tar:
                for obj in objects:
                    member_name = obj["RelativePath"]
                    if not member_name:
                        continue

                    info = tarfile.TarInfo(name=member_name)
                    # Size comes from the listing so the body can stream
                    # straight into the tar without buffering the whole
                    # object; tarfile.addfile reads exactly info.size bytes.
                    info.size = obj["Size"]

                    chunks = resumable_body_chunks(
                        src_client, source_bucket, obj["Key"], on_bytes=on_bytes
                    )
                    tar.addfile(info, IterableFileobj(chunks))
        except BaseException as exc:
            writer_error.append(exc)
        finally:
            write_file.close()

    writer_thread = threading.Thread(target=_writer, daemon=True)
    writer_thread.start()

    try:
        dst_client.upload_fileobj(PipeReader(read_file), dest_bucket, dest_key)
    finally:
        read_file.close()

    writer_thread.join()

    if writer_error:
        raise writer_error[0]

    log.info("Uploaded s3://%s/%s", dest_bucket, dest_key)


def create_zip(
    src_client,
    dst_client,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_key: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    on_bytes: Callable[[int], None] | None = None,
) -> None:
    """Create a ``.zip`` archive from S3 objects and upload it to S3.

    ``stream_zip`` returns a bytes iterable, which we wrap in
    :class:`IterableFileobj` for ``upload_fileobj``. *src_client* reads
    the per-object source bodies; *dst_client* writes the archive.

    Each source object is read via
    :func:`s3_archive.retry.resumable_body_chunks`, so a transient
    mid-object connection drop resumes from the byte offset already read.
    The member mtime comes from the object listing (``LastModified``),
    so no extra metadata GET is needed. *on_bytes*, if supplied, is
    called with the length of each source chunk read (for a
    byte-progress bar).
    """
    objects = list_objects(src_client, source_bucket, source_prefix, sort=True)
    if not objects:
        log.warning("No objects found under s3://%s/%s", source_bucket, source_prefix)
        return

    if dry_run:
        log.info(
            "Would archive %d objects to s3://%s/%s",
            len(objects),
            dest_bucket,
            dest_key,
        )
        if verbose:
            for obj in objects:
                log.info("  %s (%d bytes)", obj["Key"], obj["Size"])
        return

    log.info(
        "Creating zip from %d objects in s3://%s/%s",
        len(objects),
        source_bucket,
        source_prefix,
    )

    def _member_files():
        for obj in objects:
            member_name = obj["RelativePath"]
            if not member_name:
                continue

            modified_at = obj["LastModified"] or datetime.now(timezone.utc)
            chunks = resumable_body_chunks(src_client, source_bucket, obj["Key"], on_bytes=on_bytes)
            yield member_name, modified_at, 0o644, ZIP_64, chunks

    zip_bytes = stream_zip(_member_files())
    fileobj = IterableFileobj(zip_bytes)
    dst_client.upload_fileobj(fileobj, dest_bucket, dest_key)

    log.info("Uploaded s3://%s/%s", dest_bucket, dest_key)


def create(
    src_client,
    dst_client,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_key: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    on_bytes: Callable[[int], None] | None = None,
) -> None:
    """Dispatch on archive format. *fmt* is ``"tar.gz"`` or ``"zip"``.

    Other tar variants (``tar``, ``tar.bz2``, ``tar.xz``, ``tar.zst``)
    are not implemented for create — operators with a specific
    compression need can extend this dispatcher; the streaming model
    is the same.

    *on_bytes*, if supplied, is called with the length of each source
    chunk read from S3 (for a byte-progress bar sized against the total
    source bytes).
    """
    if fmt == "tar.gz":
        create_tar_gz(
            src_client,
            dst_client,
            source_bucket,
            source_prefix,
            dest_bucket,
            dest_key,
            dry_run=dry_run,
            verbose=verbose,
            on_bytes=on_bytes,
        )
        return
    if fmt == "zip":
        create_zip(
            src_client,
            dst_client,
            source_bucket,
            source_prefix,
            dest_bucket,
            dest_key,
            dry_run=dry_run,
            verbose=verbose,
            on_bytes=on_bytes,
        )
        return
    if fmt == "7z":
        raise UnsupportedArchiveFormatError(
            ".7z create is not supported (the SignatureHeader at the front of a "
            "7z archive references a header at the end, which is incompatible "
            "with streaming multipart uploads). Use .tar.gz or .zip instead."
        )
    raise UnsupportedArchiveFormatError(f"create: format {fmt!r} not supported (use tar.gz or zip)")
