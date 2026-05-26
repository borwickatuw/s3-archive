"""Streaming S3-prefix → archive create.

Both ``create_tar_gz`` and ``create_zip`` walk an S3 prefix and emit a
serialized archive at another S3 key. Nothing is staged on local disk —
see docs/ARCHITECTURE.md.

The tar.gz path uses ``os.pipe()`` + a writer thread because stdlib
``tarfile`` writes to a file-like sink; we point it at the pipe's
write-end and feed the read-end to ``upload_fileobj`` in parallel.
The zip path uses ``stream_zip`` directly — it produces a bytes
iterable, which we wrap in :class:`IterableFileobj`.
"""

import io
import os
import tarfile
import threading
from datetime import datetime, timezone

from stream_zip import ZIP_64, stream_zip
from tqdm import tqdm

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.iter import IterableFileobj
from s3_archive.list import list_objects
from s3_archive.log_config import get_logger

log = get_logger(__name__)

_CHUNK_SIZE = 65536


class _PipeReader:
    """Wrap an ``os.fdopen`` read-end so ``boto3.upload_fileobj`` accepts it.

    Same shape as :class:`s3_archive.iter.NonSeekableReader` but with a
    short-read loop: pipes can return less than ``size`` bytes per call,
    and boto3's multipart uploader is happier when each ``read(size)``
    returns full parts.
    """

    def __init__(self, fobj) -> None:
        self._fobj = fobj

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            return self._fobj.read()
        chunks: list[bytes] = []
        remaining = size
        while remaining > 0:
            chunk = self._fobj.read(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False


def create_tar_gz(
    client,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_key: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Create a ``.tar.gz`` archive from S3 objects and upload it to S3.

    Uses ``os.pipe()`` + a writer thread so ``tarfile.open(mode="w|gz")``
    can stream into ``boto3.upload_fileobj`` without staging on disk.
    """
    objects = list_objects(client, source_bucket, source_prefix, sort=True)
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
                for obj in tqdm(objects, desc="Archiving", disable=not verbose):
                    member_name = obj["RelativePath"]
                    if not member_name:
                        continue

                    resp = client.get_object(Bucket=source_bucket, Key=obj["Key"])
                    body = resp["Body"].read()

                    info = tarfile.TarInfo(name=member_name)
                    info.size = len(body)

                    tar.addfile(info, io.BytesIO(body))
        except BaseException as exc:
            writer_error.append(exc)
        finally:
            write_file.close()

    writer_thread = threading.Thread(target=_writer, daemon=True)
    writer_thread.start()

    try:
        client.upload_fileobj(_PipeReader(read_file), dest_bucket, dest_key)
    finally:
        read_file.close()

    writer_thread.join()

    if writer_error:
        raise writer_error[0]

    log.info("Uploaded s3://%s/%s", dest_bucket, dest_key)


def create_zip(
    client,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_key: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Create a ``.zip`` archive from S3 objects and upload it to S3.

    ``stream_zip`` returns a bytes iterable, which we wrap in
    :class:`IterableFileobj` for ``upload_fileobj``.
    """
    objects = list_objects(client, source_bucket, source_prefix, sort=True)
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
        for obj in tqdm(objects, desc="Archiving", disable=not verbose):
            member_name = obj["RelativePath"]
            if not member_name:
                continue

            resp = client.get_object(Bucket=source_bucket, Key=obj["Key"])
            modified_at = resp.get("LastModified", datetime.now(timezone.utc))

            def _chunks(body=resp["Body"]):
                while True:
                    chunk = body.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk

            yield member_name, modified_at, 0o644, ZIP_64, _chunks()

    zip_bytes = stream_zip(_member_files())
    fileobj = IterableFileobj(zip_bytes)
    client.upload_fileobj(fileobj, dest_bucket, dest_key)

    log.info("Uploaded s3://%s/%s", dest_bucket, dest_key)


def create(
    client,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_key: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Dispatch on archive format. *fmt* is ``"tar.gz"`` or ``"zip"``.

    Other tar variants (``tar``, ``tar.bz2``, ``tar.xz``, ``tar.zst``)
    are not implemented for create — operators with a specific
    compression need can extend this dispatcher; the streaming model
    is the same.
    """
    if fmt == "tar.gz":
        create_tar_gz(
            client,
            source_bucket,
            source_prefix,
            dest_bucket,
            dest_key,
            dry_run=dry_run,
            verbose=verbose,
        )
        return
    if fmt == "zip":
        create_zip(
            client,
            source_bucket,
            source_prefix,
            dest_bucket,
            dest_key,
            dry_run=dry_run,
            verbose=verbose,
        )
        return
    raise UnsupportedArchiveFormatError(f"create: format {fmt!r} not supported (use tar.gz or zip)")
