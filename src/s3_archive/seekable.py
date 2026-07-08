"""Seekable S3 object adapter + per-member seekable archive iterators.

Some read paths can't be served forward-only. ``.7z`` points its
StartHeader at metadata in the tail; ``--resume`` needs to *skip* already-
extracted members without re-reading their bytes. Both want a seekable
view of the archive object. :class:`SeekableS3Object` provides one over
``client.get_object(Range=...)`` — a Python file object (crucially, *not*
a real filename / fd), which is what keeps s3-archive off local disk.

Two consumers live here:

- :mod:`s3_archive.seven_z` wraps :class:`SeekableS3Object` in an
  :class:`io.BufferedReader` and hands it to py7zr.
- The ``--resume`` path (:mod:`s3_archive.resume` +
  :mod:`s3_archive.extract`) uses :func:`iter_zip_members_seekable` /
  :func:`iter_tar_members_seekable` to walk members by their central-
  directory / tar-header offsets, jumping over the members a prior run
  already wrote.

The two seekable member iterators deliberately mirror the streaming
iterators in :mod:`s3_archive.members`: same :class:`ArchiveMember`
shape, same lazy chunk generators, and the caller wraps them in
:func:`s3_archive.members._apply_safe_keys` so destination keys are
byte-identical to the streaming path.
"""

from __future__ import annotations

import errno
import io
import tarfile
import time
import zipfile
from collections.abc import Iterator

from s3_archive.etag import etags_equal, quote_etag
from s3_archive.exceptions import ETagMismatchError
from s3_archive.log_config import get_logger
from s3_archive.members import ArchiveMember
from s3_archive.paths import normalize_zip_separators

# One canonical retry policy shared with the sequential (tar/zip) path.
# Aliased to the historical private names so :class:`SeekableS3Object`
# (``except _TRANSIENT_ERRORS``, constructor defaults) is untouched by the
# move out of ``seven_z``.
from s3_archive.retry import DEFAULT_RETRY_DELAY_S as _DEFAULT_RETRY_DELAY_S
from s3_archive.retry import DEFAULT_RETRY_MAX_ATTEMPTS as _DEFAULT_RETRY_MAX_ATTEMPTS
from s3_archive.retry import TRANSIENT_ERRORS as _TRANSIENT_ERRORS
from s3_archive.retry import backoff_delay

log = get_logger(__name__)

_CHUNK_SIZE = 65536

# Tail prefetch covers the 7z trailing header in one round trip on archive
# open. 4 MB is generous for typical headers (<1 MB) and bounded enough
# that the wasted bytes don't matter even on tiny archives.
_TAIL_PREFETCH_BYTES = 4 * 1024 * 1024

# Default for io.BufferedReader. py7zr's bootstrap reads the 32-byte
# SignatureHeader field-by-field; 1 MB is plenty for those plus the
# header-of-header bounces (see docs/ARCHITECTURE.md § .7z). The zip /
# tar central-directory parsers issue similarly small reads, so the same
# buffer serves them.
_BUFFER_SIZE = 1024 * 1024


class SeekableS3Object(io.RawIOBase):
    """RawIOBase over an S3 object, served by ranged ``GetObject`` calls.

    Wrap in :class:`io.BufferedReader` before handing to py7zr /
    ``zipfile`` / ``tarfile`` — their header parsers issue many small
    reads, and the buffer coalesces them. Use :func:`open_seekable`
    rather than wrapping by hand: the BufferedReader *default* buffer is
    8 KiB, which turns every read into its own ranged GET and makes a
    sequential body walk ~40-70x slower than the tuned 1 MiB buffer.
    One-time tail prefetch on
    construction keeps the trailing header in memory so the header parse
    doesn't round-trip. No general-purpose cache — body reads are
    sequential per member and don't benefit from one.

    *if_match*, when given (quoted or quote-stripped ETag), pins every
    read to that object generation: the constructor's HEAD is compared
    against it (raising :class:`ETagMismatchError` on mismatch) and
    every ranged GET carries ``If-Match``, so a concurrent overwrite
    surfaces as a hard failure instead of silently mixing bytes from
    two generations across range requests.

    *size*, when given, skips the constructor's HEAD round-trip — pass
    it when the caller already knows the object's byte size (e.g. from
    a LIST entry). With both *size* and *if_match*, the upfront ETag
    comparison is skipped too; the pin still holds because every ranged
    GET carries ``If-Match``, so a stale pin surfaces as a 412 from the
    first read (the tail prefetch) instead of an
    :class:`ETagMismatchError` from HEAD.
    """

    def __init__(
        self,
        client,
        bucket: str,
        key: str,
        *,
        if_match: str | None = None,
        size: int | None = None,
        tail_prefetch_bytes: int = _TAIL_PREFETCH_BYTES,
        retry_delay_s: float = _DEFAULT_RETRY_DELAY_S,
        retry_max_attempts: int = _DEFAULT_RETRY_MAX_ATTEMPTS,
    ) -> None:
        super().__init__()
        self._client = client
        self._bucket = bucket
        self._key = key
        self._if_match = quote_etag(if_match) if if_match is not None else None
        self._retry_delay_s = retry_delay_s
        self._retry_max_attempts = retry_max_attempts
        if size is not None:
            self._size: int = size
        else:
            head = client.head_object(Bucket=bucket, Key=key)
            if self._if_match is not None:
                head_etag = head.get("ETag", "")
                if not etags_equal(head_etag, self._if_match):
                    raise ETagMismatchError(key, self._if_match, head_etag or "<absent>")
            self._size = head["ContentLength"]
        self._pos: int = 0

        prefetch = min(tail_prefetch_bytes, self._size)
        if prefetch > 0:
            start = self._size - prefetch
            self._tail_start: int = start
            self._tail_bytes: bytes = self._ranged_get(start, self._size - 1)
        else:
            # No prefetch — make the "in-memory tail" empty AND start
            # past the end of the object, so :meth:`_fetch` never tries
            # to read from it. (A naive ``_tail_start = 0`` would make
            # _fetch's ``start >= self._tail_start`` branch always
            # short-circuit to empty bytes, silently corrupting reads.)
            self._tail_start = self._size
            self._tail_bytes = b""

    def readinto(self, b) -> int:
        if self._pos >= self._size:
            return 0
        requested = len(b)
        end = min(self._pos + requested, self._size)
        chunk = self._fetch(self._pos, end)
        n = len(chunk)
        b[:n] = chunk
        self._pos += n
        return n

    def _fetch(self, start: int, end: int) -> bytes:
        if start >= self._tail_start:
            offset = start - self._tail_start
            return self._tail_bytes[offset : offset + (end - start)]
        # Crossing into the tail region — stop at tail_start; the next
        # readinto will pick up from the in-memory tail.
        fetch_end = min(end, self._tail_start)
        return self._ranged_get(start, fetch_end - 1)

    def _ranged_get(self, start: int, end_inclusive: int) -> bytes:
        """Issue one Range GET with retry-after-delay on transient failures.

        Read timeouts and connection drops are common on long-running
        per-member walks (~3000 GETs per 3 GB archive on Kopah/RGW).
        Without retry, a single stalled read on byte N kills the entire
        archive walk and wastes the bytes already pulled. We re-issue the
        same Range after an exponential backoff wait
        (:func:`s3_archive.retry.backoff_delay` — 5 s, then 15 s, 45 s,
        capped at 60 s), so a fast-recovering endpoint retries in seconds.

        Up to ``retry_max_attempts`` total tries. Non-transient errors
        (4xx, malformed responses, etc.) propagate immediately — a
        permanent failure shouldn't burn minutes in backoff.
        """
        get_kwargs: dict = {
            "Bucket": self._bucket,
            "Key": self._key,
            "Range": f"bytes={start}-{end_inclusive}",
        }
        if self._if_match is not None:
            get_kwargs["IfMatch"] = self._if_match
        for attempt in range(1, self._retry_max_attempts + 1):
            try:
                resp = self._client.get_object(**get_kwargs)
                return resp["Body"].read()
            except _TRANSIENT_ERRORS as exc:
                if attempt >= self._retry_max_attempts:
                    raise
                delay = backoff_delay(attempt, base=self._retry_delay_s)
                log.warning(
                    "s3://%s/%s: ranged GET [%d-%d] failed (%s, attempt %d/%d); retrying in %.0f s",
                    self._bucket,
                    self._key,
                    start,
                    end_inclusive,
                    type(exc).__name__,
                    attempt,
                    self._retry_max_attempts,
                    delay,
                )
                time.sleep(delay)
        # Unreachable: the final attempt either returns or re-raises.
        raise AssertionError("unreachable")  # pragma: no cover

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            new_pos = offset
        elif whence == 1:
            new_pos = self._pos + offset
        elif whence == 2:
            new_pos = self._size + offset
        else:
            raise ValueError(f"invalid whence: {whence!r}")
        if new_pos < 0:
            # Match a real file object: seeking before byte 0 is an
            # OSError(EINVAL), not a ValueError. zipfile/tarfile probe the
            # tail with a large negative whence=2 seek and *catch OSError*
            # to decide "not a valid archive" — a ValueError would instead
            # leak out past their handler.
            raise OSError(errno.EINVAL, "negative seek position", new_pos)
        self._pos = new_pos
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True


def open_seekable(
    client,
    bucket: str,
    key: str,
    *,
    if_match: str | None = None,
    size: int | None = None,
    buffer_size: int = _BUFFER_SIZE,
) -> io.BufferedReader:
    """The canonical buffered open: :class:`SeekableS3Object` wrapped at 1 MiB.

    Always reach for this (rather than hand-wrapping in
    :class:`io.BufferedReader`) — the BufferedReader *default* buffer is
    8 KiB, which turns a sequential full-body walk into one ranged GET
    per 8 KiB. Measured on a ~100 MB zip that was ~13,000 requests and
    40-70x slower than a plain download; at 1 MiB the same walk is ~100
    requests and within ~2x of a plain download.

    *if_match* pins every read to that object generation and *size*
    skips the constructor HEAD when the caller already knows the byte
    size — see :class:`SeekableS3Object`. *buffer_size* is overridable
    but the default is deliberate: big enough for sequential body
    walks, small enough that the per-seek read-ahead over-fetch stays
    cheap when ``--resume`` jumps between many small members.
    """
    raw = SeekableS3Object(client, bucket, key, if_match=if_match, size=size)
    return io.BufferedReader(raw, buffer_size=buffer_size)


def _zip_member_chunks(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> Iterator[bytes]:
    """Lazily open + stream one zip member, closing it at EOF.

    ``zf.open`` is deferred to the first pull so a member the resume loop
    *skips* (never iterates) opens nothing — no ``ZipExtFile``, not even
    the local-header GET ``zf.open`` would otherwise issue.
    """
    with zf.open(info) as fobj:
        while True:
            chunk = fobj.read(_CHUNK_SIZE)
            if not chunk:
                return
            yield chunk


def _tar_member_chunks(tar: tarfile.TarFile, member: tarfile.TarInfo) -> Iterator[bytes]:
    """Lazily ``extractfile`` + stream one tar member, closing it at EOF.

    Deferred like the zip case so a skipped member does no work. ``member``
    was already filtered to ``isfile()``, so ``extractfile`` is non-None;
    the guard is belt-and-suspenders.
    """
    extracted = tar.extractfile(member)
    if extracted is None:
        return
    with extracted as fobj:
        while True:
            chunk = fobj.read(_CHUNK_SIZE)
            if not chunk:
                return
            yield chunk


def _iter_zip_members(zf: zipfile.ZipFile, *, start_entry: int = 0) -> Iterator[ArchiveMember]:
    """Yield one :class:`ArchiveMember` per non-directory zip entry.

    Reads the central directory (``infolist``) once — each entry carries
    a reliable uncompressed ``file_size`` and a local-header offset, so
    ``zf.open`` seeks straight to the member without touching the ones
    before it. Names go through :func:`normalize_zip_separators` (the
    zip-only Windows-isms fix); :func:`s3_archive.members._apply_safe_keys`
    then applies the format-agnostic S3-key safety pass, exactly as the
    streaming path does, so destination keys match byte-for-byte.

    Entries are walked in local-header **offset order** (= the order a
    forward-only stream visits them; the CD almost always matches but
    isn't required to), and *start_entry* raw entries — counting
    directory entries, exactly like the streaming walk does — are
    skipped first. That's what lets the streaming walk hand off
    mid-archive: "I fully consumed N raw entries" maps to
    ``start_entry=N`` here.

    Directory entries are skipped by the same normalized
    trailing-``/`` test as the streaming path (NOT ``ZipInfo.is_dir()``,
    which misses a Windows-separator dir entry like ``foo\\``), so both
    walks agree entry-for-entry.
    """
    try:
        infos = sorted(zf.infolist(), key=lambda info: info.header_offset)
        for info in infos[start_entry:]:
            name = normalize_zip_separators(info.filename)
            if name.endswith("/"):
                continue
            yield ArchiveMember(
                name=name,
                size=info.file_size,
                _chunks=_zip_member_chunks(zf, info),
            )
    finally:
        # Closing a ZipFile built from a passed-in file object does NOT
        # close that object (zipfile only closes files it opened by name),
        # so the caller still owns the BufferedReader / SeekableS3Object.
        zf.close()


def iter_zip_members_seekable(fileobj, *, start_entry: int = 0) -> Iterator[ArchiveMember]:
    """Open *fileobj* as a zip and iterate its members via the central directory.

    *fileobj* must be seekable (e.g. an :class:`io.BufferedReader` over a
    :class:`SeekableS3Object`). Raises :class:`zipfile.BadZipFile`
    immediately if *fileobj* has no usable central directory — the caller
    (``extract`` under ``--resume``) turns that into a
    :class:`s3_archive.exceptions.ResumeUnsupportedError`.

    *start_entry* skips that many raw entries (offset-ordered, counting
    directory entries) — the continuation hook for the streaming walk's
    mid-archive fallback; see
    :func:`s3_archive.members.iter_archive_members`.
    """
    zf = zipfile.ZipFile(fileobj)  # BadZipFile here if no central directory
    return _iter_zip_members(zf, start_entry=start_entry)


def _iter_tar_members(tar: tarfile.TarFile) -> Iterator[ArchiveMember]:
    """Yield one :class:`ArchiveMember` per regular-file tar member.

    Opened in random-access mode (``mode="r"``, not ``"r|"``): the tar
    is scanned by seeking past each member's data to the next 512-byte-
    aligned header, so no member bytes are read until ``extractfile`` is
    called. That means a member a prior run already wrote can be skipped
    with zero payload transfer, and advancing to the next member never
    needs the current one's bytes drained (unlike the streaming path).
    """
    try:
        for member in tar:
            if not member.isfile():
                continue
            yield ArchiveMember(
                name=member.name,
                size=member.size,
                _chunks=_tar_member_chunks(tar, member),
            )
    finally:
        tar.close()


def iter_tar_members_seekable(fileobj) -> Iterator[ArchiveMember]:
    """Open *fileobj* as an uncompressed tar and iterate its members by header.

    *fileobj* must be seekable. Raises :class:`tarfile.TarError`
    immediately if *fileobj* isn't a readable tar — the caller turns that
    into a :class:`s3_archive.exceptions.ResumeUnsupportedError`.
    """
    # Not a `with`: the returned generator owns the TarFile lifecycle and
    # closes it in its own finally (mirrors the zip path) — a context
    # manager here would close it before the caller reads a single member.
    tar = tarfile.open(fileobj=fileobj, mode="r")  # noqa: SIM115  (TarError if unreadable)
    return _iter_tar_members(tar)
