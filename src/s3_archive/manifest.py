"""In-archive manifest primitives: tar-family / zip per-entry hashing and zip CD readers.

Consumed by storage-scripts' ``inventory.archive_walker`` (for the
single-pass inline walk) and available for any caller that wants
per-entry triple-hashes from a streaming archive read.

These primitives are pure transducers over a byte source — they don't
talk to S3 themselves. Callers supply the byte source (an S3 GET
body, a local file handle, or a tap that mirrors bytes into a parallel
hasher). Peak memory per entry is ~one chunk regardless of member size.
"""

import logging
import struct
import tarfile
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import zstandard
from stream_unzip import stream_unzip

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.hashing import triple_hash
from s3_archive.paths import decode_zip_filename, normalize_zip_separators, safe_member_key

log = logging.getLogger("s3_archive.manifest")

# 64 KB chunks for streaming reads
_CHUNK_SIZE = 65536


@dataclass
class ManifestEntry:
    key: str
    size: int
    md5: str
    sha256: str
    sha1: str = ""
    mtime: str = ""  # ISO 8601 UTC; "" if archive metadata didn't carry one


def _unix_to_iso(t: int | float) -> str:
    """Convert a Unix timestamp (tar mtime) to ISO 8601 UTC."""
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dos_to_iso(mod_date: int, mod_time: int) -> str:
    """Convert a DOS date+time pair (zip central directory) to ISO 8601 UTC.

    DOS timestamps are 2-second granular and carry no timezone. We record
    them as UTC (best effort — some archivers write local time without
    timezone awareness, but we have no way to distinguish). Returns ""
    if the date is invalid (some archivers write zeros for mtime).
    """
    year = ((mod_date >> 9) & 0x7F) + 1980
    month = (mod_date >> 5) & 0x0F
    day = mod_date & 0x1F
    hour = (mod_time >> 11) & 0x1F
    minute = (mod_time >> 5) & 0x3F
    second = (mod_time & 0x1F) * 2
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except ValueError:
        return ""


# Zip central-directory parsing constants (PKWARE APPNOTE 4.3.12 / 4.3.16)
_ZIP_CD_SIG = b"PK\x01\x02"
_ZIP_EOCD_SIG = b"PK\x05\x06"
_ZIP_CD_HEADER_LEN = 46
_ZIP_EOCD_LEN = 22
_ZIP_MAX_COMMENT_LEN = 65535


# Thin alias kept for existing references/tests. The canonical decoder
# (UTF-8→CP437 plus zip separator normalization) lives in
# :func:`s3_archive.paths.decode_zip_filename`; routing both the CD and
# LFH paths through it guarantees their keys move together.
_decode_zip_filename = decode_zip_filename


def zip_central_directory_from_s3(
    client, bucket: str, key: str, size: int | None = None
) -> dict[str, str]:
    """Fetch a zip file's central directory from S3; return {filename: mtime_iso}.

    Uses a single Range GET on the trailing bytes (CD + EOCD live at the
    end). When *size* is provided, the redundant HEAD round-trip is
    skipped — pass it when you've already fetched the LIST entry. The
    fallback HEAD path remains for callers without a pre-fetched size;
    it tolerates RadosGW HEAD responses that occasionally omit
    ``ContentLength``.

    Returns an empty dict on any failure — Zip64 with unusual layouts,
    truncated archives, S3 errors — and callers fall back to empty
    mtime per entry. The walk itself doesn't depend on this; we just
    lose mtime for that archive's rows.
    """
    try:
        if size is None:
            head = client.head_object(Bucket=bucket, Key=key)
            size = head.get("ContentLength")
            if size is None:
                log.warning(
                    "HEAD for s3://%s/%s returned no ContentLength; skipping CD pre-fetch",
                    bucket,
                    key,
                )
                return {}
        total = size
        # The EOCD record sits at the end of the file plus up to 65535 bytes
        # of comment, then the CD comes before it. Pull a generous tail.
        fetch_len = min(total, _ZIP_EOCD_LEN + _ZIP_MAX_COMMENT_LEN + 256 * 1024)
        start = total - fetch_len
        resp = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{total - 1}")
        tail = resp["Body"].read()

        eocd_pos = tail.rfind(_ZIP_EOCD_SIG)
        if eocd_pos < 0 or eocd_pos + _ZIP_EOCD_LEN > len(tail):
            return {}
        (
            _sig,
            _disk_num,
            _cd_start_disk,
            _num_cd_on_disk,
            _num_cd_total,
            _cd_size,
            cd_offset,
            _comment_len,
        ) = struct.unpack("<IHHHHIIH", tail[eocd_pos : eocd_pos + _ZIP_EOCD_LEN])

        cd_offset_in_tail = cd_offset - start
        if cd_offset_in_tail < 0:
            # Central directory didn't fit in our tail fetch.
            return {}

        mtimes: dict[str, str] = {}
        pos = cd_offset_in_tail
        while pos + _ZIP_CD_HEADER_LEN <= len(tail):
            if tail[pos : pos + 4] != _ZIP_CD_SIG:
                break
            (
                _sig,
                _ver_made,
                _ver_needed,
                _flags,
                _compression,
                mod_time,
                mod_date,
                _crc32,
                _comp_size,
                _uncomp_size,
                fname_len,
                extra_len,
                comment_len,
                _disk_num,
                _internal_attrs,
                _external_attrs,
                _local_offset,
            ) = struct.unpack("<IHHHHHHIIIHHHHHII", tail[pos : pos + _ZIP_CD_HEADER_LEN])

            fname_bytes = tail[pos + _ZIP_CD_HEADER_LEN : pos + _ZIP_CD_HEADER_LEN + fname_len]
            fname = _decode_zip_filename(fname_bytes)

            iso = _dos_to_iso(mod_date, mod_time)
            if iso:
                mtimes[fname] = iso

            pos += _ZIP_CD_HEADER_LEN + fname_len + extra_len + comment_len

        return mtimes
    except Exception as exc:  # noqa: BLE001 — best-effort; fall back to empty
        log.warning("zip central directory parse failed for %s: %s", key, exc)
        return {}


def zip_central_directory_from_path(path: Path) -> dict[str, str]:
    """Read a local zip file's central directory; return {filename: mtime_iso}.

    Stdlib ``zipfile`` handles the heavy lifting on a seekable file —
    no Range-GET dance. Returns "" mtime for entries whose
    ``date_time`` tuple is invalid (some archivers write zeros).
    """
    mtimes: dict[str, str] = {}
    try:
        with zipfile.ZipFile(path) as zf:
            for zi in zf.infolist():
                iso = _zipinfo_mtime_iso(zi)
                if iso:
                    # Normalize separators so these keys match the LFH-decoded
                    # names (:func:`decode_zip_filename`) the manifest builder
                    # looks up — stdlib ``zipfile`` leaves Windows ``\`` intact.
                    mtimes[normalize_zip_separators(zi.filename)] = iso
    except Exception as exc:  # noqa: BLE001 — best-effort; fall back to empty
        log.warning("zip central directory parse failed for %s: %s", path, exc)
        return {}
    return mtimes


def _hash_stream(chunks) -> tuple[int, str, str, str]:
    """Consume an iterable of byte chunks; return (size, md5, sha1, sha256).

    Thin tuple-shaped adapter over :func:`s3_archive.hashing.triple_hash`
    (the central streaming triple-hash). Peak memory is one chunk —
    does NOT buffer entry contents. Critical for tar/zip entries that
    may be many GB.
    """
    result = triple_hash(chunks)
    return result.size, result.md5, result.sha1, result.sha256


def _iter_tar_member(fileobj) -> "Iterator[bytes]":
    """Yield bytes chunks from a tarfile member fileobj."""
    while True:
        chunk = fileobj.read(_CHUNK_SIZE)
        if not chunk:
            break
        yield chunk


def _zipinfo_mtime_iso(zi: zipfile.ZipInfo) -> str:
    """Convert a ZipInfo's date_time tuple to ISO 8601 UTC; "" if invalid.

    ``ZipInfo.date_time`` is ``(year, month, day, hour, minute, second)``.
    Many archivers write zeros there for entries with no mtime; treat
    those as "no mtime" rather than 1980-01-00 (an invalid date).
    """
    year, month, day, hour, minute, second = zi.date_time
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except ValueError:
        return ""


_TAR_MODE_BY_FORMAT = {
    "tar": "r|",
    "tar.gz": "r|gz",
    "tar.bz2": "r|bz2",
    "tar.xz": "r|xz",
}


def _open_tar_stream(fileobj, archive_format: str):
    """Open a streaming :class:`tarfile.TarFile` for *fileobj*.

    *archive_format* is one of ``"tar"``, ``"tar.gz"``, ``"tar.bz2"``,
    ``"tar.xz"``, or ``"tar.zst"``. For ``"tar.zst"`` we wrap *fileobj*
    in a :class:`zstandard.ZstdDecompressor` stream reader before
    handing it to ``tarfile``; the rest are tarfile-native modes.
    """
    if archive_format == "tar.zst":
        decompressor = zstandard.ZstdDecompressor()
        return tarfile.open(fileobj=decompressor.stream_reader(fileobj), mode="r|")

    mode = _TAR_MODE_BY_FORMAT.get(archive_format)
    if mode is None:
        raise ValueError(f"not a tar-family format: {archive_format!r}")
    return tarfile.open(fileobj=fileobj, mode=mode)


def build_manifest_tar_fileobj(fileobj, archive_format: str) -> list[ManifestEntry]:
    """Stream a tar archive from any read(n)-compatible fileobj.

    *archive_format* selects the decompression layer: ``"tar"``
    (uncompressed), ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``, or
    ``"tar.zst"``.

    Caller supplies the byte source — an S3 GET body, a local file
    handle, or a tap that mirrors bytes into a parallel hasher. Peak
    memory per entry is ~one chunk regardless of member size. Entry
    mtime comes from the tar header.
    """
    entries: list[ManifestEntry] = []
    with _open_tar_stream(fileobj, archive_format) as tar:
        for member in tar:
            if not member.isfile():
                continue
            member_fileobj = tar.extractfile(member)
            if member_fileobj is None:
                continue

            size, md5, sha1, sha256 = _hash_stream(_iter_tar_member(member_fileobj))
            entries.append(
                ManifestEntry(
                    key=safe_member_key(member.name),
                    size=size,
                    md5=md5,
                    sha1=sha1,
                    sha256=sha256,
                    mtime=_unix_to_iso(member.mtime),
                )
            )
    return entries


def build_manifest_zip_chunks(
    chunks_iter: Iterator[bytes], cd_mtimes: dict[str, str]
) -> list[ManifestEntry]:
    """Stream a zip from a chunks iterator + a pre-fetched mtime map.

    ``stream_unzip`` doesn't surface mtime in its yield, so the caller
    is responsible for supplying the central-directory mtime lookup
    (empty dict is fine — all entries then get "" mtime). Peak memory
    per entry is ~one chunk.
    """
    entries: list[ManifestEntry] = []
    for name, _size, chunks in stream_unzip(chunks_iter):
        file_name = decode_zip_filename(name)
        if file_name.endswith("/"):
            # Directory entry — consume and skip
            for _ in chunks:
                pass
            continue

        size, md5, sha1, sha256 = _hash_stream(chunks)
        entries.append(
            ManifestEntry(
                # mtime is keyed by the decoded (separator-normalized) name to
                # match the CD reader; the stored key gets the extra safety pass.
                key=safe_member_key(file_name),
                size=size,
                md5=md5,
                sha1=sha1,
                sha256=sha256,
                mtime=cd_mtimes.get(file_name, ""),
            )
        )
    return entries


TAR_FAMILY_FORMATS: tuple[str, ...] = ("tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst")
"""Formats decoded via the :mod:`tarfile` family (with optional decompression)."""

TAP_SUPPORTED_FORMATS: tuple[str, ...] = TAR_FAMILY_FORMATS + ("zip",)
"""Formats :func:`build_manifest_from_tap` can walk from a forward-only byte tap.

``"7z"`` is intentionally absent — its decoder requires seeking to a
trailing header, incompatible with a forward-only stream. Callers
needing per-entry hashes from a ``.7z`` should drive the seekable
:func:`s3_archive.members.iter_archive_members` path instead.
"""

# 1 MiB outer-stream chunks for tap reads. Larger than _CHUNK_SIZE (the
# per-member read size) because the tap drives a single byte pipe and
# benefits from amortising per-read overhead on multi-GB archives.
_TAP_READ_SIZE = 1024 * 1024


def build_manifest_from_tap(
    fmt: str,
    tap,
    cd_mtimes: dict[str, str] | None = None,
) -> list[ManifestEntry]:
    """Dispatch to the tar-family or zip manifest builder by format.

    *tap* is any object exposing ``read(n) -> bytes`` over the archive's
    bytes — typically a :class:`s3_archive.hashing.HashingTap` so the
    caller can also recover the parent's triple-hash from the same pass.

    For ``"zip"``, *cd_mtimes* must be pre-fetched via
    :func:`zip_central_directory_from_s3` or
    :func:`zip_central_directory_from_path` (``{}`` is accepted; all
    entries then get ``""`` mtime). Tar formats ignore *cd_mtimes*.

    Raises :class:`UnsupportedArchiveFormatError` for formats not in
    :data:`TAP_SUPPORTED_FORMATS` (notably ``"7z"``).
    """
    if fmt in TAR_FAMILY_FORMATS:
        return build_manifest_tar_fileobj(tap, fmt)
    if fmt == "zip":
        mtimes = {} if cd_mtimes is None else cd_mtimes

        def _chunks() -> Iterator[bytes]:
            while True:
                chunk = tap.read(_TAP_READ_SIZE)
                if not chunk:
                    break
                yield chunk

        return build_manifest_zip_chunks(_chunks(), mtimes)
    raise UnsupportedArchiveFormatError(
        f"Format {fmt!r} cannot be walked from a forward-only byte tap "
        f"(supported: {', '.join(TAP_SUPPORTED_FORMATS)}). "
        f"For .7z and other seek-only formats use iter_archive_members instead."
    )
