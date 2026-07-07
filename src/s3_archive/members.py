"""Streaming archive-member iteration across tar family + zip.

``iter_archive_members`` GETs an archive object from S3 and yields one
:class:`ArchiveMember` per file entry, in archive order, without
staging any member bytes on disk. Each member exposes a chunk
iterator the caller drives (``read_all``, ``drain``, or a custom
read loop).

Two consumption patterns are supported:

- **Capture this, drain that.** ``verify-against``-style use: collect
  bodies for the small subset of tag files in memory, drain
  multi-GB payload members without buffering them.
- **Drain every member.** Anywhere a single-pass walk needs to advance
  past each entry — e.g. tarfile-with-stream tap that wants per-entry
  hashes alongside the parent hash. The :mod:`s3_archive.manifest`
  builders handle this case directly; ``iter_archive_members`` is the
  more flexible primitive for callers that don't need the
  ``ManifestEntry`` shape.

Auto-drain on next-yield: non-seekable tar / zip streams require
in-order member consumption — advancing to the next member requires
the previous member's bytes to be consumed first. If the caller
forgets to consume a member, the iterator drains it on the caller's
behalf when the next member is requested. This is safer than
"document loudly that the caller must drain" — a forgotten drain
corrupts the next member's bytes.
"""

import contextlib
import tarfile
import zipfile
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field

import zstandard
from stream_unzip import NotStreamUnzippable, UnzipError, stream_unzip

from s3_archive.exceptions import ArchiveReadError, UnsupportedArchiveFormatError
from s3_archive.iter import IterableFileobj
from s3_archive.log_config import get_logger
from s3_archive.paths import decode_zip_filename, safe_member_key
from s3_archive.retry import (
    DEFAULT_RETRY_DELAY_S,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    resumable_body_chunks,
)

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


@dataclass
class ArchiveMember:
    """One file entry inside an archive.

    *name* is the member's stored path after normalization — separators
    forward-slashed and Windows drive prefixes stripped for zip
    (:func:`s3_archive.paths.decode_zip_filename`), leading ``/`` and
    ``..`` traversal segments handled for every format
    (:func:`s3_archive.paths.safe_member_key`). *size* is the
    archive's declared size; for streaming zips this is the size from
    the local file header and may be 0 for ``stream_unzip`` entries
    that don't carry it. The caller should hash the bytes if it needs
    the actual size.

    Pull chunks via :meth:`chunks`, :meth:`read_all`, or :meth:`drain`.
    Each consumes the underlying iterator at most once; a second call
    yields no bytes (and is a no-op for :meth:`drain`).
    """

    name: str
    size: int
    _chunks: Iterator[bytes] = field(repr=False)
    _consumed: bool = field(default=False, repr=False)

    def chunks(self) -> Iterator[bytes]:
        """Yield this member's bytes as chunks.

        Idempotent in that calling :meth:`read_all` / :meth:`drain` /
        a second :meth:`chunks` after exhaustion produces no bytes.
        """
        if self._consumed:
            return
        self._consumed = True
        yield from self._chunks

    def read_all(self) -> bytes:
        """Concatenate all of this member's bytes and return them.

        Use only for files that fit comfortably in memory — e.g. BagIt
        tag files. Payload-shaped members should use :meth:`drain` or
        a custom :meth:`chunks` loop.
        """
        return b"".join(self.chunks())

    def drain(self) -> None:
        """Consume any remaining bytes without buffering them."""
        for _ in self.chunks():
            pass


def _iter_tar_chunks(fobj) -> Iterator[bytes]:
    while True:
        chunk = fobj.read(_CHUNK_SIZE)
        if not chunk:
            break
        yield chunk


def _open_tar_stream(fileobj, archive_format: str):
    """Open a streaming :class:`tarfile.TarFile` for *fileobj*.

    Mirrors :func:`s3_archive.manifest._open_tar_stream`: handles the
    tar family natively (``r|`` etc.) plus ``tar.zst`` via the
    ``zstandard`` decompressor wrapper.
    """
    if archive_format == "tar.zst":
        decompressor = zstandard.ZstdDecompressor()
        return tarfile.open(fileobj=decompressor.stream_reader(fileobj), mode="r|")

    mode = _TAR_MODES.get(archive_format)
    if mode is None:
        raise UnsupportedArchiveFormatError(f"not a tar-family format: {archive_format!r}")
    return tarfile.open(fileobj=fileobj, mode=mode)


def _iter_tar_members(fileobj, archive_format: str) -> Iterator[ArchiveMember]:
    """Yield :class:`ArchiveMember` for each regular-file tar member.

    Auto-drains a member when the caller advances without consuming
    it. Tarfile in streaming mode (``r|...``) demands the previous
    member's bytes be consumed before the iterator advances — otherwise
    advancing tries to seek backwards through a non-seekable source —
    so the drain MUST happen before ``next()`` on the tarfile iterator.
    """
    with _open_tar_stream(fileobj, archive_format) as tar:
        tar_iter = iter(tar)
        previous: ArchiveMember | None = None
        while True:
            if previous is not None:
                previous.drain()
                previous = None
            try:
                tar_member = next(tar_iter)
            except StopIteration:
                return
            if not tar_member.isfile():
                continue
            member_fileobj = tar.extractfile(tar_member)
            if member_fileobj is None:
                continue
            member = ArchiveMember(
                name=tar_member.name,
                size=tar_member.size,
                _chunks=_iter_tar_chunks(member_fileobj),
            )
            previous = member
            yield member


def _iter_zip_members(
    chunks_iter: Iterable[bytes], *, entry_counter: list[int] | None = None
) -> Iterator[ArchiveMember]:
    """Yield :class:`ArchiveMember` for each non-directory zip entry.

    Auto-drains the previous entry before advancing — ``stream_unzip``
    raises ``UnfinishedIterationError`` if its consumer doesn't drain
    each entry's chunk iterator before pulling the next.

    *entry_counter*, when given, is a one-element list whose slot is
    kept equal to the number of raw entries (directory entries
    included) whose header parse succeeded. Because advancing drains
    the previous entry first, at the moment an advance *raises* the
    counter equals the number of fully-consumed entries — which is the
    offset-ordered index of the offending entry, i.e. exactly the
    ``start_entry`` the seekable continuation should resume from.
    """
    zip_iter = stream_unzip(chunks_iter)
    previous: ArchiveMember | None = None
    while True:
        if previous is not None:
            previous.drain()
            previous = None
        try:
            name, _size, chunks = next(zip_iter)
        except StopIteration:
            return
        if entry_counter is not None:
            entry_counter[0] += 1
        file_name = decode_zip_filename(name)
        if file_name.endswith("/"):
            for _ in chunks:
                pass
            continue
        size = _size if isinstance(_size, int) else 0
        member = ArchiveMember(name=file_name, size=size, _chunks=iter(chunks))
        previous = member
        yield member


def _iter_zip_members_with_fallback(
    client, bucket: str, key: str, chunks
) -> Iterator[ArchiveMember]:
    """Stream-walk a zip; on ``NotStreamUnzippable``, continue via ranged GETs.

    Some valid zips can't be walked forward-only: on-the-fly generators
    (SwissTransfer, Google Drive) write stored members with data
    descriptors and no sizes in the local file headers (see
    :class:`s3_archive.exceptions.ZipNotStreamableError`). Streaming is
    still attempted first — it's a single sequential GET, optimal for
    every normal zip — and on the raise this hands off to the seekable
    central-directory walk *from the exact failure point*: the streaming
    walk visits entries in local-header offset order, so "N raw entries
    fully consumed" resumes at offset-ordered CD index N. Members
    already yielded are never re-yielded; total transfer stays ~one
    archive read even when the raise happens deep into a mixed zip.

    The continuation is still streaming in the sense that matters —
    bounded memory, ranged GETs, nothing on local disk.
    """
    entry_counter = [0]
    try:
        yield from _iter_zip_members(chunks, entry_counter=entry_counter)
        return
    except NotStreamUnzippable as exc:
        offender = decode_zip_filename(exc.args[0])
        log.info(
            "zip member %r is stored with a data descriptor and no size in its "
            "local header (not forward-streamable; the zip is not corrupt) — "
            "continuing from entry %d via the central directory over ranged GETs",
            offender,
            entry_counter[0],
        )
    # Reached only via the except: release the abandoned sequential GET,
    # then hand off. (Kept outside the handler so a failure in the
    # continuation isn't misreported as caused by the expected trigger.)
    with contextlib.suppress(Exception):
        chunks.close()
    # Lazy import: seekable depends on this module (ArchiveMember).
    from s3_archive.seekable import iter_zip_members_seekable, open_seekable  # noqa: PLC0415

    fileobj = open_seekable(client, bucket, key)
    try:
        yield from iter_zip_members_seekable(fileobj, start_entry=entry_counter[0])
    finally:
        fileobj.close()


def _apply_safe_keys(
    members: Iterator[ArchiveMember], *, fix_unsafe_paths: bool
) -> Iterator[ArchiveMember]:
    """Rewrite each member's ``name`` into a safe relative S3 key as it's yielded.

    The single chokepoint that covers tar, zip, and 7z — and every
    external caller (s3-bagit, storage-scripts). Raises
    :class:`s3_archive.exceptions.UnsafeArchiveMemberError` when a member
    has a ``..`` segment and *fix_unsafe_paths* is false; because the
    walk is single-pass, that's raised when the member is reached, after
    earlier members have already been yielded.
    """
    for member in members:
        member.name = safe_member_key(member.name, fix_unsafe=fix_unsafe_paths)
        yield member


def iter_archive_members(
    client,
    bucket: str,
    key: str,
    fmt: str,
    *,
    fix_unsafe_paths: bool = False,
    retry_delay_s: float = DEFAULT_RETRY_DELAY_S,
    retry_max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS,
    on_bytes: Callable[[int], None] | None = None,
) -> Iterator[ArchiveMember]:
    """GET ``s3://bucket/key`` and yield one :class:`ArchiveMember` per file entry.

    *fmt* is one of the strings returned by :func:`s3_archive.url.detect_format`:
    ``"tar"``, ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``, ``"tar.zst"``,
    ``"zip"``, or ``"7z"``.

    The body is streamed — no full-archive download. Members are
    yielded in archive order; the caller drives consumption per-member
    via the :class:`ArchiveMember` API. Forgotten members are
    auto-drained when the next one is requested.

    Every yielded member's ``name`` is normalized into a safe relative
    S3 key (see :func:`s3_archive.paths.safe_member_key`). A ``..``
    traversal segment raises
    :class:`s3_archive.exceptions.UnsafeArchiveMemberError` by default;
    pass *fix_unsafe_paths=True* to safely collapse it instead.

    For the sequential tar/zip path, the underlying byte stream is
    resumable: a transient connection drop is retried via a ranged GET
    from the byte offset already consumed (up to ``retry_max_attempts``
    *consecutive* failures, with exponential backoff between tries) —
    see :func:`s3_archive.retry.resumable_body_chunks`. *on_bytes*, if
    supplied, is called with the length of each archive chunk read from
    S3 (compressed bytes of forward progress), suitable for a
    ``tqdm.update`` bar sized against the archive's ``ContentLength``.
    ``"7z"`` cannot be decoded forward-only and uses a seekable-S3
    adapter with its own equivalent ranged-GET retry (and does not report
    *on_bytes*) — see :mod:`s3_archive.seven_z`.

    Zips that a forward-only reader can't walk (stored members with
    data descriptors and no local-header sizes — SwissTransfer /
    Drive-style generators) are handled transparently: the walk starts
    streaming and, at the first unstreamable member, continues from
    that exact entry via a ranged-GET central-directory walk (see
    :func:`_iter_zip_members_with_fallback`). No member is re-yielded
    and total transfer stays ~one archive read; like the 7z path, the
    continuation does not report *on_bytes*.
    """
    if fmt not in _TAR_MODES and fmt != "tar.zst" and fmt != "zip" and fmt != "7z":
        raise UnsupportedArchiveFormatError(f"Unsupported format: {fmt!r}")

    if fmt == "7z":
        # Lazy import: seven_z depends on this module (ArchiveMember), so
        # eager-importing it at the top of this file would be circular.
        from s3_archive.seven_z import iter_seven_z_members  # noqa: PLC0415

        yield from _apply_safe_keys(
            iter_seven_z_members(client, bucket, key), fix_unsafe_paths=fix_unsafe_paths
        )
        return

    chunks = resumable_body_chunks(
        client,
        bucket,
        key,
        retry_delay_s=retry_delay_s,
        retry_max_attempts=retry_max_attempts,
        on_bytes=on_bytes,
    )

    # Wrap the decoder-native exceptions in ArchiveReadError so every
    # caller of iter_archive_members sees one exception type for "the
    # archive bytes are bad," regardless of which decoder noticed.
    try:
        if fmt in _TAR_MODES or fmt == "tar.zst":
            # IterableFileobj re-exposes the chunk stream as a sized-read()
            # file object for tarfile.open(fileobj=…) (and the tar.zst
            # zstandard stream_reader wrapper).
            yield from _apply_safe_keys(
                _iter_tar_members(IterableFileobj(chunks), fmt),
                fix_unsafe_paths=fix_unsafe_paths,
            )
            return
        yield from _apply_safe_keys(
            _iter_zip_members_with_fallback(client, bucket, key, chunks),
            fix_unsafe_paths=fix_unsafe_paths,
        )
    except tarfile.TarError as exc:
        raise ArchiveReadError(f"tar decode failed: {exc}", cause=exc) from exc
    except UnzipError as exc:
        raise ArchiveReadError(f"zip decode failed: {exc}", cause=exc) from exc
    except zipfile.BadZipFile as exc:
        # The seekable continuation opens the central directory with
        # stdlib zipfile; a zip that both fails to stream AND has no
        # usable CD is genuinely unreadable.
        raise ArchiveReadError(f"zip central directory read failed: {exc}", cause=exc) from exc
