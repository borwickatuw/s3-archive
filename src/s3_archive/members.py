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

import tarfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

import zstandard
from stream_unzip import stream_unzip

from s3_archive.exceptions import UnsupportedArchiveFormatError

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

    *name* is the member's stored path (``member.name`` for tar,
    decoded UTF-8/CP437 for zip — see
    :func:`s3_archive.manifest._decode_zip_filename`). *size* is the
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


def _iter_zip_members(chunks_iter: Iterable[bytes]) -> Iterator[ArchiveMember]:
    """Yield :class:`ArchiveMember` for each non-directory zip entry.

    Auto-drains the previous entry before advancing — ``stream_unzip``
    raises ``UnfinishedIterationError`` if its consumer doesn't drain
    each entry's chunk iterator before pulling the next.
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
        if isinstance(name, bytes):
            try:
                file_name = name.decode("utf-8")
            except UnicodeDecodeError:
                file_name = name.decode("cp437")
        else:
            file_name = name
        if file_name.endswith("/"):
            for _ in chunks:
                pass
            continue
        size = _size if isinstance(_size, int) else 0
        member = ArchiveMember(name=file_name, size=size, _chunks=iter(chunks))
        previous = member
        yield member


def iter_archive_members(
    client,
    bucket: str,
    key: str,
    fmt: str,
) -> Iterator[ArchiveMember]:
    """GET ``s3://bucket/key`` and yield one :class:`ArchiveMember` per file entry.

    *fmt* is one of the strings returned by :func:`s3_archive.url.detect_format`:
    ``"tar"``, ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``, ``"tar.zst"``,
    ``"zip"``, or ``"7z"``.

    The body is streamed — no full-archive download. Members are
    yielded in archive order; the caller drives consumption per-member
    via the :class:`ArchiveMember` API. Forgotten members are
    auto-drained when the next one is requested.

    ``"7z"`` cannot be decoded forward-only and uses a seekable-S3
    adapter instead of the streaming body GET — see
    :mod:`s3_archive.seven_z`.
    """
    if fmt not in _TAR_MODES and fmt != "tar.zst" and fmt != "zip" and fmt != "7z":
        raise UnsupportedArchiveFormatError(f"Unsupported format: {fmt!r}")

    if fmt == "7z":
        # Lazy import: seven_z depends on this module (ArchiveMember), so
        # eager-importing it at the top of this file would be circular.
        from s3_archive.seven_z import iter_seven_z_members  # noqa: PLC0415

        yield from iter_seven_z_members(client, bucket, key)
        return

    resp = client.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]

    if fmt in _TAR_MODES or fmt == "tar.zst":
        yield from _iter_tar_members(body, fmt)
        return

    def _archive_chunks() -> Iterator[bytes]:
        while True:
            chunk = body.read(_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    yield from _iter_zip_members(_archive_chunks())
