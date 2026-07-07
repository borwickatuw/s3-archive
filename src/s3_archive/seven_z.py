"""Streaming .7z read support.

``.7z`` cannot be decoded forward-only — the StartHeader at the front
points at metadata at the tail, and the body decoder pipeline lives in
that tail header. So unlike tar/zip, this module needs a *seekable*
view of the archive. :class:`SeekableS3Object` provides one over
``client.get_object(Range=...)``, with a single tail prefetch so the
header parse doesn't burn dozens of small range GETs.

`.7z` create is intentionally not supported: the SignatureHeader
references a NextHeaderOffset/Size/CRC only known after the body and
trailing header are written, and S3 multipart's 5 MB minimum part size
makes "patch the first 32 bytes at the end" impractical. See
``docs/ARCHITECTURE.md`` § ".7z — the exception that proves the rule".

The iterator bridges py7zr's push-style ``WriterFactory`` API onto the
project's pull-style :class:`ArchiveMember` contract by spawning a
worker thread that drives ``SevenZipFile.extractall`` and writes each
member's bytes into a per-member ``os.pipe()``. The main generator
pulls metadata off a queue and yields one ``ArchiveMember`` per pipe.
"""

from __future__ import annotations

import contextlib
import lzma
import os
import queue
import struct
import threading
from collections.abc import Collection, Iterator
from dataclasses import dataclass

import py7zr
import py7zr.compressor
from py7zr.exceptions import UnsupportedCompressionMethodError
from py7zr.io import Py7zIO, WriterFactory

from s3_archive.exceptions import ArchiveReadError
from s3_archive.log_config import get_logger
from s3_archive.members import ArchiveMember
from s3_archive.native_decoders import build_native_decoder

# SeekableS3Object (the ranged-GET file-object adapter) now lives in its
# own module so the zip/tar ``--resume`` path can reuse it without
# dragging py7zr — and this module's load-time monkeypatch — into an
# import that only wants zip/tar. ``open_seekable`` is its canonical
# buffered constructor (tuned BufferedReader size).
from s3_archive.seekable import open_seekable

log = get_logger(__name__)


# Sentinel attribute on the patched method so re-imports don't double-wrap.
_PATCH_MARKER = "_s3_archive_native_decoder_patched"


def _install_native_decoder_fallback() -> None:
    """Wrap py7zr's ``_get_lzma_decompressor`` with a native-decoder fallback.

    py7zr translates 7z coder chains into a stdlib ``lzma.LZMADecompressor``
    in ``FORMAT_RAW`` mode, which requires the chain to end with an
    LZMA1/LZMA2 filter. Real-world archives produced by 7-Zip with
    ``-mx=0 -mf=Delta:2`` violate that — Copy + Delta has no terminal
    compression filter — so the call raises ``_lzma.LZMAError`` before
    any byte is decoded. The patch tries py7zr's path first (so the
    common LZMA-terminated case is unchanged), and on either an
    ``LZMAError`` or an ``UnsupportedCompressionMethodError`` it asks
    :func:`s3_archive.native_decoders.build_native_decoder` to construct
    a replacement chain. If we can't build one either, the original
    exception is re-raised so the operator sees the underlying reason.

    Applied module-level: importing :mod:`s3_archive.seven_z` patches
    py7zr globally for this process. The patch is idempotent (a marker
    attribute prevents double-wrapping on re-import).
    """
    cls = py7zr.compressor.SevenZipDecompressor
    orig = cls._get_lzma_decompressor
    if getattr(orig, _PATCH_MARKER, False):
        return

    def _get_lzma_decompressor_with_fallback(self, coders, unpacksize):  # noqa: ANN001
        try:
            return orig(self, coders, unpacksize)
        except (lzma.LZMAError, UnsupportedCompressionMethodError):
            chain = build_native_decoder(coders)
            if chain is None:
                # We can't help — let py7zr's original exception propagate
                # so the operator sees stdlib lzma's actual complaint.
                raise
            log.debug(
                "Using s3_archive native decoder for coder chain %r "
                "(py7zr/stdlib-lzma rejected it)",
                [c.get("method") for c in coders],
            )
            return chain

    setattr(_get_lzma_decompressor_with_fallback, _PATCH_MARKER, True)
    cls._get_lzma_decompressor = _get_lzma_decompressor_with_fallback


_install_native_decoder_fallback()

_CHUNK_SIZE = 65536


class _PipeSink(Py7zIO):
    """Py7zIO that forwards writes to an ``os.pipe()`` write-end.

    py7zr's contract here is narrow:

    - ``write(chunk)`` is called for each decoded chunk of one member.
    - ``seek(0, 0)`` is called once after writing finishes (py7zr
      computes CRC inline during ``decompress``; the seek isn't a real
      seek, it's just leftover from the BytesIO-style interface). We
      no-op it — the pipe consumer has already moved on.
    - ``read`` / ``flush`` / ``size`` are required by the ABC but the
      decode path doesn't read back what it wrote.

    The pipe write-end is closed by the :class:`_WriterFactory`, not by
    this class — see :meth:`_WriterFactory.create`.
    """

    def __init__(self, write_fd: int) -> None:
        self._write_fd = write_fd
        self._size = 0
        self._closed = False

    def write(self, s: bytes | bytearray) -> int:
        data = bytes(s)
        os.write(self._write_fd, data)
        self._size += len(data)
        return len(data)

    def read(self, size: int | None = None) -> bytes:  # noqa: ARG002
        return b""

    def seek(self, offset: int, whence: int = 0) -> int:  # noqa: ARG002
        return 0

    def flush(self) -> None:
        pass

    def size(self) -> int:
        return self._size

    def close_write_end(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(OSError):
            os.close(self._write_fd)


# Sentinel value pushed onto the metadata queue by the worker thread when
# extractall has finished (or raised). The main generator uses identity
# comparison to distinguish it from real ``(filename, read_fd)`` tuples.
_SENTINEL = object()


class _WriterFactory(WriterFactory):
    """Build one :class:`_PipeSink` per archive member.

    py7zr calls ``create(filename)`` exactly once per non-directory
    member in archive order. For each call we make a fresh ``os.pipe()``,
    push ``(filename, read_fd)`` onto the metadata queue, and return a
    sink that writes into the write-end. The previous member's pipe is
    closed at that point so its consumer sees EOF — py7zr does not call
    any ``close`` method on the Py7zIO, so this is the only signal we
    have that the previous member is done.

    Empty members are handled identically: ``create`` is called, no
    ``write`` arrives, and the next ``create`` (or the worker's
    ``finally``) closes the write-end → consumer sees an immediate EOF.
    """

    def __init__(self, meta_queue: queue.Queue) -> None:
        self._queue = meta_queue
        self._current: _PipeSink | None = None

    def create(self, filename: str) -> Py7zIO:
        if self._current is not None:
            self._current.close_write_end()
        read_fd, write_fd = os.pipe()
        sink = _PipeSink(write_fd)
        self._current = sink
        self._queue.put((filename, read_fd))
        return sink

    def close_current(self) -> None:
        if self._current is not None:
            self._current.close_write_end()


def _pipe_chunks(read_fd: int) -> Iterator[bytes]:
    """Yield chunks from *read_fd* until EOF.

    Does NOT close the fd — :func:`iter_seven_z_members` owns the read-fd
    lifecycle so that exactly one party closes each pipe end. Closing it
    in both this generator's ``finally`` and in the main loop's
    auto-drain creates an fd-reuse race: between the two closes,
    ``os.pipe()`` for the next member can hand back the same integer,
    and the second close shuts the wrong pipe.
    """
    while True:
        chunk = os.read(read_fd, _CHUNK_SIZE)
        if not chunk:
            return
        yield chunk


def _drain_and_close(read_fd: int) -> None:
    """Read *read_fd* to EOF, then close it. Tolerates already-closed fds."""
    with contextlib.suppress(OSError):
        while os.read(read_fd, _CHUNK_SIZE):
            pass
    with contextlib.suppress(OSError):
        os.close(read_fd)


def _open_seven_z(
    client, bucket: str, key: str, *, if_match: str | None = None
) -> py7zr.SevenZipFile:
    """Open ``s3://bucket/key`` as a :class:`py7zr.SevenZipFile`.

    Wraps a :class:`SeekableS3Object` (ranged-GET view, tail-prefetched)
    in an :class:`io.BufferedReader` and hands it to py7zr. The returned
    file's ``fp`` is that BufferedReader; :meth:`SevenZipFile.close`
    releases it, and closing a py7zr file built from a passed-in fileobj
    does not close the fileobj itself, so the caller owns the lifecycle.

    *if_match* pins every ranged GET (and the opening HEAD) to that
    object generation — see :class:`SeekableS3Object`.

    py7zr's signature-header parse can fail two ways on bad bytes:
    ``Bad7zFile`` (parent: :class:`py7zr.exceptions.ArchiveError`) if the
    format is recognized but malformed, ``struct.error`` if the file is
    too short for ``struct.unpack`` to even reach ``Bad7zFile``. Both are
    translated into :class:`ArchiveReadError` so callers don't have to
    know py7zr internals.
    """
    buffered = open_seekable(client, bucket, key, if_match=if_match)
    try:
        return py7zr.SevenZipFile(buffered, mode="r")
    except py7zr.exceptions.ArchiveError as exc:
        raise ArchiveReadError(f"7z header parse failed: {exc}", cause=exc) from exc
    except struct.error as exc:
        raise ArchiveReadError(
            f"7z header parse failed (truncated input): {exc}", cause=exc
        ) from exc


def list_seven_z_entries(client, bucket: str, key: str) -> list[tuple[str, int]]:
    """Return ``(raw_name, size)`` per file entry, reading the header only.

    The 7z trailing header carries every member's name and uncompressed
    size, so listing needs no member bytes at all — the tail prefetch on
    open covers it. Directory entries are excluded, matching
    :func:`iter_seven_z_members`. Names are raw (pre-safety-pass);
    callers that show them next to extract keys should apply
    :func:`s3_archive.paths.safe_member_key`, as ``_apply_safe_keys``
    does for the member walk.
    """
    sz = _open_seven_z(client, bucket, key)
    try:
        return [(f.filename, f.uncompressed) for f in sz.files if not f.is_directory]
    finally:
        sz.close()


@dataclass(frozen=True)
class SevenZResumeInfo:
    """What ``extract --resume`` needs to know about a .7z before resuming.

    - *resumable* — whether py7zr can seek to an arbitrary member cheaply.
      True iff the archive has ≥2 compression Folders; a fully-solid
      archive (one Folder holding every file) decodes from the block
      start for *any* member, so resume buys nothing and is refused.
    - *numfolders* — the parsed Folder count (0 when the archive has no
      main pack stream, e.g. an all-empty-files archive), kept for the
      refuse message and tests.
    - *members* — ``(raw_filename, uncompressed_size)`` for every
      non-directory entry, in archive order. The raw (pre-safety-key)
      name is what py7zr's ``targets=`` set matches on.
    """

    resumable: bool
    numfolders: int
    members: list[tuple[str, int]]


def probe_seven_z_resume(client, bucket: str, key: str) -> SevenZResumeInfo:
    """Read a .7z header and report whether ``--resume`` can seek per-member.

    Header-only and tail-prefetched (one ranged GET region), so it's cheap
    next to the transfer. Raises :class:`ArchiveReadError` on bad bytes
    (same wrapping as :func:`iter_seven_z_members`) — a corrupt archive is
    not the same as "unsupported for resume."
    """
    sz = _open_seven_z(client, bucket, key)
    try:
        # ``main_streams`` is None when there is no packed body (e.g. an
        # archive of only empty files / directories) → 0 Folders → not
        # resumable, which is the correct answer (nothing to seek to).
        main_streams = sz.header.main_streams
        numfolders = main_streams.unpackinfo.numfolders if main_streams is not None else 0
        members = [(f.filename, f.uncompressed) for f in sz.files if not f.is_directory]
    finally:
        with contextlib.suppress(Exception):  # noqa: BLE001
            sz.close()
    return SevenZResumeInfo(
        resumable=numfolders >= 2,
        numfolders=numfolders,
        members=members,
    )


def iter_seven_z_members(
    client,
    bucket: str,
    key: str,
    *,
    targets: Collection[str] | None = None,
    if_match: str | None = None,
) -> Iterator[ArchiveMember]:
    """GET ``s3://bucket/key`` and yield one :class:`ArchiveMember` per file entry.

    Unlike the tar/zip iterators in :mod:`s3_archive.members` this opens
    a seekable view of the archive — see module docstring for why.
    Members are yielded in archive order; the caller drives consumption
    per-member via the :class:`ArchiveMember` API.

    *targets*, when given, restricts extraction to those raw member names
    (``--resume``): py7zr seeks to just those members' Folders and fires
    the writer factory only for them, so the generator yields exactly the
    requested members (still in archive order). ``None`` extracts every
    member. Requires a non-solid / multi-Folder archive to be cheap — see
    :func:`probe_seven_z_resume`.

    *if_match* (quoted or quote-stripped ETag) pins the opening HEAD and
    every ranged GET to that object generation; a concurrent overwrite
    raises :class:`s3_archive.exceptions.ETagMismatchError` (up front)
    or a 412 ``ClientError`` (mid-walk) instead of mixing bytes from two
    generations across range requests.
    """
    sz = _open_seven_z(client, bucket, key, if_match=if_match)
    # Load-bearing: ``parallel`` is gated on ``not _filePassed`` inside
    # py7zr, and we depend on sequential single-thread extraction so the
    # writer-factory pipe handoff stays in order. Passing an io.IOBase
    # subclass (which BufferedReader is) sets _filePassed=True; the
    # guard catches py7zr quietly changing that behavior.
    if not sz._filePassed:
        sz.close()
        raise RuntimeError(
            "py7zr did not set _filePassed=True for a BufferedReader; "
            "the streaming extract path depends on parallel=False."
        )

    try:
        name_to_size: dict[str, int] = {
            f.filename: f.uncompressed for f in sz.files if not f.is_directory
        }

        meta_queue: queue.Queue = queue.Queue()
        worker_error: list[BaseException] = []
        factory = _WriterFactory(meta_queue)

        target_set = set(targets) if targets is not None else None

        def _worker() -> None:
            try:
                if target_set is None:
                    sz.extractall(factory=factory)
                else:
                    # ``extract`` with ``skip_notarget=True`` (the default)
                    # skips whole non-target Folders and seeks to each
                    # target Folder's pack offset — the per-member seek that
                    # makes resume cheap.
                    sz.extract(targets=target_set, factory=factory)
            except BaseException as exc:  # noqa: BLE001
                worker_error.append(exc)
            finally:
                factory.close_current()
                meta_queue.put(_SENTINEL)

        worker_thread = threading.Thread(target=_worker, daemon=True)
        worker_thread.start()

        prev_read_fd: int | None = None
        clean_exit = False
        try:
            while True:
                if prev_read_fd is not None:
                    _drain_and_close(prev_read_fd)
                    prev_read_fd = None

                item = meta_queue.get()
                if item is _SENTINEL:
                    clean_exit = True
                    break

                filename, read_fd = item
                size = name_to_size.get(filename, 0)
                prev_read_fd = read_fd
                yield ArchiveMember(
                    name=filename,
                    size=size,
                    _chunks=_pipe_chunks(read_fd),
                )
                # _pipe_chunks does NOT close the fd; the next loop's
                # _drain_and_close call owns the close. Exactly one party
                # closes each read fd — see _pipe_chunks for the fd-reuse
                # race that motivates this.

            worker_thread.join()
            if worker_error:
                err = worker_error[0]
                if isinstance(err, (py7zr.exceptions.ArchiveError, struct.error)):
                    raise ArchiveReadError(f"7z decode failed: {err}", cause=err) from err
                raise err
        finally:
            if not clean_exit:
                # Caller closed the generator (or an exception propagated
                # through the yield). Close any open pipe so the worker
                # gets BrokenPipeError on its next write and unblocks.
                if prev_read_fd is not None:
                    with contextlib.suppress(OSError):
                        os.close(prev_read_fd)
                if worker_thread.is_alive():
                    # Drain any remaining metadata so the worker can
                    # finish closing its current pipe and push SENTINEL.
                    while True:
                        try:
                            item = meta_queue.get(timeout=30)
                        except queue.Empty:
                            break
                        if item is _SENTINEL:
                            break
                        _, leftover_fd = item
                        with contextlib.suppress(OSError):
                            os.close(leftover_fd)
                    worker_thread.join(timeout=30)
    finally:
        # Close py7zr's SevenZipFile; suppress secondary errors so a
        # primary worker exception isn't masked.
        with contextlib.suppress(Exception):  # noqa: BLE001
            sz.close()
