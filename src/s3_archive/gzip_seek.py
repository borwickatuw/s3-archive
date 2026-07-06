"""Seekable gzip for compressed-tar ``extract --resume`` (v3).

A ``.tar.gz`` can't be walked per-member the way an uncompressed tar or a
zip can: a decompressor's state at uncompressed byte *X* depends on every
prior compressed byte, so you can't just range-GET into the middle. But
:mod:`indexed_gzip` builds a *seek index* during a forward decode
(periodic 32 KB ``zran`` window snapshots) and can export/import it — so a
resume run imports the index, seeks to the nearest checkpoint at/before
the death point, and re-decodes only the short tail to the next un-done
member. No full re-download, no full re-decode.

Crucially, ``IndexedGzipFile(fileobj=…)`` accepts an arbitrary seekable
Python file object with no real fd — exactly our
:class:`io.BufferedReader` over a
:class:`s3_archive.seekable.SeekableS3Object` — which is what keeps
s3-archive off local disk. ``drop_handles=False`` is required because a
passed-in fileobj can't be reopened.

The presented stream is a *decompressed* tar, so it hands straight to the
**existing** :func:`s3_archive.seekable.iter_tar_members_seekable` — the
per-member seek/skip logic is v1 code, unchanged.

**The index is a pure optimization; correctness never depends on it.** A
missing / stale / corrupt ``.idx`` degrades to more forward re-decode,
never wrong output — the tar layer always reads the actual member bytes
and the destination-as-ledger done-set governs what is skipped.
"""

from __future__ import annotations

import io
import tarfile
from collections.abc import Iterator

from s3_archive.exceptions import ResumeUnsupportedError
from s3_archive.log_config import get_logger
from s3_archive.members import ArchiveMember
from s3_archive.seekable import iter_tar_members_seekable

log = get_logger(__name__)

# ~1 GiB uncompressed between seek points. Two things at once: index size
# (indexed_gzip stores ~32 KB per point → ~32 KB/GB) and the resume
# re-decode bound (a seek lands at the nearest point ≤ ~1 GB back — the
# documented ``max(~1 GB, largest in-flight member)`` bound). Also drives
# the index-PUT checkpoint cadence in :mod:`s3_archive.extract`. A
# module-level constant so a test can monkeypatch a tiny value to force
# multiple seek points / checkpoints on a small fixture.
DEFAULT_INDEX_SPACING = 1024 * 1024 * 1024


def _import_indexed_gzip():
    """Import :mod:`indexed_gzip`, mapping absence to a clear resume refusal.

    Lazy so the compiled extension stays off the zip / uncompressed-tar /
    7z resume paths (which never touch it) and off every non-resume run.
    It's a core dependency, so ``ImportError`` here is unexpected — but a
    clean message beats a bare traceback on a long unattended job.
    """
    try:
        import indexed_gzip  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - core dep; defensive
        raise ResumeUnsupportedError(
            "--resume for .tar.gz needs the 'indexed_gzip' package, which is "
            "not importable. Reinstall s3-archive (indexed_gzip is a core "
            "dependency), or re-run without --resume."
        ) from exc
    return indexed_gzip


def open_tar_gz_seekable(
    source_fileobj,
    *,
    index_bytes: bytes | None = None,
    spacing: int = DEFAULT_INDEX_SPACING,
) -> tuple[object, Iterator[ArchiveMember]]:
    """Open a gzip-compressed tar as a per-member-seekable member iterator.

    *source_fileobj* is a seekable file object over the compressed archive
    (an :class:`io.BufferedReader` over a
    :class:`s3_archive.seekable.SeekableS3Object`). *index_bytes*, when
    given, is a previously exported :mod:`indexed_gzip` seek index; it's
    imported so seeks jump past already-decoded regions. *spacing* is the
    seek-point density (uncompressed bytes between points).

    Returns ``(igzf, members)``:

    - *igzf* is the :class:`indexed_gzip.IndexedGzipFile`; its ``tell()``
      reports the current uncompressed offset (drives the checkpoint
      cadence) and ``export_index`` snapshots the accrued seek points.
    - *members* is the **raw** (pre-safe-key) :class:`ArchiveMember`
      iterator from :func:`s3_archive.seekable.iter_tar_members_seekable`
      — the caller wraps it in :func:`s3_archive.members._apply_safe_keys`
      exactly like every other path.

    Raises :class:`ResumeUnsupportedError` if the bytes aren't a readable
    gzip-of-tar (mirroring the zip ``BadZipFile`` / tar ``TarError`` refuse
    ordering) — the archive is opened + validated here, before the caller
    writes any control marker.
    """
    igz = _import_indexed_gzip()
    igzf = igz.IndexedGzipFile(
        fileobj=source_fileobj,
        spacing=spacing,
        drop_handles=False,  # a passed-in fileobj can't be reopened
    )
    if index_bytes:
        try:
            igzf.import_index(fileobj=io.BytesIO(index_bytes))
        except igz.ZranError as exc:
            # Stale / corrupt index → pure optimization lost, not an error:
            # rebuild by forward decode. (A same-ETag index is normally
            # valid; this guards a truncated / half-written .idx.)
            log.warning(
                "resume: gzip seek index failed to import (%s); rebuilding by forward decode",
                exc,
            )
    try:
        # iter_tar_members_seekable eagerly ``tarfile.open``s the decoded
        # stream — that first read forces indexed_gzip to inflate the head,
        # so a non-gzip body raises ZranError and a non-tar body TarError,
        # both here (before any marker is written).
        members = iter_tar_members_seekable(igzf)
    except (tarfile.TarError, igz.ZranError) as exc:
        raise ResumeUnsupportedError(
            f"the .tar.gz is not a readable gzip-compressed tar ({exc}); re-run without --resume."
        ) from exc
    return igzf, members


def export_index_bytes(igzf) -> bytes:
    """Serialize *igzf*'s current seek index to bytes (for a PUT to S3).

    Round-trips through an in-memory :class:`io.BytesIO`, so the index is
    persisted straight to an S3 object — never staged on local disk.
    """
    buf = io.BytesIO()
    igzf.export_index(fileobj=buf)
    return buf.getvalue()
