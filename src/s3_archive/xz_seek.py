"""Seekable xz for compressed-tar ``extract --resume`` (v4).

A ``.tar.xz`` is per-member seekable **iff it was written multi-block**. xz
is a block format like bzip2 — each block is independently decodable — but,
unlike gzip/bzip2, the block index already lives *in the file* (the stream
footer). So there is **no companion ``.idx`` to persist**: :mod:`xz`
(``python-xz``, pure Python — zero wheel risk) reads the footer on open (a
cheap tail read, covered by ``SeekableS3Object``'s tail prefetch) and seeks
to any block from there. Resume is therefore just "open, walk members, let
the done-set skip" — the destination ledger is the only state.

The one gate is the *encoding*: the common ``xz`` / ``tar -J`` default emits
a **single** block, which has no interior seek point — that instance is Tier
D and is **refused up front** (``block_boundaries`` has < 2 entries),
mirroring the solid-``.7z`` refusal. A multi-block archive (``xz
--block-size=…`` / ``xz -T0``, or several concatenated streams) is
resumable.

As with the other seekable decoders, the decompressed tar hands straight to
the **existing** :func:`s3_archive.seekable.iter_tar_members_seekable`.
"""

from __future__ import annotations

import contextlib
import tarfile
from collections.abc import Iterator

from s3_archive.exceptions import ResumeUnsupportedError
from s3_archive.members import ArchiveMember
from s3_archive.seekable import iter_tar_members_seekable


def _import_xz():
    """Import :mod:`xz` (python-xz), mapping absence to a clear resume refusal.

    Pure-Python, so this is only a guard against a broken install. It's a
    core dependency, so ``ImportError`` here is unexpected.
    """
    try:
        import xz  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - core dep; defensive
        raise ResumeUnsupportedError(
            "--resume for .tar.xz needs the 'python-xz' package, which is not "
            "importable. Reinstall s3-archive (python-xz is a core "
            "dependency), or re-run without --resume."
        ) from exc
    return xz


def open_tar_xz_seekable(source_fileobj) -> tuple[object, Iterator[ArchiveMember]]:
    """Open a multi-block xz-compressed tar as a per-member-seekable iterator.

    *source_fileobj* is a seekable file object over the compressed archive.
    Returns ``(xzf, members)`` — *xzf* is the :class:`xz.XZFile` (its block
    index came from the file footer, so there is nothing to persist), and
    *members* is the raw (pre-safe-key) :class:`ArchiveMember` iterator.

    Raises :class:`ResumeUnsupportedError`, before the caller writes any
    marker, when the body isn't a readable xz-of-tar **or** the xz is a
    single block (no interior seek point → Tier D, re-run without
    ``--resume``).
    """
    xz = _import_xz()
    try:
        xzf = xz.XZFile(source_fileobj)
    except xz.XZError as exc:
        raise ResumeUnsupportedError(
            f"the .tar.xz is not a readable xz stream ({exc}); re-run without --resume."
        ) from exc

    # Single-block xz has no seek point interior to the encoding — resume
    # would decode from byte 0 for any member, so it buys nothing. Refuse
    # before any marker is written (mirrors the solid-.7z refusal).
    if len(xzf.block_boundaries) < 2:
        with contextlib.suppress(Exception):
            xzf.close()
        raise ResumeUnsupportedError(
            "the .tar.xz is a single xz block, so resume can't seek to an "
            "individual member; re-compress it multi-block (e.g. "
            "`xz --block-size=64MiB` or `xz -T0`), or re-run without --resume."
        )

    try:
        members = iter_tar_members_seekable(xzf)
    except (tarfile.TarError, xz.XZError) as exc:
        with contextlib.suppress(Exception):
            xzf.close()
        raise ResumeUnsupportedError(
            f"the .tar.xz is not a readable xz-compressed tar ({exc}); re-run without --resume."
        ) from exc
    return xzf, members
