"""Streaming archive extract from S3 to S3.

``extract`` streams the archive object out of S3 (via *src_client*),
decodes it via :func:`s3_archive.members.iter_archive_members`, and
``upload_fileobj`` each member back to S3 (via *dst_client*) — nothing
is staged on local disk. A 500 GB archive does not need 500 GB of free
space anywhere.

The two clients may be the same boto3 client (single-endpoint workflows)
or two clients pointed at different endpoints (e.g. archive at AWS,
extracted tree at Kopah/RGW). The streaming model is identical either
way — see docs/ARCHITECTURE.md.
"""

import io
import tarfile
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from s3_archive import resume as resume_mod
from s3_archive.exceptions import ResumeUnsupportedError
from s3_archive.iter import IterableFileobj
from s3_archive.log_config import get_logger
from s3_archive.members import ArchiveMember, _apply_safe_keys, iter_archive_members
from s3_archive.seekable import (
    _BUFFER_SIZE,
    SeekableS3Object,
    iter_tar_members_seekable,
    iter_zip_members_seekable,
)

log = get_logger(__name__)

# Fileobj-driven resume iterators: zip (central directory) and
# uncompressed tar (512-byte-aligned headers) are walked per-member
# through a SeekableS3Object file object. 7z is resumable too (v2) but
# takes a different path — py7zr push-style targeted extraction — so it's
# not in this table; see ``_begin_resume_seven_z``.
_RESUME_ITERATORS: dict[str, Callable[[object], Iterator[ArchiveMember]]] = {
    "zip": iter_zip_members_seekable,
    "tar": iter_tar_members_seekable,
}
# The one canonical set of formats ``--resume`` accepts. Non-solid 7z is
# conditionally supported (solid refuses at probe time); every format not
# here refuses up front — see docs/RESUMABLE-EXTRACT.md.
RESUMABLE_FORMATS = frozenset({*_RESUME_ITERATORS, "7z"})


@dataclass(frozen=True)
class ExtractEvent:
    """One progress event emitted during :func:`extract`.

    Designed to map cleanly onto ``tqdm.update(bytes_transferred)`` plus
    ``tqdm.set_postfix(file=member)`` without the callback needing to
    track state itself. Events come in two shapes:

    - **Byte-transfer events** during an upload (``bytes_transferred > 0``):
      ``member`` and ``member_index`` identify the file in flight,
      ``bytes_transferred`` is the delta since the last event (raw from
      boto3's per-chunk ``Callback=``), and ``member_size`` is the file's
      known uncompressed size (``None`` if the archive format doesn't
      expose it before extraction, e.g. for streamed tar entries).

    - **Member-boundary events** (``bytes_transferred == 0``): emitted
      once before each upload begins, so a UI can switch its "current
      file" indicator without waiting for the first chunk.

    Frozen dataclass: the event is shared by reference into a callback
    that may run on boto3's transfer threadpool, so it must not mutate
    after construction.
    """

    member: str
    bytes_transferred: int
    member_index: int
    member_size: int | None = None


ProgressCallback = Callable[[ExtractEvent], None]


def _dest_key(prefix: str, member_name: str) -> str:
    """Join *prefix* (may be ``""`` or end-with-``/``) and *member_name* into an S3 key."""
    if not prefix:
        return member_name
    if prefix.endswith("/"):
        return prefix + member_name
    return prefix + "/" + member_name


def _make_upload_callback(
    on_progress: ProgressCallback,
    member_name: str,
    member_index: int,
    member_size: int | None,
) -> Callable[[int], None]:
    """Build a one-arg ``Callback=`` for ``boto3.upload_fileobj``.

    Extracted out of the extract loop so the closure doesn't capture
    loop variables (ruff B023). boto3 calls this with a single int:
    bytes transferred since the previous invocation.
    """

    def _cb(bytes_transferred: int) -> None:
        on_progress(
            ExtractEvent(
                member=member_name,
                bytes_transferred=bytes_transferred,
                member_index=member_index,
                member_size=member_size,
            )
        )

    return _cb


def extract(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    fix_unsafe_paths: bool = False,
    resume: bool = False,
    on_progress: ProgressCallback | None = None,
    on_read: Callable[[int], None] | None = None,
) -> list[str]:
    """Stream an archive from S3 and upload each member back to S3.

    *src_client* reads the archive object; *dst_client* writes the
    extracted members. They may be the same boto3 client. *fmt* is one
    of the strings returned by :func:`s3_archive.url.detect_format`.
    Returns the list of member names that were (or would be) written,
    relative to *dest_prefix*.

    *on_progress*, if supplied, is invoked for byte-level *upload*
    progress events (uncompressed member bytes written to the
    destination). See :class:`ExtractEvent`. The callback may run on
    boto3's transfer threadpool, so it must be thread-safe (tqdm.update
    is).

    *on_read*, if supplied, is called with the length of each archive
    chunk *read* from the source (compressed bytes). Sized against the
    archive object's ``ContentLength`` it drives a "how far through the
    archive" progress bar. Not reported for ``7z`` (random-access
    decode). Distinct from *on_progress*, which counts uncompressed
    upload bytes and has no clean total for streamed tar entries.

    *fix_unsafe_paths* is threaded into
    :func:`s3_archive.members.iter_archive_members`: by default a member
    with a ``..`` traversal segment raises
    :class:`s3_archive.exceptions.UnsafeArchiveMemberError` (after
    earlier members are already written — the walk is single-pass);
    ``True`` safely collapses it instead.

    *resume* (opt-in, default off) continues an interrupted extract
    instead of re-processing from byte 0. It requires a per-member-
    seekable source: ``zip``, uncompressed ``tar``, and non-solid ``7z``
    (a solid 7z refuses — its single compression block can't be
    seeked into per-member). Every other format raises
    :class:`s3_archive.exceptions.ResumeUnsupportedError` up front, before
    any object is written. A member already present at the destination at
    its expected size is skipped without re-transfer (the destination is
    the progress ledger; see :mod:`s3_archive.resume`). With *resume*,
    *on_read* is not called — the seekable walk doesn't read the archive
    end-to-end — so a progress bar should be driven off *on_progress*
    instead.
    """
    log.info(
        "Extracting %s%s s3://%s/%s -> s3://%s/%s",
        "(resume) " if resume else "",
        fmt,
        archive_bucket,
        archive_key,
        dest_bucket,
        dest_prefix,
    )

    member_names: list[str] = []
    control_key: str | None = None
    done: dict[str, int] = {}
    if resume:
        members, done, control_key = _begin_resume(
            src_client,
            dst_client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            fmt,
            fix_unsafe_paths=fix_unsafe_paths,
            dry_run=dry_run,
        )
    else:
        members = iter_archive_members(
            src_client,
            archive_bucket,
            archive_key,
            fmt,
            fix_unsafe_paths=fix_unsafe_paths,
            on_bytes=on_read,
        )
    for idx, member in enumerate(members):
        # Resume skip: a member already written at its expected size is
        # done (upload_fileobj is all-or-nothing). ``done`` is empty on the
        # non-resume path, so this never fires there. Nothing is read from
        # the source for a skipped member — the lazy chunk generator is
        # simply never started. The append comes *after* the skip so the
        # returned list is "members written this run" for every format —
        # uniform, and it matches the docstring. (The 7z resume path yields
        # only undone members, so it can't report the skipped ones anyway.)
        if done.get(member.name) == member.size:
            if verbose:
                log.info("  skip (already present) %s", member.name)
            continue
        member_names.append(member.name)
        if on_progress is not None:
            # Boundary event so the UI can switch "current file" before
            # any bytes arrive.
            on_progress(
                ExtractEvent(
                    member=member.name,
                    bytes_transferred=0,
                    member_index=idx,
                    member_size=member.size if member.size else None,
                )
            )
        if dry_run:
            member.drain()
            if verbose:
                if member.size:
                    log.info("  would write %s (%d bytes)", member.name, member.size)
                else:
                    log.info("  would write %s", member.name)
            continue
        dest_key = _dest_key(dest_prefix, member.name)
        if verbose:
            log.info("  %s -> s3://%s/%s", member.name, dest_bucket, dest_key)

        if on_progress is None:
            dst_client.upload_fileobj(
                IterableFileobj(member.chunks()),
                dest_bucket,
                dest_key,
            )
        else:
            dst_client.upload_fileobj(
                IterableFileobj(member.chunks()),
                dest_bucket,
                dest_key,
                Callback=_make_upload_callback(
                    on_progress,
                    member.name,
                    idx,
                    member.size if member.size else None,
                ),
            )

    # Clean completion of a resume run: drop the control marker so only an
    # *interrupted* run leaves one behind. ``control_key`` is None on the
    # non-resume path and in dry-run, so nothing is deleted there.
    if control_key is not None:
        resume_mod.delete_control_file(dst_client, dest_bucket, control_key)

    log.info(
        "extract %s: %d files",
        "(dry-run)" if dry_run else "complete",
        len(member_names),
    )
    return member_names


def _begin_resume(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    fix_unsafe_paths: bool,
    dry_run: bool,
) -> tuple[Iterator[ArchiveMember], dict[str, int], str | None]:
    """Prepare a resumable extract, returning ``(members, done, control_key)``.

    Refuses (:class:`ResumeUnsupportedError`) before any write when *fmt*
    isn't resumable, or when the archive turns out to lack the per-member
    index resume needs (no zip central directory / unreadable tar / solid
    7z). Otherwise returns the safe-keyed member iterator, the done-set to
    skip against, and the control-file key to delete on clean completion
    (``None`` when nothing should be deleted — i.e. in dry-run).
    """
    if fmt not in RESUMABLE_FORMATS:
        raise ResumeUnsupportedError(
            f"--resume is not supported for {fmt!r} archives "
            f"(only zip, uncompressed tar, and non-solid 7z are per-member "
            f"seekable); re-run without --resume."
        )

    if fmt == "7z":
        return _begin_resume_seven_z(
            src_client,
            dst_client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            fix_unsafe_paths=fix_unsafe_paths,
            dry_run=dry_run,
        )

    iterator_factory = _RESUME_ITERATORS[fmt]
    head = src_client.head_object(Bucket=archive_bucket, Key=archive_key)
    source_etag = head["ETag"]
    source_size = head["ContentLength"]

    # Build the member iterator (which opens + validates the archive)
    # *before* writing any control file, so a genuinely unseekable archive
    # refuses without leaving an orphan marker at the destination.
    raw = SeekableS3Object(src_client, archive_bucket, archive_key)
    buffered = io.BufferedReader(raw, buffer_size=_BUFFER_SIZE)
    try:
        raw_members = iterator_factory(buffered)
    except (zipfile.BadZipFile, tarfile.TarError) as exc:
        raise ResumeUnsupportedError(
            f"{fmt} archive at s3://{archive_bucket}/{archive_key} is not "
            f"per-member seekable ({exc}); re-run without --resume."
        ) from exc
    members = _apply_safe_keys(raw_members, fix_unsafe_paths=fix_unsafe_paths)

    done, ckey = _resume_control_and_done(
        dst_client,
        dest_bucket,
        dest_prefix,
        source_etag=source_etag,
        source_size=source_size,
        fmt=fmt,
        dry_run=dry_run,
    )
    return members, done, ckey


def _resume_control_and_done(
    dst_client,
    dest_bucket: str,
    dest_prefix: str,
    *,
    source_etag: str,
    source_size: int,
    fmt: str,
    dry_run: bool,
) -> tuple[dict[str, int], str | None]:
    """Resolve the control marker and done-set for a resume run.

    Shared by every resumable format (the marker + destination-ledger
    machinery is format-agnostic — see :mod:`s3_archive.resume`). Returns
    ``(done, control_key)`` where *control_key* is ``None`` in dry-run (no
    marker written, nothing to delete on completion).
    """
    ckey = resume_mod.control_key(dest_prefix, source_etag)
    if resume_mod.control_file_exists(dst_client, dest_bucket, ckey):
        # A prior --resume run for this exact source (same ETag) began
        # here: trust the destination objects as the progress ledger.
        done = resume_mod.build_done_set(dst_client, dest_bucket, dest_prefix)
    else:
        # Fresh run — don't trust un-vouched pre-existing objects. Write the
        # marker now so an interruption partway through is itself resumable.
        # Skipped in dry-run: a preview must leave no S3 side effects.
        done = {}
        if not dry_run:
            resume_mod.write_control_file(
                dst_client,
                dest_bucket,
                ckey,
                source_etag=source_etag,
                source_size=source_size,
                fmt=fmt,
                now_iso=datetime.now(UTC).isoformat(),
            )

    # In dry-run we neither wrote nor should delete a marker; return None so
    # the caller's completion step is a no-op.
    return done, (None if dry_run else ckey)


def _begin_resume_seven_z(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    fix_unsafe_paths: bool,
    dry_run: bool,
) -> tuple[Iterator[ArchiveMember], dict[str, int], str | None]:
    """Prepare a resumable 7z extract (v2), returning ``(members, done, control_key)``.

    7z can't be walked through a fileobj like zip/tar: py7zr drives
    extraction push-style through a :class:`WriterFactory`, so instead of
    skipping done members inside the loop we compute the *undone* set up
    front and hand it to py7zr as ``targets``. The returned iterator
    therefore yields only members that still need writing.

    Refuses (:class:`ResumeUnsupportedError`) before any write when the
    archive is solid (single compression Folder) — extracting any member
    then decodes from the block start, so resume buys nothing. The probe
    precedes the control-marker write, so a refusal leaves no orphan marker
    (mirroring the zip BadZipFile ordering).
    """
    # Lazy import: seven_z pulls in py7zr (and its load-time monkeypatch);
    # keep that off the zip/tar resume path, matching members.py's dispatch.
    from s3_archive.paths import safe_member_key  # noqa: PLC0415
    from s3_archive.seven_z import (  # noqa: PLC0415
        iter_seven_z_members,
        probe_seven_z_resume,
    )

    head = src_client.head_object(Bucket=archive_bucket, Key=archive_key)
    source_etag = head["ETag"]
    source_size = head["ContentLength"]

    # Probe the header (cheap, tail-prefetched) *before* writing any control
    # marker, so a solid archive refuses without leaving an orphan behind.
    info = probe_seven_z_resume(src_client, archive_bucket, archive_key)
    if not info.resumable:
        raise ResumeUnsupportedError(
            f"the .7z at s3://{archive_bucket}/{archive_key} is solid "
            f"(single compression block), so resume can't seek to an "
            f"individual member; re-create it non-solid (e.g. "
            f"`7z a -ms=off …`), or re-run without --resume."
        )

    done, ckey = _resume_control_and_done(
        dst_client,
        dest_bucket,
        dest_prefix,
        source_etag=source_etag,
        source_size=source_size,
        fmt="7z",
        dry_run=dry_run,
    )

    # Compute the undone target set on the *raw* member names py7zr matches:
    # a member is done iff a destination object at its safe key already has
    # the expected uncompressed size. safe_member_key mirrors the loop's
    # _apply_safe_keys, so the done-set lookup lines up with the ledger. A
    # ``..`` member raises UnsafeArchiveMemberError here (fail-fast) — it
    # could never have been written anyway.
    targets: list[str] = []
    for raw_name, size in info.members:
        safe = safe_member_key(raw_name, fix_unsafe=fix_unsafe_paths)
        if done.get(safe) != size:
            targets.append(raw_name)

    members = _apply_safe_keys(
        iter_seven_z_members(src_client, archive_bucket, archive_key, targets=targets),
        fix_unsafe_paths=fix_unsafe_paths,
    )
    # ``targets`` already excludes done members, so the loop never skips —
    # but pass ``done`` through anyway for a defensive belt-and-suspenders
    # match (harmless: no member the iterator yields is in it).
    return members, done, ckey
