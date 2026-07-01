# Resumable extract — design (in progress)

**Status:** design settled; scope = Tier A then Tier B/C (see Decisions).
Moving to implementation planning. A few implementation-level details
(index storage, gzip lib) left for the plan.

## Goal

Survive whole-process death during a large `extract` (crash, reboot,
Ctrl-C, network fully out). Re-running with `--resume` continues instead
of restarting from byte 0, **re-processing at most ~1 GB — or one member,
if members are larger** (see "Checkpoint granularity" below). The behavior
should be **consistent across every format s3-archive extracts** — the user shouldn't see resume work for some formats and
silently not others.

This is distinct from the in-stream transient-drop retry
(`retry.resumable_body_chunks` + backoff), which already survives
*connection* hiccups within a single process. This is about surviving the
*process* dying.

## Settled decisions

- **Opt-in `--resume`, default off.** Matches the `wget -c` model (extract
  is a one-shot restore, not a continuous sync) and preserves the current
  overwrite behavior for library callers (s3-bagit, storage-scripts).
- **Control object at the destination prefix, named by the source
  `ETag`:** `s3://dest/<prefix>/.s3-archive-resume.<etag>.json` (surrounding
  quotes stripped). Rationale:
  - No source bucket/key leaked into a name visible in `ls`.
  - `ETag` is S3's own identifier — a human can `head-object` and
    eyeball-match it; we don't invent a key.
  - The identity guard falls out of the filename: same source → same
    ETag → same file → resume; changed source → different ETag → no match
    → fresh run automatically. (We can still warn if a *different*
    `.s3-archive-resume.*.json` is present.)
  - Different archives extracted to the same prefix → different ETags →
    independent control files (no collision).
  - Contents deliberately exclude bucket/key (no leak); hold size, format,
    a timestamp, and — for compressed formats — the seek index (below).
  - Deleted on successful completion, so only abandoned runs leave a file.
- **The destination is the authoritative progress ledger** (for seekable
  formats). Each finished member is exactly one destination object, and
  `upload_fileobj` is all-or-nothing (s3transfer aborts the multipart on
  failure — never a half-written object). So "which members are done" is
  re-derived from a single `LIST` of the dest prefix, not a stored cursor
  that could drift.

## Checkpoint granularity — the member is atomic

A member is uploaded by one `upload_fileobj` (multipart), which is
all-or-nothing: the destination object either exists in full or not at all.
So the finest point where "everything before here is durably done" is a
**member boundary** — a checkpoint mid-member would be meaningless (nothing
to resume a half-uploaded object from).

Consequences:

- **Checkpoints snap to member boundaries.** The cadence is "once ≥1 GB has
  elapsed, checkpoint at the *next* member boundary" — two bounds combined:
  no more than ~once per GB (so millions of tiny files don't each trigger a
  write), and only at a member boundary (atomicity).
- **A member larger than the interval delays the checkpoint until it
  completes.** A single 3.2 GB member isn't checkpointed until its 3.2 GB
  are fully uploaded; die at 3.1 GB in and resume redoes the whole member.
- **So the real re-processing bound is `max(~1 GB, largest in-flight
  member)`,** not a flat 1 GB.

Doing better — resuming *within* a huge member — would require running our
own S3 multipart upload and checkpointing the completed part list +
`UploadId`. Materially more complex (manual multipart lifecycle, orphaned-
upload cleanup); only worth it if archives routinely contain individual
files in the many-GB range. **Open:** do they? (Governs whether this is
needed.)

## Per-format strategy

The hard reality: whether resume needs a stored "position" at all — and
whether it's even *possible* — depends on how the format is compressed.

### Tier A — natively seekable (no checkpoint needed, re-process ≈ 0)

**zip, 7z, uncompressed `.tar`.**

- zip: read the central directory (tail GETs), jump to each member by its
  indexed offset.
- 7z: already random-access via `SeekableS3Object`.
- uncompressed tar: members are 512-byte-aligned; a ranged GET at a member
  header is a valid tar stream start.

Resume = `LIST` the dest prefix, skip members already present at the
expected size, transfer the rest. The control file is **write-once**
(identity guard only); no periodic updates, no byte cursor. Reuses the
existing `SeekableS3Object`.

**The hard requirement that shapes Tier B/C/D: the seek library must
accept an arbitrary Python file object.** We feed decoders a
`SeekableS3Object` (a Python file-like over ranged GETs) — that fileobj
contract is exactly what keeps s3-archive off local disk. A library that
demands a real filename / OS fd / mmap is a dealbreaker. This is *the*
discriminator (verified July 2026 against PyPI + project docs).

Also: you can't seek to a bare compressed offset — a decompressor's state
at byte X depends on every prior byte. The libraries below instead build a
seek **index** during a forward pass (periodic decompressor-state
snapshots) and export/import it. During extraction we're already reading
forward, so every ~1 GB we persist the current index into the control
file (this is your "write a resume position every X GB"). On resume:
import the index, seek to the last checkpoint at/before the death point,
re-decode ≤1 GB to the next un-done member, resume uploads.

### Tier B — compressed tar, reliably resumable

**`.tar.gz`, `.tar.bz2`.**

| Format | Library | Win wheels | Takes our S3 fileobj | Index persist |
|---|---|---|---|---|
| gzip | `indexed_gzip` (zlib lic.) or `rapidgzip` (MIT, parallel) | ✅ full | ✅ `fileobj=` | ✅ `export_index`/`import_index` |
| bzip2 | `indexed_bzip2` (MIT) | ✅ (cp≤3.13) | ✅ `BytesIO` | ✅ `block_offsets`/`set_block_offsets` |

Both accept our fileobj, ship Windows wheels, and persist an index. gzip
is checkpointable anywhere (32 KB `zran` window); bzip2 is inherently
≤900 KB blocks. Caveat: no `indexed_bzip2` cp3.14 Windows wheel yet — pin
Python ≤3.13 on Windows until one lands.

### Tier C — conditional (pure-Python lib, but encoding-gated)

**`.tar.xz` — resumable *iff* the archive was written multi-block.**

`python-xz` is **pure Python** (wraps stdlib `lzma`), so zero wheel risk
on Windows and it takes a file object — the only gate is the *archive's
encoding*. xz seeks at block boundaries; a multi-block `.xz` is seekable,
a single-block one is not. Default `xz` / `tar -J` often produce a single
block → that instance falls to Tier D. Block count is cheap to read from
the xz index footer (a tail GET), so we can decide up front. (No explicit
index-persist API, but re-reading the block index on open is cheap, so we
likely don't need to persist one.)

### Tier D — not resumable in our streaming model → refuse

**`.tar.zst` (all), single-block `.tar.xz`, single-frame `.tar.zst`.**

- **zstd is out regardless of encoding:** the only library with a real
  seek index, `indexed_zstd`, **requires a filename or OS fd — it will not
  accept our S3 fileobj** (would force local disk). The fileobj-friendly
  options (`zstandard`, stdlib `ZstdFile`) are forward-only (no seek
  table). So no combination gives seek + Python-fileobj for zstd. If we
  ever want it, it'd mean the custom zstd-seekable-frame format + our own
  seek-table reader — separate, larger work.
- **single-block `.tar.xz` / single-frame `.tar.zst`:** no interior seek
  point exists in the encoding — physically unseekable, any library.

**Behavior for Tier D:** no control file is written, and `--resume`
**fails fast, up front, before extracting anything** — never
warn-and-continue (a warning on a multi-hour job scrolls off and leaves a
false sense of protection). Detection is cheap: format is known from the
key; xz/zst block/frame structure from a tail GET; zstd is refused for
resume outright.

**Why single-block/frame can't be checkpointed:** you can resume cheaply
only where the compressor's history *resets* (a bzip2 block, xz block,
zstd frame) — no state crosses the boundary. Resuming *within* a
continuous-history run needs the live decoder state there: the sliding
**window/dictionary** (recent decompressed bytes that back-references
point into) + small entropy tables. Practical only when the window is
small *and* the library exposes snapshot/restore — true for gzip (32 KB;
zlib `inflateGetDictionary`/`inflatePrime`), but not a single zstd frame
(window up to ~8 MB, 128 MB+ with `--long`) or single xz/LZMA2 block
(dictionary up to 64 MB); libzstd/liblzma expose no such primitives.

## Consistency / UX

`--resume` behaves honestly on **every** format — resumes where it can,
and where it can't (Tier D, detected up front), fails fast with a clear
message ("this archive is a .tar.zst, which can't be resumed in the
streaming model; re-run without --resume") rather than silently doing
nothing or silently re-reading everything.

## Decisions

1. **Scope. (Settled.)** Implement **both** Tier A and Tier B/C, phased.
   Tier A first (zip, 7z, uncompressed tar; zero new deps), then Tier B/C
   (gzip, bzip2, multi-block xz). Tier D (zstd; single-block xz /
   single-frame zst) refuses.
2. **Phasing. (Settled.)** Tier A ships first (zero deps, covers the
   current arch_DigiBank.zip case), Tier B/C follows. Not held for a
   single uniform release.
3. **Tier D handling. (Settled.)** `--resume` fails fast up front and
   writes no control file — not warn-and-continue, to avoid a false sense
   of resume protection on a long job.
4. **`--resume` default. (Settled.)** Opt-in flag, default off (`wget -c`
   model); preserves current overwrite behavior for library callers.

### Still open (for the implementation plan to settle)

- **Index storage (Tier B/C):** inline the index in the control JSON
  (base64) vs. a companion `.s3-archive-resume.<etag>.idx` object. A
  per-1 GB index over an 852 GB archive is a few MB.
- **gzip library choice:** `indexed_gzip` (zlib license, abi3 wheels
  3.11–3.14, single-threaded) vs. `rapidgzip` (MIT, parallel/faster,
  win_amd64 only). Lean `indexed_gzip` for wheel breadth + license unless
  index-build speed on huge archives argues for `rapidgzip`.
- **Within-member resume:** deferred (member-atomic). Revisit only if
  archives routinely carry many-GB individual files — *still unconfirmed
  whether they do.*
