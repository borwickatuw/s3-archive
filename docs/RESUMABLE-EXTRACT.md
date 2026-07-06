# Resumable extract — how `--resume` works

`extract --resume` continues an interrupted extract instead of restarting
from byte 0. It exists to survive **whole-process death** during a long
extract — crash, reboot, Ctrl-C, network fully out. This is distinct from the
in-stream transient-drop retry (`retry.resumable_body_chunks` + backoff),
which already survives *connection* hiccups within a single running process;
`--resume` survives the *process* itself dying.

`--resume` is **opt-in** (default off): it follows the `wget -c` model
(extract is a one-shot restore, not a continuous sync) and preserves the
overwrite behavior that library callers (s3-bagit, storage-scripts) rely on.

## What a resume run does

1. `head` the source archive for its `ETag`.
2. Look for the control marker for that ETag at the destination prefix. If it
   exists, a prior `--resume` run for *this exact source* began here, so the
   destination objects can be trusted as a progress ledger. If it's absent,
   this is a fresh run: write the marker and don't trust any pre-existing
   objects.
3. `LIST` the destination prefix once → the **done-set** (`{member: size}`).
4. Walk the archive's members (per-format, below), skip any already present at
   the expected size, and transfer the rest.
5. On clean completion, delete the marker (and the `.idx`, for gzip), so only
   an *interrupted* run leaves artifacts behind.

For a format `--resume` can't handle, it **fails fast up front** — before
writing anything and without creating a marker — with a message telling you to
re-run without `--resume`, rather than silently restarting or silently
re-reading everything.

## The artifacts

**Control marker** — `s3://dest/<prefix>/.s3-archive-resume.<etag>.json`. Its
mere existence is the identity guard: same source → same ETag → same marker →
resume; a changed source → different ETag → no marker → a clean fresh run,
automatically. Naming it by the source ETag means nothing about the source
(bucket/key) leaks into a name visible in `ls`; ETag is S3's own identifier,
so a human can `head-object` and eyeball-match it; and different archives
extracted to the same prefix get independent markers. The body holds only
`schema_version`, `source_etag`, `source_size`, `format`, and a timestamp —
deliberately **no bucket/key** (no leak) and **no seek index** (see below).
Written once at the start of a resumable run, deleted on clean completion.

**Destination objects = the progress ledger.** Each finished member is exactly
one destination object, and `upload_fileobj` is all-or-nothing (s3transfer
aborts the multipart on failure — never a half-written object). So "which
members are done" is re-derived from a single `LIST`, never a stored cursor
that could drift. Resume artifacts (`.json`, `.idx`) are excluded from the
walk.

**Seek-index companion (gzip only)** —
`s3://dest/<prefix>/.s3-archive-resume.<etag>.idx`. gzip is the one supported
format that needs a *persisted* decompressor seek index (see Tier B). It lives
in this **separate object**, not inlined in the marker JSON — which keeps the
marker tiny and human-readable and lets the index be PUT independently on each
checkpoint. It's ETag-named like the marker (same identity guard) and deleted
alongside it on completion. It is a **pure optimization**: a missing / stale /
corrupt `.idx` degrades to more forward re-decode, never wrong output.

## Checkpoint granularity — the member is atomic

A member is uploaded by one all-or-nothing `upload_fileobj`, so the finest
point where "everything before here is durably done" is a **member boundary** —
a mid-member checkpoint would be meaningless (there's nothing to resume a
half-uploaded object from).

The gzip `.idx` is therefore PUT at member boundaries, at most once per ~1 GB
of uncompressed progress (so millions of tiny files don't each trigger a
write). A member larger than that interval delays its checkpoint until it
finishes — so the real re-processing bound on resume is
**`max(~1 GB, largest in-flight member)`**, not a flat 1 GB. (The Tier-A
formats and xz persist no index, so their marker is write-once and there's
nothing to checkpoint.)

## The constraint that shapes everything: the fileobj

We feed every decoder a `SeekableS3Object` — a Python file-like object over
ranged GETs, with no real filename or OS fd. That fileobj contract is exactly
what keeps s3-archive off local disk, so **a seek library that demands a real
filename / fd / mmap is a dealbreaker.** This is *the* discriminator between a
format we can resume and one we can't (it's what rules out zstd — see Tier D).

You can't seek to a bare compressed offset: a decompressor's state at byte X
depends on every prior byte. The seekable decoders instead either build a seek
**index** during a forward pass (periodic decompressor-state snapshots we
export/import — gzip) or carry a block index **in the file** (xz).

## Per-format support

The resumable set is `{"zip", "tar", "tar.gz", "tar.xz", "7z"}`
(`extract.RESUMABLE_FORMATS`). Everything else refuses at the format check.

### Tier A — natively seekable (no index, re-process ≈ 0)

**zip, uncompressed `.tar`, non-solid `7z`.**

- **zip** — read the central directory (tail GETs), jump to each member by its
  indexed offset.
- **uncompressed tar** — members are 512-byte-aligned; a ranged GET at a
  member header is a valid tar-stream start.
- **7z** — already random-access via `SeekableS3Object`; py7zr's
  `extract(targets=…)` seeks to just the undone members' Folders. Requires a
  **non-solid** archive (`numfolders ≥ 2`): a solid `.7z` bundles every member
  into one compression Folder, so extracting any one decodes from the block
  start (zero seek benefit) → refused (create non-solid with `7z a -ms=off …`).

No index, no checkpoint — the marker is write-once (identity guard only).

### Tier B — persisted seek index

**`.tar.gz`.** `indexed_gzip` reads + seeks over our fileobj
(`IndexedGzipFile(fileobj=…, drop_handles=False)`) and exports/imports a small
`zran` seek index (a 32 KB decompressor-window snapshot every ~1 GB). On the
first resumable run it accrues points during the forward decode and PUTs the
`.idx` at member boundaries; on resume it imports the `.idx` and seeks to a
late member, re-decoding ≤ ~1 GB instead of re-downloading the whole source.
`indexed_gzip` is a core dependency (zlib license, abi3 wheels 3.11–3.14),
chosen over an optional extra so resume just works on a long unattended job,
and over `rapidgzip` for wheel breadth + license.

### Tier C — in-file block index (no companion `.idx`)

**Multi-block `.tar.xz`.** `python-xz` (pure Python) reads xz's block index
from the stream footer on open (a cheap tail read, covered by the tail
prefetch) and seeks from there — and, crucially, it **jumps** on the member
walk's forward seeks. Nothing to persist; the destination ledger is the only
resume state. The gate is the *encoding*: xz seeks at block boundaries, so a
multi-block file is seekable and a **single-block** one is not. The default
`xz` / `tar -J` emits a single block → refused up front (block count read from
the footer), with a message to re-compress multi-block
(`xz --block-size=…` / `xz -T0`).

### Tier D — not resumable in our streaming model → refuse

**`.tar.zst` (all), single-block `.tar.xz`, `.tar.bz2`, solid `.7z`.**

- **zstd** — the only library with a real seek index, `indexed_zstd`, requires
  a filename / fd and won't accept our fileobj; the fileobj-friendly readers
  (`zstandard`, stdlib `ZstdFile`) are forward-only. No combination gives
  seek + fileobj. Would need the zstd seekable-frame format + our own
  seek-table reader — separate, larger work.
- **single-block `.tar.xz`** — no interior seek point exists in the encoding
  (physically unseekable, any library).
- **`.tar.bz2`** — bzip2 *is* a block format and `indexed_bzip2` exposes a
  persistable block-offset map, but the library **decodes through the member
  walk's short forward seeks** instead of jumping, so resume would re-download
  and re-decode the whole source anyway — saving only re-uploads, the smaller
  cost given bzip2's slow decode. The durable lesson: a library exposing a
  persistable index is **necessary but not sufficient** — it must also *jump*
  on the access pattern we actually use. Full write-up and a possible fix
  (member-offset map + direct seek to the first un-done member) live in
  [`SOMEDAY-MAYBE.md`](SOMEDAY-MAYBE.md).
- **solid `.7z`** — see Tier A.

Why single-block/frame can't be checkpointed at all: you can resume cheaply
only where the compressor's history *resets* (a bzip2 block, an xz block, a
zstd frame). Resuming *within* a continuous-history run needs the live decoder
state — the sliding window/dictionary plus entropy tables — which is practical
only when the window is small *and* the library exposes snapshot/restore: true
for gzip (32 KB window; zlib `inflateGetDictionary` / `inflatePrime`), but not
a single zstd frame (window up to ~8 MB, 128 MB+ with `--long`) or a single
xz/LZMA2 block (dictionary up to 64 MB) — libzstd / liblzma expose no such
primitives.

## How refusal reads to the user

`--resume` behaves honestly on **every** format: it resumes where it can, and
where it can't (detected up front) it fails fast with a clear message — e.g.
"this archive is a .tar.zst, which can't be resumed in the streaming model;
re-run without --resume" — rather than silently doing nothing or silently
re-reading everything. Detection is cheap: the format is known from the key;
xz block structure and 7z solidity from a tail GET / header parse; zstd and
bz2 are refused outright.

## Module map

- **`resume.py`** — format-agnostic core: `control_key` / `index_key` (both
  ETag-sanitized, via `_marker_key`), `is_control_key` (matches `.json` **and**
  `.idx` so neither pollutes the done-set), `write_control_file` /
  `control_file_exists` / `delete_control_file`, `write_index_object` /
  `read_index_object` / `delete_index_object`, and `build_done_set` (one
  paginated `LIST` → `{RelativePath: Size}`, resume artifacts excluded).
- **`seekable.py`** — `SeekableS3Object` (the ranged-GET fileobj) plus
  `iter_zip_members_seekable` / `iter_tar_members_seekable`. Both yield the
  same `ArchiveMember` shape as the streaming path and go through
  `members._apply_safe_keys`, so destination keys are byte-identical.
- **`gzip_seek.py`** — `open_tar_gz_seekable` wraps the fileobj in an
  `IndexedGzipFile` (importing any prior `.idx`), hands the decoded tar to
  `iter_tar_members_seekable`, and refuses a non-gzip / non-tar body before any
  marker; a corrupt imported index is logged and ignored (rebuild by forward
  decode). `export_index_bytes` snapshots the accrued seek points for the PUT.
- **`xz_seek.py`** — `open_tar_xz_seekable` wraps the fileobj in a `python-xz`
  `XZFile`; refuses a single-block (`block_boundaries < 2`) or non-xz / non-tar
  body before any marker. No `.idx`.
- **`seven_z.py`** — `probe_seven_z_resume` reads the 7z header (tail-fetched)
  → `SevenZResumeInfo` (`resumable = numfolders ≥ 2`, the Folder count, and the
  `(raw_name, uncompressed_size)` member index); `iter_seven_z_members(targets=…)`
  seeks to just the undone members' Folders.
- **`extract.py`** — `extract(resume=…)`: refuse non-resumable formats up
  front, build the marker + done-set, skip present members, and clean up the
  marker (and gzip's `.idx`) on completion. Each seekable family has a
  `_begin_resume_*` branch; the gzip branch additionally wraps its members in a
  self-checkpointing generator that PUTs the `.idx` at member boundaries. The
  gzip / xz decoders are closed when the walk ends.
- **`cli.py`** — the opt-in `--resume` flag; `ResumeUnsupportedError` → clean
  stderr + config exit code; an indeterminate byte bar (the seekable walk has
  no compressed-`ContentLength` total).
