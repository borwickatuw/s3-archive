# Resumable extract — design (in progress)

**Status:** design discussion. Open decisions flagged at the bottom.

## Goal

Survive whole-process death during a large `extract` (crash, reboot,
Ctrl-C, network fully out). Re-running with `--resume` continues instead
of restarting from byte 0, **re-processing at most ~1 GB** of already-done
work. The behavior should be **consistent across every format s3-archive
extracts** — the user shouldn't see resume work for some formats and
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

### Tier B — whole-stream compressed tar (needs a decompressor index)

**Reliably resumable: `.tar.gz`, `.tar.bz2`.** Also multi-block
`.tar.xz` / multi-frame `.tar.zst` *if* they happen to be encoded that
way (see Tier C for the common case where they aren't).

`gzip` can be checkpointed at any point (`zran`-style window snapshots),
and `bzip2` is inherently block-structured (≤900 KB blocks, each
independently decodable), so any archive bigger than one block is
seekable. Both are dependable, regardless of how the archive was
produced.

A gzip/bzip2/xz/zstd decompressor's state at byte X depends on every prior
byte — you cannot seek to a compressed offset and resume decoding from a
bare byte number. But you *can* periodically capture the decompressor's
state and persist it, then seek back to it. This is exactly your "write a
resume position every X GB" idea — it just has to record the decompressor
checkpoint, not only an offset.

Libraries that do this (build an index of seek points during a forward
pass, export/import it):

- gzip → `rapidgzip` or `indexed_gzip` ✅
- bzip2 → `indexed_bzip2` (block-level) ✅
- xz → `python-xz` — **only if the archive was written multi-block** ⚠️
- zstd → `indexed_zstd` — **only if the archive was written multi-frame** ⚠️

Mechanism: during the first pass we're already reading the stream
sequentially to extract; every ~1 GB we export the current seek index into
the control file (this is the periodic write you envisioned). On resume:
import the index, seek to the last checkpoint at/before where we died,
re-decode ≤1 GB to reach the next un-done member, and resume member
uploads (skipping done members via the dest ledger).

### Tier C — cannot resume (honest gap)

**Single-block `.tar.xz`, single-frame `.tar.zst`.**

If the archive was encoded as one solid block/frame, there is no interior
seek point — no library and no effort can seek into it; you can only
decode from the front. Externally-produced `tar.xz` / `tar.zst` are
*often* single-block/frame (default `xz`/`tar -J` and `zstd`/`tar --zstd`
produce exactly this), so this is a real, unavoidable gap for those
inputs. (s3-archive itself only ever *creates* `.tar.gz` and `.zip`, so it
never produces a Tier C archive; this only bites on archives made
elsewhere.)

**Behavior for Tier C:** no control file is written, and `--resume`
**fails fast, up front, before extracting anything** — it does not warn
and silently proceed without resume (a warning on a multi-hour job scrolls
off and leaves a false sense of protection). Detection: `xz` block count
is cheap to read from the stream's index footer; `zst` frame count isn't,
so a `.tar.zst` is treated as Tier C *unless* proven multi-frame
(errs toward the honest refusal).

**Why not just snapshot the decoder state mid-stream?** The general
principle: you can resume cheaply at any point where the compressor's
history *resets* (a bzip2 block, an xz block, a zstd frame) — those carry
no state across the boundary. Resuming *within* a continuous-history
stream instead needs the live decoder state at that point: the sliding
**window / dictionary** (the recent decompressed bytes that upcoming
back-references point into) plus small entropy/offset tables. That's only
practical when the window is small *and* the library exposes
snapshot/restore primitives — true for **gzip** (32 KB window; zlib's
`inflateGetDictionary`/`inflatePrime`, which is what `indexed_gzip` uses),
but **not** for a single **zstd** frame (window up to ~8 MB, or 128 MB+
with `--long`) or a single **xz/LZMA2** block (dictionary up to 64 MB) —
libzstd/liblzma give no supported way to extract or re-inject mid-stream
decoder state, so checkpointing them would mean forking the decoder. Hence
those stay Tier C.

## Consistency / UX

`--resume` behaves honestly on **every** format — resumes where it can,
and where it can't (Tier C, detected when the archive is opened), fails
fast with a clear message ("this archive is a single-frame .tar.zst, which
can't be resumed; re-run without --resume") rather than silently doing
nothing or silently re-reading everything.

## Open decisions (need input)

1. **Dependency cost.** Tier B pulls in ~3–4 C-extension deps
   (`rapidgzip`/`indexed_gzip`, `indexed_bzip2`, `indexed_zstd`,
   `python-xz`). Acceptable for full coverage? Or scope Tier B to **gzip
   only** first (s3-archive only *creates* tar.gz + zip, so gzip is the
   compressed format most likely to matter), and treat bz2/xz/zst as a
   later add?
2. **Phasing.** Ship **Tier A first** (zero new deps, covers zip/7z/tar
   including the current arch_DigiBank.zip case), then Tier B? Or hold
   until all tiers are ready so the release is uniform?
3. **Tier C handling. (Resolved.)** `--resume` fails fast up front and
   writes no control file — see Tier C above. Not warn-and-continue, to
   avoid a false sense of resume protection on a long job.
4. **Index storage.** For Tier B, inline the index in the control JSON
   (base64) vs. a companion `.s3-archive-resume.<etag>.idx` object. A
   per-1 GB index over an 852 GB archive is a few MB.
