# Someday / Maybe

Ideas worth considering but deliberately out of scope for now. Not
commitments — a parking lot so they aren't lost.

## Resumable extract for `.tar.bz2` (and other seek-hostile formats)

`--resume` covers `zip`, uncompressed `.tar`, `.tar.gz`, multi-block
`.tar.xz`, and non-solid `7z`. Three compressed-tar encodings are refused;
this note records **why**, so a future attempt doesn't re-discover it.

- **`.tar.bz2` — refused, revisit if it matters.** bzip2 *is* a block
  format (independent ≤900 KB blocks), and `indexed_bzip2` exposes a
  block-offset map (`block_offsets()` / `set_block_offsets()`) that we can
  persist. But measured over moto: `indexed_bzip2` **decodes straight
  through short forward seeks** and only jumps for large ones. The resume
  path walks the tar member-by-member, and each step is a *small* forward
  seek (past one member's data) — below its jump threshold — so it re-reads
  and re-decodes the **entire** source even with a correct block map loaded.
  (`indexed_gzip` and `python-xz` jump even on small forward seeks, which is
  why gzip/xz work.) A resume that re-downloads + re-decodes everything only
  saves the *re-upload*, which — given bzip2's slow single-threaded decode
  (~30–40 MB/s, often the wall-clock bottleneck) — is usually the *smaller*
  cost. That's the exact "forward-re-decode" approach we rejected for gzip,
  so shipping it for bz2 would contradict the feature's guarantee.
  - **The fix, if bz2 resume is ever wanted:** don't rely on the tar walk's
    incremental seeks. Persist a **member → uncompressed-offset** map during
    the first pass, then on resume seek *directly* to the first un-done
    member (one large forward seek `indexed_bzip2` *does* honor as a jump)
    and process forward from there. This nails the common "died partway,
    done = contiguous prefix" case — skipping the done prefix's download +
    decode — and would generalize the resume core beyond the current
    per-member walk. Materially more work than v3/v4; only worth it if real
    `.tar.bz2` jobs show up. (Alternative: a faster/parallel bzip2 seek
    library that jumps on short forward seeks, if one appears.)
- **Single-block `.tar.xz` — physically unseekable.** The default `xz` /
  `tar -J` emits one block, which has no interior seek point in the
  encoding. Nothing to fix in our code — the archive must be re-compressed
  multi-block (`xz --block-size=…` / `xz -T0`). Detected up front (block
  count from the footer) and refused.
- **`.tar.zst` — no fileobj-friendly seek library.** The only zstd seek-index
  library (`indexed_zstd`) requires a real filename / fd and won't accept our
  streaming `SeekableS3Object`; the fileobj-friendly zstd readers are
  forward-only. Would need the zstd seekable-frame format + our own seek-table
  reader — separate, larger work.

## Resumable extract: within-member resume

`--resume` is **member-atomic** — the finest checkpoint is a member boundary,
because each member is one all-or-nothing `upload_fileobj`. So the re-processing
bound on resume is `max(~1 GB, largest in-flight member)`: die 3.1 GB into a
3.2 GB member and the resume redoes the whole 3.2 GB.

Doing better — resuming *within* a huge member — would mean running our own S3
multipart upload and checkpointing the completed part list + `UploadId`, then
re-opening that upload on resume. Materially more complex: manual multipart
lifecycle, orphaned-upload cleanup (incomplete multiparts cost money and need a
lifecycle policy or explicit abort), and a second progress artifact beyond the
destination-object ledger.

Only worth it if archives routinely contain **individual files in the many-GB
range** — still unconfirmed whether the real workloads do. Until that's known,
member-atomic is the right stopping point.

## Resumable-extract: stale destination cleanup

Context: the planned resumable-extract feature writes a control object
to the destination prefix recording the source archive's identity
(`ETag` + size). On re-run it resumes only if the source is unchanged;
if the source *changed*, it does not resume.

Open question this defers: **what to do with destination objects left
over from a previous, different archive** extracted to the same prefix.

- **Current / baseline behavior:** treat it as the user's problem —
  overwrite the members the new archive contains, and leave any
  now-orphaned objects from the old archive in place. (This matches how
  extract behaves today: it overwrites colliding keys and never deletes.)
- **Someday:** offer real cleanup, but only behind an explicit
  `--force` (or `--overwrite` / `--delete-stale`) flag — never delete by
  default.
- **Fanciest version:** make the collision *content-aware* — detect
  whether a colliding destination object actually differs from what the
  new archive would write (size/hash), and only overwrite/replace what
  genuinely changed, leaving identical objects untouched. Avoids
  needless re-transfer and needless churn.

Deferred because safe deletion of destination data is a materially
riskier feature than the resume itself, and the baseline (overwrite,
leave orphans) is acceptable.
