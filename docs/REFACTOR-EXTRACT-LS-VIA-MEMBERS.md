# Refactor extract / list_archive to use iter_archive_members

Status: **planning** — work has not started.

## Context

v0.2.0 introduced `s3_archive.members.iter_archive_members` as the
canonical way to walk archive entries (tar family + tar.zst + zip).
The original consumer was s3-bagit's `verify-against`; inventory's
single-pass walk uses `s3_archive.manifest` builders (which are also
member-iteration loops under the hood). But `s3_archive.extract` and
`s3_archive.ls` still carry their own iteration code from before
`iter_archive_members` existed:

- `extract.py`:
  - `extract_tar` opens tarfile manually, loops `for member in tar:`,
    handles the "is regular file?" predicate, and uploads via
    `NonSeekableReader(tar.extractfile(member))`.
  - `extract_zip` runs `stream_unzip` directly, decodes filenames,
    skips directory entries, drains in the dry-run branch, uploads
    via `IterableFileobj(chunks)`.
  - Owns the private `_TAR_MODES` dict and `_CHUNK_SIZE` constant.
- `ls.py`:
  - `_list_tar` and `_list_zip` are near-copies of the extract
    versions, minus the upload step and plus a per-entry size print.
  - Imports `_CHUNK_SIZE` and `_TAR_MODES` from `extract` (the only
    cross-module use of those privates).

After this refactor:

- `extract_tar` / `extract_zip` collapse to one `extract` body that
  loops over `iter_archive_members(client, bucket, key, fmt)` and
  uploads each member.
- `_list_tar` / `_list_zip` collapse to one `list_archive` body that
  loops over the same iterator and prints per-member.
- `_TAR_MODES` and `_CHUNK_SIZE` are removed from `extract.py` (they
  live in `members.py` now, and only the dispatcher needs them).

## Why bother

Three small wins:

1. **One iteration loop.** When `.7z` support arrives, or `.tar.lz4`,
   or whatever, you add it in one place (`_iter_tar_members` or a new
   `_iter_*_members`) instead of patching three near-identical loops.
2. **Aligned semantics.** Today `extract_zip` decodes filenames as
   UTF-8 with a silent fallback to "treat the bytes as text";
   `iter_archive_members` uses the UTF-8-then-CP437 pattern from
   `s3_archive.manifest._decode_zip_filename` (the PKWARE-correct
   one). The refactor aligns extract/ls onto the correct decoder.
3. **Smaller surface.** `_TAR_MODES` and `_CHUNK_SIZE` stop being
   cross-module imports; `ls.py` loses its dependency on `extract.py`
   internals.

Not in scope: behavior changes to `extract` or `list_archive` output.
This is a pure internal refactor.

## Overall approach

- **One phase, one commit.** No tag bump needed if downstream
  consumers don't notice — and they shouldn't, since the public
  `extract` / `list_archive` signatures don't change. If any
  observable behavior shifts (e.g. zip filename decoding on a
  pathological archive), bump to v0.3.0 with a one-line release note.
- **Reuse existing fixtures.** The moto round-trip tests in
  `tests/test_extract.py`, `tests/test_ls.py`, and `tests/test_members.py`
  already cover every supported format. Add a parametrized cross-check
  that the count + total bytes from `list_archive` equal those
  reachable by manually summing `iter_archive_members(...)` for the
  same archive — that's the load-bearing assertion against drift.

## Per-file changes

### `src/s3_archive/extract.py`

```python
# Before: extract_tar opens tarfile; extract_zip runs stream_unzip.
# After: one extract() body over iter_archive_members.

from s3_archive.iter import IterableFileobj
from s3_archive.members import iter_archive_members

def extract(
    client, archive_bucket, archive_key,
    dest_bucket, dest_prefix, fmt,
    *, dry_run=False, verbose=False,
) -> list[str]:
    member_names: list[str] = []
    for member in iter_archive_members(client, archive_bucket, archive_key, fmt):
        member_names.append(member.name)
        if dry_run:
            member.drain()
            if verbose:
                log.info("  would write %s", member.name)
            continue
        dest_key = _dest_key(dest_prefix, member.name)
        if verbose:
            log.info("  %s -> s3://%s/%s", member.name, dest_bucket, dest_key)
        client.upload_fileobj(IterableFileobj(member.chunks()),
                              dest_bucket, dest_key)
    log.info("extract %s: %d files",
             "(dry-run)" if dry_run else "complete", len(member_names))
    return member_names
```

`extract_tar` and `extract_zip` become thin wrappers around `extract`
for backward compat, OR are deleted with a deprecation note (they
are not in the public `__all__`). Recommend deletion — they were
implementation details. `_TAR_MODES` and `_CHUNK_SIZE` deleted.

### `src/s3_archive/ls.py`

```python
# Before: _list_tar / _list_zip / list_archive dispatcher.
# After: one list_archive() body.

from s3_archive.members import iter_archive_members

def list_archive(client, archive_bucket, archive_key, fmt) -> tuple[int, int]:
    count = 0
    total = 0
    for member in iter_archive_members(client, archive_bucket, archive_key, fmt):
        observed = 0
        for chunk in member.chunks():
            observed += len(chunk)
        # member.size is what the format reported; observed is what we
        # actually streamed. For zips where size comes from the local
        # file header (sometimes 0), the observed count is the truth.
        member_size = member.size if member.size else observed
        _print_entry(member_size, member.name)
        count += 1
        total += member_size
    print(f"{count} files, {_format_size(total)}")
    return count, total
```

`_list_tar`, `_list_zip` deleted. The `from s3_archive.extract import
_CHUNK_SIZE, _TAR_MODES` line vanishes.

### Tests

`tests/test_extract.py`, `tests/test_ls.py`: behavior tests stand
as-is — they assert observable output, not internal structure.

Add to `tests/test_members.py` (or a new `tests/test_consistency.py`):

```python
def test_extract_count_matches_members(s3_client, tar_gz_archive):
    """extract() returns the same member set iter_archive_members yields."""
    extracted = extract(s3_client, "src", "archive", "dest", "out/", "tar.gz")
    via_members = [m.name for m in iter_archive_members(s3_client, "src", "archive", "tar.gz")]
    assert extracted == via_members  # plus or minus dry-run filtering
```

## Risks and watch-items

- **`extract_zip` filename decoder change.** Current code does
  `name.decode("utf-8")` with no fallback path for non-UTF-8 names.
  After this refactor it uses UTF-8 → CP437 (per PKWARE). Most zips
  in the wild that aren't UTF-8 are CP437, so this is a fix, not a
  regression — but any existing test asserting a specific decode
  failure pattern needs updating. Likely none.
- **`extract_tar` tar_mode parameter.** Today its signature takes
  `tar_mode` ("r|gz" etc.); after the refactor it should take the
  format string ("tar.gz"). Either delete the public `extract_tar`
  (recommended; not in `__all__`) or keep a shim that maps the old
  arg. The plan recommends deletion — calling code uses
  `extract(..., fmt=...)`, which already takes the format string.
- **`list_archive` zip size fallback.** The current `_list_zip`
  falls back to observed bytes when `stream_unzip` reports `None`
  for size. The refactor preserves this — `ArchiveMember.size` is
  whatever the format reported (possibly 0/None), and the caller
  observes during drain. Keep the same behavior; just lift the loop.
- **Public API surface.** Confirm nothing outside the repo imports
  `extract_tar` / `extract_zip` / `_list_tar` / `_list_zip` / the
  private constants. ripgrep across s3-bagit and storage-scripts
  before deleting.

## Verification

```bash
cd ~/code/s3-archive && make check    # green
grep -rE "extract_tar|extract_zip|_TAR_MODES|_CHUNK_SIZE" \
   ~/code/s3-bagit ~/code/storage-scripts --include="*.py" | grep -v .venv
# expected: no hits outside s3-bagit/s3-archive themselves
```

## Things explicitly NOT in scope

- No public API change to `extract` / `list_archive`. Same signatures,
  same return shapes.
- No new archive format support. Adding `.7z` is a separate plan
  (`docs/7Z-SUPPORT.md`).
- No CLI changes — `s3-archive extract` and `s3-archive ls` behave
  identically.
- No tag bump unless the zip-decoder change surfaces a regression.

## Cost estimate

~2 hours, one commit, one PR. Almost all of the work is deleting
code and confirming tests still pass.
