# Plan archive

Brief log of major planned changes that have shipped. The full design
docs are not preserved verbatim — the code, tests, and architecture
docs are the authoritative record. Use this file as an index:
"when did X happen, why was it shaped this way, where do I look."

For commit-level detail, `git log` is the source of truth.

## .7z read support — shipped 2026-05-27

Added streaming `.7z` extract and `ls` for the Preservation team's
archives. `create` stays unsupported (the SignatureHeader at byte 0
references metadata at the tail, which is incompatible with
streaming multipart uploads).

- **Why awkward.** `.7z` cannot be decoded forward-only — the body
  decoder pipeline (LZMA2, BCJ, etc.) lives in a trailing header.
- **Approach.** py7zr driven by `seven_z.SeekableS3Object`
  (`io.RawIOBase` over ranged `GetObject` + one-time ~4 MB tail
  prefetch), wrapped in `io.BufferedReader`. A worker thread bridges
  py7zr's push-style `WriterFactory` onto the project's pull-style
  `ArchiveMember` iterator via per-member `os.pipe()`s.
- **Where to look.** `src/s3_archive/seven_z.py` for the
  implementation; `docs/ARCHITECTURE.md` § ".7z — the exception that
  proves the rule" for the streaming-model rationale.
- **Open follow-ups.**
  - Encrypted archives (`SevenZipFile(password=...)`) — py7zr raises
    on open, exception propagates; could add `--password` plumbing
    later.
  - Multi-volume archives (`.7z.001`, `.7z.002`, ...) — the seekable
    adapter is single-key; reject if encountered.
  - Ground-truth check against a real Preservation archive before
    declaring production-ready.

## extract / ls refactor onto iter_archive_members — shipped v0.3.0

Collapsed `extract.py` and `ls.py` onto the single
`iter_archive_members` iterator introduced in v0.2.0, removing
near-duplicate iteration loops for tar / tar.zst / zip.

- **Why bother.** One iteration loop means new formats (`.7z` landed
  next on this groundwork) plug in at one site instead of three.
  Also aligned `extract`'s zip filename decoder onto the
  UTF-8-then-CP437 (PKWARE-correct) path that `manifest.py`
  already used.
- **Behavior change worth flagging.** Zip filename decoding now
  falls back to CP437 on non-UTF-8 names instead of raising. Fix,
  not regression — but caught any pathological-archive tests that
  asserted the old behavior.
- **Where to look.** `src/s3_archive/members.py` is the canonical
  iterator; `src/s3_archive/extract.py` and `src/s3_archive/ls.py`
  are now thin loops over it. Drift guard:
  `tests/test_members.py::test_extract_member_set_matches_iter_archive_members`.
