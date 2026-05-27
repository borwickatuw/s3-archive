# Plan archive

Brief log of major planned changes that have shipped. The full design
docs are not preserved verbatim — the code, tests, and architecture
docs are the authoritative record. Use this file as an index:
"when did X happen, why was it shaped this way, where do I look."

For commit-level detail, `git log` is the source of truth.

## Multi-config / cross-environment refactor — shipped 2026-05-27

Cross-repo refactor (s3-archive + s3-bagit). Added named-profile
credential support (`profile:s3://bucket/key` URLs backed by
`~/.s3cfg-<name>` INI files), dual-client `extract` / `create` /
`verify_against` / `create_bag` so reads and writes can target
different endpoints (UW Libraries Preservation: bag in AWS, extracted
tree in Kopah), and consolidated the previously-duplicated
`s3_client` and `config_cmd` modules into s3-archive as the
canonical implementation. s3-bagit now consumes both.

- **Why bother.** Operators wanted one CLI invocation that crosses
  endpoints. The streaming model already accommodated it (read-side
  and write-side clients are textually independent), so the work was
  surface-level — URL grammar, credential resolver, CLI dispatcher —
  not a re-architecture. Same refactor also collapsed two
  near-duplicated modules across the repos.
- **Approach.** Five commits across two repos:
  - **s3-archive** `eee2d77` — phase 1: `config_cmd` ported from
    s3-bagit; `s3-archive config --profile NAME`.
  - **s3-archive** `c415a46` — phase 2: `load_client(profile=...)`,
    `client_for(profile)` cache, `_reset_client_cache` test seam.
  - **s3-archive** `d8909f5` — phase 3: `ParsedS3Url` `NamedTuple` +
    `profile:s3://` parsing. Hard break of the 2-tuple shape.
  - **s3-archive** `fff7fc1` — phase 4: dual-client
    `extract(src_client, dst_client, ...)` and `create(...)`; CLI
    builds both via `client_for` up-front so a missing profile fails
    fast.
  - **s3-bagit** `0cce13e` — adopts all of the above: deletes
    `s3_bagit/s3_client.py`, shrinks `config_cmd` to a 5-line shim,
    re-aliases `ConfigError` from s3-archive, propagates ParsedS3Url
    + dual-client through `verify_against` / `create_bag` /
    `_cmd_extract`, adds the cross-endpoint acceptance test.
- **Design decisions worth remembering.**
  - **Named profile ≠ default profile chain.** A named profile reads
    ONLY `~/.s3cfg-<name>` — `$S3CMD_CONFIG` is part of the *default*
    profile's chain and is deliberately ignored for named profiles.
    Keeps profile semantics auditable (one file per profile).
  - **URL split rule** is "first `:s3://` at index > 0". That means a
    key with `:s3` in it (like `s3://b/with:s3/foo`) is NOT misparsed
    as a profile prefix; this is tested.
  - **`client_for(None)` canonicalises to `"default"`** at the lookup
    layer so callers don't have to know whether the URL carried an
    explicit prefix.
  - **Two test fixtures, two purposes.** `cross_env_clients` (mocks
    the resolver, varies bucket names per side) is the fast workhorse;
    `cross_env_real_endpoints` spins up two `ThreadedMotoServer`
    instances on ephemeral ports for the one acceptance test per
    module that exercises the real wiring.
- **Where to look.** s3-archive: `src/s3_archive/url.py`
  (`ParsedS3Url`), `src/s3_archive/s3_client.py` (`client_for`,
  `_client_cache`), `src/s3_archive/config_cmd.py` (ported from
  s3-bagit, parameterised by `tool_name`/`profile`), `tests/conftest.py`
  (autouse cache-reset + cross-env fixtures). s3-bagit:
  `tests/test_cross_endpoint.py` is the end-to-end acceptance test.
- **Open follow-ups.**
  - s3-bagit's `[tool.uv.sources]` currently uses a local path source
    (`{ path = "../s3-archive", editable = true }`). At release time,
    tag s3-archive (v1.1.0 = phases 1-3, v1.2.0 = phase 4), push,
    then switch the s3-bagit pin to `{ git = "...", tag = "v1.2.0" }`.
    s3-bagit's commit message also calls this out.
  - Released version numbering follows the plan: s3-archive
    v1.1.0/v1.2.0, s3-bagit v1.2.0 (the consolidation + dual-client
    work combined into one s3-bagit commit since the local s3-archive
    already had phase 4 by the time s3-bagit work started).

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
