# s3-archive

## Project Overview

Streaming S3 archive operations against any S3-compatible object
storage:

- `extract` — archive (tar / tar.gz / tar.bz2 / tar.xz / tar.zst /
  zip / 7z) in S3 → individual member objects at an S3 prefix.
- `create` — S3 prefix → serialized archive (.tar.gz or .zip) at an
  S3 key. (.7z create is not supported — the SignatureHeader at byte
  0 references a header at the end, which is incompatible with
  streaming multipart uploads.)
- `ls` — stream-list an archive's members without extracting.

Everything streams — nothing is ever staged on local disk. A 500 GB
archive does not need 500 GB of free space anywhere.

Built initially for UW Libraries against Kopah (Ceph RadosGW), but
written to be S3-generic — AWS S3, MinIO, DigitalOcean Spaces, etc.
all work.

### Non-goals

- **Not a generic archive library.** The shape is specifically
  "stream S3 → archive decoder → S3" and "stream S3 prefix → archive
  encoder → S3." Local-disk fallback is out of scope — if you find
  yourself wanting one, the archive is probably small enough to use
  plain `aws s3 cp` + local `tar`.
- **No BagIt code.** Verify, manifest parsing, `Payload-Oxum`,
  `bag-info.txt` semantics live in `s3-bagit`, which depends on this
  library.
- **No inventory snapshot fast path.** The
  `compare_archive_to_path` snapshot-aware optimization that used to
  live in storage-scripts' `stream_archive` is intrinsic to
  storage-scripts; if a caller needs it, it belongs in
  `storage-scripts/inventory/`, not here.

## Related Projects

- **s3-bagit** — BagIt-specific layer on top. Depends on s3-archive
  for extract / list / URL primitives.
- **storage-scripts** — UW Libraries' broader storage tooling.
  `inventory.archive_walker` consumes `s3_archive.manifest` for
  per-entry hashing during single-pass bucket walks.
- **claude-meta** — cross-repo standards and best-practice guides.

## Coding Standards

Follow user preferences in `~/.claude/CLAUDE.md` and cross-repo
guides in `claude-meta/best-practices/`. Project-specific:

- **Credential resolution is the one place we allow multi-source
  fallback.** Order: `$S3CMD_CONFIG` → `~/.s3cfg` → boto3 default
  chain (with optional `$S3_ENDPOINT_URL`). This deliberately
  overrides the global "no fallback logic" preference because the
  chain mirrors s3cmd's own behavior. All other config values still
  follow the strict one-canonical-location rule.
- **Streaming model means single-pass.** Don't add code that requires
  re-reading the archive — see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- **Ceph workaround is always-on.** boto3's
  `request_checksum_calculation="when_required"` is required for Ceph
  RadosGW and harmless on AWS S3, so it's applied unconditionally —
  no flag, no conditional.

## Project Structure

```
src/s3_archive/
    cli.py            argparse entry point (extract, create, ls)
    extract.py        streaming tar + zip extract to S3
    create.py         streaming S3-prefix → tar.gz / zip
    ls.py             stream-list an archive's members
    list.py           paginating list_objects (skip directory markers)
    manifest.py       ManifestEntry + per-entry hashing primitives
                      (consumed by storage-scripts' inventory walker
                      and by s3-bagit's verify path)
    url.py            parse_s3_url, parse_s3_prefix, detect_format
    iter.py           IterableFileobj + NonSeekableReader (the boto3
                      readable/seekable adapters)
    s3_client.py      boto3 client builder (s3cmd config or AWS chain)
    exceptions.py     ConfigError, UnsupportedArchiveFormatError
    log_config.py     tqdm-aware console logger
tests/                pytest + moto (no live S3 required)
docs/
    ARCHITECTURE.md   streaming model and S3-to-S3 design
```

## Commands

```
make install            # uv sync (dev + test deps)
make test               # uv run pytest
make test-cov           # tests + coverage report
make lint               # ruff check + ruff format --check
make format             # ruff format
make security           # bandit + pip-audit
make run ARGS='...'     # invoke s3-archive
```

Direct invocation:

```
uv run s3-archive extract s3://bucket/archive.tar.gz s3://bucket/extracted/
uv run s3-archive create  s3://bucket/src/ s3://bucket/archive.tar.gz
uv run s3-archive ls      s3://bucket/archive.tar.gz
```

Or from a clean shell:

```
uvx --from git+https://github.com/borwickatuw/s3-archive s3-archive --help
```

## Security

`make security` runs:

- `bandit` against `src/`
- `pip-audit` against the lockfile

No secrets are stored in the repo. S3 credentials come from
`$S3CMD_CONFIG`, `~/.s3cfg`, or boto3's default chain (`AWS_*` env vars,
`~/.aws/credentials`, IAM role) — see `.env.example`.

## Cross-Repository Ideas

When you discover patterns, improvements, or ideas that might apply to
other repositories, capture them:

    claude-idea s3-archive "Description of the pattern or improvement"
