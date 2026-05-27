# Multi-config / cross-environment plan

Plan for adding named-profile credential support, cross-environment
operations (archive in one S3, extracted tree in another), and
consolidating duplicated `s3_client` code into s3-archive as the
single canonical implementation.

Cross-repo: changes land in **s3-archive** and **s3-bagit**.

## Motivation

Two distinct problems, addressed together because they share the same
refactor surface:

1. **Cross-environment workflows.** UW Libraries' Preservation team
   stores bags in AWS but extracts to Ceph/RGW (Kopah). Today the
   tools assume one S3 endpoint per invocation. The streaming model
   has no architectural objection to two endpoints — the boto3
   client used for reads is independent of the one used for
   writes — but the CLI surface, URL parsers, credential resolver,
   and `config` command all currently bake in the one-endpoint
   assumption.

2. **Duplicated `s3_client` code.** `s3-bagit/src/s3_bagit/s3_client.py`
   and `s3-archive/src/s3_archive/s3_client.py` are near-identical
   (the only diff is the `ConfigError` import path). The same
   duplication exists for `config_cmd.py` (currently only in
   s3-bagit). With s3-bagit already depending on s3-archive, the
   canonical home for both is s3-archive.

## URL shape

The profile lives with the URL, not as a separate flag, so each
endpoint of an operation carries its own addressing:

```
profile:s3://bucket/key
```

- `aws-preservation:s3://bags/x.tar.gz` — explicit profile.
- `s3://bags/x.tar.gz` — bare; resolves as `default`.
- Split rule: if the input contains the literal `:s3://`, the
  substring before the first `:` is the profile name.
- Profile name grammar: `[A-Za-z0-9_-]+`.

Rejected alternatives:

- `--src-profile NAME` flags: separates profile from URL → users
  juggle two tokens that semantically belong together.
- `s3+profile://...` custom scheme: parseable by `urllib.parse` with
  no special-casing, but the `s3+` prefix is visual noise on every
  URL.
- `s3://profile@bucket/key`: free `urllib.parse` support via the
  userinfo slot, but reads like an embedded credential and the `@`
  is easy to miss.

## Credential storage

Per-profile s3cmd INI files:

```
~/.s3cfg            # 'default' profile (today's behavior, unchanged)
~/.s3cfg-<name>     # named profile, e.g. ~/.s3cfg-aws-preservation
```

Each file is a vanilla single-`[default]`-section s3cmd INI — the
same shape s3cmd itself reads. An operator can drive a named profile
with `s3cmd --config=~/.s3cfg-<name>` directly if they want.

Rejected alternatives:

- AWS-style `~/.aws/credentials` + `~/.aws/config` profiles. boto3
  supports these natively (`Session(profile_name=...)`, `endpoint_url`
  per profile since boto3 1.28). The cost is dropping the s3cmd-INI
  alignment that's been the project's convention.
- Multi-section single `~/.s3cfg`. Python's `configparser` reads it
  fine, but s3cmd itself only honors `[default]`, so non-default
  sections would silently be s3-archive-specific despite the file
  looking like s3cmd's. Confusing.

## Resolver chain

`s3_archive.s3_client.load_client(profile: str = "default")`:

- `profile == "default"` — existing chain, unchanged:
  `$S3CMD_CONFIG` → `~/.s3cfg` → boto3 default chain (with optional
  `$S3_ENDPOINT_URL`).
- `profile == "<name>"` — reads `~/.s3cfg-<name>` only. If absent,
  `ConfigError` with a one-line "run `s3-archive config --profile
  <name>`" hint. No fallback: an explicit profile means an explicit
  file.

A `client_for(profile)` cache (keyed by profile name) lets the same
process reuse a client across many calls without re-parsing the INI
or re-handshaking with the endpoint.

## `config` command

Moves from `s3-bagit/src/s3_bagit/config_cmd.py` to
`s3-archive/src/s3_archive/config_cmd.py`. Signature:

```python
def run_config(*, tool_name: str = "s3-archive", profile: str = "default") -> int
```

- `tool_name` only affects user-facing prompt strings — preserves
  s3-bagit's "Configure S3 credentials for s3-bagit." header when
  s3-bagit dispatches into it.
- `profile == "default"` writes `~/.s3cfg` (today's behavior).
- `profile == "<name>"` writes `~/.s3cfg-<name>`.
- Profile-name validation (`[A-Za-z0-9_-]+`) happens at CLI parse
  time so an invalid name doesn't get as far as a connection test.

CLI surface:

```
s3-archive config                       # writes ~/.s3cfg
s3-archive config --profile aws-prsv    # writes ~/.s3cfg-aws-prsv
s3-bagit  config [--profile NAME]       # thin wrapper → run_config(tool_name="s3-bagit")
```

`questionary` becomes a runtime dependency of s3-archive (moved from
s3-bagit's deps).

## Dual-client refactor

Function signatures:

```python
extract(src_client, dst_client, archive_bucket, archive_key,
        dest_bucket, dest_prefix, fmt, *, dry_run, verbose)

create_tar_gz(src_client, dst_client, source_bucket, source_prefix,
              dest_bucket, dest_key, ...)
create_zip(src_client, dst_client, ...)
```

- `ls` stays single-client (it's read-only against one URL).
- `manifest.py` / `list.py` helpers already take `(client, url)`
  pairs at each call site — no signature change needed; each call
  site picks the right client by side.
- CLI behavior when only one URL has a profile: the other side uses
  `default`. Same-profile both sides → both clients resolve to the
  same cached instance.

## s3-bagit changes

Driven by the consolidation:

1. **Delete `s3-bagit/src/s3_bagit/s3_client.py`.** Import everything
   from `s3_archive.s3_client` instead. `s3_bagit.exceptions.ConfigError`
   gets re-aliased from `s3_archive.exceptions.ConfigError` so existing
   `except ConfigError` sites in s3-bagit keep working.
2. **Delete `s3-bagit/src/s3_bagit/config_cmd.py`.** s3-bagit's
   `config` subcommand becomes:
   ```python
   from s3_archive.config_cmd import run_config
   return run_config(tool_name="s3-bagit", profile=args.profile)
   ```
3. **`verify_against` and other dual-URL bagit paths learn profiles.**
   Same dual-client pattern as `extract` / `create`.
4. **s3-bagit CLI accepts `profile:s3://...` URLs** wherever it
   already accepts `s3://...`.

## Phasing

Five commits, each independently useful:

1. **Move `config` to s3-archive, parameterize for profiles.**
   - New `s3_archive.config_cmd` with `tool_name` + `profile`.
   - New `s3-archive config` subcommand.
   - Add `questionary` to s3-archive deps.
   - s3-bagit's `config_cmd.py` shrinks to a one-line dispatch into
     `s3_archive.config_cmd.run_config`.
   - No behavior change for default-profile operators.

2. **Consolidate `s3_client` into s3-archive.**
   - Delete `s3_bagit.s3_client`; re-export from `s3_archive.s3_client`.
   - `ConfigError` re-aliased in `s3_bagit.exceptions` from
     `s3_archive.exceptions`.
   - Resolver gains the `profile` parameter; `default` chain
     unchanged, named profiles read `~/.s3cfg-<name>`.
   - `client_for(profile)` cache added.

3. **URL parsers learn `profile:s3://...`.**
   - `parse_s3_url` / `parse_s3_prefix` return profile alongside
     bucket/key. Bare URLs → profile `None` → resolves as `default`.
   - Profile-name grammar validated at parse time.
   - Both s3-archive and s3-bagit URL parsers updated (or the s3-bagit
     copy deleted in favor of importing from s3-archive — same logic
     as the `s3_client` consolidation).

4. **Dual-client refactor of `extract` / `create`.**
   - Signatures take `src_client`, `dst_client`.
   - CLI dispatchers build the two clients from the two URLs' profiles.
   - New moto tests exercise the cross-environment path with two
     separate mock endpoints.

5. **s3-bagit catches up.**
   - `verify_against` and any other dual-URL paths learn profiles.
   - s3-bagit CLI accepts profile-prefixed URLs across all subcommands.
   - End-to-end test: bag in one mock endpoint, extracted tree in
     another, `verify` succeeds.

## Backward compatibility

- Bare `s3://bucket/key` URLs continue to work identically.
- Operators with only `~/.s3cfg` see no behavior change.
- The `$S3CMD_CONFIG` env var continues to override `~/.s3cfg` for
  the default profile only — it has no role for named profiles.

## Out of scope

- **Local filesystem endpoints** (e.g. `file:///path/...` as a
  src/dst). The project's stated non-goal of "no local-disk fallback"
  stands.
- **Per-profile boto3 tuning** (region, signature version, addressing
  style). The `~/.s3cfg-<name>` schema covers what UW workflows need;
  the resolver can grow these fields later without a URL-shape change.
- **Profile discovery / listing UI** (`s3-archive config --list`).
  `ls ~/.s3cfg*` does the job; a real listing command can land later
  if operators want it.

## Open questions

- **`config --delete --profile NAME`?** Removing a profile is `rm
  ~/.s3cfg-<name>`. Worth a CLI affordance, or leave it as a shell
  task? Leaning leave-it-out for v1.
- **Migration helper from existing `~/.s3cfg` → named profile?** Same
  question: `cp ~/.s3cfg ~/.s3cfg-<name>` does it. Leaving out for v1.
