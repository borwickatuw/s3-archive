# s3-archive

Streaming S3 archive operations against any S3-compatible object
storage. Nothing ever touches local disk — a 500 GB archive does not
need 500 GB of free space anywhere. Works against AWS S3, UW Libraries'
Kopah, MinIO, DigitalOcean Spaces, Backblaze B2, Wasabi — anything that
speaks S3.

```
s3-archive extract <archive_url> <dest_prefix_url>   # archive in S3 → members at an S3 prefix
s3-archive create  <src_prefix_url> <archive_url>    # S3 prefix → archive in S3
s3-archive ls      <archive_url>                     # peek inside an archive without extracting
s3-archive config  [--profile NAME]                  # interactive S3 credentials setup
```

URLs accept an optional `profile:` prefix
(`profile_name:s3://bucket/key`) for multi-provider workflows — see
[Multiple S3 providers](#multiple-s3-providers) below.

Supported formats: `tar`, `tar.gz` / `tgz`, `tar.bz2` / `tbz2`,
`tar.xz` / `txz`, `tar.zst`, `zip`, `7z` (extract / ls only — `.7z`
create is not supported; use `.tar.gz` or `.zip`).

## Quick start

### Install [uv](https://docs.astral.sh/uv/)

```
curl -LsSf https://astral.sh/uv/install.sh | sh        # macOS / Linux
irm https://astral.sh/uv/install.ps1 | iex             # Windows PowerShell
```

### Run it from the published git URL

```
uvx --from git+https://github.com/borwickatuw/s3-archive s3-archive --help
```

Or install as a `uv` tool:

```
uv tool install git+https://github.com/borwickatuw/s3-archive
s3-archive --help
```

### Credentials

The easy path is `s3-archive config` — interactive prompts for
endpoint URL, access key, and secret key, then writes them to a
standard `~/.s3cfg` file (s3cmd-compatible, `0600` perms). Press
Enter on the endpoint prompt for AWS S3 defaults.

If you'd rather wire it up manually, pick one:

- **s3cmd config** — if `~/.s3cfg` already targets your endpoint,
  nothing else to do. Set `$S3CMD_CONFIG` to point at a non-default
  path.
- **AWS-style env vars** — `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`.
  For non-AWS endpoints, also set `S3_ENDPOINT_URL=https://...`.
- **boto3 default chain** — `~/.aws/credentials`, IAM role, AWS SSO,
  etc.

See [`.env.example`](.env.example) for the full resolution order.

### Multiple S3 providers

If you work across more than one S3-compatible provider — say, AWS for
archival and Kopah (Ceph/RGW) for working copies — give each provider
a **profile** and prefix URLs with the profile name. One interactive
`config` run per profile:

```bash
s3-archive config                       # default profile → ~/.s3cfg
s3-archive config --profile kopah       # → ~/.s3cfg-kopah
s3-archive config --profile aws-prsv    # → ~/.s3cfg-aws-prsv
```

Then use the profile in URLs. The two URLs in a command don't have to
share a profile — that's the whole point of cross-endpoint workflows:

```bash
# Extract a bag from AWS-Preservation into Kopah:
s3-archive extract aws-prsv:s3://bags/x.tar.gz kopah:s3://extracted/

# `ls` and `create` accept the same syntax:
s3-archive ls aws-prsv:s3://bags/x.tar.gz
s3-archive create kopah:s3://src/ aws-prsv:s3://archives/snapshot.tar.gz
```

A bare `s3://...` URL uses the `default` profile. Profile names must
match `[A-Za-z0-9_-]+` (letters, digits, underscore, hyphen). Each
`~/.s3cfg-<name>` file is a complete s3cmd INI, so `s3cmd` itself can
read one with `s3cmd --config=~/.s3cfg-kopah`.

Named profiles read **only** `~/.s3cfg-<name>` — `$S3CMD_CONFIG` and
the boto3 default chain are part of the *default profile's*
resolution only and are intentionally ignored for named profiles, so
an unrelated env var can't accidentally hijack one. If `~/.s3cfg-<name>`
is missing, the error message includes the exact command to fix it
(`s3-archive config --profile <name>`).

## Common tasks

### Extract an archive

```
s3-archive extract s3://my-bucket/incoming/snapshot.tar.gz s3://my-bucket/extracted/
```

Streams the archive out of S3, decodes on the fly, and writes each
member back to the destination prefix.

### List an archive's contents

```
s3-archive ls s3://my-bucket/incoming/snapshot.tar.gz
```

Useful as a sanity check before a multi-GB extract.

### Create an archive

```
s3-archive create s3://my-bucket/source-dir/ s3://my-bucket/archives/snapshot.tar.gz
```

The destination URL's extension determines the format (`.tar.gz`,
`.tgz`, or `.zip`).

## Library use

`s3-archive` is also a Python library:

```python
import boto3
from s3_archive import extract, list_archive, detect_format, parse_s3_url

client = boto3.client("s3")
extract(client, "src-bucket", "incoming/snapshot.tar.gz", "dest-bucket", "extracted/", "tar.gz")
list_archive(client, "src-bucket", "incoming/snapshot.tar.gz", "tar.gz")
```

The streaming-extract / streaming-list primitives, the
`IterableFileobj` / `NonSeekableReader` adapters, and the
`s3_archive.manifest` per-entry hashing primitives (phase 3) are
designed to be reusable from downstream code — `s3-bagit` and UW
Libraries' `storage-scripts` are the two known consumers.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the streaming
model and the rationale behind the `os.pipe()` writer-thread shape.

## License

BSD 3-Clause — see [`LICENSE`](LICENSE).
