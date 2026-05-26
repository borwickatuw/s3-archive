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
```

Supported formats: `tar`, `tar.gz` / `tgz`, `tar.bz2` / `tbz2`,
`tar.xz` / `txz`, `tar.zst`, `zip`.

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

Pick one:

- **s3cmd config** — if `~/.s3cfg` already targets your endpoint,
  nothing else to do. Set `$S3CMD_CONFIG` to point at a non-default
  path.
- **AWS-style env vars** — `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`.
  For non-AWS endpoints, also set `S3_ENDPOINT_URL=https://...`.
- **boto3 default chain** — `~/.aws/credentials`, IAM role, AWS SSO,
  etc.

See [`.env.example`](.env.example) for the full resolution order.

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
