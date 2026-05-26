# Architecture

How s3-archive moves bytes around without ever touching local disk.

## The constraint

Archives in S3 are frequently tens of GB, sometimes hundreds.
Workstations don't reliably have the free disk space to download an
archive, operate on it, and re-upload. The whole tool only makes sense
if both operations stream **S3 → S3**.

That single requirement drives most of the code shape below.

## Extract

For both formats, the archive object is fetched from S3 with one
`get_object` call, whose `Body` is a `botocore.response.StreamingBody`.
That body is then handed to a streaming decoder, which yields one
member at a time, and each member is pushed to S3 via
`upload_fileobj`.

```
        ┌──────────────────────────┐
        │ S3                       │
        │  s3://src/archive.tar.gz │
        └────────────┬─────────────┘
                     │  get_object().Body
                     ▼
        ┌──────────────────────────┐
        │ tarfile (or stream_unzip)│
        │   r|gz mode — no seek    │
        └────────────┬─────────────┘
                     │  member-by-member
                     ▼
        ┌──────────────────────────┐
        │ upload_fileobj()         │
        │   per-member multipart   │
        └────────────┬─────────────┘
                     │
                     ▼
        ┌──────────────────────────┐
        │ S3                       │
        │  s3://dest/...           │
        └──────────────────────────┘
```

Nothing is buffered between the decoder and the uploader except the
chunk of the current member being passed through.

### Adapter for non-seekable sources

`boto3.upload_fileobj` dispatches its upload strategy on
`readable()` / `seekable()`. The streaming sources here — `tarfile`'s
`extractfile()` in `r|gz` mode, and `stream_unzip`'s per-member chunk
iterators — are read-once and not seekable, and they don't expose
those two methods at all. Calling `upload_fileobj` on them raw raises
`AttributeError`.

`iter.NonSeekableReader` and `iter.IterableFileobj` add the two
methods (returning `True` and `False` respectively), which steers
boto3 to its `UploadNonSeekableInputManager` path. That path uses
chunked single-part uploads, which is what we want for streaming.

## Create

`create` is the inverse of `extract`: it walks an S3 prefix and emits
a serialized archive (`.tar.gz` or `.zip`) at another S3 key. The
same "nothing on local disk" constraint applies.

For `.tar.gz`, Python's stdlib `tarfile` writes to a file-like
destination. We point it at the write-end of an `os.pipe()`, run that
writer on a worker thread, and have the main thread feed the
pipe's read-end into `client.upload_fileobj`:

```
        ┌──────────────────────────┐
        │ S3 list_objects_v2 +     │
        │ get_object per object    │
        └────────────┬─────────────┘
                     │  body chunks
                     ▼
        ┌──────────────────────────┐
        │ tarfile w|gz             │
        └────────────┬─────────────┘
                     │  os.pipe()
                     ▼
        ┌──────────────────────────┐
        │ worker thread:           │
        │ upload_fileobj(read_end) │
        └────────────┬─────────────┘
                     ▼
        ┌──────────────────────────┐
        │ S3                       │
        │  s3://dest/archive.tar.gz│
        └──────────────────────────┘
```

Broken-pipe semantics make error propagation clean: if the uploader
dies mid-stream, the writer side sees `BrokenPipeError` on its next
flush. The create function joins the worker thread before returning
and re-raises whichever exception fired.

For `.zip`, `stream-zip` exposes a bytes iterable directly — no pipe
needed; the iterable is wrapped in `IterableFileobj` and handed to
`upload_fileobj`.

> Create lands in phase 4 of the extraction plan; the CLI currently
> raises `NotImplementedError` for `create`. The streaming model above
> is the target shape.

## What's deliberately NOT here

- **No local-disk fallback.** If you find yourself wanting one, the
  archive is probably small enough to use plain `aws s3 cp` + local
  `tar`.
- **No BagIt semantics.** Manifests, `Payload-Oxum`, and bag verification
  live in `s3-bagit`, which depends on this library for the streaming
  archive plumbing.
- **No inventory-snapshot fast path.** The snapshot-aware comparator
  that used to live in storage-scripts' `stream_archive` is intrinsic
  to storage-scripts — see that repo's `inventory/` library.
