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

### Resumable stream on transient drops

tar and zip both decode strictly forward, so the sequential `Body`
read can survive an isolated mid-stream connection drop
(`ResponseStreamingError` / `IncompleteRead` — the class of failure
Kopah/RadosGW throws on long transfers). Instead of the raw `Body`,
the decoder reads from `members._resumable_body_chunks`, which tracks
the byte offset it has already emitted and, on a transient error,
re-issues `get_object(Range="bytes=<pos>-")` and continues. The
decoder sees one continuous, correct byte stream and never learns the
HTTP connection was replaced — the break happens *during* `read()`,
before the chunk is yielded downstream, so the resume offset is exact
(no gap, no overlap).

The retry budget is on *consecutive* failures with no forward
progress: any successful chunk zeroes the counter, so a long extract
that survives several isolated hiccups over hours isn't killed by a
total-attempt cap, while a genuinely dead endpoint still gives up
after `retry_max_attempts`. This mirrors the ranged-GET retry on the
seekable 7z path (`seven_z.SeekableS3Object`); both share one policy
in `s3_archive.retry`.

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

## .7z — the exception that proves the rule

`.7z` cannot be decoded forward-only: the 32-byte SignatureHeader at
the front references a metadata block at the tail, and the body
decoder pipeline (LZMA2, BCJ, etc.) lives in that tail header. So
unlike tar/zip, the 7z path needs a *seekable* view of the archive.

`s3_archive.seven_z.SeekableS3Object` provides one as an
`io.RawIOBase` over ranged `GetObject` calls, with a one-time
~4 MB tail prefetch held in memory for the duration of the extract.
That gets wrapped in `io.BufferedReader` (1 MB buffer) and handed
to `py7zr.SevenZipFile`. The buffer coalesces the chatty
field-by-field reads of the SignatureHeader; the tail prefetch
keeps the trailing header in RAM so the parse doesn't round-trip.
Per-Folder body reads are large and bypass the buffer naturally —
net network volume is the same as for tar/zip, just split across a
small number of `Range:` GETs.

py7zr's output side uses a push-style `WriterFactory` API, which
doesn't fit the project's pull-style `ArchiveMember` iterator. The
bridge is a worker thread driving `SevenZipFile.extractall` against
a factory that hands out per-member `os.pipe()` write-ends; the main
generator pulls `(filename, read_fd)` metadata off a queue and yields
one `ArchiveMember` whose chunk iterator reads from the pipe. The
worker thread sees natural backpressure from the pipe buffer
(default 64 KB on Linux), and broken-pipe semantics surface
consumer-side abandonment cleanly. See `src/s3_archive/seven_z.py`
for the fd-ownership details.

`.7z` **create** is not supported. The SignatureHeader needs
`NextHeaderOffset`/`Size`/`CRC` values that aren't known until the
body and trailing header are written, and S3 multipart's 5 MB
minimum part size makes "patch the first 32 bytes after the fact"
impractical.

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
