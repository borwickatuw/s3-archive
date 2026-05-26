# .7z support — design notes

Scratchpad for figuring out whether and how to support `.7z` in
s3-archive. Status: **exploratory.** Captures the format constraints,
the candidate approaches, library survey, and an early
recommendation. Expect rewrites.

## Why .7z is awkward

The current extract / ls path is **one sequential pass over the
archive body**: `get_object` once, hand the `StreamingBody` to a
decoder, emit members. That works because tar and zip can both be
decoded forward-only:

- `.tar` puts each member's metadata immediately before its bytes.
- `.zip` has a central-directory footer, but `stream-unzip` does
  member-by-member decoding from the **local file headers** in the
  body (the central directory is a redundant index).

`.7z` cannot be decoded that way. Its layout:

```
[SignatureHeader, 32 bytes]
  ├── magic "7z\xbc\xaf\x27\x1c"   (6)
  ├── version                       (2)
  ├── StartHeaderCRC                (4)
  └── StartHeader                  (20)
      ├── NextHeaderOffset  (u64)   ← offset from byte 32
      ├── NextHeaderSize    (u64)
      └── NextHeaderCRC     (u32)

[Pack streams: the actual compressed file data]
  ... contiguous bytes for one or more "Folders" ...

[Header / EncodedHeader, typically at end of file]
  Streams info: which Folders exist, what coders they use,
  which substreams (files) come out, sizes, CRCs, names.
```

The metadata block at `32 + NextHeaderOffset` is what tells you which
bytes in the body are which file. Without it you cannot decode
anything — you don't even know what the coder pipeline is (LZMA2?
BCJ filter? Delta?). So a strict forward-only sequential scan is
impossible: you have to look at the tail before you can decode the
body.

## What 7z actually needs (it's less than it looks)

Crucially, **the body itself does not require random access**. The
header tells you where each pack stream starts and how long it is,
and within each pack stream the decoder pipeline is sequential. So
the minimum-viable random-access pattern is:

1. **Bootstrap** — three small range GETs:
   - bytes `[0, 32)` to parse SignatureHeader → learn header
     location.
   - bytes `[32+NextHeaderOffset, 32+NextHeaderOffset+NextHeaderSize)`
     to grab the header bytes.
   - If the header is itself encoded (the spec allows an
     `EncodedHeader` indirection), decode it. That's a tiny pack
     stream living somewhere known by the StartHeader pointer.
2. **Body** — for each Folder (see below) we need, issue a
   **sequential** range GET for its pack stream(s) and pipe the
   bytes through the decoder pipeline. Pack streams are stored
   contiguously, so this is the same shape as our current tar/zip
   path, just N times in a row.

Bootstrap is small (header is typically well under 1 MB even for
multi-GB archives). The body reads are sequential and bounded by
archive size — same network volume as a single sequential GET, just
split into a handful of `Range:` GETs.

This **is not "single GetObject streaming"** like tar/zip. It is
"small metadata bootstrap + sequential body reads, no disk."

## What "compress parts of files together" means — Folders and solid mode

A 7z archive is organized into **Folders** (this is the 7z spec's
term; it has nothing to do with filesystem directories). A Folder is:

```
one or more pack streams  →  coder pipeline (e.g. LZMA2 → BCJ)
                          →  one or more unpacked substreams
                          →  one or more files
```

- **Non-solid 7z**: one Folder per file. Easy — each file is its own
  independent compressed stream.
- **Solid 7z**: one Folder contains many files concatenated. To get
  file N you must decode files 1…N-1 first.

The Preservation team's archives are likely solid (that's the 7z
default and the reason 7z compresses better than zip on many-small-
files corpora). Solid is **not a blocker** for our streaming model —
once we know the Folder's pack stream location and coder pipeline,
we decode the whole Folder linearly and emit each contained file in
order as its bytes pop out. It only becomes painful if a caller
wants to extract one file out of a large solid Folder: you'd still
have to decode the whole thing up to and including that file. For
"extract everything" and "ls", solid is fine.

## Library survey

Where we'd plug into existing code rather than implement a 7z
parser from scratch.

### py7zr

Pure-Python, actively maintained, the de facto choice.

- API requires a **seekable** `BinaryIO` for read mode. Hands a
  `read`/`seek`/`tell` file-like to its internal parser.
- Has a usable lower-level module, `py7zr.archiveinfo`, which is
  what parses the header out of the file. Potentially we could
  reuse the header parser and drive the actual decompression
  ourselves through stdlib `lzma`.
- Streams output via a `WriterFactory` / `Py7zIO` interface — we'd
  pass a factory that returns objects which forward writes to
  `upload_fileobj`.

Implication: py7zr works with us if we give it a **seekable
file-like over S3**. Building that adapter is straightforward
(range GET on `read` after `seek`).

### libarchive (libarchive-c)

The 7z reader **fundamentally requires** a seek callback —
`archive_read_open_fd` with a pipe-like fd will fail with a fatal
error on 7z (libarchive issue #609, #445). Same constraint as
py7zr but enforced at the C level. We'd register a seek callback
backed by ranged S3 GETs. Probably more invasive than py7zr for
the same outcome.

### LZMA SDK / 7zMain.c

Reference C implementation. Same `Read` + `Seek` + `GetFileSize`
callback interface. Not a serious option in a Python project unless
we bind it ourselves.

### Roll our own (mini-parser)

7z header parsing is gnarly but bounded (~1k lines of Python
based on the spec). Decompression is `lzma.LZMADecompressor` with
`FORMAT_RAW` and explicit filters. Almost certainly **not** the
right first move, but worth knowing the door is open if py7zr
proves too restrictive.

## Adapter: seekable file-like over S3

This is the common substrate for the py7zr and libarchive paths.
Shape:

```python
class SeekableS3Object:
    def __init__(self, client, bucket, key):
        self._client = client
        self._bucket = bucket
        self._key = key
        self._size = client.head_object(...)["ContentLength"]
        self._pos = 0

    def seek(self, offset, whence=0): ...     # update self._pos
    def tell(self): return self._pos
    def read(self, n=-1):
        end = self._size if n < 0 else min(self._pos + n, self._size)
        resp = self._client.get_object(
            Bucket=self._bucket,
            Key=self._key,
            Range=f"bytes={self._pos}-{end-1}",
        )
        data = resp["Body"].read()
        self._pos = end
        return data
    def seekable(self): return True
    def readable(self): return True
```

Refinements:

- **Read-ahead buffer** so `read(8)` followed by `read(8)` doesn't
  cost two round trips. Coalesce sequential reads into chunks of,
  say, 1–4 MB.
- **Tail prefetch** — on first open, pull the last few MB into a
  cache. The header is almost always there.
- s3fs (`S3FileSystem.open(...)`) already implements this pattern
  with `fill_cache`/`block_size` knobs. Could just depend on it
  rather than write our own. Tradeoff is one more transitive
  dependency (aiobotocore via fsspec) for ~50 lines of code we'd
  otherwise own.

## Candidate approaches for `extract` and `ls`

### A. py7zr + seekable-S3 adapter (recommended starting point)

1. Build / borrow a seekable S3 adapter with tail prefetch + small
   read-ahead cache.
2. Hand it to `py7zr.SevenZipFile(file=adapter, mode="r")`.
3. Iterate members; for each, upload via a `WriterFactory` that
   wraps `upload_fileobj` per member.

Pros: smallest code surface; reuses the maintained parser and
decoders; covers every coder combination 7z supports.

Cons: py7zr may issue many small `read`/`seek` calls during header
parsing — without read-ahead this could be hundreds of round
trips. Mitigated entirely by the buffer/prefetch.

### B. py7zr.archiveinfo for header parsing + DIY decompression

1. Tail-prefetch the header.
2. Use `py7zr.archiveinfo` to parse it into Folder/Substream
   structs.
3. For each Folder: one ranged GET of its pack stream(s), piped
   through `lzma.LZMADecompressor(format=lzma.FORMAT_RAW,
   filters=[...])` built from the Folder's coder chain. Split the
   decompressed output into substreams (files) by the sizes the
   header gave us. Upload each as it appears.

Pros: only one range GET per Folder for body bytes (after
bootstrap); maximally aligned with the project's streaming
philosophy.

Cons: more code to maintain, especially around the coder chain
(BCJ, BCJ2, Delta, AES — though AES is encryption, separate issue).
Risk of behavior drift vs. the canonical decoder. Realistic only
after (A) is working as a baseline.

### C. libarchive-c + seek callbacks

Same shape as (A) but backed by libarchive. Adds a C dependency
and the seek callback is in C-callback-ish territory through ctypes.
No obvious advantage over (A) for this project.

### D. Reject .7z and ask Preservation to repackage

The "non-engineering" answer. Has merit if 7z volume is low or if
the team can pipe `7z x | tar c` on their end before handing files
off. Worth a conversation regardless — it eliminates the whole
problem class. But assume for planning purposes that we can't
externally mandate this.

### E. Bounded local-disk fallback

A size-threshold escape hatch: if the .7z is under N GB, spool to
`/tmp`, run py7zr against the local file, upload. Above the
threshold, raise. Conflicts with the project's "nothing on local
disk" stance, but it's strictly easier than (A) and might be a
reasonable shim for the 90th-percentile archive while a proper
streaming implementation lands. Flag as a fallback, not the
target.

## What about `create` (writing .7z to S3)?

Almost certainly **not supported** for the foreseeable future:

- The SignatureHeader at the start needs `NextHeaderOffset` /
  `NextHeaderSize` / CRC, which are only known once the body and
  trailing header have been written.
- 7-Zip itself solves this by seeking back to byte 0 after writing
  the body+header. Multipart S3 uploads can replace parts before
  `CompleteMultipartUpload`, but the 5 MB minimum part size means
  the SignatureHeader (32 bytes) can't live in its own
  small first part without padding the archive.
- The Preservation use case appears to be **extract from .7z**
  (consuming archives produced elsewhere), not **create .7z**.

Recommendation: support extract and ls only; have `create` raise
`UnsupportedArchiveFormatError` for `.7z` with a clear message
pointing to `.tar.gz` or `.zip` instead.

## Early recommendation

Pursue **approach A**: py7zr driven by a seekable-S3 adapter with
a tail prefetch + small read-ahead buffer. Reasons:

- Smallest code we have to own and test.
- Lets the maintained library handle every 7z coder combination.
- The "violation" of the strict sequential-streaming model is
  bounded and well-understood: bootstrap is a few small range
  GETs; body reads are sequential and equal in volume to the
  archive itself.
- If profiling later shows excessive round trips, we have a clear
  escalation path to (B) without throwing away the adapter.

Open questions to settle before writing code:

- Does `py7zr.archiveinfo` expose enough to consider (B) without
  rewriting the parser?
- What does the Preservation team's 7z corpus look like — solid?
  multi-volume? encrypted? AES support would be a separate
  workstream.
- Is s3fs an acceptable dependency, or do we want to keep the
  adapter in-tree?
- How does the seekable-S3 adapter interact with the boto3 client
  the rest of the codebase already builds (s3cmd config / AWS
  chain) — should the adapter take a client, or a bucket+key plus
  config?

## References

- 7z format spec (canonical): https://github.com/ip7z/7zip/blob/main/DOC/7zFormat.txt
- py7zr format notes: https://py7zr.readthedocs.io/en/latest/archive_format.html
- py7zr API: https://py7zr.readthedocs.io/en/latest/api.html
- py7zr advanced (WriterFactory/Py7zIO): https://py7zr.readthedocs.io/en/v1.1.2/advanced.html
- libarchive seek-callback requirement: https://github.com/libarchive/libarchive/issues/609
- s3fs S3File (seekable file-like over S3): https://s3fs.readthedocs.io/en/latest/api.html
