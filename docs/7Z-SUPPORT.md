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
- **Tail prefetch** — on first open, pull the last few MB into
  memory. The header is almost always there.
- s3fs (`S3FileSystem.open(...)`) already implements this pattern
  with `fill_cache`/`block_size` knobs. Could just depend on it
  rather than write our own. Tradeoff is one more transitive
  dependency (aiobotocore via fsspec) for ~50 lines of code we'd
  otherwise own.

### Buffering — RAM, not disk

The adapter is **in-memory only**. No on-disk cache, no LRU, no
"mini filesystem." Total RAM ceiling on the order of 20 MB:

- ~16 MB **tail prefetch** held as a `bytes` object during the
  bootstrap, freed when extract completes.
- ~1–4 MB **sequential read-ahead** window, refilled as it drains.

This is a **buffer**, not a cache: a cache stores data in case
you want it later, a buffer holds data on its way through. After
the header bootstrap, the access pattern is sequential within
each Folder, so there is nothing worth remembering — we never
re-read bytes we've passed.

> **Load-bearing assumption to verify before committing to this
> design.** py7zr's *interface* requires a seekable file-like
> (it calls `seek()` freely). The claim that its *runtime
> behavior* is sequential-after-bootstrap is reasoning from the
> 7z format (Folder pack streams are decoded linearly) plus
> general knowledge — not verified against py7zr's source. The
> first prototype should instrument the adapter to log every
> `seek` / `read` py7zr issues on a representative archive. If
> py7zr in fact seeks around the body during decode, a forward
> read-ahead is insufficient and the choices become (a) a larger
> in-memory LRU of recent blocks, (b) a disk-backed cache, or
> (c) switch to approach B and drive decompression ourselves.

The only scenario that would warrant a disk cache is unpredictable
random access (e.g. FUSE-mount-style `pread()` from anywhere),
which is not the access pattern py7zr produces. If a future caller
needs that shape, it should be solved at a different layer, not
inside this adapter.

Memory ceiling sanity check for a 250 GB archive: 16 MB tail + 4
MB read-ahead + ~64 MB LZMA2 decoder window + one in-flight
member chunk for `upload_fileobj` ≈ **~100 MB RSS**, the same
order of magnitude as the existing tar.gz path.

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

## Performance — what to expect vs .tar.gz

Rough mental model for a hypothetical 250 GB archive on a 1 Gbps
link, single host:

| Dimension                   | .tar.gz                    | .7z (approach A)                |
| --------------------------- | -------------------------- | ------------------------------- |
| Bytes pulled from S3        | ~250 GB sequential         | ~250 GB sequential + a few KB metadata range GETs |
| Bootstrap round trips       | 1 GetObject                | 2–3 small range GETs            |
| Decompression speed         | gzip ~150–300 MB/s / core  | LZMA2 ~30–80 MB/s / core        |
| Parallelism within archive  | decode is fast, irrelevant | one Folder = one core; solid 7z = ~no parallelism |
| Likely bottleneck           | network                    | CPU (LZMA2 decoder)             |
| Ballpark wall-clock         | ~30–45 min                 | ~60–100 min                     |
| Memory footprint            | small                      | bounded by LZMA2 window (~32–64 MB typical) |
| Per-member upload to S3     | identical                  | identical                       |

The headline number is **roughly 2x wall-clock**, and the cause is
LZMA2 being a denser/slower codec than gzip. It is **not** an
artifact of the streaming-via-range-GET approach. There is no
"per-member round-trip-to-S3 fanout" problem during the body pass:
once we know the Folder layout, we issue one sequential range GET
per Folder (or per contiguous Folder group) and the decoder eats it
linearly.

Places where the gap could blow out — all avoidable:

- **No read-ahead in the seekable adapter.** py7zr's header parser
  is likely to do many small `read`/`seek` calls. Each one becomes
  an HTTP round trip without buffering, and bootstrap alone could
  cost minutes. Mitigation: 1–4 MB read-ahead window + a 16 MB tail
  prefetch on first open. This is the single most important piece
  of the adapter.
- **High-latency S3 endpoint** (cross-region, slow link) **plus
  many small Folders** (non-solid archive with thousands of files).
  Per-request setup latency stacks up. Mitigation: when consecutive
  Folders are physically contiguous in the file (the common case),
  fold them into one range GET.
- **Exotic coder chains** (BCJ2 with interleaved pack streams). Still
  sequential; just more decoder bookkeeping. py7zr handles it;
  marginal CPU hit.

What this means in practice: a 250 GB solid 7z from Preservation is
**technically readable and not painful** — call it "2x slower
extract than the equivalent tar.gz." It's not a category change in
behavior, and there's no architectural cliff to worry about. The
main engineering risk is the bootstrap-roundtrip trap, which is
solved by the adapter's buffering and is an implementation detail,
not a research problem.

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
