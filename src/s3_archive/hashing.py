"""Streaming multi-hash primitives over a bytes source.

Computing several hashes in one streaming pass is cheap; I/O dominates.
The classic shape — MD5 + SHA-1 + SHA-256 in one pass — covers both
fixity and identity matching: MD5 / SHA-1 are present for compatibility
with legacy manifests, while SHA-256 is the identity key.

MD5 and SHA-1 are used here for content-addressed identity and legacy
fixity manifests, not for any security boundary. The
``usedforsecurity=False`` kwarg tells the hashlib backend (and Bandit)
that this is the intended use.

Public surface:

- :func:`triple_hash` / :class:`TripleHash` — the common "md5 + sha1 +
  sha256 in one pass" case.
- :func:`multi_hash` — same shape with an arbitrary algorithm list (for
  BagIt manifests).
- :func:`body_chunks` — adapter to iterate an S3 ``StreamingBody`` (or
  any read(n)-shaped object) as 64 KiB chunks.
- :func:`stream_hash_object` — full path: GET an object and hash it in
  one streaming pass.
- :class:`HashingTap` — read(n)-compatible fileobj that hashes every
  byte returned. Used by callers that hand the fileobj to ``tarfile``
  or ``stream_unzip`` and want the parent-object hash alongside.

These primitives are pure transducers over a byte source — they don't
talk to S3 themselves except where the function signature makes it
explicit. Peak memory per call is ~one chunk regardless of object size.
"""

import hashlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

_DEFAULT_CHUNK_SIZE = 65536


@dataclass(frozen=True, slots=True)
class TripleHash:
    """Hex digests + total byte count from a single streaming md5+sha1+sha256 pass."""

    md5: str
    sha1: str
    sha256: str
    size: int


def triple_hash(chunks: Iterable[bytes]) -> TripleHash:
    """Consume *chunks*; return md5+sha1+sha256 hex digests and total byte count.

    The caller controls chunk size and the source of the chunks (S3
    body, open file, in-memory buffer, etc.). Peak memory is ~one chunk.
    """
    md5 = hashlib.new("md5", usedforsecurity=False)
    sha1 = hashlib.new("sha1", usedforsecurity=False)
    sha256 = hashlib.new("sha256")
    total = 0
    for chunk in chunks:
        md5.update(chunk)
        sha1.update(chunk)
        sha256.update(chunk)
        total += len(chunk)
    return TripleHash(
        md5=md5.hexdigest(),
        sha1=sha1.hexdigest(),
        sha256=sha256.hexdigest(),
        size=total,
    )


def multi_hash(chunks: Iterable[bytes], algorithms: Iterable[str]) -> dict[str, str]:
    """Consume *chunks*; return ``{algorithm: hexdigest}`` for each requested algorithm.

    *algorithms* names anything ``hashlib.new()`` accepts. Useful for
    BagIt verification, where the manifests in use are discovered at
    runtime (e.g. md5+sha256, or sha256+sha512). When *algorithms* is
    empty the returned dict is empty (no bytes are read).
    """
    hashers = {algo: hashlib.new(algo, usedforsecurity=False) for algo in algorithms}
    if not hashers:
        return {}
    for chunk in chunks:
        for hasher in hashers.values():
            hasher.update(chunk)
    return {algo: hasher.hexdigest() for algo, hasher in hashers.items()}


def body_chunks(body, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> Iterator[bytes]:
    """Iterate a read(n)-shaped object (S3 ``StreamingBody``, file, ...) as chunks.

    Stops on the first empty read. The default 64 KiB chunk size matches
    every other streaming-hash call site in this library.
    """
    while True:
        chunk = body.read(chunk_size)
        if not chunk:
            return
        yield chunk


def stream_hash_object(
    client,
    bucket: str,
    key: str,
    algorithms: Iterable[str],
    *,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> dict[str, str]:
    """GET ``s3://bucket/key`` and stream-hash it with every algorithm in *algorithms*.

    Returns ``{algorithm: hexdigest}``. One S3 GET feeds every hasher,
    so a multi-algorithm bag (sha256 + sha512) costs one egress per
    object, not two.
    """
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    return multi_hash(body_chunks(body, chunk_size), algorithms)


class HashingTap:
    """read(n)-compatible fileobj that multi-hashes bytes as they're pulled.

    Wraps an iterator of byte chunks (e.g. ``body_chunks(s3_body)`` or a
    backend's chunk stream) and presents a ``read(n)`` API so
    ``tarfile`` / ``stream_unzip`` / ``boto3.upload_fileobj`` can
    consume it. Every byte returned is fed into the configured hashers
    incrementally.

    After the consumer finishes pulling bytes — and after a final
    :meth:`drain` to absorb any trailing bytes the consumer didn't read
    — :meth:`hexdigests` returns the per-algorithm digests.

    Peak memory is one chunk buffer (~64 KiB) regardless of the
    upstream object size — that's what makes multi-TB archive walks
    inline. The bounded-read loop matches the tarfile contract:
    ``tarfile.addfile(info, tap)`` reads exactly ``info.size`` bytes;
    short reads from the underlying iterator are looped until *n* is
    satisfied or the upstream hits EOF.

    Algorithms default to MD5 + SHA-1 + SHA-256 — the "triple hash"
    used everywhere in inventory and the streaming-archive walk. Pass
    a single-element tuple (``("sha256",)``) for BagIt's per-payload
    hash; tag-file hashing uses the in-memory algorithms directly.
    """

    def __init__(
        self,
        chunks: Iterable[bytes],
        algorithms: Iterable[str] = ("md5", "sha1", "sha256"),
    ) -> None:
        self._chunks = iter(chunks)
        self._buf = bytearray()
        self._eof = False
        self._hashers = {algo: hashlib.new(algo, usedforsecurity=False) for algo in algorithms}
        self._size = 0

    @property
    def size(self) -> int:
        """Total number of bytes hashed so far (post-:meth:`drain`: full upstream size)."""
        return self._size

    def _update_hashes(self, data: bytes) -> None:
        for hasher in self._hashers.values():
            hasher.update(data)
        self._size += len(data)

    def read(self, n: int = -1) -> bytes:
        """Return up to *n* bytes from the wrapped chunk iterator.

        Matches the stdlib ``RawIOBase.read`` contract: ``n=-1`` reads
        until EOF; otherwise returns at most *n* bytes (may return
        fewer at EOF). For ``n >= 0`` the underlying iterator is
        looped until *n* bytes have been buffered or EOF is hit, so
        tarfile's "addfile reads exactly info.size bytes" contract is
        honored even when the upstream yields short chunks. Every byte
        returned is hashed.
        """
        if n is None or n < 0:
            # Read-all path: drain the iterator and the buffer.
            while not self._eof:
                try:
                    chunk = next(self._chunks)
                except StopIteration:
                    self._eof = True
                    break
                self._buf.extend(chunk)
            data = bytes(self._buf)
            self._buf.clear()
            self._update_hashes(data)
            return data

        while len(self._buf) < n and not self._eof:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._eof = True
                break
            self._buf.extend(chunk)

        if n >= len(self._buf):
            data = bytes(self._buf)
            self._buf.clear()
        else:
            data = bytes(self._buf[:n])
            del self._buf[:n]
        self._update_hashes(data)
        return data

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def drain(self) -> None:
        """Consume any remaining bytes (for hash completeness).

        tar.gz parsers stop reading after the last member header but
        leave a tail (padding, trailing blocks). For the parent hash
        to match a straight :func:`triple_hash` of the same bytes, the
        tail must be consumed too. Hashes the unhashed remainder of
        the internal buffer first, then drains the iterator. Call
        before :meth:`hexdigests` / :meth:`finalize_triple`.
        """
        if self._buf:
            data = bytes(self._buf)
            self._buf.clear()
            self._update_hashes(data)
        while not self._eof:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._eof = True
                break
            self._update_hashes(chunk)

    def hexdigests(self) -> dict[str, str]:
        """Return ``{algorithm: hexdigest}`` for every configured algorithm."""
        return {algo: hasher.hexdigest() for algo, hasher in self._hashers.items()}

    def finalize_triple(self) -> TripleHash:
        """Return a :class:`TripleHash` — convenience for the md5+sha1+sha256 default."""
        digests = self.hexdigests()
        return TripleHash(
            md5=digests["md5"],
            sha1=digests["sha1"],
            sha256=digests["sha256"],
            size=self._size,
        )
