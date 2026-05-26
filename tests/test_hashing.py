"""Tests for s3_archive.hashing primitives.

Known-vector coverage for :func:`triple_hash`, :func:`multi_hash`, and
:func:`stream_hash_object`; behavioral coverage for
:class:`HashingTap`'s ``read(n)`` / ``drain`` contract — drained tap
output must equal a straight :func:`triple_hash` of the same bytes.
"""

import hashlib
import io

from s3_archive.hashing import (
    HashingTap,
    TripleHash,
    body_chunks,
    multi_hash,
    stream_hash_object,
    triple_hash,
)

# Known-vector test fixtures
_HELLO = b"hello"
_HELLO_MD5 = "5d41402abc4b2a76b9719d911017c592"
_HELLO_SHA1 = "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
_HELLO_SHA256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

_EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
_EMPTY_SHA1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class TestTripleHash:
    def test_empty(self):
        result = triple_hash(iter([]))
        assert result == TripleHash(
            md5=_EMPTY_MD5,
            sha1=_EMPTY_SHA1,
            sha256=_EMPTY_SHA256,
            size=0,
        )

    def test_known_vector_single_chunk(self):
        result = triple_hash([_HELLO])
        assert result.size == 5
        assert result.md5 == _HELLO_MD5
        assert result.sha1 == _HELLO_SHA1
        assert result.sha256 == _HELLO_SHA256

    def test_chunked_equals_single_blob(self):
        data = b"the quick brown fox jumps over the lazy dog"
        a = triple_hash([data])
        b = triple_hash([data[:5], data[5:20], data[20:]])
        assert a == b

    def test_large_synthetic(self):
        chunk = b"A" * 1024
        n_chunks = 16 * 1024  # 16 MiB total
        result = triple_hash(chunk for _ in range(n_chunks))
        assert result.size == 16 * 1024 * 1024
        assert result.sha256 == hashlib.sha256(b"A" * result.size).hexdigest()


class TestMultiHash:
    def test_empty_algorithms(self):
        # No algorithms requested => no work done, no bytes read.
        consumed = []

        def gen():
            for chunk in [b"abc", b"def"]:
                consumed.append(chunk)
                yield chunk

        result = multi_hash(gen(), [])
        assert result == {}
        assert consumed == []

    def test_single_algorithm(self):
        result = multi_hash([_HELLO], ["sha256"])
        assert result == {"sha256": _HELLO_SHA256}

    def test_multiple_algorithms_single_pass(self):
        # md5 + sha256 over "hello"
        result = multi_hash([_HELLO], ["md5", "sha256"])
        assert result == {"md5": _HELLO_MD5, "sha256": _HELLO_SHA256}

    def test_sha512_supported(self):
        result = multi_hash([_HELLO], ["sha512"])
        assert result == {"sha512": hashlib.sha512(_HELLO).hexdigest()}


class TestBodyChunks:
    def test_emits_chunks_until_eof(self):
        body = io.BytesIO(b"X" * (65536 + 100))
        chunks = list(body_chunks(body))
        assert len(chunks) == 2
        assert len(chunks[0]) == 65536
        assert len(chunks[1]) == 100

    def test_custom_chunk_size(self):
        body = io.BytesIO(b"abcdefgh")
        chunks = list(body_chunks(body, chunk_size=3))
        assert chunks == [b"abc", b"def", b"gh"]


class TestStreamHashObject:
    def test_round_trip(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="obj", Body=_HELLO)
        digests = stream_hash_object(s3_client, "src-bucket", "obj", ["md5", "sha256"])
        assert digests == {"md5": _HELLO_MD5, "sha256": _HELLO_SHA256}


class TestHashingTap:
    """The HashingTap is the load-bearing primitive for inline archive walks.

    Its drained output must match a straight :func:`triple_hash` of the
    same bytes, even when the consumer reads less than the whole body
    before drain().
    """

    def test_drain_full_bytes_matches_triple_hash(self):
        data = b"This is the parent archive's bytes. " * 1000
        chunks = [data[i : i + 64] for i in range(0, len(data), 64)]

        tap = HashingTap(iter(chunks))
        # Consume some bytes through read() to exercise the tap path.
        first = tap.read(100)
        assert len(first) == 100
        # Drain the rest.
        tap.drain()

        digests = tap.hexdigests()
        expected = triple_hash([data])
        assert digests["md5"] == expected.md5
        assert digests["sha1"] == expected.sha1
        assert digests["sha256"] == expected.sha256
        assert tap.size == len(data)

    def test_drain_after_partial_buffer(self):
        # Buffer holds bytes pulled but not returned; drain() must
        # hash those before consuming the iterator.
        chunks = [b"AAAA", b"BBBB", b"CCCC", b"DDDD"]
        tap = HashingTap(iter(chunks))
        # n=2 leaves "AA" in the buffer's tail
        first = tap.read(2)
        assert first == b"AA"
        tap.drain()
        expected = triple_hash([b"AAAABBBBCCCCDDDD"])
        assert tap.hexdigests()["sha256"] == expected.sha256
        assert tap.size == 16

    def test_read_n_returns_exactly_n_bytes_until_eof(self):
        """tarfile reads exactly info.size bytes; short reads would corrupt the tar."""
        # Upstream yields 7-byte chunks; the consumer asks for 10 at a time.
        body = b"X" * 100
        chunks = [body[i : i + 7] for i in range(0, len(body), 7)]
        tap = HashingTap(iter(chunks))
        collected = []
        while True:
            data = tap.read(10)
            if not data:
                break
            collected.append(data)
        assert b"".join(collected) == body
        # All but possibly the last read returned exactly 10 bytes.
        full_reads = collected[:-1]
        assert all(len(c) == 10 for c in full_reads)

    def test_read_all_with_negative_n(self):
        chunks = [b"abc", b"def", b"ghi"]
        tap = HashingTap(iter(chunks))
        assert tap.read(-1) == b"abcdefghi"
        assert tap.hexdigests()["sha256"] == hashlib.sha256(b"abcdefghi").hexdigest()

    def test_custom_algorithms(self):
        tap = HashingTap(iter([_HELLO]), algorithms=("sha256",))
        tap.drain()
        assert tap.hexdigests() == {"sha256": _HELLO_SHA256}

    def test_finalize_triple(self):
        tap = HashingTap(iter([_HELLO]))
        tap.drain()
        result = tap.finalize_triple()
        assert result == TripleHash(
            md5=_HELLO_MD5,
            sha1=_HELLO_SHA1,
            sha256=_HELLO_SHA256,
            size=5,
        )

    def test_readable_seekable(self):
        tap = HashingTap(iter([b"x"]))
        assert tap.readable() is True
        assert tap.seekable() is False
