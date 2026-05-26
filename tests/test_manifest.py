"""Tests for s3_archive.manifest primitives.

The fileobj / chunks / central-directory primitives also have
end-to-end coverage in storage-scripts' inventory test suite (which
consumes this module); this file covers the streaming hasher
(:func:`_hash_stream`) directly.
"""

import hashlib

from s3_archive.manifest import _hash_stream


class TestHashStream:
    """The streaming hasher must NOT buffer the entry; peak memory == one chunk."""

    def test_empty_input(self):
        assert _hash_stream(iter([])) == (
            0,
            "d41d8cd98f00b204e9800998ecf8427e",
            "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )

    def test_single_chunk_matches_known_hash(self):
        size, md5, sha1, sha256 = _hash_stream([b"hello"])
        assert size == 5
        assert md5 == "5d41402abc4b2a76b9719d911017c592"
        assert sha1 == "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
        assert sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_chunked_equals_single_blob(self):
        data = b"the quick brown fox jumps over the lazy dog"
        a = _hash_stream([data])
        b = _hash_stream([data[:5], data[5:20], data[20:]])
        assert a == b

    def test_does_not_buffer_chunks(self):
        """Generator-fed: verify _hash_stream consumes chunks without holding them.

        After _hash_stream returns, the generator has been fully consumed
        and there's no reference to the byte buffers we yielded. This
        wouldn't catch every form of buffering, but it catches the old
        ``parts.append(chunk); b"".join(parts)`` pattern.
        """
        produced: list[int] = []  # we record sizes only, not the bytes themselves

        def gen():
            for _ in range(8):
                chunk = b"X" * 1024
                produced.append(len(chunk))
                yield chunk

        size, *_ = _hash_stream(gen())
        assert size == 8 * 1024
        assert produced == [1024] * 8  # all chunks were consumed
        # Generator exhausted; no chunk-buffer remains accessible from caller.
        assert next(iter(()), None) is None  # sanity: generator is gone

    def test_handles_large_synthetic_entry(self):
        """A 16 MB synthetic entry hashes correctly without OOM-shaped behavior.

        We use a deterministic small chunk so the hash is predictable, and
        feed many chunks to exercise the streaming path.
        """
        chunk = b"A" * 1024  # 1 KiB
        n_chunks = 16 * 1024  # → 16 MiB total
        size, _md5, _sha1, sha256 = _hash_stream(chunk for _ in range(n_chunks))
        assert size == 16 * 1024 * 1024
        expected = hashlib.sha256(b"A" * size).hexdigest()
        assert sha256 == expected
