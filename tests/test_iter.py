"""Tests for s3_archive.iter (IterableFileobj + NonSeekableReader)."""

import io

from s3_archive.iter import IterableFileobj, NonSeekableReader


class TestIterableFileobj:
    def test_read_all(self):
        f = IterableFileobj([b"hello ", b"world"])
        assert f.read() == b"hello world"

    def test_read_chunked(self):
        f = IterableFileobj([b"abcdef", b"ghij"])
        assert f.read(4) == b"abcd"
        assert f.read(4) == b"efgh"
        assert f.read(4) == b"ij"
        assert f.read(4) == b""

    def test_read_size_negative_returns_remainder(self):
        f = IterableFileobj([b"ab", b"cd", b"ef"])
        assert f.read(3) == b"abc"
        assert f.read(-1) == b"def"

    def test_read_size_none_is_remainder(self):
        f = IterableFileobj([b"ab", b"cd"])
        assert f.read(None) == b"abcd"

    def test_readable_seekable(self):
        f = IterableFileobj([b"x"])
        assert f.readable() is True
        assert f.seekable() is False

    def test_empty_iterable(self):
        f = IterableFileobj([])
        assert f.read(10) == b""
        assert f.read() == b""


class TestNonSeekableReader:
    def test_passes_through_read(self):
        src = io.BytesIO(b"hello world")
        r = NonSeekableReader(src)
        assert r.read(5) == b"hello"
        assert r.read() == b" world"

    def test_read_negative_size_reads_all(self):
        src = io.BytesIO(b"abc")
        r = NonSeekableReader(src)
        assert r.read(-1) == b"abc"

    def test_read_none_reads_all(self):
        src = io.BytesIO(b"abc")
        r = NonSeekableReader(src)
        assert r.read(None) == b"abc"

    def test_readable_seekable(self):
        r = NonSeekableReader(io.BytesIO(b""))
        assert r.readable() is True
        assert r.seekable() is False
