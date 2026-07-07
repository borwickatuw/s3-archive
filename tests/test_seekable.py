"""Tests for s3_archive.seekable's canonical buffered open.

:class:`SeekableS3Object` itself has behavioral coverage through the
resume / 7z suites; this file guards :func:`open_seekable` — the tuned
BufferedReader wrap. The GET-count test is the regression guard for the
8 KiB-default-buffer trap: a consumer once wrapped the raw object in a
bare ``io.BufferedReader`` and a 105 MB sequential walk became ~13,000
ranged GETs (40-70x slower than a plain download).
"""

import io
from unittest import mock

import pytest

from s3_archive.exceptions import ETagMismatchError
from s3_archive.seekable import SeekableS3Object, open_seekable


class TestOpenSeekable:
    def test_returns_buffered_reader_over_seekable_raw(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="obj", Body=b"hello world")
        buffered = open_seekable(s3_client, "src-bucket", "obj")
        assert isinstance(buffered, io.BufferedReader)
        assert isinstance(buffered.raw, SeekableS3Object)
        assert buffered.read() == b"hello world"

    def test_if_match_passes_through(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="obj", Body=b"pinned")
        etag = s3_client.head_object(Bucket="src-bucket", Key="obj")["ETag"]
        assert open_seekable(s3_client, "src-bucket", "obj", if_match=etag).read() == b"pinned"
        with pytest.raises(ETagMismatchError):
            open_seekable(s3_client, "src-bucket", "obj", if_match="deadbeef")

    def test_sequential_walk_coalesces_ranged_gets(self, s3_client):
        """Reading a 12 MiB body in 8 KiB application reads must cost
        ~a-dozen ranged GETs (1 MiB buffer + 4 MiB tail prefetch), not
        one GET per read. A default-size BufferedReader would issue
        ~1,500 GETs here — that's the regression this pins down.
        """
        size = 12 * 1024 * 1024
        s3_client.put_object(Bucket="src-bucket", Key="big", Body=b"x" * size)

        with mock.patch.object(s3_client, "get_object", wraps=s3_client.get_object) as spy:
            buffered = open_seekable(s3_client, "src-bucket", "big")
            total = 0
            while chunk := buffered.read(8192):
                total += len(chunk)

        assert total == size
        # 8 MiB before the tail prefetch @ 1 MiB buffer + the prefetch
        # GET itself + slack for read-ahead boundary effects.
        assert spy.call_count <= 20, spy.call_count
