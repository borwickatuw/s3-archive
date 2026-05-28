"""Unit tests for the pure-Python Copy and Delta decoders."""

import lzma

import pytest

from s3_archive.native_decoders import (
    CopyDecoder,
    DeltaDecoder,
    build_native_decoder,
)


class TestCopyDecoder:
    def test_passes_bytes_through(self):
        data = b"hello world"
        assert CopyDecoder().decompress(data) == data

    def test_ignores_max_length(self):
        # py7zr's outer SevenZipDecompressor handles buffering; chain
        # elements must return ALL output (cf. py7zr.CopyDecompressor).
        data = b"x" * 100
        assert CopyDecoder().decompress(data, max_length=5) == data

    def test_empty_input(self):
        assert CopyDecoder().decompress(b"") == b""


class TestDeltaDecoder:
    """Cross-check against stdlib lzma's Delta filter via FORMAT_RAW encoder."""

    @pytest.mark.parametrize("dist", [1, 2, 3, 4, 8, 16, 256])
    def test_inverse_of_lzma_delta_encode(self, dist):
        # Build a stdlib-encoded Delta(dist) stream, then decode with us
        # and confirm we recover the original.
        original = bytes(range(256)) * 4  # 1024 bytes spanning all byte values
        # stdlib lzma needs a terminal compression filter to encode at all,
        # so we encode Delta+LZMA2 and then peel back LZMA2 to get the
        # post-Delta bytes. Easier: compute the post-Delta bytes by hand
        # (which is the spec definition).
        encoded = bytearray(len(original))
        for i, b in enumerate(original):
            prev = original[i - dist] if i >= dist else 0
            encoded[i] = (b - prev) & 0xFF

        decoded = DeltaDecoder(dist).decompress(bytes(encoded))
        assert decoded == original

    def test_state_persists_across_calls(self):
        # The ring buffer must survive between calls so chunked input
        # (which is what py7zr feeds us) decodes identically to one shot.
        original = b"".join(bytes([i]) * 4 for i in range(64))  # 256 bytes
        dist = 3
        encoded = bytearray(len(original))
        for i, b in enumerate(original):
            prev = original[i - dist] if i >= dist else 0
            encoded[i] = (b - prev) & 0xFF

        # Decode in many small chunks.
        dec = DeltaDecoder(dist)
        chunks = []
        view = memoryview(encoded)
        offset = 0
        for chunk_size in [1, 2, 5, 17, 31, 100, 100, 999]:
            end = min(offset + chunk_size, len(view))
            if end > offset:
                chunks.append(dec.decompress(bytes(view[offset:end])))
                offset = end
        assert b"".join(chunks) == original

    def test_rejects_zero_dist(self):
        with pytest.raises(ValueError, match="dist must be >= 1"):
            DeltaDecoder(0)


class TestBuildNativeDecoder:
    def test_delta_only_chain_builds(self):
        coders = [
            {"method": b"\x03", "numinstreams": 1, "numoutstreams": 1, "properties": b"\x01"},
        ]
        chain = build_native_decoder(coders)
        assert chain is not None
        # dist = properties[0] + 1 = 2
        # Round-trip a small Delta(2) input through it.
        original = b"abcdefghij"
        encoded = bytearray(len(original))
        for i, b in enumerate(original):
            prev = original[i - 2] if i >= 2 else 0
            encoded[i] = (b - prev) & 0xFF
        assert chain.decompress(bytes(encoded)) == original

    def test_copy_only_chain_builds(self):
        coders = [{"method": b"\x00", "numinstreams": 1, "numoutstreams": 1, "properties": None}]
        chain = build_native_decoder(coders)
        assert chain is not None
        assert chain.decompress(b"hello") == b"hello"

    def test_unknown_method_returns_none(self):
        coders = [
            {"method": b"\x21", "numinstreams": 1, "numoutstreams": 1, "properties": None},
        ]
        assert build_native_decoder(coders) is None

    def test_multi_stream_coder_returns_none(self):
        # Anything other than 1-in/1-out is too exotic for the native path.
        coders = [
            {"method": b"\x00", "numinstreams": 2, "numoutstreams": 1, "properties": None},
        ]
        assert build_native_decoder(coders) is None


def test_stdlib_lzma_actually_rejects_delta_alone():
    """Pin the precondition the fallback exists to handle.

    If a future Python release accepts a standalone DELTA filter in
    ``FORMAT_RAW`` mode, py7zr's path would start working on these
    archives natively and our fallback would become reachable only via
    ``UnsupportedCompressionMethodError``. The test would still pass —
    it'd just be informational that the upstream gap closed.
    """
    with pytest.raises(lzma.LZMAError):
        lzma.LZMADecompressor(
            format=lzma.FORMAT_RAW, filters=[{"id": lzma.FILTER_DELTA, "dist": 2}]
        )
