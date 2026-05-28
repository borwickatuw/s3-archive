"""Pure-Python decoders for 7z coder chains that py7zr's stdlib-lzma path can't handle.

py7zr decodes most folders by translating the 7z coder chain into a stdlib
``lzma.LZMADecompressor(FORMAT_RAW, filters=[...])``. ``FORMAT_RAW``
requires the chain to *terminate* with a compression filter (LZMA1/LZMA2),
so any folder whose coder chain ends in something else — e.g. a folder
compressed only with the Copy method ("Store"), optionally prefiltered
with Delta — raises ``_lzma.LZMAError: Invalid or unsupported options``
before any byte is decoded. 7-Zip itself produces such files (``-mx=0
-mf=Delta:2``) and they're legal per the 7z format; py7zr just can't
hand them to stdlib lzma.

This module supplies replacement decoders for those chains. Each decoder
exposes the minimum API py7zr's ``SevenZipDecompressor._decompress`` loop
consumes — ``.decompress(data, max_length=-1) -> bytes`` — so the
returned object can be slotted into ``self.chain`` in place of the
``lzma.LZMADecompressor`` that would have been built.

The registry is keyed by 7z method byte. To add a new decoder, implement
the ``.decompress(data, max_length=-1)`` shape and register it in
``_DECODER_BUILDERS``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

# 7z coder method bytes (from the 7z format specification).
METHOD_COPY = b"\x00"
METHOD_DELTA = b"\x03"


class CopyDecoder:
    """Pass-through decoder for the 7z Copy ("Store") method.

    py7zr already has its own ``CopyDecompressor`` reached via the
    ``_get_alternative_decompressor`` path, so in practice this class is
    only used as a building block when a chain like ``[Delta, Copy]``
    routes through ``_get_lzma_decompressor`` and we need to reconstruct
    the no-op end of the chain ourselves.

    ``max_length`` is intentionally ignored: py7zr's chain elements all
    return their full output and let the outer ``SevenZipDecompressor``
    buffer any excess (see e.g. py7zr's own ``CopyDecompressor`` and
    ``BCJDecoder``). Truncating here would silently drop bytes and is
    the exact bug that caused an infinite loop during early testing of
    this module — py7zr's outer ``decompress(fp, max_length)`` loop
    spins forever if the chain returns empty without consuming input.
    """

    def decompress(
        self,
        data: bytes | bytearray | memoryview,
        max_length: int = -1,  # noqa: ARG002 — kept for py7zr chain-element API
    ) -> bytes:
        return bytes(data)


class DeltaDecoder:
    """Inverse of the 7z Delta(dist) filter.

    The Delta filter, applied at encode time, replaces each byte ``b[i]``
    with ``b[i] - b[i - dist]`` (mod 256), with bytes before position 0
    treated as zero. To invert, walk forward and add back the
    ``dist``-prior *output* byte:

        out[i] = (in[i] + out[i - dist]) & 0xFF

    State that crosses ``decompress`` calls: the last ``dist`` output
    bytes. We keep them in a ring buffer indexed by ``write_pos``; the
    read position is always the same as the write position because we
    overwrite each slot immediately after reading it (we won't need that
    slot again until we've moved ``dist`` bytes further along).

    ``max_length`` is intentionally ignored — see :class:`CopyDecoder`
    for the rationale.
    """

    def __init__(self, dist: int) -> None:
        if dist < 1:
            raise ValueError(f"Delta dist must be >= 1, got {dist}")
        self.dist = dist
        # Ring buffer of the last ``dist`` output bytes; initialized to 0
        # to model "bytes before the start are zero" per the 7z spec.
        self._history = bytearray(dist)
        self._pos = 0

    def decompress(
        self,
        data: bytes | bytearray | memoryview,
        max_length: int = -1,  # noqa: ARG002 — kept for py7zr chain-element API
    ) -> bytes:
        out = bytearray(len(data))
        history = self._history
        dist = self.dist
        pos = self._pos
        for i, b in enumerate(data):
            decoded = (b + history[pos]) & 0xFF
            out[i] = decoded
            history[pos] = decoded
            pos += 1
            if pos == dist:
                pos = 0
        self._pos = pos
        return bytes(out)


class _NativeChain:
    """Compose multiple decoders into a single decompressor.

    py7zr's ``_get_lzma_decompressor`` returns one decompressor for a
    contiguous run of "native" coders; we mirror that by composing our
    per-coder decoders into one object exposing the same ``.decompress``
    API. The coders are applied in *decode* order — that's the reverse
    of the order they appear in py7zr's coder list (encode order), so
    callers must reverse before passing in.
    """

    def __init__(self, decoders: Sequence[object]) -> None:
        self._decoders = list(decoders)

    def decompress(
        self,
        data: bytes | bytearray | memoryview,
        max_length: int = -1,  # noqa: ARG002 — kept for py7zr chain-element API
    ) -> bytes:
        result: bytes | bytearray | memoryview = data
        for dec in self._decoders:
            result = dec.decompress(result, -1)
        return result if isinstance(result, bytes) else bytes(result)


def _coder_to_decoder(coder: dict[str, Any]) -> object | None:
    """Build a decoder for a single 7z coder dict, or return None if unsupported."""
    method = coder.get("method")
    if method == METHOD_COPY:
        return CopyDecoder()
    if method == METHOD_DELTA:
        # Delta properties byte encodes (dist - 1); see 7z format docs.
        props = coder.get("properties")
        if props is None or len(props) < 1:
            return None
        dist = props[0] + 1
        return DeltaDecoder(dist)
    return None


def build_native_decoder(coders: Sequence[dict[str, Any]]) -> _NativeChain | None:
    """Try to build a decoder chain for *coders*. Returns None if any coder is unsupported.

    *coders* arrives in py7zr's *encode* order — the order the bytes were
    transformed at archive-creation time. We must apply the *inverse*
    transforms in reverse order, so the returned chain runs the
    decoders in reverse-of-input order.

    Returning ``None`` (rather than raising) lets the caller decide what
    to do — typically: re-raise the original exception py7zr produced
    so the operator sees the stdlib-lzma error message intact.
    """
    decoders: list[object] = []
    for coder in coders:
        if coder.get("numinstreams") != 1 or coder.get("numoutstreams") != 1:
            return None
        dec = _coder_to_decoder(coder)
        if dec is None:
            return None
        decoders.append(dec)
    # Reverse: py7zr passes coders in encode order; we apply inverses in
    # reverse order to get back to the original bytes.
    decoders.reverse()
    return _NativeChain(decoders)
