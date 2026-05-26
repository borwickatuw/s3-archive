"""File-like adapters for boto3's streaming upload path.

``boto3.upload_fileobj`` dispatches its upload strategy on
``readable()`` / ``seekable()``. The streaming sources used by this
library — ``tarfile.extractfile()`` in ``r|gz`` mode, the per-member
chunk iterators from ``stream_unzip``, and the read-end of an
``os.pipe()`` — are read-once and not seekable, and they don't always
expose those two methods at all. Wrapping them in
:class:`NonSeekableReader` / :class:`IterableFileobj` steers boto3 to
its ``UploadNonSeekableInputManager`` path (chunked single-part uploads)
rather than crashing on missing attributes.
"""

from collections.abc import Iterable


class NonSeekableReader:
    """Wrap a ``.read()``-only source so s3transfer's upload path accepts it."""

    def __init__(self, source) -> None:
        self._source = source

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            return self._source.read()
        return self._source.read(size)

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return True


class IterableFileobj:
    """Wrap a bytes iterable in the same non-seekable file-like protocol."""

    def __init__(self, iterable: Iterable[bytes]) -> None:
        self._iter = iter(iterable)
        self._buf = b""

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunks = [self._buf]
            chunks.extend(self._iter)
            self._buf = b""
            return b"".join(chunks)
        while len(self._buf) < size:
            try:
                self._buf += next(self._iter)
            except StopIteration:
                break
        result = self._buf[:size]
        self._buf = self._buf[size:]
        return result

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return True


class PipeReader:
    """Wrap an ``os.fdopen`` read-end so ``boto3.upload_fileobj`` accepts it.

    Same shape as :class:`NonSeekableReader` but with a short-read loop:
    pipes can return less than ``size`` bytes per call, and boto3's
    multipart uploader is happier when each ``read(size)`` returns full
    parts.
    """

    def __init__(self, fobj) -> None:
        self._fobj = fobj

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            return self._fobj.read()
        chunks: list[bytes] = []
        remaining = size
        while remaining > 0:
            chunk = self._fobj.read(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False
