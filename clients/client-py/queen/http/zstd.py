"""
zstd for the binary push body and the framed pop answer.

It comes from the standard library where that has it (``compression.zstd``,
Python 3.14 and later) and from the ``zstandard`` package otherwise
(``pip install "queen-mq[zstd]"``). Imported on first use: a client that
compresses no push and reads no framed pop needs neither.
"""

import threading
from typing import Any, Callable, Optional, Tuple

_Codec = Tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]]

_lock = threading.Lock()
_codec: Optional[_Codec] = None
_local = threading.local()


def _load() -> _Codec:
    try:
        from compression import zstd as std  # type: ignore[import-not-found]

        # One call with the whole input: the frame carries its content size.
        return (lambda raw: std.compress(raw, level=1)), std.decompress
    except ImportError:
        pass

    try:
        import zstandard
    except ImportError:
        raise RuntimeError(
            "zstd is not available: push_compression and framed_pop need Python 3.14 or later, "
            'or the zstandard package (pip install "queen-mq[zstd]")'
        ) from None

    # A zstandard context serves one thread at a time: one of each per thread.
    def compress(raw: bytes) -> bytes:
        cctx: Any = getattr(_local, "cctx", None)
        if cctx is None:
            cctx = _local.cctx = zstandard.ZstdCompressor(level=1, write_content_size=True)
        return cctx.compress(raw)  # type: ignore[no-any-return]

    def decompress(frame: bytes) -> bytes:
        dctx: Any = getattr(_local, "dctx", None)
        if dctx is None:
            dctx = _local.dctx = zstandard.ZstdDecompressor()
        return dctx.decompressobj().decompress(frame)  # type: ignore[no-any-return]

    return compress, decompress


def require() -> _Codec:
    """The (compress, decompress) pair; raises RuntimeError when there is no zstd."""
    global _codec
    if _codec is None:
        with _lock:
            if _codec is None:
                _codec = _load()
    return _codec


def compress(raw: bytes) -> bytes:
    """One zstd frame of ``raw``, at the fastest level. The frame carries its
    content size, which the broker asks of a frame it stores as it came."""
    return require()[0](raw)


def decompress(frame: bytes) -> bytes:
    return require()[1](frame)
