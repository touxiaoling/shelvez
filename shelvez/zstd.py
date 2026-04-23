"""Zstandard 压缩封装。

- Python >= 3.14：使用官方标准库 ``compression.zstd``。
- Python <  3.14：回退到第三方 ``zstandard`` 库。

两种后端都暴露相同的 ``ZstdCompressor`` 接口（``compress`` / ``decompress``
/ ``optimize_dict``），使上层代码无需关心具体实现。
"""

from __future__ import annotations

import random
import sys
import threading

# 训练字典时，采样上限。超过则随机抽样，避免 CPU 爆炸但保持代表性。
_DICT_TRAIN_SAMPLE_CAP = 100_000


def _maybe_sample(samples: list[bytes]) -> list[bytes]:
    if len(samples) > _DICT_TRAIN_SAMPLE_CAP:
        return random.sample(samples, _DICT_TRAIN_SAMPLE_CAP)
    return samples


if sys.version_info >= (3, 14):
    from compression.zstd import (
        CompressionParameter,
        ZstdCompressor as _StdZstdCompressor,
        ZstdDict,
        decompress as _zstd_decompress,
        train_dict,
    )

    class ZstdCompressor:
        def __init__(self, level: int = 3, zstd_dict: bytes | ZstdDict | None = None):
            self.level = level

            if zstd_dict is not None and not isinstance(zstd_dict, ZstdDict):
                zstd_dict = ZstdDict(zstd_dict)
            self._zstd_dict = zstd_dict

            self._options = {
                CompressionParameter.compression_level: level,
                CompressionParameter.checksum_flag: 0,
                CompressionParameter.dict_id_flag: 0,
            }

            # ``_StdZstdCompressor`` 是 stream-stateful 且非线程安全的。
            # 用 threading.local 为每个线程缓存一个 compressor，既能避免
            # 在 ``check_same_thread=False`` 的 sqlite 连接下踩状态，又能
            # 复用 compressor 的 LUT，省掉每次压缩的分配开销。
            self._local = threading.local()

        def _get_compressor(self) -> _StdZstdCompressor:
            c = getattr(self._local, "compressor", None)
            if c is None:
                c = _StdZstdCompressor(options=self._options, zstd_dict=self._zstd_dict)
                self._local.compressor = c
            return c

        def compress(self, data: bytes) -> bytes:
            """一次性压缩 ``data`` 并返回完整 zstd 帧。"""
            return self._get_compressor().compress(data, _StdZstdCompressor.FLUSH_FRAME)

        def decompress(self, data: bytes) -> bytes:
            """解压单个 zstd 帧。

            ``compression.zstd.ZstdDecompressor`` 是 stream-stateful 的：一帧
            解完后 ``eof`` 为真，继续喂下一帧会抛 ``EOFError``。所以直接用
            模块级的一次性函数，语义最清晰也避免了显式重建。
            """
            return _zstd_decompress(data, zstd_dict=self._zstd_dict)

        @staticmethod
        def optimize_dict(samples: list[bytes]) -> bytes:
            samples = _maybe_sample(samples)
            total_size = sum(map(len, samples))
            dict_size = max(total_size // 100, 256)
            dict_size = min(dict_size, 109_000)
            zstd_dict = train_dict(samples, dict_size)
            return zstd_dict.dict_content

else:
    import zstandard as zstd  # ty: ignore[unresolved-import]

    class ZstdCompressor:
        def __init__(self, level: int = 3, zstd_dict=None):
            self.level = level

            if not ((zstd_dict is None) or isinstance(zstd_dict, zstd.ZstdCompressionDict)):
                zstd_dict = zstd.ZstdCompressionDict(zstd_dict)

            compression_params = zstd.ZstdCompressionParameters(write_checksum=False, write_dict_id=False)

            # zstandard 的 ZstdCompressor/ZstdDecompressor 顶层对象内部
            # 每次 ``compress`` / ``decompress`` 都会重新建 ctx，所以本身
            # 在多线程下是安全的，无需 threading.local。
            self._compressor = zstd.ZstdCompressor(level=level, compression_params=compression_params, dict_data=zstd_dict)
            self._decompressor = zstd.ZstdDecompressor(dict_data=zstd_dict)

        def compress(self, data: bytes) -> bytes:
            return self._compressor.compress(data)

        def decompress(self, data: bytes) -> bytes:
            return self._decompressor.decompress(data)

        @staticmethod
        def optimize_dict(samples: list[bytes]) -> bytes:
            samples = _maybe_sample(samples)
            total_size = sum(map(len, samples))
            dict_size = max(total_size // 100, 256)
            dict_size = min(dict_size, 109_000)
            zstd_dict = zstd.train_dictionary(dict_size, samples)
            return zstd_dict.as_bytes()
