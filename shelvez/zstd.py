"""Zstandard 压缩封装。

- Python >= 3.14：使用官方标准库 ``compression.zstd``。
- Python <  3.14：回退到第三方 ``zstandard`` 库。

两种后端都暴露相同的 ``ZstdCompressor`` 接口（``compress`` / ``decompress``
/ ``optimize_dict``），使上层代码无需关心具体实现。
"""

from __future__ import annotations

import sys

if sys.version_info >= (3, 14):
    from compression.zstd import (
        CompressionParameter,
        ZstdCompressor as _StdZstdCompressor,
        ZstdDecompressor as _StdZstdDecompressor,
        ZstdDict,
        train_dict,
    )

    class ZstdCompressor:
        def __init__(self, level: int = 9, zstd_dict: bytes | ZstdDict | None = None):
            self.level = level

            if zstd_dict is not None and not isinstance(zstd_dict, ZstdDict):
                zstd_dict = ZstdDict(zstd_dict)
            self._zstd_dict = zstd_dict

            options = {
                CompressionParameter.compression_level: level,
                CompressionParameter.checksum_flag: 0,
                CompressionParameter.dict_id_flag: 0,
            }
            self._options = options

            self._compressor = _StdZstdCompressor(options=options, zstd_dict=zstd_dict)

        def compress(self, data: bytes) -> bytes:
            """一次性压缩 ``data`` 并返回完整 zstd 帧。"""
            return self._compressor.compress(data, _StdZstdCompressor.FLUSH_FRAME)

        def decompress(self, data: bytes) -> bytes:
            """解压单个 zstd 帧。"""
            decompressor = _StdZstdDecompressor(zstd_dict=self._zstd_dict)
            return decompressor.decompress(data)

        @staticmethod
        def optimize_dict(samples: list[bytes]) -> bytes:
            total_size = sum(map(len, samples))
            dict_size = max(total_size // 100, 256)
            dict_size = min(dict_size, 109_000)
            zstd_dict = train_dict(samples, dict_size)
            return zstd_dict.dict_content

else:
    import zstandard as zstd

    class ZstdCompressor:
        def __init__(self, level: int = 9, zstd_dict=None):
            self.level = level

            if not ((zstd_dict is None) or isinstance(zstd_dict, zstd.ZstdCompressionDict)):
                zstd_dict = zstd.ZstdCompressionDict(zstd_dict)

            compression_params = zstd.ZstdCompressionParameters(write_checksum=False, write_dict_id=False)

            self._compressor = zstd.ZstdCompressor(level=level, compression_params=compression_params, dict_data=zstd_dict)
            self._decompressor = zstd.ZstdDecompressor(dict_data=zstd_dict)

        def compress(self, data: bytes) -> bytes:
            return self._compressor.compress(data)

        def decompress(self, data: bytes) -> bytes:
            return self._decompressor.decompress(data)

        @staticmethod
        def optimize_dict(samples: list[bytes]) -> bytes:
            total_size = sum(map(len, samples))
            dict_size = max(total_size // 100, 256)
            dict_size = min(dict_size, 109_000)
            zstd_dict = zstd.train_dictionary(dict_size, samples)
            return zstd_dict.as_bytes()
