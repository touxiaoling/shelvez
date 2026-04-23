"""Tests for :mod:`shelvez.zstd`.

The module exposes a backend-agnostic ``ZstdCompressor`` class that switches
between the stdlib ``compression.zstd`` (Python >= 3.14) and the third-party
``zstandard`` package. These tests target the public surface only:
``compress`` / ``decompress`` roundtrip, custom levels, dictionary support
and ``optimize_dict``.
"""

from __future__ import annotations

import os

import pytest

from shelvez.zstd import ZstdCompressor


@pytest.fixture
def samples() -> list[bytes]:
    # Repeated structure helps the trained dictionary actually shrink output.
    return [f'{{"user":"alice","n":{i},"tag":"hello-world-payload"}}'.encode("utf-8") for i in range(200)]


class TestRoundtrip:
    def test_basic_roundtrip(self):
        compressor = ZstdCompressor()
        payload = b"hello world" * 100
        blob = compressor.compress(payload)
        assert isinstance(blob, bytes)
        assert blob != payload
        assert compressor.decompress(blob) == payload

    def test_empty_payload(self):
        compressor = ZstdCompressor()
        assert compressor.decompress(compressor.compress(b"")) == b""

    def test_random_binary_payload(self):
        compressor = ZstdCompressor()
        payload = os.urandom(4096)
        assert compressor.decompress(compressor.compress(payload)) == payload

    @pytest.mark.parametrize("level", [1, 9, 19])
    def test_levels_are_accepted(self, level: int):
        compressor = ZstdCompressor(level=level)
        payload = b"abc" * 500
        assert compressor.decompress(compressor.compress(payload)) == payload


class TestDictionary:
    def test_optimize_dict_returns_bytes(self, samples):
        zdict = ZstdCompressor.optimize_dict(samples)
        assert isinstance(zdict, bytes)
        assert len(zdict) > 0

    def test_dict_improves_compression(self, samples):
        plain = ZstdCompressor()
        zdict = ZstdCompressor.optimize_dict(samples)
        dict_based = ZstdCompressor(zstd_dict=zdict)

        plain_total = sum(len(plain.compress(s)) for s in samples)
        dict_total = sum(len(dict_based.compress(s)) for s in samples)

        # With a properly trained dictionary on homogeneous samples, the
        # compressed size must strictly shrink.
        assert dict_total < plain_total

    def test_dict_decompress_roundtrip(self, samples):
        zdict = ZstdCompressor.optimize_dict(samples)
        compressor = ZstdCompressor(zstd_dict=zdict)
        for s in samples[:20]:
            assert compressor.decompress(compressor.compress(s)) == s

    def test_another_compressor_with_same_dict_can_decompress(self, samples):
        """Simulates the shelvez reopen flow: a fresh ``ZstdCompressor``
        instantiated with the persisted dictionary bytes must read data
        produced by the previous instance."""
        zdict = ZstdCompressor.optimize_dict(samples)
        writer = ZstdCompressor(zstd_dict=zdict)
        blobs = [writer.compress(s) for s in samples[:10]]

        reader = ZstdCompressor(zstd_dict=zdict)
        for original, blob in zip(samples[:10], blobs):
            assert reader.decompress(blob) == original
