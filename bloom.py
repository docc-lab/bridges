"""
Bloom filter: port of blueprint-docc-mod/runtime/plugins/bloom/bloom.go.
Space-efficient probabilistic set with minimal serialization (bit array only).
Used by path-bridge and CGPB bridge types in the trace simulator.
"""

import math
import struct
from typing import Tuple


# MurmurHash3 128-bit constants (same as Go)
_C1_128 = 0x87C37B91114253D5
_C2_128 = 0x4CF5AD432745937F


def _fmix64(k: int) -> int:
    """Finalization mix (64-bit)."""
    k = (k ^ (k >> 33)) & 0xFFFFFFFFFFFFFFFF
    k = (k * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    k = (k ^ (k >> 33)) & 0xFFFFFFFFFFFFFFFF
    k = (k * 0xC4CEB9FE1A85EC53) & 0xFFFFFFFFFFFFFFFF
    k = (k ^ (k >> 33)) & 0xFFFFFFFFFFFFFFFF
    return k


def _murmur_hash3_128(data: bytes, seed: int) -> Tuple[int, int]:
    """
    MurmurHash3 128-bit variant: two 64-bit values.
    Matches bloom.go murmurHash3_128 (little-endian 16-byte blocks, same tail handling).
    """
    h1 = seed & 0xFFFFFFFFFFFFFFFF
    h2 = seed & 0xFFFFFFFFFFFFFFFF
    length = len(data)
    nblocks = length // 16

    for i in range(nblocks):
        off = i * 16
        k1, k2 = struct.unpack_from("<QQ", data, off)

        k1 = (k1 * _C1_128) & 0xFFFFFFFFFFFFFFFF
        k1 = ((k1 << 31) | (k1 >> 33)) & 0xFFFFFFFFFFFFFFFF
        k1 = (k1 * _C2_128) & 0xFFFFFFFFFFFFFFFF
        h1 ^= k1
        h1 = ((h1 << 27) | (h1 >> 37)) & 0xFFFFFFFFFFFFFFFF
        h1 = (h1 * 5 + 0x52DCE729) & 0xFFFFFFFFFFFFFFFF

        k2 = (k2 * _C2_128) & 0xFFFFFFFFFFFFFFFF
        k2 = ((k2 << 33) | (k2 >> 31)) & 0xFFFFFFFFFFFFFFFF
        k2 = (k2 * _C1_128) & 0xFFFFFFFFFFFFFFFF
        h2 ^= k2
        h2 = ((h2 << 31) | (h2 >> 33)) & 0xFFFFFFFFFFFFFFFF
        h2 = (h2 * 5 + 0x38495AB5) & 0xFFFFFFFFFFFFFFFF

    tail = data[nblocks * 16:]
    tlen = len(tail)
    k1, k2 = 0, 0

    # Tail: match Go fallthrough. k2 from tail[8:16] (if tlen > 8), then k1 from tail[0:8].
    if tlen > 8:
        for j in range(min(8, tlen - 8)):
            k2 ^= tail[8 + j] << (j * 8)
        k2 = (k2 * _C2_128) & 0xFFFFFFFFFFFFFFFF
        k2 = ((k2 << 33) | (k2 >> 31)) & 0xFFFFFFFFFFFFFFFF
        k2 = (k2 * _C1_128) & 0xFFFFFFFFFFFFFFFF
        h2 ^= k2
    if tlen > 0:
        for j in range(min(8, tlen)):
            k1 ^= tail[j] << (j * 8)
        k1 = (k1 * _C1_128) & 0xFFFFFFFFFFFFFFFF
        k1 = ((k1 << 31) | (k1 >> 33)) & 0xFFFFFFFFFFFFFFFF
        k1 = (k1 * _C2_128) & 0xFFFFFFFFFFFFFFFF
        h1 ^= k1

    h1 ^= length
    h2 ^= length
    h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
    h2 = (h2 + h1) & 0xFFFFFFFFFFFFFFFF
    h1 = _fmix64(h1)
    h2 = _fmix64(h2)
    h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
    h2 = (h2 + h1) & 0xFFFFFFFFFFFFFFFF
    return h1, h2


def _base_hashes(data: bytes) -> Tuple[int, int, int, int]:
    """Four base hashes (same as Go baseHashes: two seeds for MurmurHash3 128)."""
    h1, h2 = _murmur_hash3_128(data, 0)
    h3, h4 = _murmur_hash3_128(data, 1)
    return h1, h2, h3, h4


def estimate_parameters(n: int, p: float) -> Tuple[int, int]:
    """
    Optimal m (bits) and k (hash count) for expected size n and false positive rate p.
    m = - (n * ln(p)) / (ln(2)^2),  k = (m / n) * ln(2)
    """
    if p <= 0 or p >= 1:
        p = 0.01
    if n <= 0:
        n = 1000
    ln2 = math.log(2)
    ln2_sq = ln2 * ln2
    m = max(1, int(math.ceil(-n * math.log(p) / ln2_sq)))
    k = max(1, int(math.ceil(m / n * ln2)))
    return m, k


class BloomFilter:
    """
    Bloom filter with m bits and k hash functions.
    Add/Test use double hashing: (h1 + i*h2) % m, same as Go bits.
    """

    def __init__(self, m: int, k: int):
        if m <= 0 or k <= 0:
            raise ValueError("m and k must be positive")
        self._m = m
        self._k = k
        self._bits = bytearray((m + 7) // 8)

    def m(self) -> int:
        return self._m

    def k(self) -> int:
        return self._k

    def add(self, data: bytes) -> None:
        h1, h2, _, _ = _base_hashes(data)
        for i in range(self._k):
            pos = (h1 + i * h2) % self._m
            byte_idx = pos // 8
            bit_idx = pos % 8
            self._bits[byte_idx] |= 1 << bit_idx

    def test(self, data: bytes) -> bool:
        h1, h2, _, _ = _base_hashes(data)
        for i in range(self._k):
            pos = (h1 + i * h2) % self._m
            byte_idx = pos // 8
            bit_idx = pos % 8
            if (self._bits[byte_idx] & (1 << bit_idx)) == 0:
                return False
        return True

    def serialize(self) -> str:
        """Encode bit array as hex (no m/k in payload)."""
        return bytes(self._bits).hex()

    def byte_size(self) -> int:
        """Raw byte size of the bit array in transit (e.g. over the wire)."""
        return len(self._bits)

    @classmethod
    def deserialize(cls, serialized: str, m: int, k: int) -> "BloomFilter":
        """
        Rebuild from hex-encoded bit array. m and k must be provided.
        Empty string or decode/size mismatch -> new empty filter with given m, k.
        """
        bf = cls(m, k)
        if not serialized:
            return bf
        try:
            data = bytes.fromhex(serialized)
        except (ValueError, TypeError):
            return bf
        expected = (m + 7) // 8
        if len(data) != expected:
            return bf
        bf._bits = bytearray(data)
        return bf

    @classmethod
    def new_with_estimates(cls, n: int, p: float) -> "BloomFilter":
        """Create filter optimized for n elements and false positive rate p."""
        m, k = estimate_parameters(n, p)
        return cls(m, k)
