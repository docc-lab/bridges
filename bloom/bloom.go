// Package bloom is a bit-exact port of bridges/bloom.py.
//
// The reference is the Python implementation, which itself is a port of
// blueprint-docc-mod/runtime/plugins/bloom/bloom.go. Bitmap bytes produced by
// Add(data) on identical inputs must equal the Python output byte-for-byte;
// the trace simulator's bridge handlers depend on this.
package bloom

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	mathbits "math/bits"
)

const (
	c1_128 uint64 = 0x87C37B91114253D5
	c2_128 uint64 = 0x4CF5AD432745937F
)

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xFF51AFD7ED558CCD
	k ^= k >> 33
	k *= 0xC4CEB9FE1A85EC53
	k ^= k >> 33
	return k
}

// MurmurHash3_128 returns the two 64-bit halves of MurmurHash3's 128-bit
// variant. Matches bloom.py's _murmur_hash3_128 (little-endian 16-byte blocks,
// same tail handling, same finalization).
func MurmurHash3_128(data []byte, seed uint64) (h1, h2 uint64) {
	h1 = seed
	h2 = seed
	length := len(data)
	nblocks := length / 16

	for i := 0; i < nblocks; i++ {
		off := i * 16
		k1 := binary.LittleEndian.Uint64(data[off:])
		k2 := binary.LittleEndian.Uint64(data[off+8:])

		k1 *= c1_128
		k1 = (k1 << 31) | (k1 >> 33)
		k1 *= c2_128
		h1 ^= k1
		h1 = (h1 << 27) | (h1 >> 37)
		h1 += h2
		h1 = h1*5 + 0x52DCE729

		k2 *= c2_128
		k2 = (k2 << 33) | (k2 >> 31)
		k2 *= c1_128
		h2 ^= k2
		h2 = (h2 << 31) | (h2 >> 33)
		h2 += h1
		h2 = h2*5 + 0x38495AB5
	}

	tail := data[nblocks*16:]
	tlen := len(tail)
	var k1, k2 uint64

	// Tail: match Go fallthrough. k2 from tail[8:16] (if tlen > 8), then
	// k1 from tail[0:8].
	if tlen > 8 {
		nb := tlen - 8
		if nb > 8 {
			nb = 8
		}
		for j := 0; j < nb; j++ {
			k2 ^= uint64(tail[8+j]) << (uint(j) * 8)
		}
		k2 *= c2_128
		k2 = (k2 << 33) | (k2 >> 31)
		k2 *= c1_128
		h2 ^= k2
	}
	if tlen > 0 {
		nb := tlen
		if nb > 8 {
			nb = 8
		}
		for j := 0; j < nb; j++ {
			k1 ^= uint64(tail[j]) << (uint(j) * 8)
		}
		k1 *= c1_128
		k1 = (k1 << 31) | (k1 >> 33)
		k1 *= c2_128
		h1 ^= k1
	}

	h1 ^= uint64(length)
	h2 ^= uint64(length)
	h1 += h2
	h2 += h1
	h1 = fmix64(h1)
	h2 = fmix64(h2)
	h1 += h2
	h2 += h1
	return h1, h2
}

// BaseHashes returns the four base hashes used by the filter, matching
// bits-and-blooms v2 baseHashes: h1/h2 = murmur128(data), h3/h4 =
// murmur128(data || 0x01) (the library appends one byte to its streaming
// hasher and re-sums).
func BaseHashes(data []byte) (h1, h2, h3, h4 uint64) {
	h1, h2 = MurmurHash3_128(data, 0)
	var buf [64]byte
	ext := append(buf[:0], data...)
	ext = append(ext, 1)
	h3, h4 = MurmurHash3_128(ext, 0)
	return
}

// location returns the ith probe position (pre-modulus), matching
// bits-and-blooms v2 location(): h[i%2] + i*h[2+(((i+(i%2))%4)/2)], with
// native uint64 wraparound. Mixing all four hashes prevents the probe
// schedule from collapsing when any single hash is degenerate mod m (the
// plain h1 + i*h2 schedule sets/tests a single bit for ~1/m of all keys).
func location(h *[4]uint64, i uint32) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
}

// EstimateParameters returns optimal m (bits) and k (hash count) for expected
// element count n and false-positive rate p.
//
//	m = ceil(-n * ln(p) / ln(2)^2)
//	k = ceil(m / n * ln(2))
func EstimateParameters(n int, p float64) (m, k uint32) {
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	if n <= 0 {
		n = 1000
	}
	ln2 := math.Log(2)
	ln2sq := ln2 * ln2
	mFloat := math.Ceil(-float64(n) * math.Log(p) / ln2sq)
	if mFloat < 1 {
		mFloat = 1
	}
	m = uint32(mFloat)
	if PrimeM {
		// A prime modulus gives the double-hashing probe schedule a full period
		// (gcd(step,m)=1), so the k probes can't collapse onto a few colliding
		// bits at small m — recovering the nominal FPR without growing capacity.
		up := nextPrime(m)
		if PrimeMByteCap && (up+7)/8 > (m+7)/8 {
			// Rounding up would spill into another byte; drop to the previous
			// prime instead so the on-wire byte count never exceeds the raw size.
			m = prevPrime(m)
		} else {
			m = up
		}
	}
	kFloat := math.Ceil(float64(m) / float64(n) * ln2)
	if kFloat < 1 {
		kFloat = 1
	}
	k = uint32(kFloat)
	return
}

// PrimeM, when true, rounds the bloom bit count to a prime in
// EstimateParameters (see the prime-modulus note there). Off by default.
var PrimeM bool

// PrimeMByteCap, when set alongside PrimeM, keeps the prime modulus within the
// raw size's byte budget: round up to the next prime unless that would use more
// bytes, in which case round down to the previous prime. Zero byte overhead.
var PrimeMByteCap bool

func isPrime(n uint32) bool {
	if n < 2 {
		return false
	}
	if n%2 == 0 {
		return n == 2
	}
	for d := uint32(3); d*d <= n; d += 2 {
		if n%d == 0 {
			return false
		}
	}
	return true
}

func nextPrime(n uint32) uint32 {
	for !isPrime(n) {
		n++
	}
	return n
}

func prevPrime(n uint32) uint32 {
	for n >= 2 && !isPrime(n) {
		n--
	}
	if n < 2 {
		return 2
	}
	return n
}

// Filter is a bloom filter with m bits and k hash functions. Add/Test derive
// probe positions via the bits-and-blooms v2 location() schedule over four
// murmur base hashes; see location() for why plain double hashing is not
// safe at the small m values bridge checkpointing produces.
type Filter struct {
	m    uint32
	k    uint32
	bits []byte
}

// New constructs an empty filter. m and k must be positive.
func New(m, k uint32) (*Filter, error) {
	if m == 0 || k == 0 {
		return nil, errors.New("bloom: m and k must be positive")
	}
	return &Filter{
		m:    m,
		k:    k,
		bits: make([]byte, (m+7)/8),
	}, nil
}

// NewWithEstimates constructs a filter sized for n elements and rate p.
func NewWithEstimates(n int, p float64) *Filter {
	m, k := EstimateParameters(n, p)
	f, _ := New(m, k)
	return f
}

// M returns the bit count.
func (f *Filter) M() uint32 { return f.m }

// K returns the hash count.
func (f *Filter) K() uint32 { return f.k }

// ByteSize returns the raw byte size of the bit array (i.e. the on-wire size).
func (f *Filter) ByteSize() int { return len(f.bits) }

// PopCount returns the number of set bits — the filter's current fill, used to
// estimate actual load and the realized false-positive rate.
func (f *Filter) PopCount() int {
	n := 0
	for _, b := range f.bits {
		n += mathbits.OnesCount8(b)
	}
	return n
}

// Add inserts data into the filter.
func (f *Filter) Add(data []byte) {
	h1, h2, h3, h4 := BaseHashes(data)
	h := [4]uint64{h1, h2, h3, h4}
	mu := uint64(f.m)
	for i := uint32(0); i < f.k; i++ {
		pos := location(&h, i) % mu
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

// Test returns true if data may have been added (no false negatives).
func (f *Filter) Test(data []byte) bool {
	h1, h2, h3, h4 := BaseHashes(data)
	h := [4]uint64{h1, h2, h3, h4}
	mu := uint64(f.m)
	for i := uint32(0); i < f.k; i++ {
		pos := location(&h, i) % mu
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// ToBytes returns a copy of the raw bit array.
func (f *Filter) ToBytes() []byte {
	out := make([]byte, len(f.bits))
	copy(out, f.bits)
	return out
}

// Serialize returns the bit array as a hex string (display/output only).
func (f *Filter) Serialize() string {
	return hex.EncodeToString(f.bits)
}

// Deserialize rebuilds a filter from raw bytes (or a hex string via the helper
// DeserializeHex). On size mismatch or empty input, returns a fresh empty
// filter — matching Python's BloomFilter.deserialize.
func Deserialize(data []byte, m, k uint32) *Filter {
	f, err := New(m, k)
	if err != nil {
		return nil
	}
	if len(data) == 0 {
		return f
	}
	expected := int((m + 7) / 8)
	if len(data) != expected {
		return f
	}
	copy(f.bits, data)
	return f
}

// DeserializeHex is the hex-string variant of Deserialize.
func DeserializeHex(s string, m, k uint32) *Filter {
	if s == "" {
		f, _ := New(m, k)
		return f
	}
	data, err := hex.DecodeString(s)
	if err != nil {
		f, _ := New(m, k)
		return f
	}
	return Deserialize(data, m, k)
}
