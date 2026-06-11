package bloom

import (
	"encoding/hex"
	"testing"
)

// Golden vectors generated from bridges/bloom.py. Any drift here indicates
// the Go port has diverged from the Python reference and the trace simulator
// will produce non-bit-exact bloom bytes.

type murmurVec struct {
	hexInput string
	h1, h2   uint64
}

var murmurSeed0 = []murmurVec{
	{"", 0x0000000000000000, 0x0000000000000000},
	{"61", 0x85555565f6597889, 0xe6b53a48510e895a},
	{"6162", 0x938b11ea16ed1b2e, 0xe65ea7019b52d4ad},
	{"616263", 0xb4963f3f3fad7867, 0x3ba2744126ca2d52},
	{"61626364", 0xb87bb7d64656cd4f, 0xf2003e886073e875},
	{"6162636465666768", 0xcc8a0ab037ef8c02, 0x48890d60eb6940a1},
	{"616263646566676869", 0x0547c0cff13c7964, 0x79b53df5b741e033},
	{"6162636465666768696a6b6c6d6e6f", 0x8abe2451890c2ffb, 0x6a548c2d9c962a61},
	{"6162636465666768696a6b6c6d6e6f70", 0xc4ca3ca3224cb723, 0x4333d695b331eb1a},
	{"6162636465666768696a6b6c6d6e6f7071", 0x7564747f88bda657, 0xecda499da1110de4},
	{"38656532376131313562366661363235", 0x6c618855c4931145, 0xe010e635a565254e},
	{"39323866313838656632343039383131", 0xe13dc5ff5c24c760, 0x48fcb80c34a121ce},
	{
		"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
			"202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		0xffd5522d8d812301, 0xa22238eb56338ea1,
	},
	{
		"0000000000000000000000000000000000000000000000000000000000000000",
		0x31723c284ade5cd0, 0x10c01d1bb3342d3c,
	},
	{"ffffffffffffffffffffffffffffffffff", 0x93b3db80aa4d392f, 0xfc12d3488581444c},
}

var murmurSeed1 = []murmurVec{
	{"", 0x4610abe56eff5cb5, 0x51622daa78f83583},
	{"61", 0x47eae1073748cf70, 0x6be0518ad2ed3728},
	{"6162", 0x9370e98468f5948c, 0xe62a56363f63c769},
	{"616263", 0x9c88be4e9a8a61f0, 0xca12c88bf31b256c},
	{"61626364", 0x53b0edab77ffba4b, 0x91c087f92e3ab577},
	{"6162636465666768", 0x16c08ff0c10bb14e, 0x65df722c5b7b072d},
	{"616263646566676869", 0xdccea85d31b90dd1, 0xdfd530e64ca11d09},
	{"6162636465666768696a6b6c6d6e6f", 0x6561317daaa100aa, 0x4f5bd7feb4b5dfe5},
	{"6162636465666768696a6b6c6d6e6f70", 0x5e10a4a0eb1c64ef, 0x0207d437e44c438d},
	{"6162636465666768696a6b6c6d6e6f7071", 0x664664444483a7c5, 0xd8432e08b5c6c5a0},
	{"38656532376131313562366661363235", 0xaa5b70e06dab079f, 0x73ef678961766b99},
	{"39323866313838656632343039383131", 0xee54e9ab661ba490, 0xb3b6a6bbbc15bcdd},
	{
		"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
			"202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		0x26ed32e6f446a66e, 0x39601f7cfab13ee6,
	},
	{
		"0000000000000000000000000000000000000000000000000000000000000000",
		0x79ebfa3b21f3bb45, 0x683992024737bed4,
	},
	{"ffffffffffffffffffffffffffffffffff", 0x3e108ce5bbae5c35, 0xe5d921e1b7687925},
}

func TestMurmurHash3_128(t *testing.T) {
	for _, v := range murmurSeed0 {
		data, err := hex.DecodeString(v.hexInput)
		if err != nil {
			t.Fatalf("bad hex %q: %v", v.hexInput, err)
		}
		h1, h2 := MurmurHash3_128(data, 0)
		if h1 != v.h1 || h2 != v.h2 {
			t.Errorf("seed=0 input=%s: got (%016x, %016x), want (%016x, %016x)",
				v.hexInput, h1, h2, v.h1, v.h2)
		}
	}
	for _, v := range murmurSeed1 {
		data, _ := hex.DecodeString(v.hexInput)
		h1, h2 := MurmurHash3_128(data, 1)
		if h1 != v.h1 || h2 != v.h2 {
			t.Errorf("seed=1 input=%s: got (%016x, %016x), want (%016x, %016x)",
				v.hexInput, h1, h2, v.h1, v.h2)
		}
	}
}

type estVec struct {
	n    int
	p    float64
	m, k uint32
}

var estVecs = []estVec{
	{1, 0.0001, 20, 14},
	{10, 0.001, 144, 10},
	{1000, 0.01, 9586, 7},
	{5, 0.0001, 96, 14},
}

func TestEstimateParameters(t *testing.T) {
	for _, v := range estVecs {
		m, k := EstimateParameters(v.n, v.p)
		if m != v.m || k != v.k {
			t.Errorf("n=%d p=%v: got (m=%d, k=%d), want (m=%d, k=%d)",
				v.n, v.p, m, k, v.m, v.k)
		}
	}
}

// Golden bloom byte sequences: for each (cpd, p), insert the listed span IDs
// in order, recording the bitmap bytes after each insertion. Generated from
// bridges/bloom.py.
type bloomSeq struct {
	cpd      int
	expected []string // hex bitmap after each Add()
}

var bloomSpanIDs = []string{
	"8ee27a115b6fa625",
	"98655be61af2ecc4",
	"926ce147adaffad4",
	"aabbccddeeff0011",
	"0000000000000000",
}

var bloomSeqs = []bloomSeq{
	{1, []string{
		"bba202",
		"bbbb0b",
		"bbbb0b",
		"fbfb0f",
		"fffb0f",
	}},
	{3, []string{
		"8080288892200102",
		"c1bd298892208903",
		"e3bda9a8b2a88d03",
		"f7bdf9a9b2b88d03",
		"f7bdf9b9b2f99d03",
	}},
	{10, []string{
		"000000800103022220000000000000080800000202000101",
		"00000880110b02322010018000000009880000020b800109",
		"280088a8110b2ab22010118000010019880000020ba00109",
		"2800c8b9510b2ab22090118000010019880010421be00109",
		"2801c8b9510b6ab3209411c001011019880012421be10309",
	}},
}

func TestBloomAddSequences(t *testing.T) {
	for _, seq := range bloomSeqs {
		m, k := EstimateParameters(seq.cpd, 0.0001)
		f, err := New(m, k)
		if err != nil {
			t.Fatalf("New(%d,%d): %v", m, k, err)
		}
		for i, sid := range bloomSpanIDs {
			f.Add([]byte(sid))
			got := hex.EncodeToString(f.ToBytes())
			if got != seq.expected[i] {
				t.Errorf("cpd=%d step=%d after_add=%s: got %s, want %s",
					seq.cpd, i, sid, got, seq.expected[i])
			}
		}
	}
}

func TestDeserializeRoundTrip(t *testing.T) {
	m, k := EstimateParameters(3, 0.0001)
	f, _ := New(m, k)
	for _, sid := range bloomSpanIDs[:3] {
		f.Add([]byte(sid))
	}
	bytes := f.ToBytes()
	g := Deserialize(bytes, m, k)
	if hex.EncodeToString(g.ToBytes()) != hex.EncodeToString(bytes) {
		t.Errorf("round-trip mismatch")
	}
	for _, sid := range bloomSpanIDs[:3] {
		if !g.Test([]byte(sid)) {
			t.Errorf("Test failed for %s after deserialize", sid)
		}
	}
}

func TestDeserializeSizeMismatchEmpty(t *testing.T) {
	m, k := EstimateParameters(3, 0.0001)
	f := Deserialize([]byte{0xff, 0xff}, m, k)
	if f == nil {
		t.Fatal("expected non-nil filter on size mismatch")
	}
	for _, b := range f.ToBytes() {
		if b != 0 {
			t.Errorf("expected fresh empty filter on size mismatch, got %x", f.ToBytes())
			break
		}
	}
}
