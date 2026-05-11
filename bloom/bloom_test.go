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
	{"6162636465666768696a6b6c6d6e6f70", 0x2d87e528e8e904da, 0x5a3791da31979037},
	{"6162636465666768696a6b6c6d6e6f7071", 0xa31559aebd386341, 0x4951905316b7f030},
	{"38656532376131313562366661363235", 0x48b0d429007933e0, 0xd5e9d9ce0b15d0a1},
	{"39323866313838656632343039383131", 0x2a250b6b98d6d6d6, 0x7e36e9f561339878},
	{
		"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
			"202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		0x7cda024b02ea4442, 0xab5b36a8550f168e,
	},
	{
		"0000000000000000000000000000000000000000000000000000000000000000",
		0x72860edc439f3197, 0x22502ad8d7d69e94,
	},
	{"ffffffffffffffffffffffffffffffffff", 0x4da9f995940cd531, 0x4494d81c4c256ec4},
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
	{"6162636465666768696a6b6c6d6e6f70", 0x286e615e58a554c4, 0x67983d3e3ebe6041},
	{"6162636465666768696a6b6c6d6e6f7071", 0x936e528bb5b79148, 0x03672630061ecf3d},
	{"38656532376131313562366661363235", 0x6312ee3c356c9272, 0xff84c7e1f5cabe9d},
	{"39323866313838656632343039383131", 0x62a7006acf18403a, 0x8a4807de7d0c5b50},
	{
		"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
			"202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		0x630b025e30ab923a, 0x3e93705c7d90c108,
	},
	{
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xe307b0929e2fbdc4, 0x62f732fe7fb876bb,
	},
	{"ffffffffffffffffffffffffffffffffff", 0x6f36e2521b2238cc, 0xea3c341c333e4f67},
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
		"841002",
		"f77c0e",
		"ffff0f",
		"ffff0f",
		"ffff0f",
	}},
	{3, []string{
		"60100c8261100c02",
		"64599ea669104c02",
		"e55bbff669106c02",
		"ed5bbff6691aee02",
		"fd5fbff6791eef02",
	}},
	{10, []string{
		"aa2a00000000000000000000551500000000000000000000",
		"aa6a00021080000420000108551502108000042000000000",
		"aa6a010230c0000428000109571542908000142000020400",
		"aa6a890230c0080428880109d71d4290880014a800028408",
		"aa6b89023cc008642888010fd71d7290880017a8001a8408",
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
