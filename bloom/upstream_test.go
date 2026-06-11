package bloom

import (
	"fmt"
	"math/rand"
	"testing"

	upstream "github.com/bits-and-blooms/bloom"
	"github.com/spaolacci/murmur3"
)

// TestMurmurMatchesSpaolacci pins our MurmurHash3_128 (seed 0) to the
// canonical implementation upstream uses, across input lengths that exercise
// both the block path (>= 16 bytes) and the tail path. The original port had
// dropped the h1 += h2 / h2 += h1 cross-mixing in the block loop, which
// silently corrupted every 16-byte span-ID-hex key.
func TestMurmurMatchesSpaolacci(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for _, n := range []int{0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 64, 100} {
		for trial := 0; trial < 50; trial++ {
			data := make([]byte, n)
			rng.Read(data)
			h1, h2 := MurmurHash3_128(data, 0)
			s1, s2 := murmur3.Sum128(data)
			if h1 != s1 || h2 != s2 {
				t.Fatalf("len=%d data=%x: ours=(%016x,%016x) spaolacci=(%016x,%016x)",
					n, data, h1, h2, s1, s2)
			}
		}
	}
}

// TestProbePositionsMatchUpstream validates that our probe schedule is
// bit-faithful to bits-and-blooms (the library this filter is meant to be a
// compressed-export variant of): for random keys, our positions mod m must
// equal upstream Locations(data, k) mod m, in order.
func TestProbePositionsMatchUpstream(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, cpd := range []int{1, 3, 5, 10} {
		m, k := EstimateParameters(cpd, 0.0001)
		for trial := 0; trial < 200; trial++ {
			data := []byte(fmt.Sprintf("%016x", rng.Uint64()))
			h1, h2, h3, h4 := BaseHashes(data)
			h := [4]uint64{h1, h2, h3, h4}
			up := upstream.Locations(data, uint(k))
			for i := uint32(0); i < k; i++ {
				ours := location(&h, i) % uint64(m)
				theirs := up[i] % uint64(m)
				if ours != theirs {
					t.Fatalf("cpd=%d key=%s probe %d: ours=%d upstream=%d", cpd, data, i, ours, theirs)
				}
			}
		}
	}
}

// TestFilterAgreesWithUpstream drives identical Add/Test sequences through
// our filter and the upstream library; every Test result must agree.
func TestFilterAgreesWithUpstream(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	m, k := EstimateParameters(3, 0.0001)
	ours, _ := New(m, k)
	theirs := upstream.New(uint(m), uint(k))

	var members [][]byte
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("%016x", rng.Uint64()))
		ours.Add(data)
		theirs.Add(data)
		members = append(members, data)
	}
	for _, data := range members {
		if !ours.Test(data) {
			t.Errorf("false negative on member %s", data)
		}
	}
	for i := 0; i < 100000; i++ {
		data := []byte(fmt.Sprintf("%016x", rng.Uint64()))
		if ours.Test(data) != theirs.Test(data) {
			t.Fatalf("Test disagreement on %s: ours=%t upstream=%t", data, ours.Test(data), theirs.Test(data))
		}
	}
}

// TestEffectiveFPRate is the regression test for the degenerate-probe bug:
// at the cpd=3 geometry (m=58, k=14), the measured FP rate over random
// foreign keys must be within an order of magnitude of the configured 1e-4.
// The old h1+i*h2 schedule measured ~2e-2 here, with ~1.7% of keys testing
// positive in half of all filters.
func TestEffectiveFPRate(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	m, k := EstimateParameters(3, 0.0001)
	trials, fps := 0, 0
	for i := 0; i < 5000; i++ {
		f, _ := New(m, k)
		for j := 0; j < 3; j++ {
			f.Add([]byte(fmt.Sprintf("%016x", rng.Uint64()&0xffffffff)))
		}
		for j := 0; j < 10; j++ {
			if f.Test([]byte(fmt.Sprintf("%016x", rng.Uint64()&0xffffffff))) {
				fps++
			}
			trials++
		}
	}
	rate := float64(fps) / float64(trials)
	t.Logf("measured FP rate: %d/%d = %.2e (configured 1e-4)", fps, trials, rate)
	if rate > 1e-3 {
		t.Fatalf("FP rate %.2e exceeds 1e-3: probe schedule may have regressed", rate)
	}
}
