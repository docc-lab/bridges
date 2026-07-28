package bloom

import (
	"encoding/hex"
	"testing"
)

func TestPrehashReal(t *testing.T) {
	// real leaf d1944ac0's _br bloom (12 bytes) from traces_pb.json
	bl, _ := hex.DecodeString("9a244b836461920c4dd22059")
	f := DeserializePrehashed(bl, 96, 14)
	t.Logf("popcount=%d prehashed=%v", f.PopCount(), f.prehashed)
	// its true intermediates must test positive (recon passes 16-hex-char keys)
	for _, id := range []string{"000b7e6884c35376", "6121642e39780067", "eb80ba19483f10ce"} {
		if !f.Test([]byte(id)) {
			t.Errorf("intermediate %s: expected PRESENT, got absent", id)
		}
	}
	// the leaf itself was not inserted -> should be absent
	if f.Test([]byte("d1944ac07e6edff9")) {
		t.Logf("note: leaf tests present (bloom FP)")
	}
}
