package recon

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestHardEvidenceSatisfiedOnCertainRootFallback covers the two day-1 traces
// that produced the only hard-evidence violations in a 521k-trace corpus. Both
// contain a route unit that exhausts every Bloom candidate; before the certain
// root fallback its head was left unparented, which silently broke the
// mandatory fanout obligations of unrelated carriers whose ancestry ran through
// it. The fanout the fallback must place was itself already routed under a
// named node the newly attached carriers' filters exclude, so the fallback also
// has to give that fanout a private ancestry up to the certain window root.
//
// The assertion is the engine's own invariant: an emitted topology contradicts
// no applicable hard evidence. PB0 carries no fanout witnesses, so HA does not
// apply to it.
func TestHardEvidenceSatisfiedOnCertainRootFallback(t *testing.T) {
	engines := []struct {
		name string
		fn   func([]Span, Config) Result
	}{
		{"pb0", ReconstructPB0},
		{"cgp0", ReconstructCGP0},
		{"sb3", func(s []Span, c Config) Result { return ReconstructSB3(s, c).Topology }},
	}
	paths, err := filepath.Glob("testdata/hardevidence_*.gob")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, path := range paths {
		traces, err := LoadDumpedTraces(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if len(traces) == 0 {
			t.Fatalf("%s: no traces", path)
		}
		for _, tr := range traces {
			for _, eng := range engines {
				t.Run(fmt.Sprintf("%016x/%s", tr.TID, eng.name), func(t *testing.T) {
					res := eng.fn(tr.Survivors, tr.Cfg)
					parent, ha, _ := sb3CheckHardEvidenceForMode(tr.Survivors, res, eng.name != "pb0")
					if parent != 0 || ha != 0 || res.GreedyAMQConflicts != 0 {
						t.Errorf("hard evidence violated: parent=%d ha=%d amq=%d",
							parent, ha, res.GreedyAMQConflicts)
					}
					if n := len(res.Unanchored); n != 0 {
						t.Errorf("unanchored orphans: %d", n)
					}
				})
			}
		}
	}
}
