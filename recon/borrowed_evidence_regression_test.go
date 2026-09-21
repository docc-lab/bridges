package recon

import (
	"fmt"
	"testing"
)

// TestBorrowedEvidenceYieldsToExactEvidence covers the day-2 trace that
// produced the only hard-evidence violations in an 843,274-trace corpus
// (shard 46 at 25% loss). An orphan fragment borrowed a carrier's window Bloom
// on a two-hit false positive: the filter admitted the orphan root and its
// named parent, yet the carrier lay on an unrelated branch under the same
// window root. The engine then held that borrowed filter as hard evidence. It
// rejected the fanout that a sibling's HA record required at depth 8, so
// every route for the shared parent was refused, the unit dangled, the
// obligations of two other carriers ran through it, and two HA conflicts were
// reported while the last-resort attach failed without a trace.
//
// The fixture must now reconstruct cleanly for every engine, and the emitted
// topology must carry the HA-required ancestry that the borrowed filter
// denied. PB0 carries no fanout witnesses, so HA does not apply to it.
func TestBorrowedEvidenceYieldsToExactEvidence(t *testing.T) {
	const (
		haCarrierA uint64 = 0x68640f091029810a // HA: fanout 9554c6d9... at depth 8
		fanoutA    uint64 = 0x9554c6d9463aa32d
		haCarrierB uint64 = 0xa64b6ebf4c22f7a5 // HA: fanout b1b2554d... at depth 10
		fanoutB    uint64 = 0xb1b2554d4ae1697a
		orphanRoot uint64 = 0xaada68fd71ce380f // the fragment that borrowed wrongly
		trueBorrow uint64 = 0xa64b6ebf4c22f7a5 // its only true bloom-bearing descendant
	)
	traces, err := LoadDumpedTraces("testdata/borrowedevidence_36e959b69e640101.gob")
	if err != nil || len(traces) != 1 {
		t.Fatalf("fixture: %v (%d traces)", err, len(traces))
	}
	tr := traces[0]

	engines := []struct {
		name string
		fn   func([]Span, Config) Result
	}{
		{"pb0", ReconstructPB0},
		{"cgp0", ReconstructCGP0},
		{"sb3", func(s []Span, c Config) Result { return ReconstructSB3(s, c).Topology }},
	}
	for _, eng := range engines {
		t.Run("default/"+eng.name, func(t *testing.T) {
			res := eng.fn(tr.Survivors, tr.Cfg)
			assertClean(t, tr.Survivors, res, eng.name != "pb0")
			if eng.name == "pb0" {
				return
			}
			if !sb3HasAncestor(res.ReconParent, haCarrierA, fanoutA) {
				t.Errorf("carrier %016x does not descend through HA fanout %016x", haCarrierA, fanoutA)
			}
			if !sb3HasAncestor(res.ReconParent, haCarrierB, fanoutB) {
				t.Errorf("carrier %016x does not descend through HA fanout %016x", haCarrierB, fanoutB)
			}
			// With exact ancestry checked at borrow time, the orphan borrows
			// from its true descendant instead of the false-positive carrier.
			for _, b := range res.Bridges {
				if b.OrphanID == orphanRoot && b.ViaCarrier != 0 && b.ViaCarrier != trueBorrow {
					t.Errorf("orphan %016x borrowed from %016x, want %016x", orphanRoot, b.ViaCarrier, trueBorrow)
				}
			}
		})
	}

	// Without borrow-time validation the false-positive borrow is admitted, as
	// it was historically. The routing-time retraction must then be what
	// restores the invariant, and it must report that it did so.
	greedyBorrowValidation = false
	defer func() { greedyBorrowValidation = true }()
	for _, eng := range engines[1:] {
		t.Run("retraction/"+eng.name, func(t *testing.T) {
			res := eng.fn(tr.Survivors, tr.Cfg)
			assertClean(t, tr.Survivors, res, true)
			if res.GreedyBorrowRetractions == 0 {
				t.Errorf("expected the borrowed filter to be retracted; got 0 retractions")
			}
			if !sb3HasAncestor(res.ReconParent, haCarrierA, fanoutA) || !sb3HasAncestor(res.ReconParent, haCarrierB, fanoutB) {
				t.Errorf("HA-required ancestry missing after retraction")
			}
		})
	}
	// The ablation reproduces the historical failure, and the failure is now
	// loud: a unit that knows a certain ancestor but cannot reach it counts.
	t.Run("ablation-reproduces-violation/cgp0", func(t *testing.T) {
		cfg := tr.Cfg
		cfg.GreedyNoBorrowRetraction = true
		res := ReconstructCGP0(tr.Survivors, cfg)
		_, ha, _ := sb3CheckHardEvidenceForMode(tr.Survivors, res, true)
		if ha == 0 && res.GreedyUnroutedUnits == 0 {
			t.Errorf("ablation unexpectedly clean: the fixture no longer exercises the defect")
		}
		if res.GreedyHardConflicts == 0 {
			t.Errorf("hard_conflicts must be nonzero when a unit with a certain ancestor dangles")
		}
	})
}

func assertClean(t *testing.T, survivors []Span, res Result, includeHA bool) {
	t.Helper()
	parent, ha, _ := sb3CheckHardEvidenceForMode(survivors, res, includeHA)
	if parent != 0 || ha != 0 || res.GreedyAMQConflicts != 0 || res.GreedyUnroutedUnits != 0 {
		t.Errorf("hard evidence violated: parent=%d ha=%d amq=%d unrouted=%d",
			parent, ha, res.GreedyAMQConflicts, res.GreedyUnroutedUnits)
	}
	if res.GreedyHardConflicts != 0 {
		t.Errorf("hard_conflicts=%d, want 0", res.GreedyHardConflicts)
	}
	if n := len(res.Unanchored); n != 0 {
		t.Errorf("unanchored orphans: %d (%s)", n, fmt.Sprintf("%x", res.Unanchored[:min(n, 4)]))
	}
}
