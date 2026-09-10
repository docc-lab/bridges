package recon

import (
	"testing"

	"bridges/bridge"
)

// These overlapping windows start at depths 4 and 5. Saturated Blooms make
// every ID look plausible, so only checkpoint identity can keep them apart.
// Span 207 has a missing parent but a connected leaf naming checkpoint 205.
func overlappingCheckpointWindows(cfg Config) []Span {
	carrier := func(id, parent uint64, depth int, root uint64, leaf bool) Span {
		bits := make([]byte, (cfg.BloomM+7)/8)
		for i := range bits {
			bits[i] = 0xff
		}
		prefix := bridge.BigEndian8(root)
		return Span{SpanID: id, ParentID: parent, Depth: depth, CkptPrefix: prefix[:cfg.PrefixLen], BloomBits: bits, LeafCarrier: leaf}
	}
	return []Span{
		carrier(100, 0, 0, 100, false),
		{SpanID: 101, ParentID: 100, Depth: 1},
		carrier(102, 101, 2, 100, false),
		{SpanID: 103, ParentID: 102, Depth: 3},
		carrier(104, 103, 4, 102, false),
		{SpanID: 201, ParentID: 100, Depth: 1},
		carrier(202, 201, 2, 100, false),
		{SpanID: 203, ParentID: 202, Depth: 3},
		{SpanID: 204, ParentID: 203, Depth: 4},
		carrier(205, 204, 5, 202, false),
		{SpanID: 207, ParentID: 206, Depth: 7},
		carrier(208, 207, 8, 205, true),
		carrier(209, 206, 7, 205, true),
		carrier(110, 109, 10, 104, true),
	}
}

func TestVariableCheckpointWindowsPruneForeignAnchorsAndFanouts(t *testing.T) {
	for _, mode := range []string{"pb0", "cgp0", "sb3"} {
		t.Run(mode, func(t *testing.T) {
			cfg := NewPCRBConfig(8, 8, 1e-4)
			cfg.RandomizedCheckpoints = true
			cfg.NoFanout = mode == "pb0"
			survivors := overlappingCheckpointWindows(cfg)
			if !cfg.NoFanout {
				survivors[11].HA = []HAEntry{{ParentID: 206, Depth: 7}}
			}
			sk := cgpParse(survivors, cfg)
			cgpResolveEvidence(sk, cfg)
			cgpResolveAnchors(sk, cfg)
			e := sb3CollectFragmentEvidence(sk, cfg)[110]
			if !e.resolved || e.ckpt.SpanID != 104 {
				t.Fatal("target fragment did not resolve to checkpoint 104")
			}
			for _, a := range e.anchors {
				if a.SpanID == 207 || a.SpanID == 205 {
					t.Errorf("accepted anchor %d from checkpoint 205's window", a.SpanID)
				}
			}
			for _, ids := range e.fanouts {
				for _, id := range ids {
					if id == 206 {
						t.Error("accepted witnessed fanout from checkpoint 205's window")
					}
				}
			}
			// PB0 and CGP0 exercise the complete greedy pipeline. SB3 shares
			// these candidates; sparse structure is tested with emitted payloads.
			var result Result
			switch mode {
			case "pb0":
				result = ReconstructPB0(survivors, cfg)
			case "cgp0":
				result = ReconstructCGP0(survivors, cfg)
			default:
				return
			}
			if !sb3HasAncestor(result.ReconParent, 110, 104) || sb3HasAncestor(result.ReconParent, 110, 205) {
				t.Fatal("greedy selection attached the target fragment to another window")
			}
		})
	}
}

func TestCheckpointWindowsSeparateIncomingAndOutgoingRoles(t *testing.T) {
	cfg := NewPCRBConfig(8, 8, 1e-4)
	cfg.RandomizedCheckpoints = true
	survivors := overlappingCheckpointWindows(cfg)
	sk := cgpParse(survivors, cfg)
	for _, tc := range []struct{ carrier, root uint64 }{
		{102, 100}, {104, 102}, {205, 202}, {208, 205}, {209, 205}, {110, 104},
	} {
		hits := sk.checkpoints.matches(sk.byID[tc.carrier])
		if len(hits) != 1 || hits[0].SpanID != tc.root {
			t.Fatalf("carrier %d did not resolve to incoming checkpoint %d", tc.carrier, tc.root)
		}
	}
	for _, tc := range []struct{ span, root uint64 }{
		{103, 102}, {104, 104}, {205, 205}, {206, 205}, {207, 205}, {208, 205},
	} {
		if !sk.checkpoints.allows(tc.span, []*Span{sk.byID[tc.root]}) {
			t.Errorf("span %d lost outgoing checkpoint %d", tc.span, tc.root)
		}
	}
	if sk.checkpoints.allows(104, []*Span{sk.byID[102]}) {
		t.Fatal("checkpoint 104's descendants were assigned its preceding window")
	}
}

func TestCheckpointWindowsResolveOnlySupportedPrefixCollisions(t *testing.T) {
	// Every small fixture ID has the same one-byte prefix. Connected checkpoint
	// records resolve some windows; the disconnected leaf's root stays ambiguous.
	cfg := NewPCRBConfig(8, 1, 1e-4)
	cfg.RandomizedCheckpoints = true
	sk := cgpParse(overlappingCheckpointWindows(cfg), cfg)
	for _, tc := range []struct{ carrier, root uint64 }{{104, 102}, {205, 202}} {
		hits := sk.checkpoints.matches(sk.byID[tc.carrier])
		if len(hits) != 1 || hits[0].SpanID != tc.root {
			t.Fatalf("connected checkpoint evidence did not resolve carrier %d", tc.carrier)
		}
	}
	if hits := sk.checkpoints.matches(sk.byID[110]); len(hits) < 2 || commonCheckpointDepth(hits) {
		t.Fatal("unconnected prefix collision was silently assigned a window root")
	}
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	for _, f := range sk.frags {
		if f.root.SpanID == 110 && f.anchorCkpt != nil {
			t.Fatal("ambiguous fragment received an arbitrary checkpoint ceiling")
		}
	}
}

func TestBorrowedCheckpointWindowConstrainsLaterCandidates(t *testing.T) {
	cfg := NewPCRBConfig(8, 8, 1e-4)
	cfg.RandomizedCheckpoints, cfg.NoFanout = true, true
	var survivors []Span
	for _, s := range overlappingCheckpointWindows(cfg) {
		switch s.SpanID {
		case 208:
			// A missing span at depth 8 disconnects span 207 from this leaf.
			s.SpanID, s.ParentID, s.Depth = 209, 208, 9
		case 209:
			continue
		}
		survivors = append(survivors, s)
	}
	sk := cgpParse(survivors, cfg)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	evidence := sb3CollectFragmentEvidence(sk, cfg)
	if e := evidence[207]; !e.resolved || e.frag.viaCarrier != 209 || e.ckpt.SpanID != 205 {
		t.Fatal("fragment 207 did not borrow the covering window from leaf 209")
	}
	for _, a := range evidence[110].anchors {
		if a.SpanID == 207 {
			t.Fatal("a fragment assigned to checkpoint 205 was offered to checkpoint 104")
		}
	}
	result := ReconstructPB0(survivors, cfg)
	if !sb3HasAncestor(result.ReconParent, 110, 104) || !sb3HasAncestor(result.ReconParent, 207, 205) {
		t.Fatal("greedy routes failed to preserve their assigned checkpoint roots")
	}
}
