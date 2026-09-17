package recon

import (
	"fmt"
	"testing"

	"bridges/bloom"
	"bridges/bridge"
)

// The two leaf payloads share the ancestry right -> missing -> A, then
// diverge. X is a real false positive in F's filter and a negative in G's.
// Losing missing and N creates F=(A,U,V,CF) and G=(CG). Joining G below A
// must not silently inherit a previously guessed missing -> X attachment.
func amqJoinFixture(t *testing.T, mode string, ranged bool) ([]Span, Config) {
	t.Helper()
	const root, right, x = uint64(0xf000001), uint64(0x10000000000), uint64(0x46e97)
	const missing, a, u, v = right + 1, right + 2, right + 3, right + 4
	const cf, n, cg, xl = right + 5, right + 6, right + 7, right + 8
	const cpd = 8
	cfg := NewPCRBConfig(cpd, 8, 1e-4)
	cfg.FPBits, cfg.SB3TopoOnly = 64, true
	cfg.RandomizedCheckpoints, cfg.CheckpointMin = ranged, cpd
	var h bridge.Handler
	switch mode {
	case "pb0":
		p := bridge.NewPCRBBridgeHandler(cpd, 8, cfg.BloomFP)
		p.Capture = true
		h = p
	case "cgp0":
		p := bridge.NewCGPRBBridgeHandler(cpd, 8, cfg.BloomFP)
		p.Capture = true
		h = p
	case "sb3":
		p := bridge.NewSB3Handler(cpd, 8, cfg.BloomFP, nil)
		p.Capture, p.TopoOnly, p.FPBits = true, true, 64
		h = p
	}
	if ranged {
		// Keep this small counterexample inside one window, but encode its
		// assigned distance through the actual 2:8 policy, not a fixed range.
		policy := &bridge.CheckpointRange{Min: 2, Max: cpd}
		for ; policy.Seed < 256; policy.Seed++ {
			probe := bridge.NewPCRBBridgeHandler(cpd, 8, cfg.BloomFP)
			if err := bridge.ConfigureCheckpoints(probe, policy); err != nil {
				t.Fatal(err)
			}
			r := probe.OnStart(&bridge.Event{TraceID: 1, SpanID: root}, 1)
			if policy.AssignedDistance(r.CheckpointTTL) == cpd {
				break
			}
		}
		if policy.Seed == 256 {
			t.Fatal("no seed assigned the full window")
		}
		cfg.CheckpointMin = policy.Min
		if err := bridge.ConfigureCheckpoints(h, policy); err != nil {
			t.Fatal(err)
		}
	}
	nodes := []struct {
		id, parent uint64
		depth, seq int
	}{
		{root, 0, 0, 1}, {right, root, 1, 1}, {missing, right, 2, 1},
		{a, missing, 3, 1}, {u, a, 4, 1}, {v, u, 5, 1}, {cf, v, 6, 1},
		{n, a, 4, 2}, {cg, n, 5, 1}, {x, root, 1, 2}, {xl, x, 2, 1},
	}
	payloads := make(map[uint64][]byte)
	for _, s := range nodes {
		r := h.OnStart(&bridge.Event{TraceID: 1, SpanID: s.id, ParentID: s.parent}, s.seq)
		if r.Payload != nil {
			payloads[s.id] = r.Payload
		}
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		s := nodes[i]
		r := h.OnEnd(&bridge.Event{TraceID: 1, SpanID: s.id, ParentID: s.parent})
		if r.Payload != nil {
			payloads[s.id] = r.Payload
		}
	}
	var survivors []Span
	for _, s := range nodes {
		if s.id == missing || s.id == n {
			continue
		}
		v := Span{SpanID: s.id, ParentID: s.parent, Depth: s.depth}
		if p := payloads[s.id]; p != nil {
			var err error
			switch mode {
			case "pb0":
				v.Depth, v.CkptPrefix, v.BloomBits, err = DecodePCRBPayload(p, cfg)
			case "cgp0":
				v.Depth, v.CkptPrefix, v.BloomBits, v.HA, err = DecodeCGPRBPayload(p, cfg)
			case "sb3":
				v.Depth, v.CkptPrefix, v.BloomBits, v.HA, v.SparseOrdinals, err = DecodeSB3SpanPayload(p, cfg)
			}
			if err != nil {
				t.Fatal(err)
			}
			v.LeafCarrier = v.Depth%cpd != 0
			v.WindowCPD, v.BloomM, v.BloomK, err = DecodeBloomGeometry(p, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if s.id == cf || s.id == cg {
				key := bridge.HexOf(x)
				if got := bloom.Deserialize(v.BloomBits, v.BloomM, v.BloomK).Test(key[:]); got != (s.id == cf) {
					t.Fatalf("fixture carrier %x: unexpected membership of X", s.id)
				}
			}
		}
		survivors = append(survivors, v)
	}
	return survivors, cfg
}

// Independently check the emitted parent map rather than the candidate sets
// or tracker state. All fixtures here carry complete checkpoint-root IDs.
func assertCarrierAMQs(t *testing.T, survivors []Span, cfg Config, result Result) {
	t.Helper()
	for _, carrier := range survivors {
		if carrier.BloomBits == nil || carrier.ParentID == 0 {
			continue
		}
		var root uint64
		for _, b := range carrier.CkptPrefix {
			root = root<<8 | uint64(b)
		}
		m, k := carrier.BloomM, carrier.BloomK
		if m == 0 {
			m, k = cfg.BloomM, cfg.BloomK
		}
		bf := bloom.Deserialize(carrier.BloomBits, m, k)
		seen := make(map[uint64]bool)
		for id := result.ReconParent[carrier.SpanID]; id != root; id = result.ReconParent[id] {
			if id == 0 || seen[id] {
				t.Fatalf("carrier %x does not reach checkpoint %x", carrier.SpanID, root)
			}
			seen[id] = true
			if !result.ReconAnon[id] && !bf.Test([]byte(fmt.Sprintf("%016x", id))) {
				t.Fatalf("carrier %x rejects reconstructed ancestor %x", carrier.SpanID, id)
			}
		}
	}
}

func TestGreedyAMQChecksAncestryAcrossFragmentJoins(t *testing.T) {
	for _, mode := range []string{"pb0", "cgp0", "sb3"} {
		for _, ranged := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ranged=%t", mode, ranged), func(t *testing.T) {
				survivors, cfg := amqJoinFixture(t, mode, ranged)
				var result Result
				switch mode {
				case "pb0":
					result = ReconstructPB0(survivors, cfg)
				case "cgp0":
					result = ReconstructCGP0(survivors, cfg)
				case "sb3":
					r := ReconstructSB3(survivors, cfg)
					result = r.Topology
					if !r.Compatible {
						t.Errorf("SB3 incompatible: %s", r.Reason)
					}
				}
				assertCarrierAMQs(t, survivors, cfg, result)
				if result.GreedyHardConflicts != 0 {
					t.Fatalf("hard conflicts: %d", result.GreedyHardConflicts)
				}
			})
		}
	}
}

func TestGreedyAMQPendingFilterAndRollback(t *testing.T) {
	const root, right, x = uint64(0xf000001), uint64(0x10000000000), uint64(0x46e97)
	const missing, a, n, cg = right + 1, right + 2, right + 6, right + 7
	survivors, cfg := amqJoinFixture(t, "pb0", true)
	cfg.NoFanout, cfg.SB3IgnoreOrdinals = true, true
	sk := cgpParse(survivors, cfg)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	units := sb3IntersectRouteUnits(sk, sb3CollectFragmentEvidence(sk, cfg))
	parent := sb3SeedGreedyParent(sk, units)
	tracker := newGreedyAMQTracker(sk, cfg, parent)
	if tracker.conflicts(parent) != 0 {
		t.Fatal("fixture starts with contradictory literal evidence")
	}
	// Join G first. Its filter must wait at missing, alongside F's filter,
	// even though A and its parent do not yet connect to the checkpoint.
	parent[n] = a
	_, ok := tracker.tryEdges([]uint64{n}, parent)
	if !ok {
		t.Fatal("incomplete upstream path was rejected")
	}
	if len(tracker.waiting[missing]) != 2 {
		t.Fatalf("filters waiting above F: %d, want both F and G", len(tracker.waiting[missing]))
	}
	parent[missing] = x
	if _, ok := tracker.tryEdges([]uint64{missing}, parent); ok {
		t.Fatal("pending G filter did not reject X")
	}
	delete(parent, missing)
	// A trial with the correct ancestor can pass AMQ checks but be rejected
	// by another constraint. Its rollback must restore both pending filters.
	parent[missing] = right
	trial, ok := tracker.tryEdges([]uint64{missing}, parent)
	if !ok {
		t.Fatal("correct upstream path was rejected")
	}
	trial.rollback()
	delete(parent, missing)
	parent[missing] = x
	if _, ok := tracker.tryEdges([]uint64{missing}, parent); ok {
		t.Fatal("rolled-back trial lost the pending downstream AMQ")
	}
	delete(parent, missing)
	parent[missing] = right
	if _, ok := tracker.tryEdges([]uint64{missing}, parent); !ok {
		t.Fatal("correct retry failed after rollback")
	}
	if !sb3HasAncestor(parent, cg, root) || tracker.conflicts(parent) != 0 {
		t.Fatal("completed path failed the independent ancestry audit")
	}
}

func TestGreedyAMQReportsContradictoryInput(t *testing.T) {
	survivors, cfg := amqJoinFixture(t, "pb0", false)
	const cf = uint64(0x10000000005)
	for i := range survivors {
		if survivors[i].SpanID == cf {
			// An empty payload cannot describe this carrier's surviving parent
			// chain. Preserve the literal edges and report the invalid evidence.
			survivors[i].BloomBits = make([]byte, len(survivors[i].BloomBits))
		}
	}
	result := ReconstructPB0(survivors, cfg)
	if result.GreedyAMQConflicts != 1 || result.GreedyHardConflicts != 1 {
		t.Fatalf("invalid carrier was not reported: AMQ=%d hard=%d", result.GreedyAMQConflicts, result.GreedyHardConflicts)
	}
	for _, s := range survivors {
		if s.ParentID != 0 && result.ReconParent[s.SpanID] != s.ParentID {
			t.Fatalf("changed literal parent of %x to accommodate invalid AMQ", s.SpanID)
		}
	}
}
