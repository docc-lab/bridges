package recon

import (
	"fmt"
	"testing"

	"bridges/bridge"
)

// reverseCollected mirrors the reconstruction harness's collected record: the
// span's own exported payload (if any) and the returned-truss bundle it exported.
type reverseCollected struct {
	spanID, parentID uint64
	depth            int
	br, ckpt         []byte
	demoted          bool // carried only returned trusses: not a checkpoint
}

// reverseReconFixture replays one synthetic trace through the real
// ReverseHandler and returns the collected records, truth, DEEs, and config.
func reverseReconFixture(t *testing.T, mode string, policy *bridge.CheckpointRange, fixedCPD int, rc bridge.ReverseConfig) ([]reverseCollected, []TruthSpan, []bridge.DEEQuad, Config) {
	t.Helper()
	cpd := fixedCPD
	if policy != nil {
		cpd = policy.MaxDistance()
	}
	cfg := NewPCRBConfig(cpd, 8, 1e-12)
	cfg.FPBits = 64
	if policy != nil {
		cfg.RandomizedCheckpoints = true
		cfg.CheckpointMin = policy.Min
	}
	var base bridge.Handler
	var dees []bridge.DEEQuad
	switch mode {
	case "pb0":
		p := bridge.NewPCRBBridgeHandler(cpd, 8, 1e-12)
		p.Capture = true
		base = p
	case "cgp0":
		p := bridge.NewCGPRBBridgeHandler(cpd, 8, 1e-12)
		p.Capture = true
		base = p
	case "sb3":
		p := bridge.NewSB3Handler(cpd, 8, 1e-12, nil)
		p.Capture = true
		p.FPBits = 64
		p.DEESink = func(_ uint64, raw []byte) {
			q, err := bridge.DecodeDEEQuads(raw, 64)
			if err != nil {
				t.Fatal(err)
			}
			dees = append(dees, q...)
		}
		base = p
	default:
		t.Fatalf("unknown mode %s", mode)
	}
	if policy != nil {
		if err := bridge.ConfigureCheckpoints(base, policy); err != nil {
			t.Fatal(err)
		}
	}
	h, err := bridge.NewReverseHandler(base, rc)
	if err != nil {
		t.Fatal(err)
	}
	// Unequal fanouts with leaves at many depths, so unscheduled leaves occur
	// above, below, and directly under original checkpoints.
	var truth []TruthSpan
	var build func(uint64, int)
	build = func(parent uint64, depth int) {
		id := uint64(len(truth) + 1)
		truth = append(truth, TruthSpan{SpanID: id, ParentID: parent, Depth: depth})
		if depth >= 9 {
			return
		}
		build(id, depth+1)
		if depth%2 == 0 {
			build(id, depth+1) // second branch
		}
		if depth%3 == 1 {
			// an early leaf directly under this node
			leaf := uint64(len(truth) + 1)
			truth = append(truth, TruthSpan{SpanID: leaf, ParentID: id, Depth: depth + 1})
		}
	}
	build(0, 0)
	records := make(map[uint64]*reverseCollected, len(truth))
	seq := make(map[uint64]int)
	for _, s := range truth {
		seq[s.ParentID]++
		r := h.OnStart(&bridge.Event{TraceID: 77, SpanID: s.SpanID, ParentID: s.ParentID}, seq[s.ParentID])
		records[s.SpanID] = &reverseCollected{spanID: s.SpanID, parentID: s.ParentID, depth: -1, br: r.Payload}
	}
	for i := len(truth) - 1; i >= 0; i-- {
		s := truth[i]
		r := h.OnEnd(&bridge.Event{TraceID: 77, SpanID: s.SpanID, ParentID: s.ParentID})
		rec := records[s.SpanID]
		rec.depth = r.Depth
		if r.Payload != nil {
			rec.br = r.Payload
		}
		if r.Reverse != nil {
			if len(r.Reverse.CheckpointContext) > 0 {
				rec.ckpt = append([]byte(nil), r.Reverse.CheckpointContext...)
			}
			if r.Reverse.PromotedCheckpoint {
				rec.br, rec.demoted = nil, true
			}
		}
	}
	h.EvictTrace(77)
	out := make([]reverseCollected, 0, len(truth))
	for _, s := range truth {
		rec := records[s.SpanID]
		if rec.depth != s.Depth {
			t.Fatalf("span %d handler depth %d, truth %d", s.SpanID, rec.depth, s.Depth)
		}
		out = append(out, *rec)
	}
	return out, truth, dees, cfg
}

// decodeReverseCollected mirrors harness.decodeSpan for the greedy engines.
func decodeReverseCollected(t *testing.T, mode string, rec reverseCollected, cfg Config) Span {
	t.Helper()
	sp := Span{SpanID: rec.spanID, ParentID: rec.parentID, Depth: rec.depth}
	if rec.br == nil {
		return sp
	}
	var err error
	switch mode {
	case "pb0":
		sp.Depth, sp.CkptPrefix, sp.BloomBits, err = DecodePCRBPayload(rec.br, cfg)
	case "cgp0":
		sp.Depth, sp.CkptPrefix, sp.BloomBits, sp.HA, err = DecodeCGPRBPayload(rec.br, cfg)
	case "sb3":
		sp.Depth, sp.CkptPrefix, sp.BloomBits, sp.HA, sp.SparseOrdinals, err = DecodeSB3SpanPayload(rec.br, cfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RandomizedCheckpoints {
		sp.LeafCarrier = bridge.IsLeafPayload(rec.br)
		if sp.WindowCPD, sp.BloomM, sp.BloomK, err = DecodeBloomGeometry(rec.br, cfg); err != nil {
			t.Fatal(err)
		}
	} else {
		sp.LeafCarrier = sp.Depth%max(1, cfg.CPD) != 0
	}
	return sp
}

func TestReverseReconstruction(t *testing.T) {
	type scenario struct {
		name string
		drop func(rec reverseCollected, records map[uint64]reverseCollected, rejected map[uint64]bool) bool
	}
	scenarios := []scenario{
		{"origins", func(rec reverseCollected, _ map[uint64]reverseCollected, rejected map[uint64]bool) bool {
			return rejected[rec.spanID]
		}},
		{"origins_and_parents", func(rec reverseCollected, records map[uint64]reverseCollected, rejected map[uint64]bool) bool {
			if rejected[rec.spanID] {
				return true
			}
			if rec.br != nil {
				return false
			}
			for _, child := range records {
				if child.parentID == rec.spanID && rejected[child.spanID] {
					return true
				}
			}
			return false
		}},
		{"all_unprotected", func(rec reverseCollected, _ map[uint64]reverseCollected, _ map[uint64]bool) bool {
			return rec.br == nil
		}},
	}
	policies := []struct {
		name string
		rc   bridge.ReverseConfig
	}{
		{"inverse_depth", bridge.ReverseConfig{Policy: "inverse_depth", LeafRejectProbability: 1, Seed: 42}},
		{"promote_all", bridge.ReverseConfig{Policy: "probability", Probability: 1, LeafRejectProbability: 1, Seed: 42}},
		{"absorb_at_checkpoints", bridge.ReverseConfig{Policy: "probability", Probability: 0, LeafRejectProbability: 1, Seed: 42}},
		{"half_rejected", bridge.ReverseConfig{Policy: "inverse_depth", LeafRejectProbability: 0.5, Seed: 7}},
	}
	windows := []struct {
		name   string
		policy *bridge.CheckpointRange
		fixed  int
	}{
		{"random2_4", &bridge.CheckpointRange{Min: 2, Max: 4, Seed: 42}, 0},
		{"random1_5", &bridge.CheckpointRange{Min: 1, Max: 5, Seed: 3}, 0},
		{"fixed3", nil, 3},
	}
	for _, mode := range []string{"pb0", "cgp0", "sb3"} {
		for _, w := range windows {
			for _, pol := range policies {
				for _, sc := range scenarios {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", mode, w.name, pol.name, sc.name), func(t *testing.T) {
						collected, truth, dees, cfg := reverseReconFixture(t, mode, w.policy, w.fixed, pol.rc)
						byID := make(map[uint64]reverseCollected, len(collected))
						for _, rec := range collected {
							byID[rec.spanID] = rec
						}
						// Rejected origins: leaves without their own payload. Every such
						// leaf must appear exactly once in some surviving owner's bundle.
						hasChild := make(map[uint64]bool)
						for _, rec := range collected {
							hasChild[rec.parentID] = true
						}
						rejected := make(map[uint64]bool)
						for _, rec := range collected {
							if !hasChild[rec.spanID] && rec.br == nil {
								rejected[rec.spanID] = true
							}
						}
						if pol.rc.LeafRejectProbability == 1 && len(rejected) == 0 {
							t.Fatal("fixture produced no rejected leaves")
						}
						// Trusses are checkpoint payloads: every carried bundle is retained
						// whether or not the carrying span's ordinary record survives.
						var evidence []ReverseEvidence
						demoted := 0
						for _, rec := range collected {
							if rec.demoted {
								demoted++
								if rec.ckpt == nil || rec.br != nil {
									t.Fatalf("demoted carrier %d must hold trusses and no own payload", rec.spanID)
								}
							}
							if rec.ckpt == nil {
								continue
							}
							ev, err := DecodeReverseEvidence(rec.spanID, rec.ckpt, cfg)
							if err != nil {
								t.Fatal(err)
							}
							evidence = append(evidence, ev...)
						}
						seen := make(map[uint64]int)
						for _, e := range evidence {
							seen[e.OriginSpanID]++
						}
						if len(seen) != len(rejected) {
							t.Fatalf("evidence covers %d origins, rejected %d", len(seen), len(rejected))
						}
						for id, n := range seen {
							if !rejected[id] || n != 1 {
								t.Fatalf("origin %d exported %d times, rejected=%t", id, n, rejected[id])
							}
						}
						if pol.name == "promote_all" && demoted == 0 {
							t.Fatal("promote_all produced no demoted carrier")
						}
						if pol.name == "absorb_at_checkpoints" && demoted != 0 {
							t.Fatalf("absorb_at_checkpoints demoted %d carriers", demoted)
						}

						dropped := make(map[uint64]struct{})
						var survivors []Span
						for _, rec := range collected {
							if sc.drop(rec, byID, rejected) {
								dropped[rec.spanID] = struct{}{}
								continue
							}
							survivors = append(survivors, decodeReverseCollected(t, mode, rec, cfg))
						}
						merged, err := MergeReverseEvidence(survivors, evidence)
						if err != nil {
							t.Fatal(err)
						}
						unknown := 0
						byIDSpan := make(map[uint64]*Span, len(merged))
						for i := range merged {
							s := &merged[i]
							byIDSpan[s.SpanID] = s
							if s.ParentUnknown {
								unknown++
								if !rejected[s.SpanID] || !s.LeafCarrier || s.BloomBits == nil {
									t.Fatalf("bad evidence-only origin %+v", *s)
								}
								if _, gone := dropped[s.SpanID]; !gone {
									t.Fatalf("surviving origin %d became evidence-only", s.SpanID)
								}
							}
						}
						wantUnknown := 0
						for id := range rejected {
							if _, gone := dropped[id]; gone {
								wantUnknown++
							}
						}
						if unknown != wantUnknown {
							t.Fatalf("evidence-only origins %d, want %d", unknown, wantUnknown)
						}
						// Reverse-promoted checkpoints are leaves: never window roots.
						idx := newCheckpointIndex(byIDSpan, cfg)
						for _, roots := range idx.byPrefix {
							for _, r := range roots {
								if r.ParentUnknown || r.LeafCarrier {
									t.Fatalf("span %d became a window root: %+v", r.SpanID, *r)
								}
							}
						}
						// At total loss the survivor set is exactly the intended checkpoint
						// set, so no Bloom candidate is ever probed and every checkpoint
						// joins its named root.
						if sc.name == "all_unprotected" {
							for _, s := range merged {
								if s.BloomBits == nil && s.ParentID != 0 {
									t.Fatalf("non-checkpoint survivor %d at total loss", s.SpanID)
								}
							}
						}

						var res Result
						switch mode {
						case "pb0":
							res = ReconstructPB0(merged, cfg)
						case "cgp0":
							res = ReconstructCGP0(merged, cfg)
						case "sb3":
							r := ReconstructSB3WithDEE(merged, dees, cfg)
							res = r.Topology
							if !r.Compatible || !r.StructureStatus.Complete {
								t.Fatalf("SB3 incompatible: %s; structure=%+v", r.Reason, r.StructureStatus)
							}
						}
						if res.GreedyHardConflicts != 0 || len(res.Unanchored) != 0 {
							t.Fatalf("hard conflicts=%d unanchored=%v", res.GreedyHardConflicts, res.Unanchored)
						}
						var iso CGP2Iso
						if mode == "pb0" {
							iso = ScorePBPathStrict(res, merged, truth, dropped)
						} else {
							iso = ScoreCGP2Strict(res, merged, truth, dropped)
						}
						if !iso.Clean() {
							t.Fatalf("not clean: %+v", iso)
						}
						// Every evidence-only origin must be reconnected to its true parent
						// chain: the emitted parent map must place it under its truth parent
						// or under an anonymous node standing in for that parent.
						tparent := make(map[uint64]uint64, len(truth))
						for _, s := range truth {
							tparent[s.SpanID] = s.ParentID
						}
						for _, s := range merged {
							if !s.ParentUnknown {
								continue
							}
							p, ok := res.ReconParent[s.SpanID]
							if !ok {
								t.Fatalf("origin %d not attached", s.SpanID)
							}
							if byIDSpan[tparent[s.SpanID]] != nil && p != tparent[s.SpanID] {
								t.Fatalf("origin %d attached to %d, true surviving parent %d", s.SpanID, p, tparent[s.SpanID])
							}
						}
					})
				}
			}
		}
	}
}
