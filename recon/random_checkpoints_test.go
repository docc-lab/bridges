package recon

import (
	"fmt"
	"testing"

	"bridges/bridge"
)

func TestRandomCheckpointReconstruction(t *testing.T) {
	for _, mode := range []string{"pb0", "cgp0", "sb3"} {
		for _, maxCPD := range []int{4, 8} {
			for _, seed := range []int64{1, 2, 42, 91} {
				for _, drop := range []int{0, 1, 2} {
					t.Run(fmt.Sprintf("%s/cpd%d/seed%d/drop%d", mode, maxCPD, seed, drop), func(t *testing.T) {
						policy := &bridge.CheckpointRange{Min: maxCPD / 4, Max: maxCPD, Seed: seed}
						cfg := NewPCRBConfig(policy.MaxDistance(), 8, 1e-12)
						cfg.RandomizedCheckpoints = true
						cfg.CheckpointMin = policy.Min
						cfg.FPBits = 64
						var h bridge.Handler
						var dees []bridge.DEEQuad
						switch mode {
						case "pb0":
							p := bridge.NewPCRBBridgeHandler(5, 8, 1e-12)
							p.Capture = true
							h = p
						case "cgp0":
							p := bridge.NewCGPRBBridgeHandler(5, 8, 1e-12)
							p.Capture = true
							h = p
						case "sb3":
							p := bridge.NewSB3Handler(5, 8, 1e-12, nil)
							p.Capture = true
							p.FPBits = 64
							h = p
							p.DEESink = func(_ uint64, raw []byte) {
								q, err := bridge.DecodeDEEQuads(raw, 64)
								if err != nil {
									t.Fatal(err)
								}
								dees = append(dees, q...)
							}
						}
						if err := bridge.ConfigureCheckpoints(h, policy); err != nil {
							t.Fatal(err)
						}
						// All children start before any ends; reversing the starts gives
						// valid nesting and concurrent siblings with nontrivial DEEs.
						var truth []TruthSpan
						var build func(uint64, int)
						build = func(parent uint64, depth int) {
							id := uint64(len(truth) + 1)
							truth = append(truth, TruthSpan{SpanID: id, ParentID: parent, Depth: depth})
							if depth < 2*maxCPD+1 {
								build(id, depth+1)
								if depth%3 == 1 {
									build(id, depth+1)
								}
							}
						}
						build(0, 0)
						payloads := make(map[uint64][]byte)
						seq := make(map[uint64]int)
						for _, s := range truth {
							seq[s.ParentID]++
							r := h.OnStart(&bridge.Event{TraceID: 77, SpanID: s.SpanID, ParentID: s.ParentID}, seq[s.ParentID])
							if r.Payload != nil {
								payloads[s.SpanID] = r.Payload
							}
						}
						for i := len(truth) - 1; i >= 0; i-- {
							s := truth[i]
							r := h.OnEnd(&bridge.Event{TraceID: 77, SpanID: s.SpanID, ParentID: s.ParentID})
							if r.Payload != nil {
								payloads[s.SpanID] = r.Payload
							}
						}
						dropped := make(map[uint64]struct{})
						var survivors []Span
						for _, s := range truth {
							p := payloads[s.SpanID]
							if p == nil && (drop == 1 || drop == 2 && s.SpanID%3 != 0) {
								dropped[s.SpanID] = struct{}{}
								continue
							}
							v := Span{SpanID: s.SpanID, ParentID: s.ParentID, Depth: s.Depth}
							var err error
							if p != nil {
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
								v.LeafCarrier = bridge.IsLeafPayload(p)
								v.WindowCPD, v.BloomM, v.BloomK, err = DecodeBloomGeometry(p, cfg)
								if err != nil {
									t.Fatal(err)
								}
							}
							survivors = append(survivors, v)
						}
						var result Result
						switch mode {
						case "pb0":
							result = ReconstructPB0(survivors, cfg)
						case "cgp0":
							result = ReconstructCGP0(survivors, cfg)
						case "sb3":
							r := ReconstructSB3WithDEE(survivors, dees, cfg)
							result = r.Topology
							if !r.Compatible || !r.StructureStatus.Complete {
								t.Fatalf("SB3 incompatible: %s; structure=%+v", r.Reason, r.StructureStatus)
							}
						}
						assertCarrierAMQs(t, survivors, cfg, result)
						if result.GreedyHardConflicts != 0 {
							t.Fatalf("hard conflicts: %d", result.GreedyHardConflicts)
						}
						var score CGP2Iso
						if mode == "pb0" {
							score = ScorePBPathStrict(result, survivors, truth, dropped)
						} else {
							score = ScoreCGP2Strict(result, survivors, truth, dropped)
						}
						if !score.Clean() {
							t.Fatalf("incorrect reconstruction: %+v", score)
						}
					})
				}
			}
		}
	}
}
