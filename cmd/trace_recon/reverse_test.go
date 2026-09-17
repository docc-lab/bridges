package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"bridges/bridge"
	"bridges/corpus"
)

// reverseTraceFixture builds one trace with unequal fanouts and early leaves at
// many depths. All spans start before any ends, deep spans end first, so the
// event order is valid for reverse routing (children return before parents).
func reverseTraceFixture(tid uint64) (corpus.StoredTrace, int) {
	base := tid * 1000
	type node struct{ id, parent uint64 }
	var nodes []node
	var build func(uint64, int)
	build = func(parent uint64, depth int) {
		id := base + uint64(len(nodes)+1)
		nodes = append(nodes, node{id, parent})
		if depth >= 8 {
			return
		}
		build(id, depth+1)
		if depth%2 == 0 {
			build(id, depth+1)
		}
		if depth%3 == 1 {
			nodes = append(nodes, node{base + uint64(len(nodes)+1), id})
		}
	}
	build(0, 0)
	st := corpus.StoredTrace{TraceID: tid}
	ts := int64(0)
	for _, n := range nodes {
		st.Events = append(st.Events, corpus.StoredEvent{Kind: uint8(corpus.KindStart), SpanID: n.id, ParentID: n.parent, TS: ts})
		ts++
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		st.Events = append(st.Events, corpus.StoredEvent{Kind: uint8(corpus.KindEnd), SpanID: n.id, ParentID: n.parent, TS: ts})
		ts++
	}
	return st, len(nodes)
}

// TestReverseReconstructionHarness drives the real harness with reverse trusses:
// replay through ReverseHandler, collection loss that may drop returned origins'
// ordinary records, bundle decoding, evidence binding, reconstruction, and
// strict scoring. With a negligible Bloom FPR every trace must reconstruct
// cleanly and every drop rate must have reconstruction obligations.
func TestReverseReconstructionHarness(t *testing.T) {
	dir := t.TempDir()
	tids := []uint64{5, 11, 23}
	var counts []uint32
	var stored []corpus.StoredTrace
	for _, tid := range tids {
		st, n := reverseTraceFixture(tid)
		stored = append(stored, st)
		counts = append(counts, uint32(n))
	}
	meta := &corpus.Meta{Services: []string{"svc"}, TraceOrder: tids, SpanCounts: counts}
	if err := corpus.WriteMeta(filepath.Join(dir, "meta.bin"), meta); err != nil {
		t.Fatal(err)
	}
	storePath := filepath.Join(dir, "traces.store")
	w, err := corpus.NewTraceStoreWriter(storePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range stored {
		if err := w.WriteTrace(st.TraceID, st.Events); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	policies := []bridge.ReverseConfig{
		{Policy: "inverse_depth", LeafRejectProbability: 1, Seed: 42},
		{Policy: "probability", Probability: 1, LeafRejectProbability: 1, Seed: 42},
		{Policy: "ttl", TTLMin: 2, TTLMax: 4, LeafRejectProbability: 1, Seed: 42},
	}
	for _, mode := range []string{"pb0", "cgp0", "sb3"} {
		for _, randomized := range []bool{true, false} {
			for _, rc := range policies {
				t.Run(fmt.Sprintf("%s/random=%t/%s", mode, randomized, rc.Policy), func(t *testing.T) {
					c := config{mode: mode, corpusDir: dir, traceStore: storePath, traceCount: len(tids),
						workers: 1, checkpointDistance: 3, prefixLen: 8, bloomFP: 1e-12, fpBits: 64,
						dropRate: 1, seed: 42, perTraceDropSeed: true, bottomUp: true, chainCheck: true,
						dropRates: "0.05,0.5,1", timingPath: filepath.Join(dir, "timing_{dc}.csv")}
					if randomized {
						c.checkpointPolicy = &bridge.CheckpointRange{Min: 2, Max: 4, Seed: 42}
						c.checkpointDistance = 4
					}
					rc := rc
					c.reverse = &rc
					ha := newHarness(c)
					if _, ok := ha.h.(*bridge.ReverseHandler); !ok {
						t.Fatal("harness handler is not wrapped for reverse trusses")
					}
					runFromTraceStore(c, ha)
					ha.drain()
					for r, acc := range ha.mdAcc {
						if acc.nt != len(tids) {
							t.Fatalf("rate %s: %d traces scored, want %d", ha.mdDC[r], acc.nt, len(tids))
						}
						if acc.empty != 0 {
							t.Fatalf("rate %s: %d traces had no reconstruction obligation; returned origins must create one", ha.mdDC[r], acc.empty)
						}
						if acc.clean != acc.nt {
							t.Fatalf("rate %s: clean %d of %d; wrong=%d constraint=%d", ha.mdDC[r], acc.clean, acc.nt, acc.edgeWrong, acc.constraintWrong)
						}
						if acc.greedyHardConflicts != 0 {
							t.Fatalf("rate %s: %d hard conflicts", ha.mdDC[r], acc.greedyHardConflicts)
						}
					}
				})
			}
		}
	}
}
