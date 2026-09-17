package main

import (
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"bridges/corpus"
)

func serialTraceFixture(tid uint64) corpus.StoredTrace {
	// Three overlapping children under A produce fanout, ordinal, EE, and
	// delayed-END evidence. The third child starts after the first has ended.
	base := tid * 100
	parents := map[uint64]uint64{1: 0, 2: 1, 3: 2, 4: 3, 5: 2, 6: 5, 7: 4, 8: 2}
	steps := []int{1, 2, 3, 4, 5, 6, 7, -7, -4, -3, 8, -8, -6, -5, -2, -1}
	st := corpus.StoredTrace{TraceID: tid}
	for i, step := range steps {
		kind := uint8(corpus.KindStart)
		if step < 0 {
			step, kind = -step, corpus.KindEnd
		}
		id := uint64(step)
		parent := parents[id]
		if parent != 0 {
			parent += base
		}
		st.Events = append(st.Events, corpus.StoredEvent{
			Kind: kind, SpanID: base + id, ParentID: parent, ServiceID: 0, TS: int64(i),
		})
	}
	return st
}

func TestSerialTraceStoreFinishesEachTraceAndPreservesScores(t *testing.T) {
	dir := t.TempDir()
	meta := &corpus.Meta{Services: []string{"svc"}, TraceOrder: []uint64{17, 31, 9}, SpanCounts: []uint32{8, 8, 8}}
	if err := corpus.WriteMeta(filepath.Join(dir, "meta.bin"), meta); err != nil {
		t.Fatal(err)
	}
	storePath := filepath.Join(dir, "traces.store")
	w, err := corpus.NewTraceStoreWriter(storePath)
	if err != nil {
		t.Fatal(err)
	}
	// Metadata selection differs from storage/completion order.
	stored := []corpus.StoredTrace{serialTraceFixture(9), serialTraceFixture(31), serialTraceFixture(17)}
	for _, st := range stored {
		if err := w.WriteTrace(st.TraceID, st.Events); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"pb0", "cgp0", "sb3"} {
		for _, multi := range []bool{false, true} {
			for _, perTrace := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/multi=%t/perTrace=%t", mode, multi, perTrace), func(t *testing.T) {
					c := config{mode: mode, corpusDir: dir, traceStore: storePath, traceCount: 2,
						workers: 1, checkpointDistance: 3, prefixLen: 8, bloomFP: 1e-4,
						fpBits: 64, dropRate: 0.75, seed: 42, perTraceDropSeed: perTrace,
						bottomUp: true, chainCheck: true, timingPath: filepath.Join(dir, "timing.csv")}
					if multi {
						c.dropRates = "0.05,0.25,0.5,0.75,0.95,1"
					}
					parallel := newHarness(c)
					runFromTraceStore(c, parallel)
					parallel.drain()
					c.serialTraces = true
					serial := newHarness(c)
					selected := map[uint64]bool{17: true, 31: true}
					read, expectedFinished := 0, 0
					next := func() (corpus.StoredTrace, error) {
						// This assertion runs at the next READ, so replay and
						// scoring of another trace cannot hide in a pipeline.
						if multi {
							for rate, a := range serial.mdAcc {
								if a.nt != expectedFinished {
									t.Fatalf("read another trace before rate %d finished: got %d, want %d", rate, a.nt, expectedFinished)
								}
							}
						} else if serial.cg2.nt != expectedFinished {
							t.Fatalf("read another trace before reconstruction/scoring finished")
						}
						if read == len(stored) {
							return corpus.StoredTrace{}, io.EOF
						}
						st := stored[read]
						read++
						if selected[st.TraceID] {
							expectedFinished++
						}
						return st, nil
					}
					if err := runSerialTraceStore(next, selected, serial); err != nil {
						t.Fatal(err)
					}
					serial.drain()
					if !reflect.DeepEqual(serial.cg2, parallel.cg2) || !reflect.DeepEqual(serial.mdAcc, parallel.mdAcc) {
						t.Fatal("serial replay/reconstruction changed accuracy or evidence counters")
					}
					compareTiming := func(got, want []traceTiming) {
						t.Helper()
						if len(got) != 2 || len(got) != len(want) {
							t.Fatalf("timing coverage: got %d, want 2", len(got))
						}
						for i := range got {
							a, b := got[i], want[i]
							if a.ns <= 0 || b.ns <= 0 {
								t.Fatal("missing reconstruction timing")
							}
							a.ns, b.ns = 0, 0
							if a != b {
								t.Fatalf("changed trace selection, drops, or reconstruction obligation: %v != %v", a, b)
							}
						}
					}
					if multi {
						for r := range serial.mdRates {
							compareTiming(serial.mdTiming[r], parallel.mdTiming[r])
						}
					} else {
						compareTiming(serial.timingRecs, parallel.timingRecs)
					}
				})
			}
		}
	}
}
