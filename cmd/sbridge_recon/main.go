// Command sbridge_recon runs S-Bridge TOPOLOGY reconstruction over a trace
// store, under per-trace-seeded random span drops, and reports how often the
// surviving payloads rebuild the exact call-graph shape + fingerprints.
//
// This is the topology milestone: no event-structure (ordering) yet, so it runs
// each trace through a fresh SBridgeHandler (chains + ckpt4 are per-trace; the
// cross-trace DEE queue only affects payload SIZE, not topology). Random
// sampling (--sample) keeps it quick on the full day-1 store.
//
//	sbridge_recon --store day1.store --cpd 4 --drop-rate 0.5 --sample 10000
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/recon"
)

func main() {
	storePath := flag.String("store", "", "trace store path (corpus.TraceStore)")
	cpd := flag.Int("cpd", 4, "checkpoint distance")
	dropRate := flag.Float64("drop-rate", 0.5, "per-span drop probability (per-trace seeded)")
	first := flag.Int("first", 0, "if >0, take the FIRST N traces and stop (fast; no full-file read)")
	sample := flag.Int("sample", 0, "if >0, reservoir-sample this many random traces (reads the whole store)")
	sampleSeed := flag.Int64("sample-seed", 1, "seed for --sample selection")
	dropSeed := flag.Int64("drop-seed", 1, "base seed mixed with each trace id for drop decisions")
	progress := flag.Int("progress", 0, "print a progress line every N traces (0 = silent)")
	flag.Parse()
	if *storePath == "" {
		fmt.Fprintln(os.Stderr, "error: --store required")
		os.Exit(2)
	}

	traces, err := loadTraces(*storePath, *first, *sample, *sampleSeed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(1)
	}

	var nTraces, nUnsolvable, nOrphanAmbig, nWrong, nCorrect int
	var sumEmit, sumOrphans, sumOPlaced, sumOAmbig, sumONoPlace int
	t0 := time.Now()
	for i, tr := range traces {
		payloads, truth, spans := emitTrace(tr, *cpd)
		dropped := dropSet(tr, *dropRate, *dropSeed, *cpd)

		inputs := make([]recon.SBInput, 0, len(payloads))
		for sid, p := range payloads {
			if _, gone := dropped[sid]; gone {
				continue
			}
			inputs = append(inputs, recon.SBInput{SpanID: sid, Payload: p})
		}
		sumEmit += len(inputs)

		// Orphans: surviving non-emitting spans (real ids + emitted ordinal/depth).
		var orphans []recon.SBOrphan
		for _, s := range spans {
			if _, isEmit := payloads[s.id]; isEmit {
				continue
			}
			if _, gone := dropped[s.id]; gone {
				continue
			}
			orphans = append(orphans, recon.SBOrphan{SpanID: s.id, ParentID: s.parent, Depth: s.depth, Ordinal: s.ord})
		}
		sumOrphans += len(orphans)

		res := recon.ReconstructSBridge(inputs, orphans, recon.Config{CPD: *cpd})
		v := recon.ScoreSBridgeUnderDrop(res, truth)
		nTraces++
		sumOPlaced += res.OrphanPlaced
		sumOAmbig += res.OrphanAmbiguous
		sumONoPlace += res.OrphanNoPlace
		switch {
		case v.Unsolvable:
			nUnsolvable++ // ckpt4 collision
		case res.OrphanAmbiguous > 0:
			nOrphanAmbig++ // orphan 4-tuple collision
		case !v.Correct:
			nWrong++ // misattachment WITHOUT a flagged collision == design bug
		default:
			nCorrect++
		}
		if *progress > 0 && (i+1)%*progress == 0 {
			fmt.Fprintf(os.Stderr, "  %d/%d correct=%d ckpt4ambig=%d orphanambig=%d WRONG=%d (%s)\n",
				i+1, len(traces), nCorrect, nUnsolvable, nOrphanAmbig, nWrong, time.Since(t0).Round(time.Second))
		}
	}

	pct := func(a, b int) float64 {
		if b == 0 {
			return 0
		}
		return 100 * float64(a) / float64(b)
	}
	fmt.Printf("=== sbridge topology: cpd=%d drop=%.2f sample=%d ===\n", *cpd, *dropRate, *sample)
	fmt.Printf("traces=%d  correct=%d (%.4f%%)\n", nTraces, nCorrect, pct(nCorrect, nTraces))
	fmt.Printf("failures: ckpt4_ambiguous=%d (%.4f%%)  orphan_ambiguous=%d (%.4f%%)  WRONG[no-collision=BUG]=%d (%.4f%%)\n",
		nUnsolvable, pct(nUnsolvable, nTraces), nOrphanAmbig, pct(nOrphanAmbig, nTraces), nWrong, pct(nWrong, nTraces))
	fmt.Printf("orphans:  total=%d  placed=%d  ambiguous=%d  no_placeholder=%d  (avg %.1f orphans, %.1f emitters / trace)\n",
		sumOrphans, sumOPlaced, sumOAmbig, sumONoPlace, float64(sumOrphans)/float64(nTraces), float64(sumEmit)/float64(nTraces))
	fmt.Printf("[topo MUST be 100%% unless a key collides; WRONG>0 means the design failed, not the run]\n")
}

// spanInfo is per-span data needed to build orphan keys (real ids + ordinal/depth).
type spanInfo struct {
	id, parent  uint64
	ord, depth  int
}

// emitTrace replays one trace through a fresh SBridgeHandler in stored stream
// order, capturing each emitting span's serialized _br payload, the ground-truth
// tree (children indexed by 1-based start ordinal — the same ordinal the handler
// assigns), and per-span (parent, ordinal, depth) for orphan matching.
func emitTrace(tr corpus.StoredTrace, cpd int) (map[uint64][]byte, recon.SBTruth, []spanInfo) {
	h := bridge.NewSBridgeHandler(cpd, nil)
	// Charge the per-span attributes reconstruction relies on: _d (depth) and
	// _oc (ordinal chain, for non-checkpoint self-placement). Topology recon
	// works either way (the chain carries position), but this keeps the cost
	// accounting honest for what a real sbridge deployment would emit.
	h.EmitDepth = true
	h.EmitOC = true
	payloads := map[uint64][]byte{}
	h.EmitSink = func(_ /*tid*/, sid uint64, payload []byte) {
		payloads[sid] = append([]byte(nil), payload...)
	}
	truth := recon.SBTruth{ChildByOrd: map[uint64]map[int]uint64{}}
	nextSeq := map[uint64]int{}
	var spans []spanInfo
	parent := map[uint64]uint64{}
	for _, e := range tr.Events {
		ev := &bridge.Event{TraceID: tr.TraceID, SpanID: e.SpanID, ParentID: e.ParentID, ServiceID: e.ServiceID}
		if e.Kind == corpus.KindStart {
			seq := 0
			if e.ParentID != 0 {
				seq = nextSeq[e.ParentID] + 1
				nextSeq[e.ParentID] = seq
				m := truth.ChildByOrd[e.ParentID]
				if m == nil {
					m = map[int]uint64{}
					truth.ChildByOrd[e.ParentID] = m
				}
				m[seq] = e.SpanID
			} else {
				truth.RootID = e.SpanID
			}
			parent[e.SpanID] = e.ParentID
			spans = append(spans, spanInfo{id: e.SpanID, parent: e.ParentID, ord: seq})
			h.OnStart(ev, seq)
		} else {
			h.OnEnd(ev)
		}
	}
	h.EvictTrace(tr.TraceID)

	// Fill absolute depths (memoized walk up the parent map).
	depth := map[uint64]int{}
	var d func(uint64) int
	d = func(s uint64) int {
		if v, ok := depth[s]; ok {
			return v
		}
		p := parent[s]
		if p == 0 {
			depth[s] = 0
			return 0
		}
		v := d(p) + 1
		depth[s] = v
		return v
	}
	for i := range spans {
		spans[i].depth = d(spans[i].id)
	}
	return payloads, truth, spans
}

// dropSet picks the dropped span ids for one trace. CHECKPOINTS (depth%cpd==0)
// are never droppable — they're the protected carriers, exactly as in every
// other mode (trace_recon protects _br carriers identically). Only non-checkpoint
// spans are candidates; a per-trace splitmix seed gives an independent uniform
// draw per candidate (sorted id order), so the decision is order/partition-
// invariant.
func dropSet(tr corpus.StoredTrace, rate float64, base int64, cpd int) map[uint64]struct{} {
	parent := map[uint64]uint64{}
	var ids []uint64
	for _, e := range tr.Events {
		if e.Kind == corpus.KindStart {
			parent[e.SpanID] = e.ParentID
			ids = append(ids, e.SpanID)
		}
	}
	depth := map[uint64]int{}
	var d func(uint64) int
	d = func(s uint64) int {
		if v, ok := depth[s]; ok {
			return v
		}
		p := parent[s]
		if p == 0 {
			depth[s] = 0
			return 0
		}
		v := d(p) + 1
		depth[s] = v
		return v
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	cands := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if d(id)%cpd != 0 { // skip checkpoints
			cands = append(cands, id)
		}
	}
	st := splitmix(tr.TraceID + uint64(base))
	dropped := map[uint64]struct{}{}
	for _, id := range cands {
		var u float64
		u, st = nextFloat(st)
		if u < rate {
			dropped[id] = struct{}{}
		}
	}
	return dropped
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// nextFloat advances a splitmix state and returns a uniform [0,1) plus the new state.
func nextFloat(st uint64) (float64, uint64) {
	st = splitmix(st)
	return float64(st>>11) / float64(1<<53), st
}

// loadTraces reads the store. first>0 takes the first N traces and stops;
// otherwise sample>0 reservoir-samples N random traces (reading the whole store);
// otherwise all traces.
func loadTraces(path string, first, sample int, seed int64) ([]corpus.StoredTrace, error) {
	r, err := corpus.OpenTraceStore(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var all []corpus.StoredTrace
	st := splitmix(uint64(seed))
	i := 0
	for {
		if first > 0 && len(all) >= first {
			break
		}
		tr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if first > 0 {
			all = append(all, tr)
			continue
		}
		if sample <= 0 {
			all = append(all, tr)
			continue
		}
		if len(all) < sample {
			all = append(all, tr)
		} else {
			var u float64
			u, st = nextFloat(st)
			if j := int(u * float64(i+1)); j < sample {
				all[j] = tr
			}
		}
		i++
	}
	return all, nil
}
