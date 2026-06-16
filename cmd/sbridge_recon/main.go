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
	sample := flag.Int("sample", 0, "if >0, reservoir-sample this many random traces")
	sampleSeed := flag.Int64("sample-seed", 1, "seed for --sample selection")
	dropSeed := flag.Int64("drop-seed", 1, "base seed mixed with each trace id for drop decisions")
	progress := flag.Int("progress", 0, "print a progress line every N traces (0 = silent)")
	flag.Parse()
	if *storePath == "" {
		fmt.Fprintln(os.Stderr, "error: --store required")
		os.Exit(2)
	}

	traces, err := loadTraces(*storePath, *sample, *sampleSeed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(1)
	}

	var nTraces, nUnsolvable, nCorrect int
	var sumReal, sumEmit int
	t0 := time.Now()
	for i, tr := range traces {
		payloads, truth := emitTrace(tr, *cpd)
		dropped := dropSet(tr, *dropRate, *dropSeed, *cpd)

		inputs := make([]recon.SBInput, 0, len(payloads))
		for sid, p := range payloads {
			if _, gone := dropped[sid]; gone {
				continue
			}
			inputs = append(inputs, recon.SBInput{SpanID: sid, Payload: p})
		}
		sumEmit += len(inputs)

		res := recon.ReconstructSBridge(inputs, recon.Config{CPD: *cpd})
		v := recon.ScoreSBridgeUnderDrop(res, truth)
		nTraces++
		switch {
		case v.Unsolvable:
			nUnsolvable++
		case v.Correct:
			nCorrect++
			sumReal += res.CountReal()
		}
		if *progress > 0 && (i+1)%*progress == 0 {
			fmt.Fprintf(os.Stderr, "  %d/%d traces, correct=%d unsolvable=%d (%s)\n",
				i+1, len(traces), nCorrect, nUnsolvable, time.Since(t0).Round(time.Second))
		}
	}

	solvable := nTraces - nUnsolvable
	pct := func(a, b int) float64 {
		if b == 0 {
			return 0
		}
		return 100 * float64(a) / float64(b)
	}
	fmt.Printf("=== sbridge topology: cpd=%d drop=%.2f sample=%d ===\n", *cpd, *dropRate, *sample)
	fmt.Printf("traces=%d  solvable=%d (%.2f%%)  unsolvable=%d (%.2f%%)\n",
		nTraces, solvable, pct(solvable, nTraces), nUnsolvable, pct(nUnsolvable, nTraces))
	fmt.Printf("topo_correct=%.4f%% (of solvable)   = %.4f%% (of all)\n",
		pct(nCorrect, solvable), pct(nCorrect, nTraces))
	if nCorrect > 0 {
		fmt.Printf("avg reconstructed real spans/correct trace=%.1f (of %.1f surviving emitters/trace)\n",
			float64(sumReal)/float64(nCorrect), float64(sumEmit)/float64(nTraces))
	}
}

// emitTrace replays one trace through a fresh SBridgeHandler in stored stream
// order, capturing each emitting span's serialized _br payload, and builds the
// ground-truth tree (children indexed by 1-based start ordinal — the same
// ordinal the handler assigns).
func emitTrace(tr corpus.StoredTrace, cpd int) (map[uint64][]byte, recon.SBTruth) {
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
			h.OnStart(ev, seq)
		} else {
			h.OnEnd(ev)
		}
	}
	h.EvictTrace(tr.TraceID)
	return payloads, truth
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

// loadTraces reads the store; if sample>0 it reservoir-samples that many traces.
func loadTraces(path string, sample int, seed int64) ([]corpus.StoredTrace, error) {
	r, err := corpus.OpenTraceStore(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var all []corpus.StoredTrace
	st := splitmix(uint64(seed))
	i := 0
	for {
		tr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
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
