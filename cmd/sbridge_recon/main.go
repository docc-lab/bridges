// Command sbridge_recon runs S-Bridge TOPOLOGY reconstruction over a trace
// store, under per-trace-seeded random span drops, and reports how often the
// surviving payloads rebuild the exact call-graph shape + fingerprints.
//
// It reconstructs both phases: topology (call-graph shape + fingerprints) and,
// where topology rebuilt exactly, the event structure (ordering) — gathering each
// parent's start order (ordinals) and end order (EE blocks + the inline DEE
// side-store), running the bottom-up timestamp sweep, and scoring the result
// against the corpus total-order two ways: exact event-order and critical-path.
// Each trace runs through a fresh SBridgeHandler. Random sampling (--sample)
// keeps it quick on the full day-1 store.
//
//	sbridge_recon --store day1.store --cpd 4 --drop-rate 0.5 --sample 10000
package main

import (
	"bufio"
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
	topoOnly := flag.Bool("topo-only", false, "emit topology only (no EE/DEE); reconstruct + score the call-graph shape, skip the event-structure phase")
	fpBits := flag.Int("fp-bits", 16, "non-checkpoint fingerprint width in bits (emit + recon must match)")
	noOrdinal := flag.Bool("no-ordinal", false, "drop the interior ordinal: emit _o=depth only, place severed survivors by (depth, own-fp, parent-fp)")
	progress := flag.Int("progress", 0, "print a progress line every N traces (0 = silent)")
	timing := flag.String("timing", "", "write per-trace topology recon_ns to this CSV and print a TIMING summary (topology + structure phases, µs)")
	flag.Parse()
	if *storePath == "" {
		fmt.Fprintln(os.Stderr, "error: --store required")
		os.Exit(2)
	}

	var nTraces, nUnsolvable, nSevAmbig, nWrong, nCorrect int
	var sumEmit, sumSurv, sumIdent, sumSevPlaced, sumSevNoPlace, sumReal int
	var nStruct, nAccepted, nEventOK, nCPOK, nDeeAmbig int // Phase-2 (event structure) tally
	var topoNanos, structNanos []int64                    // per-trace recon times (with --timing)
	var sumParents, sumEndOK int                          // per-parent end-order recovery (accepted traces only)
	t0 := time.Now()
	process := func(i int, tr corpus.StoredTrace) {
		payloads, truth, spans, dees := emitTrace(tr, *cpd, *topoOnly, *fpBits, *noOrdinal)
		dropped := dropSet(tr, *dropRate, *dropSeed, *cpd)

		inputs := make([]recon.SBInput, 0, len(payloads))
		for sid, p := range payloads {
			if _, gone := dropped[sid]; gone {
				continue
			}
			inputs = append(inputs, recon.SBInput{SpanID: sid, Payload: p})
		}
		sumEmit += len(inputs)

		// Every surviving span: connected ones attach by their real parent edge,
		// severed ones (parent dropped) by their (depth, ordinal, own-fp, parent-fp).
		var survivors []recon.SBSurvivor
		for _, s := range spans {
			if _, gone := dropped[s.id]; gone {
				continue
			}
			survivors = append(survivors, recon.SBSurvivor{SpanID: s.id, ParentID: s.parent, Ordinal: s.ord, Depth: s.depth})
		}
		sumSurv += len(survivors)

		tRec := time.Now()
		res := recon.ReconstructSBridge(inputs, survivors, recon.Config{CPD: *cpd, FPBits: *fpBits, NoOrdinal: *noOrdinal})
		if *timing != "" {
			topoNanos = append(topoNanos, time.Since(tRec).Nanoseconds())
		}
		v := recon.ScoreSBridgeUnderDrop(res, truth)
		nTraces++
		sumIdent += res.Identified
		sumSevPlaced += res.SeveredPlaced
		sumSevNoPlace += res.SeveredNoPlace
		sumReal += res.CountReal()
		switch {
		case v.Unsolvable:
			nUnsolvable++ // ckpt4 collision
		case res.SeveredAmbiguous > 0:
			nSevAmbig++ // severed 4-tuple collision
		case !v.Correct:
			nWrong++ // misattachment with no flagged collision == design bug
		default:
			nCorrect++
		}

		// Phase 2 — event structure — only meaningful when the topology rebuilt
		// exactly. Build true timestamps over EVERY span (survivors and dropped),
		// reconstruct the ordering, and score it against the corpus total-order.
		// Skipped under --topo-only (no EE/DEE were emitted).
		if !*topoOnly && v.Correct && !v.Unsolvable && res.SeveredAmbiguous == 0 {
			endPos := make(map[uint64]int64, len(spans))
			for _, s := range spans {
				endPos[s.id] = s.endSeq
			}
			tStruct := time.Now()
			sr := recon.ScoreStructure(res, truth, endPos, dees)
			if *timing != "" {
				structNanos = append(structNanos, time.Since(tStruct).Nanoseconds())
			}
			nStruct++
			if sr.DEEAmbiguous {
				nDeeAmbig++ // ambiguous DEE -> trace REJECTED, not reconstructed
			} else {
				nAccepted++
				sumParents += sr.NParents
				sumEndOK += sr.EndOrderOK
				if sr.EventOrderOK {
					nEventOK++
				}
				if sr.CriticalPath {
					nCPOK++
				}
			}
		}

		if *progress > 0 && (i+1)%*progress == 0 {
			fmt.Fprintf(os.Stderr, "  %d done | topo correct=%d ckpt4ambig=%d sevambig=%d WRONG=%d | struct accepted=%d rejected=%d eventOK=%d cpOK=%d (%s)\n",
				i+1, nCorrect, nUnsolvable, nSevAmbig, nWrong, nAccepted, nDeeAmbig, nEventOK, nCPOK, time.Since(t0).Round(time.Second))
		}
	}

	// A full run STREAMS the store one trace at a time (bounded memory — the
	// unfiltered day-1 store is far larger than RAM). --first / --sample buffer
	// only their small subset.
	if *first <= 0 && *sample <= 0 {
		r, err := corpus.OpenTraceStore(*storePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open store: %v\n", err)
			os.Exit(1)
		}
		defer r.Close()
		for i := 0; ; i++ {
			tr, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "read: %v\n", err)
				os.Exit(1)
			}
			process(i, tr)
		}
	} else {
		traces, err := loadTraces(*storePath, *first, *sample, *sampleSeed)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load: %v\n", err)
			os.Exit(1)
		}
		for i, tr := range traces {
			process(i, tr)
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
	fmt.Printf("failures: ckpt4_ambiguous=%d (%.4f%%)  severed_ambiguous=%d (%.4f%%)  WRONG[no-collision=BUG]=%d (%.4f%%)\n",
		nUnsolvable, pct(nUnsolvable, nTraces), nSevAmbig, pct(nSevAmbig, nTraces), nWrong, pct(nWrong, nTraces))
	fmt.Printf("coverage: reconstructed_real_spans=%d of survivors=%d (%.4f%%)   [edge=%d, severed-placed=%d, severed-noplace=%d]\n",
		sumReal, sumSurv, pct(sumReal, sumSurv), sumIdent, sumSevPlaced, sumSevNoPlace)
	fmt.Printf("[topo MUST be 100%% unless a key collides; WRONG>0 means the design failed, not the run]\n")
	if *topoOnly {
		fmt.Printf("--- structure: SKIPPED (--topo-only; no EE/DEE emitted) ---\n")
		fmt.Printf("total ambiguity (ckpt4 + severed)=%d (%.4f%% of all traces)\n",
			nUnsolvable+nSevAmbig, pct(nUnsolvable+nSevAmbig, nTraces))
	} else {
		fmt.Printf("--- structure (event ordering; %d topo-correct, %d REJECTED for DEE ambiguity, %d accepted) ---\n",
			nStruct, nDeeAmbig, nAccepted)
		fmt.Printf("rejected (DEE ambiguous)=%d (%.4f%% of topo-correct)\n", nDeeAmbig, pct(nDeeAmbig, nStruct))
		fmt.Printf("of ACCEPTED traces (order vs order): full_event_order_correct=%d (%.4f%%)   critical_path_correct=%d (%.4f%%)\n",
			nEventOK, pct(nEventOK, nAccepted), nCPOK, pct(nCPOK, nAccepted))
		fmt.Printf("per-parent END-ORDER recovered=%d of %d multi-child parents (%.4f%%)\n",
			sumEndOK, sumParents, pct(sumEndOK, sumParents))
		totalAmbig := nUnsolvable + nSevAmbig + nDeeAmbig
		fmt.Printf("total ambiguity (ckpt4 + severed + DEE, all counted wrong)=%d (%.4f%% of all traces)\n",
			totalAmbig, pct(totalAmbig, nTraces))
	}

	if *timing != "" {
		timingSummary("topology", topoNanos)
		timingSummary("structure", structNanos)
		if err := writeTimingCSV(*timing, topoNanos); err != nil {
			fmt.Fprintf(os.Stderr, "timing csv: %v\n", err)
		}
	}
}

// timingSummary prints a percentile summary of per-trace recon times (µs),
// matching trace_recon's TIMING line for cross-scheme comparison.
func timingSummary(label string, ns []int64) {
	if len(ns) == 0 {
		return
	}
	s := append([]int64(nil), ns...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var total int64
	for _, v := range s {
		total += v
	}
	us := func(v int64) float64 { return float64(v) / 1000 }
	pctl := func(p float64) int64 { return s[int(p*float64(len(s)-1))] }
	fmt.Printf("TIMING[%s] traces=%d total=%s mean=%.2fus p50=%.2f p90=%.2f p99=%.2f max=%.2f (us)\n",
		label, len(s), time.Duration(total).Round(time.Millisecond),
		us(total/int64(len(s))), us(pctl(0.50)), us(pctl(0.90)), us(pctl(0.99)), us(s[len(s)-1]))
}

// writeTimingCSV writes one recon_ns per line (for offline distribution compare).
func writeTimingCSV(path string, ns []int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "recon_ns")
	for _, v := range ns {
		fmt.Fprintln(w, v)
	}
	return w.Flush()
}

// spanInfo is per-span data needed to build orphan keys (real ids + ordinal/depth)
// and to score the event structure (the span's real start/end timestamps).
type spanInfo struct {
	id, parent uint64
	ord, depth int
	endSeq     int64 // index of this span's END event in the corpus total-order (tie-broken)
}

// emitTrace replays one trace through a fresh SBridgeHandler in stored stream
// order, capturing each emitting span's serialized _br payload, the ground-truth
// tree (children indexed by 1-based start ordinal — the same ordinal the handler
// assigns), and per-span (parent, ordinal, depth) for orphan matching.
func emitTrace(tr corpus.StoredTrace, cpd int, topoOnly bool, fpBits int, noOrdinal bool) (map[uint64][]byte, recon.SBTruth, []spanInfo, [][]byte) {
	h := bridge.NewSBridgeHandler(cpd, nil)
	// Interior spans export _o = varint(ordinal)||varint(depth); charge it so the
	// cost accounting matches a real sbridge deployment. (Reconstruction itself
	// uses the survivors' real ordinal/depth, not the decoded bytes.)
	h.EmitOC = true
	h.TopoOnly = topoOnly // drop EE/DEE; topology emission is unchanged
	h.OmitOrdinal = noOrdinal
	if fpBits > 0 {
		h.FPBits = fpBits
	}
	payloads := map[uint64][]byte{}
	h.EmitSink = func(_ /*tid*/, sid uint64, payload []byte) {
		payloads[sid] = append([]byte(nil), payload...)
	}
	// DEEs are accumulated inline into a per-trace side store (functionally
	// equivalent to hunting them down on a future cross-trace request).
	var dees [][]byte
	h.DEESink = func(_ uint64, q []byte) { dees = append(dees, append([]byte(nil), q...)) }
	truth := recon.SBTruth{ChildByOrd: map[uint64]map[int]uint64{}}
	nextSeq := map[uint64]int{}
	var spans []spanInfo
	parent := map[uint64]uint64{}
	// END-event position in the trace's total-ordered (tie-broken) event stream.
	// This — not the raw end timestamp — is the ground-truth end order: the events
	// are already canonically ordered (the same order the handler builds EE/DEE
	// from), so ties have one well-defined position, no invented tiebreak.
	endPos := map[uint64]int64{}
	for i, e := range tr.Events {
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
			endPos[e.SpanID] = int64(i)
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
		spans[i].endSeq = endPos[spans[i].id]
	}
	return payloads, truth, spans, dees
}

// dropSet picks the dropped span ids for one trace. CHECKPOINTS are never
// droppable, and in this design the checkpoint set is { root, every leaf, every
// depth%cpd==0 node } — they all carry the _br breadcrumb. Only INTERIOR
// non-leaf, non-depth-checkpoint spans (which carry just _d + _o) are drop
// candidates. A per-trace splitmix seed gives an independent uniform draw per
// candidate (sorted id order), so the decision is order/partition-invariant.
func dropSet(tr corpus.StoredTrace, rate float64, base int64, cpd int) map[uint64]struct{} {
	parent := map[uint64]uint64{}
	hasKids := map[uint64]bool{}
	var ids []uint64
	for _, e := range tr.Events {
		if e.Kind == corpus.KindStart {
			parent[e.SpanID] = e.ParentID
			if e.ParentID != 0 {
				hasKids[e.ParentID] = true
			}
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
		// droppable iff interior: not root, not a leaf, not a depth-checkpoint.
		if parent[id] != 0 && hasKids[id] && d(id)%cpd != 0 {
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
