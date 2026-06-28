// Command stream_eval generates synthetic traces ONLINE (in memory), runs both
// S-Bridge (topology + full event-structure) and cgprb topology reconstruction
// on each, accumulates aggregate stats, and discards the trace — so an arbitrary
// number of traces can be analyzed with O(one-trace)/worker memory and ZERO disk.
//
// Trace i is a pure function of (--base-seed + i) and the generation config: each
// worker owns a disjoint index range and seeds a fresh rng per trace, so results
// are fully reproducible and partition-invariant across --workers.
//
//	stream_eval --shape slab --spindle --spindle-period 40 --fanout-dist uber \
//	            --depth 120 --max-spans 8000 --cpd 8 --drop-rate 0.5 --n 100000
//
// Drop semantics mirror the corpus tools EXACTLY: S-Bridge uses sbridge_recon's
// splitmix per-trace drop over interior (non-root, non-leaf, non-depth-checkpoint)
// spans; cgprb uses trace_recon's mixSeed per-trace drop over non-_br spans. The
// two schemes therefore drop different sets for the same trace (as they do in the
// corpus path) — comparable in RATE, not in the exact realization.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"bridges/bloom"
	"bridges/bridge"
	"bridges/corpus"
	"bridges/gen"
	"bridges/recon"
)

func main() {
	var c gen.Config
	var n int
	var baseSeed int64
	var workers, cpd, fpBits int
	var dropRate float64
	var sbDropSeed, cgDropSeed int64
	flag.StringVar(&c.Shape, "shape", "slab", "tree shape (see trace_gen): chain|star|kary|skewed|branch|spine|deepwide|slab")
	flag.IntVar(&c.Depth, "depth", 60, "max tree depth (absolute for slab/spine; mean/cap for --depth-dist)")
	flag.IntVar(&c.Fanout, "fanout", 3, "children per node (star/kary/deepwide-K); mean for poisson/geometric")
	flag.StringVar(&c.FanoutDist, "fanout-dist", "uber", "offspring law: fixed|zipf|poisson|geometric|uber")
	flag.Float64Var(&c.FanoutS, "fanout-s", 1.2, "zipf exponent for --fanout-dist zipf (>1)")
	flag.IntVar(&c.FanoutMin, "fanout-min", 1, "--shape slab: minimum per-node fan-out (0 allows early death)")
	flag.IntVar(&c.FanoutMax, "fanout-max", 256, "cap on sampled fan-out per node (slab: also range max)")
	flag.BoolVar(&c.Spindle, "spindle", false, "--shape slab: spindle/re-spindle the continuation cap")
	flag.IntVar(&c.SpindlePeriod, "spindle-period", 0, "--shape slab --spindle: depth per spindle lobe (0 = one)")
	flag.StringVar(&c.DepthDist, "depth-dist", "fixed", "per-trace max-depth law: fixed|zipf")
	flag.Float64Var(&c.DepthS, "depth-s", 1.5, "zipf exponent for --depth-dist zipf (>1)")
	flag.IntVar(&c.MaxSpans, "max-spans", 20000, "hard cap on spans per trace (slab: the size dial)")
	flag.Float64Var(&c.Concurrency, "concurrency", 0.0, "0 = siblings sequential, 1 = fully overlapping")
	flag.IntVar(&c.Services, "services", 16, "number of distinct service ids")
	flag.Int64Var(&c.BaseDurUS, "base-dur-us", 1_000_000, "root span duration (µs)")
	flag.IntVar(&cpd, "cpd", 8, "checkpoint distance (both schemes)")
	flag.Float64Var(&dropRate, "drop-rate", 0.5, "per-span drop probability (per-trace seeded)")
	flag.IntVar(&fpBits, "fp-bits", 16, "S-Bridge non-checkpoint fingerprint width in bits")
	var prefixLen int
	flag.IntVar(&prefixLen, "prefix-len", bridge.DefaultPCRPrefixLen, "checkpoint-root width in bytes (1-8): CGPRB prefix and S-Bridge window anchor. 8 = full-width.")
	flag.IntVar(&n, "n", 1000, "number of traces to generate+evaluate")
	flag.Int64Var(&baseSeed, "base-seed", 1, "base seed; trace i uses rng seeded base-seed+i")
	flag.Int64Var(&sbDropSeed, "sb-drop-seed", 1, "S-Bridge per-trace drop base seed (matches sbridge_recon --drop-seed)")
	flag.Int64Var(&cgDropSeed, "cg-drop-seed", 1, "cgprb per-trace drop base seed (matches trace_recon --seed under --per-trace-drop-seed)")
	flag.IntVar(&workers, "workers", runtime.NumCPU(), "parallel workers")
	var primeM, primeMByteCap bool
	flag.BoolVar(&primeM, "prime-m", false, "round bloom bit count to a prime (fixes small-m double-hash clustering)")
	flag.BoolVar(&primeMByteCap, "prime-m-bytecap", false, "with --prime-m: keep the prime within the raw size's byte budget (down to prev prime if up would add a byte)")
	flag.Parse()
	bloom.PrimeM = primeM // set before any bloom sizing (emit + recon must match)
	bloom.PrimeMByteCap = primeMByteCap
	if workers < 1 {
		workers = 1
	}
	// Slab budget sizing (one config constant, shared by all traces/workers).
	c.SlabMeanW = gen.EstimateMeanFanout(&c)
	// cgprb recon config: same constructor/defaults as trace_recon --mode cgprb.
	// Bloom FP target overridable via env (must match the emit side) for sweeping.
	cgCfg := recon.NewPCRBConfig(cpd, prefixLen, bloomFPTarget())
	sbCfg := recon.Config{CPD: cpd, FPBits: fpBits, PrefixLen: prefixLen}

	t0 := time.Now()
	results := make([]stats, workers)
	var done int64 // completed traces (atomic), for the progress meter
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * n / workers
		hi := (w + 1) * n / workers
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			st := &results[w]
			for i := lo; i < hi; i++ {
				// Trace i: pure function of base-seed+i and the gen config. Build the
				// samplers on this trace's own rng so each trace is independent and
				// reproducible regardless of worker count / partitioning.
				rng := rand.New(rand.NewSource(baseSeed + int64(i)))
				fanoutOf := gen.MakeFanoutSampler(&c, rng)
				depthOf := gen.MakeDepthSampler(&c, rng)
				tid := rng.Uint64()
				spans := gen.Trace(&c, rng, fanoutOf, depthOf)
				if os.Getenv("TRACE_RECON_FANOUTDIST") != "" {
					// Emit the fan-out WIDTH histogram (children per node with >=2
					// children) for this trace, then skip recon. Aggregated offline.
					cc := make(map[uint64]int)
					for _, s := range spans {
						if s.Parent != 0 {
							cc[s.Parent]++
						}
					}
					hist := make(map[int]int)
					for _, w := range cc {
						if w >= 1 { // all non-leaf nodes (include single-child)
							hist[w]++
						}
					}
					fmt.Fprint(os.Stderr, "FOW")
					for w, n := range hist {
						fmt.Fprintf(os.Stderr, " %d:%d", w, n)
					}
					fmt.Fprintln(os.Stderr)
					continue
				}
				st.spans += int64(len(spans))
				st.spansPer = append(st.spansPer, int64(len(spans)))
				st.nTraces++
				ev := storedEvents(tid, spans)
				evalSBridge(st, tid, ev, sbCfg, cpd, dropRate, sbDropSeed, fpBits, len(spans))
				evalCGPRB(st, tid, ev, cgCfg, cpd, dropRate, cgDropSeed, len(spans))
				if d := atomic.AddInt64(&done, 1); d%1000 == 0 {
					el := time.Since(t0).Seconds()
					fmt.Fprintf(os.Stderr, "  progress: %d/%d traces  %.0fs  %.0f traces/s\n", d, n, el, float64(d)/el)
				}
			}
		}(w, lo, hi)
	}
	wg.Wait()
	var agg stats
	for w := range results {
		agg.add(&results[w])
	}
	agg.report(c, cpd, dropRate, fpBits, n, baseSeed, workers, time.Since(t0))
}

// stats is the aggregate (per-worker, then merged).
type stats struct {
	nTraces int
	spans   int64
	// S-Bridge topology
	sbCorrect, sbWrong, sbCkpt4, sbSevAmbig int
	// S-Bridge structure (full)
	sbStruct, sbAccepted, sbDeeAmbig, sbEventOK, sbCPOK int
	// cgprb
	cgTopoCorrect, cgStrict, cgMisattached, cgAmbiguous, cgAmbiguousBad int
	cgMisattachAnc                                                      int // of cgMisattached, anchor is a true ancestor (band-math); complement = non-ancestor (bloom-FP/cross-branch)
	// per-trace recon timing (ns) and span counts, for distributions
	sbTopoNs, sbStructNs, cgNs, spansPer []int64
	// per-trace amortized recon time (ns/span), for per-span distributions
	sbTopoPerSpan, sbFullPerSpan, cgPerSpan []float64
}

func (a *stats) add(b *stats) {
	a.nTraces += b.nTraces
	a.spans += b.spans
	a.sbCorrect += b.sbCorrect
	a.sbWrong += b.sbWrong
	a.sbCkpt4 += b.sbCkpt4
	a.sbSevAmbig += b.sbSevAmbig
	a.sbStruct += b.sbStruct
	a.sbAccepted += b.sbAccepted
	a.sbDeeAmbig += b.sbDeeAmbig
	a.sbEventOK += b.sbEventOK
	a.sbCPOK += b.sbCPOK
	a.cgTopoCorrect += b.cgTopoCorrect
	a.cgStrict += b.cgStrict
	a.cgMisattached += b.cgMisattached
	a.cgMisattachAnc += b.cgMisattachAnc
	a.cgAmbiguous += b.cgAmbiguous
	a.cgAmbiguousBad += b.cgAmbiguousBad
	a.sbTopoNs = append(a.sbTopoNs, b.sbTopoNs...)
	a.sbStructNs = append(a.sbStructNs, b.sbStructNs...)
	a.cgNs = append(a.cgNs, b.cgNs...)
	a.spansPer = append(a.spansPer, b.spansPer...)
	a.sbTopoPerSpan = append(a.sbTopoPerSpan, b.sbTopoPerSpan...)
	a.sbFullPerSpan = append(a.sbFullPerSpan, b.sbFullPerSpan...)
	a.cgPerSpan = append(a.cgPerSpan, b.cgPerSpan...)
}

// summarizeF returns count, mean, p50, p99, max of xs (raw units, float).
func summarizeF(xs []float64) (n int, mean, p50, p99, mx float64) {
	n = len(xs)
	if n == 0 {
		return
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	var s float64
	for _, v := range cp {
		s += v
	}
	at := func(p float64) float64 { return cp[int(p*float64(n-1))] }
	return n, s / float64(n), at(0.5), at(0.99), cp[n-1]
}

// summarize returns count, mean, p50, p90, p99, max of xs (raw units).
func summarize(xs []int64) (n int, mean, p50, p90, p99, mx float64) {
	n = len(xs)
	if n == 0 {
		return
	}
	cp := append([]int64(nil), xs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	var s int64
	for _, v := range cp {
		s += v
	}
	at := func(p float64) float64 { return float64(cp[int(p*float64(n-1))]) }
	return n, float64(s) / float64(n), at(0.5), at(0.9), at(0.99), float64(cp[n-1])
}

func (a *stats) report(c gen.Config, cpd int, drop float64, fpBits, n int, baseSeed int64, workers int, dt time.Duration) {
	pct := func(x, y int) float64 {
		if y == 0 {
			return 0
		}
		return 100 * float64(x) / float64(y)
	}
	fmt.Printf("=== stream_eval: shape=%s depth=%d fanout-dist=%s max-spans=%d cpd=%d drop=%.2f fp-bits=%d ===\n",
		c.Shape, c.Depth, c.FanoutDist, c.MaxSpans, cpd, drop, fpBits)
	fmt.Printf("traces=%d  spans=%d  base-seed=%d  workers=%d  wall=%s  (%.0f traces/s, %.2fM spans/s, ZERO disk)\n",
		a.nTraces, a.spans, baseSeed, workers, dt.Round(time.Millisecond),
		float64(a.nTraces)/dt.Seconds(), float64(a.spans)/dt.Seconds()/1e6)
	fmt.Printf("--- S-Bridge topology ---\n")
	fmt.Printf("  correct=%d (%.4f%%)  WRONG[no-collision=BUG]=%d (%.4f%%)  ckpt4_ambiguous=%d (%.4f%%)  severed_ambiguous=%d (%.4f%%)\n",
		a.sbCorrect, pct(a.sbCorrect, a.nTraces), a.sbWrong, pct(a.sbWrong, a.nTraces),
		a.sbCkpt4, pct(a.sbCkpt4, a.nTraces), a.sbSevAmbig, pct(a.sbSevAmbig, a.nTraces))
	fmt.Printf("--- S-Bridge structure (full; %d topo-correct, %d REJECTED for DEE ambiguity, %d accepted) ---\n",
		a.sbStruct, a.sbDeeAmbig, a.sbAccepted)
	fmt.Printf("  rejected(DEE ambiguous)=%d (%.4f%% of topo-correct)  event_order_correct=%d (%.4f%%)  critical_path_correct=%d (%.4f%%)\n",
		a.sbDeeAmbig, pct(a.sbDeeAmbig, a.sbStruct), a.sbEventOK, pct(a.sbEventOK, a.sbAccepted), a.sbCPOK, pct(a.sbCPOK, a.sbAccepted))
	fmt.Printf("--- cgprb topology ---\n")
	fmt.Printf("  topo_correct=%d (%.4f%%)  strict_shape=%d (%.4f%%)  misattached=%d  ambiguous=%d (of which misattached=%d)\n",
		a.cgTopoCorrect, pct(a.cgTopoCorrect, a.nTraces), a.cgStrict, pct(a.cgStrict, a.nTraces),
		a.cgMisattached, a.cgAmbiguous, a.cgAmbiguousBad)
	fmt.Printf("  misattach split: ancestor(band-math)=%d (%.1f%%)  non-ancestor(bloom-FP/cross-branch)=%d (%.1f%%)\n",
		a.cgMisattachAnc, pct(a.cgMisattachAnc, a.cgMisattached),
		a.cgMisattached-a.cgMisattachAnc, pct(a.cgMisattached-a.cgMisattachAnc, a.cgMisattached))
	fmt.Printf("--- per-solver recon timing (per trace, µs) ---\n")
	for _, e := range []struct {
		name string
		xs   []int64
	}{{"SB-topology", a.sbTopoNs}, {"SB-structure", a.sbStructNs}, {"cgprb-solver", a.cgNs}} {
		n, mean, p50, p90, p99, mx := summarize(e.xs)
		fmt.Printf("  %-13s n=%d  mean=%.1f p50=%.1f p90=%.1f p99=%.1f max=%.1f\n",
			e.name, n, mean/1000, p50/1000, p90/1000, p99/1000, mx/1000)
	}
	_, sm, sp50, sp90, sp99, smx := summarize(a.spansPer)
	fmt.Printf("--- spans/trace ---  mean=%.0f p50=%.0f p90=%.0f p99=%.0f max=%.0f\n", sm, sp50, sp90, sp99, smx)
	fmt.Printf("--- amortized recon time (per SPAN, ns/span) ---\n")
	for _, e := range []struct {
		name string
		xs   []float64
	}{{"SB-topology", a.sbTopoPerSpan}, {"SB-full", a.sbFullPerSpan}, {"cgprb-solver", a.cgPerSpan}} {
		n, mean, p50, p99, mx := summarizeF(e.xs)
		fmt.Printf("  %-13s n=%d  mean=%.1f p50=%.1f p99=%.1f max=%.1f\n", e.name, n, mean, p50, p99, mx)
	}
}

// storedEvents builds the within-trace, canonically-ordered event stream for one
// trace (same order trace_gen's store/corpus emit), so both recon pipelines see
// the spans exactly as they would from a materialized store.
func storedEvents(tid uint64, spans []gen.Span) []corpus.StoredEvent {
	type ev struct {
		ts       int64
		kind     uint8
		depth    int
		sid, pid uint64
		svc      uint16
	}
	out := make([]ev, 0, 2*len(spans))
	for _, s := range spans {
		out = append(out, ev{s.Start, corpus.KindStart, s.Depth, s.ID, s.Parent, s.Svc})
		out = append(out, ev{s.End, corpus.KindEnd, s.Depth, s.ID, s.Parent, s.Svc})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.kind != b.kind {
			return a.kind < b.kind // start before end
		}
		ar, br := int16(a.depth), int16(b.depth)
		if a.kind == corpus.KindEnd {
			ar, br = -ar, -br // ends: deeper first
		}
		if ar != br {
			return ar < br
		}
		return a.sid < b.sid // same trace; tid is constant
	})
	se := make([]corpus.StoredEvent, len(out))
	for i, e := range out {
		se[i] = corpus.StoredEvent{Kind: e.kind, SpanID: e.sid, ParentID: e.pid, ServiceID: e.svc, TS: e.ts * 1000}
	}
	return se
}

// ===================== S-Bridge per-trace (mirrors cmd/sbridge_recon) =====================

type sbSpanInfo struct {
	id, parent uint64
	ord, depth int
	endSeq     int64
}

func evalSBridge(st *stats, tid uint64, events []corpus.StoredEvent, cfg recon.Config, cpd int, rate float64, dropSeed int64, fpBits, nspans int) {
	tr := corpus.StoredTrace{TraceID: tid, Events: events}
	payloads, truth, spans, dees := sbEmitTrace(tr, cpd, false, fpBits, false, cfg.PrefixLen)
	dropped := sbDropSet(tr, rate, dropSeed, cpd)

	inputs := make([]recon.SBInput, 0, len(payloads))
	for sid, p := range payloads {
		if _, gone := dropped[sid]; gone {
			continue
		}
		inputs = append(inputs, recon.SBInput{SpanID: sid, Payload: p})
	}
	var survivors []recon.SBSurvivor
	for _, s := range spans {
		if _, gone := dropped[s.id]; gone {
			continue
		}
		survivors = append(survivors, recon.SBSurvivor{SpanID: s.id, ParentID: s.parent, Ordinal: s.ord, Depth: s.depth})
	}
	t0 := time.Now()
	res := recon.ReconstructSBridge(inputs, survivors, cfg)
	topoNs := time.Since(t0).Nanoseconds()
	st.sbTopoNs = append(st.sbTopoNs, topoNs)
	if nspans > 0 {
		st.sbTopoPerSpan = append(st.sbTopoPerSpan, float64(topoNs)/float64(nspans))
	}
	v := recon.ScoreSBridgeUnderDrop(res, truth)
	switch {
	case v.Unsolvable:
		st.sbCkpt4++
	case res.SeveredAmbiguous > 0:
		st.sbSevAmbig++
	case !v.Correct:
		st.sbWrong++
	default:
		st.sbCorrect++
	}
	// Full structure phase — only when topology rebuilt exactly. (topo-only mode
	// would yield the IDENTICAL topology verdict above; only this phase differs,
	// so we run the full emit once and read both.)
	if v.Correct && !v.Unsolvable && res.SeveredAmbiguous == 0 {
		endPos := make(map[uint64]int64, len(spans))
		for _, s := range spans {
			endPos[s.id] = s.endSeq
		}
		ts := time.Now()
		sr := recon.ScoreStructure(res, truth, endPos, dees)
		structNs := time.Since(ts).Nanoseconds()
		st.sbStructNs = append(st.sbStructNs, structNs)
		if nspans > 0 {
			st.sbFullPerSpan = append(st.sbFullPerSpan, float64(topoNs+structNs)/float64(nspans))
		}
		st.sbStruct++
		if sr.DEEAmbiguous {
			st.sbDeeAmbig++
		} else {
			st.sbAccepted++
			if sr.EventOrderOK {
				st.sbEventOK++
			}
			if sr.CriticalPath {
				st.sbCPOK++
			}
		}
	}
}

// sbEmitTrace replays one trace through a fresh SBridgeHandler (verbatim from
// cmd/sbridge_recon/main.go).
func sbEmitTrace(tr corpus.StoredTrace, cpd int, topoOnly bool, fpBits int, noOrdinal bool, ckptBytes int) (map[uint64][]byte, recon.SBTruth, []sbSpanInfo, [][]byte) {
	h := bridge.NewSBridgeHandler(cpd, nil)
	h.EmitOC = true
	h.TopoOnly = topoOnly
	h.OmitOrdinal = noOrdinal
	if fpBits > 0 {
		h.FPBits = fpBits
	}
	if ckptBytes > 0 {
		h.CkptBytes = ckptBytes
	}
	payloads := map[uint64][]byte{}
	h.EmitSink = func(_, sid uint64, payload []byte) {
		payloads[sid] = append([]byte(nil), payload...)
	}
	var dees [][]byte
	h.DEESink = func(_ uint64, q []byte) { dees = append(dees, append([]byte(nil), q...)) }
	truth := recon.SBTruth{ChildByOrd: map[uint64]map[int]uint64{}}
	nextSeq := map[uint64]int{}
	var spans []sbSpanInfo
	parent := map[uint64]uint64{}
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
			spans = append(spans, sbSpanInfo{id: e.SpanID, parent: e.ParentID, ord: seq})
			h.OnStart(ev, seq)
		} else {
			endPos[e.SpanID] = int64(i)
			h.OnEnd(ev)
		}
	}
	h.EvictTrace(tr.TraceID)
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

// sbDropSet picks dropped span ids (verbatim from cmd/sbridge_recon/main.go):
// interior (non-root, non-leaf, non-depth-checkpoint) spans only, splitmix-seeded
// per trace, drawn in sorted id order.
func sbDropSet(tr corpus.StoredTrace, rate float64, base int64, cpd int) map[uint64]struct{} {
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
		if parent[id] != 0 && hasKids[id] && d(id)%cpd != 0 {
			cands = append(cands, id)
		}
	}
	stt := splitmix(tr.TraceID + uint64(base))
	dropped := map[uint64]struct{}{}
	for _, id := range cands {
		var u float64
		u, stt = nextFloat(stt)
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

func nextFloat(st uint64) (float64, uint64) {
	st = splitmix(st)
	return float64(st>>11) / float64(1<<53), st
}

// ===================== cgprb per-trace (mirrors cmd/trace_recon) =====================

type cgColl struct {
	spanID, parentID uint64
	depth            int
	br               []byte
}

var cgp2DumpOnce sync.Once

// bloomFPTarget returns the bloom false-positive target used to size the CGP-RB
// blooms on BOTH the emit and reconstruction sides (they must match). Defaults to
// bridge.DefaultBloomFPRate; override with TRACE_RECON_BLOOMFP to sweep geometry.
func bloomFPTarget() float64 {
	if v := os.Getenv("TRACE_RECON_BLOOMFP"); v != "" {
		if p, err := strconv.ParseFloat(v, 64); err == nil && p > 0 && p < 1 {
			return p
		}
	}
	return bridge.DefaultBloomFPRate
}

func evalCGPRB(st *stats, tid uint64, events []corpus.StoredEvent, cfg recon.Config, cpd int, rate float64, dropSeed int64, nspans int) {
	prefixLen := cfg.PrefixLen
	if prefixLen <= 0 {
		prefixLen = bridge.DefaultPCRPrefixLen
	}
	h := bridge.NewCGPRBBridgeHandler(cpd, prefixLen, bloomFPTarget())
	h.Capture = true
	var spans []cgColl
	idx := map[uint64]int{}
	nextSeq := map[uint64]int{}
	for _, e := range events {
		ev := &bridge.Event{TraceID: tid, SpanID: e.SpanID, ParentID: e.ParentID, ServiceID: e.ServiceID}
		if e.Kind == corpus.KindStart {
			seq := 0
			if e.ParentID != 0 {
				seq = nextSeq[e.ParentID] + 1
				nextSeq[e.ParentID] = seq
			}
			r := h.OnStart(ev, seq)
			idx[e.SpanID] = len(spans)
			spans = append(spans, cgColl{spanID: e.SpanID, parentID: e.ParentID, depth: -1, br: r.Payload})
		} else {
			r := h.OnEnd(ev)
			if j, ok := idx[e.SpanID]; ok {
				spans[j].depth = r.Depth
				if r.Payload != nil {
					spans[j].br = r.Payload
				}
			}
		}
	}
	h.EvictTrace(tid)

	// Per-trace drop: non-_br (non-checkpoint) candidates, mixSeed-seeded, spanID order.
	cands := make([]uint64, 0, len(spans))
	for _, s := range spans {
		if s.br == nil {
			cands = append(cands, s.spanID)
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	drng := rand.New(rand.NewSource(mixSeed(tid, dropSeed)))
	dropped := map[uint64]struct{}{}
	for _, id := range cands {
		if drng.Float64() < rate {
			dropped[id] = struct{}{}
		}
	}

	truth := make([]recon.TruthSpan, len(spans))
	for i, s := range spans {
		truth[i] = recon.TruthSpan{SpanID: s.spanID, ParentID: s.parentID, Depth: s.depth}
	}
	survivors := make([]recon.Span, 0, len(spans)-len(dropped))
	for _, s := range spans {
		if _, gone := dropped[s.spanID]; gone {
			continue
		}
		sp := recon.Span{SpanID: s.spanID, ParentID: s.parentID, Depth: s.depth}
		if s.br != nil {
			d, prefix, bits, haEntries, err := recon.DecodeCGPRBPayload(s.br, cfg)
			if err != nil {
				// malformed payload should never happen for self-generated traces
				continue
			}
			sp.Depth = d
			sp.CkptPrefix = prefix
			sp.BloomBits = bits
			sp.LeafCarrier = d%cpd != 0
			sp.HA = haEntries
		}
		survivors = append(survivors, sp)
	}
	tc := time.Now()
	var res recon.Result
	if os.Getenv("TRACE_RECON_CGP2") == "1" {
		res = recon.ReconstructCGP2(survivors, cfg) // from-scratch reconstructor
	} else {
		res = recon.ReconstructCGPRB(survivors, cfg)
	}
	cgNs := time.Since(tc).Nanoseconds()
	st.cgNs = append(st.cgNs, cgNs)
	if nspans > 0 {
		st.cgPerSpan = append(st.cgPerSpan, float64(cgNs)/float64(nspans))
	}
	topo := recon.ScoreCGPTopology(res, truth, dropped, tid)
	sc := recon.ScorePCR(res, truth, dropped)
	if os.Getenv("TRACE_RECON_CGP2DUMP") == "1" && len(res.ReconParent) > 0 {
		cgp2DumpOnce.Do(func() { recon.DumpCGP2Edges(survivors, truth, res, dropped) })
	}
	if os.Getenv("TRACE_RECON_CGP2ISO") == "1" {
		iso := recon.ScoreCGP2Iso(res, truth, dropped)
		fmt.Fprintf(os.Stderr, "CGP2ISO realNodes=%d edgeExact=%d edgeAnonOK=%d edgeWrong=%d | survNodes=%d survExact=%d | namedSyn=%d namedExact=%d\n",
			iso.RealNodes, iso.EdgeExact, iso.EdgeAnonOK, iso.EdgeWrong, iso.SurvNodes, iso.SurvExact, iso.NamedSyn, iso.NamedExact)
	}
	if os.Getenv("TRACE_RECON_CGP2WRONGDEPTH") == "1" && len(res.ReconParent) > 0 {
		recon.DumpCGP2WrongDepths(truth, res, dropped, cpd)
	}
	if os.Getenv("TRACE_RECON_CGP2GPD") == "1" && len(res.ReconParent) > 0 {
		ex, gd, gdw, gs, gsw, cSum, cN := recon.GrandparentDecomp(survivors, truth, res, dropped, cpd)
		fmt.Fprintf(os.Stderr, "CGP2GPD exposed=%d gDrop=%d gDropWrong=%d gSurv=%d gSurvWrong=%d cousinSum=%d cousinN=%d\n", ex, gd, gdw, gs, gsw, cSum, cN)
	}
	if os.Getenv("TRACE_RECON_CGP2SURVCHK") == "1" && len(res.ReconParent) > 0 {
		bad, holeInv, wrongSurv, miss, missPS, tot, _ := recon.CheckSurvivorSyntheticEdges(survivors, res)
		fmt.Fprintf(os.Stderr, "CGP2SURVCHK total=%d badSynthetic=%d holeInvented=%d wrongSurvivor=%d missing=%d missingParentSurvived=%d\n",
			tot, bad, holeInv, wrongSurv, miss, missPS)
	}
	if topo.TopoCorrect {
		st.cgTopoCorrect++
	}
	if topo.StrictShapeCorrect {
		st.cgStrict++
	}
	st.cgMisattached += sc.Misattached
	st.cgMisattachAnc += sc.MisattachAncestor
	st.cgAmbiguous += sc.Ambiguous
	st.cgAmbiguousBad += sc.AmbiguousBad
}

// mixSeed derives a per-trace drop seed (verbatim from cmd/trace_recon/main.go).
func mixSeed(tid uint64, base int64) int64 {
	x := tid + uint64(base)*0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return int64(x)
}
