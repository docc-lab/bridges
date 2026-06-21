// Command trace_gen produces SYNTHETIC trace corpora with controlled structure,
// so the bridge schemes can be measured across the (depth × fan-out × width ×
// concurrency × drop) space the real Uber corpus only samples one point of.
//
// It emits any subset of four formats from one generation pass (so every format
// describes the IDENTICAL traces):
//
//	corpus  -> <out>/events.bin + <out>/meta.bin   (trace_sim --corpus <out>)
//	store   -> <out>/synth.store                   (sbridge_recon --store …)
//	jaeger  -> <out>/jaeger.json                   (Jaeger query-API JSON; loader reads it)
//	otel    -> <out>/otel.json                     (OTLP/JSON resourceSpans)
//
//	trace_gen --out DIR --n 1000 --shape kary --depth 4 --fanout 3 \
//	          --formats corpus,store,jaeger,otel --seed 1
//
// The generation logic lives in package bridges/gen so cmd/stream_eval can
// generate the IDENTICAL traces in memory and stream them through analysis
// without ever writing a corpus.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"bridges/corpus"
	"bridges/gen"
)

func main() {
	var c gen.Config
	var n int
	var out, formats string
	flag.IntVar(&n, "n", 1000, "number of traces to generate")
	flag.StringVar(&c.Shape, "shape", "kary", "tree shape: chain | star | kary | skewed | branch | spine | deepwide | slab. branch = Galton-Watson with --fanout-dist; spine = ONE deep path (Kesten) + bushes; deepwide = K=--fanout parallel deep paths + bushes; slab = STRESS: per-node fan-out in [--fanout-min,--fanout-max] to absolute --depth, continuation-capped to --max-spans")
	flag.IntVar(&c.Depth, "depth", 4, "max tree depth (chain/kary); mean/cap for --depth-dist")
	flag.IntVar(&c.Fanout, "fanout", 3, "children per node (star/kary); mean for poisson/geometric")
	flag.StringVar(&c.FanoutDist, "fanout-dist", "fixed", "offspring law for --shape branch/spine: fixed | zipf | poisson | geometric | uber (empirical Uber day1 law: chain-dominated + power-law branch tail)")
	flag.Float64Var(&c.FanoutS, "fanout-s", 1.2, "zipf exponent for --fanout-dist zipf (>1; lower = heavier tail)")
	flag.IntVar(&c.FanoutMin, "fanout-min", 1, "--shape slab: minimum per-node fan-out (0 allows lineages to die early)")
	flag.BoolVar(&c.Spindle, "spindle", false, "--shape slab: taper the continuation-cap into spindle lobes (width rises to a peak, tapers to a thin neck, then RE-SPINDLES) — realistic spindle texture at any depth, instead of a flat brick")
	flag.IntVar(&c.SpindlePeriod, "spindle-period", 0, "--shape slab --spindle: depth per spindle lobe before re-spindling (0 = one spindle over the whole --depth; smaller = more lobes stacked to reach deep)")
	flag.IntVar(&c.FanoutMax, "fanout-max", 256, "cap on sampled fan-out per node (--shape slab: also the max of the per-node fan-out range)")
	flag.StringVar(&c.DepthDist, "depth-dist", "fixed", "per-trace max-depth law: fixed | zipf (mostly shallow, rare deep, capped at --depth)")
	flag.Float64Var(&c.DepthS, "depth-s", 1.5, "zipf exponent for --depth-dist zipf (>1)")
	flag.IntVar(&c.MaxSpans, "max-spans", 20000, "hard cap on spans per trace (kary/skewed can explode)")
	flag.Float64Var(&c.Concurrency, "concurrency", 0.0, "0 = siblings strictly sequential, 1 = fully overlapping")
	flag.IntVar(&c.Services, "services", 16, "number of distinct service names")
	flag.Int64Var(&c.BaseDurUS, "base-dur-us", 1_000_000, "root span duration in MICROSECONDS (children subdivide it; timestamps are µs-granular, matching Jaeger)")
	flag.Int64Var(&c.Seed, "seed", 1, "RNG seed (deterministic output)")
	flag.StringVar(&out, "out", "", "output directory (required)")
	flag.StringVar(&formats, "formats", "corpus,store", "comma list: corpus,store,jaeger,otel")
	flag.Parse()
	if out == "" {
		fmt.Fprintln(os.Stderr, "error: --out required")
		os.Exit(2)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	want := map[string]bool{}
	for _, f := range strings.Split(formats, ",") {
		want[strings.TrimSpace(f)] = true
	}

	rng := rand.New(rand.NewSource(c.Seed))
	svcNames := make([]string, c.Services)
	for i := range svcNames {
		svcNames[i] = fmt.Sprintf("svc%d", i)
	}

	fanoutOf := gen.MakeFanoutSampler(&c, rng)
	depthOf := gen.MakeDepthSampler(&c, rng)
	// Estimate the mean per-node fan-out so the slab continuation-cap can size
	// traces to the span budget regardless of which --fanout-dist feeds it. Uses
	// a throwaway rng, so the generation stream is unperturbed.
	c.SlabMeanW = gen.EstimateMeanFanout(&c)

	traceIDs := make([]uint64, n)
	traces := make([][]gen.Span, n)
	for i := 0; i < n; i++ {
		tid := rng.Uint64()
		traceIDs[i] = tid
		traces[i] = gen.Trace(&c, rng, fanoutOf, depthOf)
	}

	if want["corpus"] {
		if err := emitCorpus(out, traceIDs, traces, svcNames); err != nil {
			fail("corpus", err)
		}
	}
	if want["store"] {
		if err := emitStore(out, traceIDs, traces); err != nil {
			fail("store", err)
		}
	}
	if want["jaeger"] {
		if err := emitJaeger(out, traceIDs, traces, svcNames); err != nil {
			fail("jaeger", err)
		}
	}
	if want["otel"] {
		if err := emitOTel(out, traceIDs, traces, svcNames); err != nil {
			fail("otel", err)
		}
	}
	total := 0
	depths := make([]int, len(traces))
	for i, t := range traces {
		total += len(t)
		md := 0
		for _, s := range t {
			if s.Depth > md {
				md = s.Depth
			}
		}
		depths[i] = md
	}
	sort.Ints(depths)
	dp := func(p float64) int { return depths[int(p*float64(len(depths)-1))] }
	fmt.Fprintf(os.Stderr, "generated %d traces, %d spans (%s) -> %s [%s]\n",
		n, total, c.Shape, out, formats)
	fmt.Fprintf(os.Stderr, "  depth: p50=%d p90=%d p99=%d max=%d   spans/trace mean=%d\n",
		dp(0.50), dp(0.90), dp(0.99), depths[len(depths)-1], total/len(traces))

	// Where does the BRANCHING live? The depth line above is just the spine
	// length (deepest leaf). cgprb covering ambiguity can only arise at
	// MULTI-CHILD parents, so report mean span depth and the depth distribution
	// of those branch points — that, not the spine length, is the dimension
	// cgprb is actually stressed on.
	var sumDepth, nSpans int
	var brDepth, fanouts []int
	for _, t := range traces {
		kids := map[uint64]int{}
		dep := map[uint64]int{}
		for _, s := range t {
			sumDepth += s.Depth
			nSpans++
			dep[s.ID] = s.Depth
			if s.Parent != 0 {
				kids[s.Parent]++
			}
		}
		for pid, k := range kids {
			if k >= 2 {
				brDepth = append(brDepth, dep[pid])
				fanouts = append(fanouts, k)
			}
		}
	}
	sort.Ints(brDepth)
	sort.Ints(fanouts)
	pct := func(xs []int, p float64) int {
		if len(xs) == 0 {
			return 0
		}
		return xs[int(p*float64(len(xs)-1))]
	}
	sumF := 0
	for _, f := range fanouts {
		sumF += f
	}
	meanF := 0.0
	if len(fanouts) > 0 {
		meanF = float64(sumF) / float64(len(fanouts))
	}
	fmt.Fprintf(os.Stderr, "  span-depth mean=%.1f | branch-points: n=%d (%.1f/trace) depth p50=%d p90=%d max=%d | fanout mean=%.2f p90=%d max=%d\n",
		float64(sumDepth)/float64(nSpans), len(brDepth), float64(len(brDepth))/float64(len(traces)),
		pct(brDepth, 0.50), pct(brDepth, 0.90), pct(brDepth, 1.0),
		meanF, pct(fanouts, 0.90), pct(fanouts, 1.0))
}

func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "emit %s: %v\n", what, err)
	os.Exit(1)
}

// ---- ordering helpers (match trace_prep's per-event key) ----

type ev struct {
	ts    int64
	kind  uint8
	depth int
	tid   uint64
	sid   uint64
	pid   uint64
	svc   uint16
}

func lessEv(a, b ev) bool {
	if a.ts != b.ts {
		return a.ts < b.ts
	}
	if a.kind != b.kind {
		return a.kind < b.kind // start (0) before end (1)
	}
	ar, br := int16(a.depth), int16(b.depth)
	if a.kind == corpus.KindEnd {
		ar, br = -ar, -br // ends: deeper first
	}
	if ar != br {
		return ar < br
	}
	if a.tid != b.tid {
		return a.tid < b.tid
	}
	return a.sid < b.sid
}

func traceEvents(tid uint64, spans []gen.Span) []ev {
	out := make([]ev, 0, 2*len(spans))
	for _, s := range spans {
		out = append(out,
			ev{s.Start, corpus.KindStart, s.Depth, tid, s.ID, s.Parent, s.Svc},
			ev{s.End, corpus.KindEnd, s.Depth, tid, s.ID, s.Parent, s.Svc})
	}
	return out
}

// ---- emitters ----

func emitCorpus(out string, tids []uint64, traces [][]gen.Span, svc []string) error {
	eventsPath, metaPath := corpus.Paths(out)
	w, err := corpus.CreateEvents(eventsPath)
	if err != nil {
		return err
	}
	var all []ev
	counts := make([]uint32, len(traces))
	for i, sp := range traces {
		counts[i] = uint32(len(sp))
		all = append(all, traceEvents(tids[i], sp)...)
	}
	sort.Slice(all, func(i, j int) bool { return lessEv(all[i], all[j]) }) // global order
	for _, e := range all {
		if err := w.Write(corpus.Event{TS: e.ts * 1000, SpanID: e.sid, ParentID: e.pid,
			TraceID: e.tid, Depth: uint16(e.depth), ServiceID: e.svc, Kind: e.kind}); err != nil { // µs->ns
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return corpus.WriteMeta(metaPath, &corpus.Meta{Services: svc, TraceOrder: tids, SpanCounts: counts})
}

func emitStore(out string, tids []uint64, traces [][]gen.Span) error {
	w, err := corpus.NewTraceStoreWriter(filepath.Join(out, "synth.store"))
	if err != nil {
		return err
	}
	for i, sp := range traces {
		evs := traceEvents(tids[i], sp)
		sort.Slice(evs, func(a, b int) bool { return lessEv(evs[a], evs[b]) }) // within-trace order
		se := make([]corpus.StoredEvent, len(evs))
		for k, e := range evs {
			se[k] = corpus.StoredEvent{Kind: e.kind, SpanID: e.sid, ParentID: e.pid, ServiceID: e.svc, TS: e.ts * 1000} // µs->ns
		}
		if err := w.WriteTrace(tids[i], se); err != nil {
			return err
		}
	}
	return w.Close()
}

// Jaeger query-API JSON: {data:[{traceID, spans:[{spanID, references:[CHILD_OF],
// startTime(µs), duration(µs), processID}], processes:{pN:{serviceName}}}]}.
func emitJaeger(out string, tids []uint64, traces [][]gen.Span, svc []string) error {
	type ref struct {
		RefType string `json:"refType"`
		TraceID string `json:"traceID"`
		SpanID  string `json:"spanID"`
	}
	type jspan struct {
		TraceID       string `json:"traceID"`
		SpanID        string `json:"spanID"`
		OperationName string `json:"operationName"`
		References    []ref  `json:"references"`
		StartTime     int64  `json:"startTime"` // µs
		Duration      int64  `json:"duration"`  // µs
		ProcessID     string `json:"processID"`
	}
	type proc struct {
		ServiceName string `json:"serviceName"`
	}
	type jtrace struct {
		TraceID   string          `json:"traceID"`
		Spans     []jspan         `json:"spans"`
		Processes map[string]proc `json:"processes"`
	}
	data := make([]jtrace, 0, len(traces))
	for i, sp := range traces {
		thex := fmt.Sprintf("%016x", tids[i])
		jt := jtrace{TraceID: thex, Processes: map[string]proc{}}
		for _, s := range sp {
			pid := fmt.Sprintf("p%d", s.Svc)
			jt.Processes[pid] = proc{ServiceName: svc[s.Svc]}
			var refs []ref
			if s.Parent != 0 {
				refs = []ref{{RefType: "CHILD_OF", TraceID: thex, SpanID: fmt.Sprintf("%016x", s.Parent)}}
			}
			jt.Spans = append(jt.Spans, jspan{
				TraceID: thex, SpanID: fmt.Sprintf("%016x", s.ID), OperationName: "op",
				References: refs, StartTime: s.Start, Duration: s.End - s.Start, // already µs (Jaeger native)
				ProcessID: pid,
			})
		}
		data = append(data, jt)
	}
	return writeJSON(filepath.Join(out, "jaeger.json"), map[string]any{"data": data})
}

// OTLP/JSON: resourceSpans grouped by service.name, each with one scopeSpans.
func emitOTel(out string, tids []uint64, traces [][]gen.Span, svc []string) error {
	type kv struct {
		Key   string `json:"key"`
		Value struct {
			S string `json:"stringValue"`
		} `json:"value"`
	}
	type ospan struct {
		TraceID           string `json:"traceId"`
		SpanID            string `json:"spanId"`
		ParentSpanID      string `json:"parentSpanId,omitempty"`
		Name              string `json:"name"`
		Kind              int    `json:"kind"`
		StartTimeUnixNano string `json:"startTimeUnixNano"`
		EndTimeUnixNano   string `json:"endTimeUnixNano"`
	}
	// group spans by service
	bySvc := make([][]ospan, len(svc))
	for i, sp := range traces {
		thex := fmt.Sprintf("%032x", tids[i]) // 16-byte trace id
		for _, s := range sp {
			parent := ""
			if s.Parent != 0 {
				parent = fmt.Sprintf("%016x", s.Parent)
			}
			bySvc[s.Svc] = append(bySvc[s.Svc], ospan{
				TraceID: thex, SpanID: fmt.Sprintf("%016x", s.ID), ParentSpanID: parent,
				Name: "op", Kind: 2,
				StartTimeUnixNano: fmt.Sprintf("%d", s.Start*1000), EndTimeUnixNano: fmt.Sprintf("%d", s.End*1000), // µs->ns
			})
		}
	}
	var resourceSpans []any
	for si, spans := range bySvc {
		if len(spans) == 0 {
			continue
		}
		attr := kv{Key: "service.name"}
		attr.Value.S = svc[si]
		resourceSpans = append(resourceSpans, map[string]any{
			"resource":   map[string]any{"attributes": []kv{attr}},
			"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "trace_gen"}, "spans": spans}},
		})
	}
	return writeJSON(filepath.Join(out, "otel.json"), map[string]any{"resourceSpans": resourceSpans})
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(bw)
	if err := enc.Encode(v); err != nil {
		return err
	}
	return bw.Flush()
}
