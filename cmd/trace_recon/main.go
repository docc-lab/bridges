// Command trace_recon is the reconstruction test harness (P-Bridge only for
// now). It runs spans through the bridge handler with payload capture on,
// applies a seeded drop policy to non-checkpoint spans, reconstructs each
// trace from the surviving spans + payloads, and scores the reconstruction
// against the pre-drop ground truth.
//
// The whole pipeline is streaming and per-trace: a trace is dropped,
// reconstructed, and scored the moment its last event is processed, then its
// state is freed. Two input modes, mirroring trace_sim:
//
//	--corpus DIR   read events.bin + meta.bin produced by trace_prep
//	(no flag)      read JSON files from <input_dir> directly
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/loader"
	"bridges/recon"
)

type config struct {
	inputDir           string
	corpusDir          string
	outputPath         string
	checkpointDistance int
	dropRate           float64
	seed               int64
	traceCount         int
	requireClean       bool
	bottomUp           bool
	bloomFP            float64
	chainCheck         bool
}

func parseFlags() config {
	var c config
	mode := ""
	flag.StringVar(&c.outputPath, "o", "", "Output JSON file (required)")
	flag.StringVar(&c.outputPath, "output", "", "Output JSON file (required)")
	flag.StringVar(&c.corpusDir, "corpus", "", "Read events.bin + meta.bin from this corpus dir")
	flag.StringVar(&mode, "mode", "pb", "Bridge mode (only pb supported)")
	flag.IntVar(&c.checkpointDistance, "checkpoint-distance", 1, "Checkpoint distance")
	flag.Float64Var(&c.bloomFP, "bloom-fp", bridge.DefaultBloomFPRate, "Target bloom false-positive rate (sets bloom geometry on both the emit and reconstruction sides)")
	flag.Float64Var(&c.dropRate, "drop-rate", 1.0, "Probability of dropping each non-checkpoint span (1.0 = drop all)")
	order := ""
	flag.StringVar(&order, "order", "bottom-up", "Orphan processing order: bottom-up (deepest-first, default) or independent")
	consistency := ""
	flag.StringVar(&consistency, "consistency", "chain", "Anchor candidate consistency check: chain (test the candidate's nameable ancestry against the bloom) or none")
	flag.Int64Var(&c.seed, "seed", 42, "RNG seed for the drop policy")
	flag.IntVar(&c.traceCount, "trace-count", 0, "Max number of traces to process (0 = all). Corpus mode: first N of the corpus trace order; events of other traces are skipped during streaming")
	flag.BoolVar(&c.requireClean, "require-clean", true, "Cleanliness filter (JSON mode only)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <input_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s --corpus <corpus_dir> [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if mode != "pb" {
		fmt.Fprintf(os.Stderr, "error: only -mode pb is supported (got %q)\n", mode)
		os.Exit(2)
	}
	switch order {
	case "independent":
	case "bottom-up":
		c.bottomUp = true
	default:
		fmt.Fprintf(os.Stderr, "error: -order must be independent or bottom-up (got %q)\n", order)
		os.Exit(2)
	}
	switch consistency {
	case "chain":
		c.chainCheck = true
	case "none":
	default:
		fmt.Fprintf(os.Stderr, "error: -consistency must be chain or none (got %q)\n", consistency)
		os.Exit(2)
	}
	if c.corpusDir == "" {
		if flag.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "error: input_dir or --corpus required")
			flag.Usage()
			os.Exit(2)
		}
		c.inputDir = flag.Arg(0)
	}
	if c.outputPath == "" {
		fmt.Fprintln(os.Stderr, "error: -o/--output required")
		os.Exit(2)
	}
	if c.dropRate < 0 || c.dropRate > 1 {
		fmt.Fprintln(os.Stderr, "error: --drop-rate must be in [0,1]")
		os.Exit(2)
	}
	return c
}

// collSpan is one span's collected record: standard fields plus captured
// bridge attributes.
type collSpan struct {
	spanID   uint64
	parentID uint64
	depth    int    // handler-derived absolute depth (ground truth for scoring)
	br       []byte // _br payload; nil = the span carried only _d
}

// harness drives the per-trace collect -> drop -> reconstruct -> score loop.
type harness struct {
	h       *bridge.PathBridgeHandler
	cfg     recon.Config
	rng     *rand.Rand
	dropAll bool
	rate    float64

	spansByTID map[uint64][]collSpan
	idxByKey   map[spanKey]int
	openByTID  map[uint64]int
	// nextSeq is nested per trace so a closed trace's counters can be freed;
	// a flat (traceID, parentID) map grows with every parent span ever seen
	// (gigabytes over the full corpus).
	nextSeq map[uint64]map[uint64]int

	scores map[uint64]recon.Score
}

type spanKey struct{ traceID, spanID uint64 }

func newHarness(c config) *harness {
	h := bridge.NewPathBridgeHandler(c.checkpointDistance, c.bloomFP)
	h.EmitDepth = true
	h.Capture = true
	cfg := recon.NewConfig(c.checkpointDistance, c.bloomFP)
	cfg.BottomUp = c.bottomUp
	cfg.ChainCheck = c.chainCheck
	return &harness{
		h:          h,
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(c.seed)),
		dropAll:    c.dropRate >= 1.0,
		rate:       c.dropRate,
		spansByTID: make(map[uint64][]collSpan),
		idxByKey:   make(map[spanKey]int),
		openByTID:  make(map[uint64]int),
		nextSeq:    make(map[uint64]map[uint64]int),
	}
}

func (ha *harness) beginTrace(tid uint64, spanCount int) {
	ha.openByTID[tid] = 2 * spanCount
}

func (ha *harness) onEvent(ts int64, kind bridge.Kind, tid, sid, pid uint64, serviceID uint16) {
	ev := &bridge.Event{TraceID: tid, SpanID: sid, ParentID: pid, ServiceID: serviceID}
	if kind == bridge.KindStart {
		seqNum := 0
		if pid != 0 {
			m := ha.nextSeq[tid]
			if m == nil {
				m = make(map[uint64]int)
				ha.nextSeq[tid] = m
			}
			seqNum = m[pid] + 1
			m[pid] = seqNum
		}
		r := ha.h.OnStart(ev, seqNum)
		ha.idxByKey[spanKey{tid, sid}] = len(ha.spansByTID[tid])
		ha.spansByTID[tid] = append(ha.spansByTID[tid], collSpan{
			spanID: sid, parentID: pid, depth: -1, br: r.Payload,
		})
	} else {
		r := ha.h.OnEnd(ev)
		if idx, ok := ha.idxByKey[spanKey{tid, sid}]; ok {
			s := &ha.spansByTID[tid][idx]
			s.depth = r.Depth
			if r.Payload != nil {
				s.br = r.Payload
			}
		}
	}

	ha.openByTID[tid]--
	if ha.openByTID[tid] == 0 {
		ha.h.EvictTrace(tid)
		ha.finishTrace(tid)
		delete(ha.openByTID, tid)
	}
}

// finishTrace runs drop -> reconstruct -> score for one closed trace and
// frees its collected state.
func (ha *harness) finishTrace(tid uint64) {
	spans := ha.spansByTID[tid]
	delete(ha.spansByTID, tid)
	delete(ha.nextSeq, tid)
	for _, s := range spans {
		delete(ha.idxByKey, spanKey{tid, s.spanID})
	}

	// Drop policy: only non-checkpoint spans (no _br) are candidates;
	// _br carriers ride the high-priority queue and are never dropped.
	dropped := make(map[uint64]struct{})
	for _, s := range spans {
		if s.br != nil {
			continue
		}
		if ha.dropAll || ha.rng.Float64() < ha.rate {
			dropped[s.spanID] = struct{}{}
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
			d, bits, err := recon.DecodePBPayload(s.br, ha.cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "trace %016x span %016x: %v\n", tid, s.spanID, err)
				os.Exit(1)
			}
			sp.Depth = d
			sp.BloomBits = bits
		}
		survivors = append(survivors, sp)
	}

	res := recon.ReconstructPB(survivors, ha.cfg)
	ha.scoresStore(tid, recon.ScorePB(res, truth, dropped))
}

func (ha *harness) scoresStore(tid uint64, sc recon.Score) {
	if ha.scores == nil {
		ha.scores = make(map[uint64]recon.Score)
	}
	ha.scores[tid] = sc
}

// output is the per-trace score arrays, in trace load order.
type output struct {
	CheckpointDistance int     `json:"checkpoint_distance"`
	DropRate           float64 `json:"drop_rate"`
	BloomFP            float64 `json:"bloom_fp"`
	Order              string  `json:"order"`
	Consistency        string  `json:"consistency"`
	Seed               int64   `json:"seed"`
	NumTraces          int     `json:"num_traces"`
	NumSpans           []int   `json:"num_spans"`
	NumDropped         []int   `json:"num_dropped"`
	NumOrphans         []int   `json:"num_orphans"`
	NumReconnected     []int   `json:"num_reconnected"`
	NumAnchorCorrect   []int   `json:"num_anchor_correct"`
	NumGapCorrect      []int   `json:"num_gap_correct"`
	NumMisattached     []int   `json:"num_misattached"`
	NumUnanchored      []int   `json:"num_unanchored"`
	NumSynthetic       []int   `json:"num_synthetic"`
	NumBorrowed        []int   `json:"num_borrowed_bloom"`
}

func main() {
	c := parseFlags()
	ha := newHarness(c)

	var traceOrder []uint64
	if c.corpusDir != "" {
		traceOrder = runFromCorpus(c, ha)
	} else {
		traceOrder = runFromJSON(c, ha)
	}

	orderName := "independent"
	if c.bottomUp {
		orderName = "bottom-up"
	}
	out := output{
		CheckpointDistance: c.checkpointDistance,
		DropRate:           c.dropRate,
		BloomFP:            c.bloomFP,
		Order:              orderName,
		Consistency:        map[bool]string{true: "chain", false: "none"}[c.chainCheck],
		Seed:               c.seed,
		NumTraces:          len(traceOrder),
	}
	var agg recon.Score
	for _, tid := range traceOrder {
		sc := ha.scores[tid]
		out.NumSpans = append(out.NumSpans, sc.Spans)
		out.NumDropped = append(out.NumDropped, sc.Dropped)
		out.NumOrphans = append(out.NumOrphans, sc.Orphans)
		out.NumReconnected = append(out.NumReconnected, sc.Reconnected)
		out.NumAnchorCorrect = append(out.NumAnchorCorrect, sc.AnchorCorrect)
		out.NumGapCorrect = append(out.NumGapCorrect, sc.GapCorrect)
		out.NumMisattached = append(out.NumMisattached, sc.Misattached)
		out.NumUnanchored = append(out.NumUnanchored, sc.Unanchored)
		out.NumSynthetic = append(out.NumSynthetic, sc.Synthetic)
		out.NumBorrowed = append(out.NumBorrowed, sc.Borrowed)
		agg.Spans += sc.Spans
		agg.Dropped += sc.Dropped
		agg.Orphans += sc.Orphans
		agg.Reconnected += sc.Reconnected
		agg.AnchorCorrect += sc.AnchorCorrect
		agg.GapCorrect += sc.GapCorrect
		agg.Misattached += sc.Misattached
		agg.Unanchored += sc.Unanchored
		agg.Synthetic += sc.Synthetic
		agg.Borrowed += sc.Borrowed
	}

	f, err := os.Create(c.outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output: %v\n", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "write output: %v\n", err)
		os.Exit(1)
	}
	f.Close()

	pct := func(n, d int) float64 {
		if d == 0 {
			return 0
		}
		return 100 * float64(n) / float64(d)
	}
	fmt.Fprintf(os.Stderr, "traces=%d spans=%d dropped=%d orphans=%d\n",
		len(traceOrder), agg.Spans, agg.Dropped, agg.Orphans)
	fmt.Fprintf(os.Stderr, "reconnected=%d (%.2f%%) anchor_correct=%d (%.2f%%) gap_correct=%d (%.2f%%)\n",
		agg.Reconnected, pct(agg.Reconnected, agg.Orphans),
		agg.AnchorCorrect, pct(agg.AnchorCorrect, agg.Orphans),
		agg.GapCorrect, pct(agg.GapCorrect, agg.Orphans))
	fmt.Fprintf(os.Stderr, "misattached=%d unanchored=%d synthetic=%d borrowed_bloom=%d\n",
		agg.Misattached, agg.Unanchored, agg.Synthetic, agg.Borrowed)
	fmt.Fprintf(os.Stderr, "ambiguous=%d (of which misattached=%d); non-ambiguous misattached=%d\n",
		agg.Ambiguous, agg.AmbiguousBad, agg.Misattached-agg.AmbiguousBad)
	fmt.Fprintf(os.Stderr, "Wrote %d traces to %s\n", len(traceOrder), c.outputPath)
}

func runFromJSON(c config, ha *harness) []uint64 {
	t0 := time.Now()
	traces, _, err := loadTracesFromDir(c.inputDir, c.traceCount, c.requireClean)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading traces: %v\n", err)
		os.Exit(1)
	}
	if len(traces) == 0 {
		fmt.Fprintln(os.Stderr, "No traces loaded.")
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Loaded %d traces in %s\n", len(traces), time.Since(t0).Round(time.Millisecond))

	traceOrder := make([]uint64, len(traces))
	for i, t := range traces {
		traceOrder[i] = t.TraceID
		ha.beginTrace(t.TraceID, len(t.Spans))
	}

	// Same global event sort as trace_sim (ts, kind, depth-rank, tid, sid).
	type ev struct {
		ts        int64
		kind      bridge.Kind
		depth     int
		tid, sid  uint64
		pid       uint64
		serviceID uint16
	}
	var events []ev
	for _, t := range traces {
		for _, s := range t.Spans {
			events = append(events, ev{s.StartNS, bridge.KindStart, s.Depth, t.TraceID, s.SpanID, s.ParentID, s.ServiceID})
			events = append(events, ev{s.EndNS, bridge.KindEnd, s.Depth, t.TraceID, s.SpanID, s.ParentID, s.ServiceID})
		}
	}
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		var ar, br int
		if a.kind == bridge.KindStart {
			ar, br = a.depth, b.depth
		} else {
			ar, br = -a.depth, -b.depth
		}
		if ar != br {
			return ar < br
		}
		if a.tid != b.tid {
			return a.tid < b.tid
		}
		return a.sid < b.sid
	})

	for _, e := range events {
		ha.onEvent(e.ts, e.kind, e.tid, e.sid, e.pid, e.serviceID)
	}
	return traceOrder
}

func runFromCorpus(c config, ha *harness) []uint64 {
	eventsPath, metaPath := corpus.Paths(c.corpusDir)
	meta, err := corpus.ReadMeta(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading meta: %v\n", err)
		os.Exit(1)
	}
	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening events: %v\n", err)
		os.Exit(1)
	}
	defer er.Close()
	traceOrder := append([]uint64(nil), meta.TraceOrder...)
	spanCounts := meta.SpanCounts
	if c.traceCount > 0 && c.traceCount < len(traceOrder) {
		traceOrder = traceOrder[:c.traceCount]
		spanCounts = spanCounts[:c.traceCount]
	}
	fmt.Fprintf(os.Stderr, "Opened corpus (%d traces, processing %d)\n",
		len(meta.TraceOrder), len(traceOrder))

	selected := make(map[uint64]struct{}, len(traceOrder))
	for i, tid := range traceOrder {
		selected[tid] = struct{}{}
		ha.beginTrace(tid, int(spanCounts[i]))
	}

	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "corpus read error: %v\n", err)
			os.Exit(1)
		}
		// Skip events of unselected traces: the handler never sees them, so
		// the selected traces' simulation matches a corpus containing only
		// the selection (PB state is strictly per-trace).
		if _, ok := selected[ce.TraceID]; !ok {
			continue
		}
		ha.onEvent(ce.TS, bridge.Kind(ce.Kind), ce.TraceID, ce.SpanID, ce.ParentID, ce.ServiceID)
	}
	return traceOrder
}

// loadTracesFromDir mirrors cmd/trace_sim's loader: all *.json files in dir,
// alphabetical order.
func loadTracesFromDir(dir string, traceCount int, requireClean bool) ([]loader.Trace, *loader.ServiceTable, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > 5 && name[len(name)-5:] == ".json" {
			files = append(files, name)
		}
	}
	sort.Strings(files)

	services := loader.NewServiceTable()
	out := make([]loader.Trace, 0, len(files))
	for _, name := range files {
		ts, err := loader.LoadTraceFile(filepath.Join(dir, name), services, requireClean)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", name, err)
			continue
		}
		out = append(out, ts...)
		if traceCount > 0 && len(out) >= traceCount {
			out = out[:traceCount]
			break
		}
	}
	return out, services, nil
}
