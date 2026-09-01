// Command pb_jaeger_recon reconstructs real bridge-augmented Jaeger traces (the
// DeathStarBench social-network corpus, cpd6, no-prime) and reports the path /
// call-graph reconstruction error rate per drop rate.
//
// The bridge payloads are already on the wire (span tag _br = [1B depth][8B
// anchor/checkpoint-prefix][12B bloom], m=96 k=14, prehashed span-id hashing),
// so we do NOT re-emit them: we parse each trace's true structure, drop spans
// per-trace-seeded across every rate, and hand the survivors (with their carried
// blooms) to the reconstructor in Prehashed mode.
//
//	--mode pb2  : path bridge     -> ReconstructPB2 + ScorePB2Path (connectivity)
//	--mode cgp2 : call-graph brdg -> ReconstructCGP2 + ScoreCGP2Strict (conn+topo)
//
// The CGPB app did NOT actually emit the HA fan-out array, so in cgp2 mode we
// SYNTHESIZE it from ground truth, replicating bridge/cgprb.go's rule exactly
// (only a parent's 2nd-started child appends (parent,childDepth); the entry
// flows down first-child edges only; checkpoints reset), and attach it to the
// _br-carrying spans — faking what the app should have appended.
//
// A trace is clean iff the canonical score is Clean(); correctExclEmpty =
// clean/feasible.
//
// Build (needs the CP-SAT solver): go build -tags cpsat -o pb_jaeger_recon ./cmd/pb_jaeger_recon
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"bridges/recon"
)

var (
	dbg  = os.Getenv("PBDEBUG") != ""
	dbgN atomic.Int64
)

type jTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type jRef struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
}
type jSpan struct {
	SpanID     string `json:"spanID"`
	StartTime  int64  `json:"startTime"`
	References []jRef `json:"references"`
	Tags       []jTag `json:"tags"`
}
type jTrace struct {
	TraceID string  `json:"traceID"`
	Spans   []jSpan `json:"spans"`
}

func spanU64(hexID string) (uint64, bool) {
	if len(hexID) == 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(hexID, 16, 64)
	return v, err == nil
}

// acc holds one drop rate's tally, matching the sweep's cg2 counters.
type acc struct {
	nt, empty, feas, clean int
	realNodes, edgeExact   int
}

func (a *acc) add(b *acc) {
	a.nt += b.nt
	a.empty += b.empty
	a.feas += b.feas
	a.clean += b.clean
	a.realNodes += b.realNodes
	a.edgeExact += b.edgeExact
}

type spanRec struct {
	id, parent    uint64
	start         int64
	bloom, prefix []byte
	ha            []recon.HAEntry // synthesized (cgp2 mode), attached to _br carriers
}

func processTrace(t *jTrace, rates []float64, cfg recon.Config, cgp bool, accs []acc) {
	n := len(t.Spans)
	if n == 0 {
		return
	}
	idOf := make(map[string]uint64, n)
	present := make(map[uint64]bool, n)
	for _, s := range t.Spans {
		if u, ok := spanU64(s.SpanID); ok {
			idOf[s.SpanID] = u
			present[u] = true
		}
	}
	spans := make([]spanRec, 0, n)
	parent := make(map[uint64]uint64, n)
	for _, s := range t.Spans {
		id, ok := idOf[s.SpanID]
		if !ok {
			continue
		}
		var pid uint64
		for _, r := range s.References {
			if r.RefType == "CHILD_OF" {
				if pu, ok := idOf[r.SpanID]; ok {
					pid = pu
				}
				break
			}
		}
		var bloom, prefix []byte
		for _, tg := range s.Tags {
			if tg.Key == "_br" {
				if raw, err := base64.StdEncoding.DecodeString(tg.Value); err == nil && len(raw) == 21 {
					prefix = raw[1:9] // [1B depth][8B anchor/prefix][12B bloom]
					bloom = raw[9:21]
				}
				break
			}
		}
		spans = append(spans, spanRec{id: id, parent: pid, start: s.StartTime, bloom: bloom, prefix: prefix})
		parent[id] = pid
	}
	// tree depth (root=0), memoized.
	depth := make(map[uint64]int, len(spans))
	var depthOf func(uint64) int
	depthOf = func(id uint64) int {
		if d, ok := depth[id]; ok {
			return d
		}
		depth[id] = 0 // cycle guard
		p := parent[id]
		d := 0
		if p != 0 && present[p] {
			d = 1 + depthOf(p)
		}
		depth[id] = d
		return d
	}
	for _, s := range spans {
		depthOf(s.id)
	}
	idx := make(map[uint64]int, len(spans))
	for i, s := range spans {
		idx[s.id] = i
	}

	// --- CGP2: synthesize the HA fan-out array (bridge/cgprb.go semantics) ---
	if cgp {
		// sibling start-rank (1-based): order children by (startTime, id).
		kids := make(map[uint64][]int)
		for i, s := range spans {
			if s.parent != 0 && present[s.parent] {
				kids[s.parent] = append(kids[s.parent], i)
			}
		}
		seq := make([]int, len(spans)) // parentSeqNum; 0 for roots
		for _, ks := range kids {
			sort.Slice(ks, func(a, b int) bool {
				if spans[ks[a]].start != spans[ks[b]].start {
					return spans[ks[a]].start < spans[ks[b]].start
				}
				return spans[ks[a]].id < spans[ks[b]].id
			})
			for r, ci := range ks {
				seq[ci] = r + 1
			}
		}
		// process parents before children; propHA is what a span passes to its 1st child.
		order := make([]int, len(spans))
		for i := range order {
			order[i] = i
		}
		sort.Slice(order, func(a, b int) bool { return depth[spans[order[a]].id] < depth[spans[order[b]].id] })
		myHA := make(map[uint64][]recon.HAEntry, len(spans))
		propHA := make(map[uint64][]recon.HAEntry, len(spans))
		for _, i := range order {
			s := spans[i]
			d := depth[s.id]
			var h []recon.HAEntry
			switch seq[i] {
			case 1:
				h = propHA[s.parent] // first child inherits the spine's HA
			case 2:
				h = []recon.HAEntry{{ParentID: s.parent, Depth: d}} // 2nd child records the branch
			} // 3rd+ children and roots: none
			myHA[s.id] = h
			if d%cfg.CPD == 0 {
				propHA[s.id] = nil // checkpoint resets
			} else {
				propHA[s.id] = h
			}
		}
		// attach to the _br carriers (the spans that emit HA on the wire).
		for i := range spans {
			if spans[i].bloom != nil {
				spans[i].ha = myHA[spans[i].id]
			}
		}
	}

	// truth (parent only needed by the scorers).
	truth := make([]recon.TruthSpan, len(spans))
	for i, s := range spans {
		truth[i] = recon.TruthSpan{SpanID: s.id, ParentID: s.parent, Depth: depth[s.id]}
	}
	// per-trace-seeded drop draw: one uniform per non-root span (nested rates).
	h := fnv.New64a()
	h.Write([]byte(t.TraceID))
	rng := rand.New(rand.NewSource(int64(h.Sum64())))
	ord := make([]int, len(spans))
	for i := range ord {
		ord[i] = i
	}
	sort.Slice(ord, func(a, b int) bool { return spans[ord[a]].id < spans[ord[b]].id })
	u := make([]float64, len(spans))
	for _, i := range ord {
		if depth[spans[i].id] == 0 {
			u[i] = 2 // root never drops
		} else {
			u[i] = rng.Float64()
		}
	}
	for ri, rate := range rates {
		dropped := make(map[uint64]struct{})
		for i, s := range spans {
			if u[i] < rate {
				dropped[s.id] = struct{}{}
			}
		}
		survivors := make([]recon.Span, 0, len(spans))
		for i, s := range spans {
			if u[i] < rate {
				continue
			}
			d := depth[s.id]
			survivors = append(survivors, recon.Span{
				SpanID:      s.id,
				ParentID:    s.parent,
				Depth:       d,
				BloomBits:   s.bloom,
				CkptPrefix:  s.prefix,
				HA:          s.ha,
				LeafCarrier: s.bloom != nil && d%cfg.CPD != 0,
			})
		}
		var res recon.Result
		var iso recon.CGP2Iso
		if cgp {
			res = recon.ReconstructCGP2(survivors, cfg)
		} else {
			res = recon.ReconstructPB2(survivors, cfg)
		}
		a := &accs[ri]
		a.nt++
		if res.Reconnected == 0 {
			a.empty++
			continue
		}
		a.feas++
		if cgp {
			iso = recon.ScoreCGP2Strict(res, survivors, truth, dropped)
		} else {
			iso = recon.ScorePBPathStrict(res, survivors, truth, dropped)
		}
		a.realNodes += iso.RealNodes
		a.edgeExact += iso.EdgeExact
		if iso.Clean() {
			a.clean++
		}
		if dbg && rate == 0.5 && dbgN.Add(1) <= 6 {
			fmt.Fprintf(os.Stderr, "[dbg] spans=%d drop=%d surv=%d | Reconnected=%d bridges=%d edgeWrong=%d\n",
				len(spans), len(dropped), len(survivors), res.Reconnected, len(res.Bridges), iso.EdgeWrong)
		}
	}
}

func main() {
	tracesPath := flag.String("traces", "/users/tomislav/pb_traces_cpd6/pb/traces_pb.json", "Jaeger traces JSON")
	mode := flag.String("mode", "pb2", "pb2 (path) or cgp2 (call-graph)")
	cpd := flag.Int("cpd", 6, "checkpoint distance")
	bm := flag.Uint("bloom-m", 96, "bloom bits m")
	bk := flag.Uint("bloom-k", 14, "bloom hash count k")
	ratesS := flag.String("drop-rates", "0.05,0.25,0.5,0.75,0.95,1.0", "comma-separated drop rates")
	workers := flag.Int("workers", runtime.NumCPU(), "worker goroutines")
	prefixLen := flag.Int("prefix-len", 8, "checkpoint-prefix length (bytes) in _br")
	sample := flag.Int("sample", 0, "process only the first N traces (0 = all)")
	flag.Parse()
	cgp := *mode == "cgp2"
	if *mode != "pb2" && *mode != "cgp2" {
		fmt.Fprintln(os.Stderr, "mode must be pb2 or cgp2")
		os.Exit(1)
	}

	var rates []float64
	for _, s := range strings.Split(*ratesS, ",") {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad rate:", s)
			os.Exit(1)
		}
		rates = append(rates, f)
	}
	cfg := recon.Config{CPD: *cpd, BloomM: uint32(*bm), BloomK: uint32(*bk), PrefixLen: *prefixLen, Prehashed: true}

	f, err := os.Open(*tracesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for { // advance to the value of "data" (an array)
		tok, err := dec.Token()
		if err != nil {
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
		if s, ok := tok.(string); ok && s == "data" {
			break
		}
	}
	if _, err := dec.Token(); err != nil { // consume '['
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	jobs := make(chan *jTrace, 256)
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := make([]acc, len(rates))
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]acc, len(rates))
			for t := range jobs {
				processTrace(t, rates, cfg, cgp, local)
			}
			mu.Lock()
			for i := range local {
				total[i].add(&local[i])
			}
			mu.Unlock()
		}()
	}
	var seen int
	for dec.More() {
		var t jTrace
		if err := dec.Decode(&t); err != nil {
			fmt.Fprintln(os.Stderr, "decode:", err)
			break
		}
		jobs <- &t
		seen++
		if *sample > 0 && seen >= *sample {
			break
		}
	}
	close(jobs)
	wg.Wait()

	fmt.Printf("%s reconstruction on real Jaeger traces (cpd=%d, no-prime, m=%d k=%d, prehashed%s)\n",
		strings.ToUpper(*mode), *cpd, *bm, *bk, map[bool]string{true: ", synthesized HA"}[cgp])
	fmt.Printf("traces processed: %d\n\n", seen)
	fmt.Printf("%-6s %10s %9s %8s %14s %12s\n", "drop", "traces", "feasible", "empty", "correctExcl%", "edgeExact%")
	for i, r := range rates {
		a := total[i]
		ex := 100.0
		if a.feas > 0 {
			ex = 100 * float64(a.clean) / float64(a.feas)
		}
		ee := 100.0
		if a.realNodes > 0 {
			ee = 100 * float64(a.edgeExact) / float64(a.realNodes)
		}
		fmt.Printf("%-6.2f %10d %9d %8d %13.2f%% %11.2f%%\n", r, a.nt, a.feas, a.empty, ex, ee)
	}
}
