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
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"bridges/corpus"
)

type span struct {
	id, parent uint64
	svc        uint16
	start, end int64
	depth      int
}

type config struct {
	n           int
	shape       string
	depth       int
	fanout      int
	fanoutDist  string
	fanoutS     float64
	fanoutMin   int
	fanoutMax   int
	slabMeanW     float64 // estimated mean per-node fan-out (floored at fanoutMin), for slab budget sizing
	spindle       bool
	spindlePeriod int
	depthDist   string
	depthS      float64
	maxSpans    int
	concurrency float64
	services    int
	baseDurUS   int64
	seed        int64
	out         string
	formats     string
}

func main() {
	var c config
	flag.IntVar(&c.n, "n", 1000, "number of traces to generate")
	flag.StringVar(&c.shape, "shape", "kary", "tree shape: chain | star | kary | skewed | branch | spine | deepwide | slab. branch = Galton-Watson with --fanout-dist; spine = ONE deep path (Kesten) + bushes; deepwide = K=--fanout parallel deep paths + bushes; slab = STRESS: per-node fan-out in [--fanout-min,--fanout-max] to absolute --depth, continuation-capped to --max-spans")
	flag.IntVar(&c.depth, "depth", 4, "max tree depth (chain/kary); mean/cap for --depth-dist")
	flag.IntVar(&c.fanout, "fanout", 3, "children per node (star/kary); mean for poisson/geometric")
	flag.StringVar(&c.fanoutDist, "fanout-dist", "fixed", "offspring law for --shape branch/spine: fixed | zipf | poisson | geometric | uber (empirical Uber day1 law: chain-dominated + power-law branch tail)")
	flag.Float64Var(&c.fanoutS, "fanout-s", 1.2, "zipf exponent for --fanout-dist zipf (>1; lower = heavier tail)")
	flag.IntVar(&c.fanoutMin, "fanout-min", 1, "--shape slab: minimum per-node fan-out (0 allows lineages to die early)")
	flag.BoolVar(&c.spindle, "spindle", false, "--shape slab: taper the continuation-cap into spindle lobes (width rises to a peak, tapers to a thin neck, then RE-SPINDLES) — realistic spindle texture at any depth, instead of a flat brick")
	flag.IntVar(&c.spindlePeriod, "spindle-period", 0, "--shape slab --spindle: depth per spindle lobe before re-spindling (0 = one spindle over the whole --depth; smaller = more lobes stacked to reach deep)")
	flag.IntVar(&c.fanoutMax, "fanout-max", 256, "cap on sampled fan-out per node (--shape slab: also the max of the per-node fan-out range)")
	flag.StringVar(&c.depthDist, "depth-dist", "fixed", "per-trace max-depth law: fixed | zipf (mostly shallow, rare deep, capped at --depth)")
	flag.Float64Var(&c.depthS, "depth-s", 1.5, "zipf exponent for --depth-dist zipf (>1)")
	flag.IntVar(&c.maxSpans, "max-spans", 20000, "hard cap on spans per trace (kary/skewed can explode)")
	flag.Float64Var(&c.concurrency, "concurrency", 0.0, "0 = siblings strictly sequential, 1 = fully overlapping")
	flag.IntVar(&c.services, "services", 16, "number of distinct service names")
	flag.Int64Var(&c.baseDurUS, "base-dur-us", 1_000_000, "root span duration in MICROSECONDS (children subdivide it; timestamps are µs-granular, matching Jaeger)")
	flag.Int64Var(&c.seed, "seed", 1, "RNG seed (deterministic output)")
	flag.StringVar(&c.out, "out", "", "output directory (required)")
	flag.StringVar(&c.formats, "formats", "corpus,store", "comma list: corpus,store,jaeger,otel")
	flag.Parse()
	if c.out == "" {
		fmt.Fprintln(os.Stderr, "error: --out required")
		os.Exit(2)
	}
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	want := map[string]bool{}
	for _, f := range strings.Split(c.formats, ",") {
		want[strings.TrimSpace(f)] = true
	}

	rng := rand.New(rand.NewSource(c.seed))
	svcNames := make([]string, c.services)
	for i := range svcNames {
		svcNames[i] = fmt.Sprintf("svc%d", i)
	}

	fanoutOf := makeFanoutSampler(&c, rng)
	depthOf := makeDepthSampler(&c, rng)
	// Estimate the mean per-node fan-out (floored at fanout-min) so the slab
	// continuation-cap can size traces to the span budget regardless of which
	// --fanout-dist (heavy-tailed zipf/geometric/uber) feeds it. Uses a throwaway
	// rng so the generation stream is unperturbed.
	{
		er := makeFanoutSampler(&c, rand.New(rand.NewSource(c.seed^0x9e3779b9)))
		sum := 0
		for i := 0; i < 8192; i++ {
			w := er()
			if w < c.fanoutMin {
				w = c.fanoutMin
			}
			sum += w
		}
		c.slabMeanW = float64(sum) / 8192.0
		if c.slabMeanW < 1 {
			c.slabMeanW = 1
		}
	}
	traceIDs := make([]uint64, c.n)
	traces := make([][]span, c.n)
	for i := 0; i < c.n; i++ {
		tid := rng.Uint64()
		traceIDs[i] = tid
		traces[i] = genTrace(&c, rng, fanoutOf, depthOf)
	}

	if want["corpus"] {
		if err := emitCorpus(c.out, traceIDs, traces, svcNames); err != nil {
			fail("corpus", err)
		}
	}
	if want["store"] {
		if err := emitStore(c.out, traceIDs, traces); err != nil {
			fail("store", err)
		}
	}
	if want["jaeger"] {
		if err := emitJaeger(c.out, traceIDs, traces, svcNames); err != nil {
			fail("jaeger", err)
		}
	}
	if want["otel"] {
		if err := emitOTel(c.out, traceIDs, traces, svcNames); err != nil {
			fail("otel", err)
		}
	}
	total := 0
	depths := make([]int, len(traces))
	for i, t := range traces {
		total += len(t)
		md := 0
		for _, s := range t {
			if s.depth > md {
				md = s.depth
			}
		}
		depths[i] = md
	}
	sort.Ints(depths)
	dp := func(p float64) int { return depths[int(p*float64(len(depths)-1))] }
	fmt.Fprintf(os.Stderr, "generated %d traces, %d spans (%s) -> %s [%s]\n",
		c.n, total, c.shape, c.out, c.formats)
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
			sumDepth += s.depth
			nSpans++
			dep[s.id] = s.depth
			if s.parent != 0 {
				kids[s.parent]++
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

// genTrace builds one trace: a tree of the requested shape with valid contained
// timestamps and random (uniform-fingerprint) span ids. Per-trace max depth is
// drawn from depthOf; offspring counts (for --shape branch) from fanoutOf.
func genTrace(c *config, rng *rand.Rand, fanoutOf, depthOf func() int) []span {
	maxDepth := depthOf()
	spans := []span{{id: rng.Uint64(), svc: uint16(rng.Intn(c.services)), depth: 0}}
	newChild := func(pid uint64, depth int) int {
		spans = append(spans, span{id: rng.Uint64(), parent: pid, svc: uint16(rng.Intn(c.services)), depth: depth})
		return len(spans) - 1
	}
	// ordinary (unconditioned) branching subtree rooted at idx, adding spans until
	// len(spans) reaches cap — naturally terminating per the fan-out law.
	grow := func(idx, depth, cap int) {
		type frame struct{ idx, depth int }
		q := []frame{{idx, depth}}
		for len(q) > 0 && len(spans) < cap {
			f := q[0]
			q = q[1:]
			if f.depth >= maxDepth {
				continue
			}
			k := offspring(c, f.depth, rng, fanoutOf)
			for j := 0; j < k && len(spans) < cap; j++ {
				q = append(q, frame{newChild(spans[f.idx].id, f.depth+1), f.depth + 1})
			}
		}
	}
	if c.shape == "spine" {
		// Branching process CONDITIONED to reach maxDepth (Kesten spine
		// decomposition). Build the backbone FIRST (guarantees depth), THEN hang
		// ordinary realistic subtrees off each backbone node under a per-node span
		// budget. Realistic per-node fan-out, arbitrary depth, bounded width — even
		// when the fan-out law is supercritical (the budget caps the bushiness
		// instead of letting a near-root subtree starve the backbone).
		prev := 0
		backbone := []int{0}
		for d := 1; d <= maxDepth && len(spans) < c.maxSpans; d++ {
			prev = newChild(spans[prev].id, d)
			backbone = append(backbone, prev)
		}
		perNode := (c.maxSpans - len(spans)) / len(backbone)
		for _, bi := range backbone {
			cap := len(spans) + perNode
			if cap > c.maxSpans {
				cap = c.maxSpans
			}
			for j := 1; j < fanoutOf() && len(spans) < cap; j++ {
				d := spans[bi].depth + 1
				grow(newChild(spans[bi].id, d), d, cap)
			}
		}
	} else if c.shape == "deepwide" {
		// K PARALLEL deep paths (generalized Kesten spine) + realistic bushes. Real
		// Uber traces are wide AND deep — ~38 leaves reach the deep frontier — which
		// the single spine (1 deep path) can't represent. Here K = --fanout sets the
		// number of deep paths and --depth their length, so depth x width are two
		// independent sweep knobs (the 2D flame-plot axes). Bushes off the backbone
		// nodes follow the offspring law (use --fanout-dist uber) and supply the
		// many shallow leaves real traces also have.
		K := c.fanout
		if K < 1 {
			K = 1
		}
		backbone := []int{0}
		for k := 0; k < K && len(spans) < c.maxSpans; k++ {
			prev := 0 // each deep path is a fresh chain off the root
			for d := 1; d <= maxDepth && len(spans) < c.maxSpans; d++ {
				prev = newChild(spans[prev].id, d)
				backbone = append(backbone, prev)
			}
		}
		perNode := 0
		if len(backbone) > 0 {
			perNode = (c.maxSpans - len(spans)) / len(backbone)
		}
		for _, bi := range backbone {
			cap := len(spans) + perNode
			if cap > c.maxSpans {
				cap = c.maxSpans
			}
			for j := 1; j < fanoutOf() && len(spans) < cap; j++ {
				d := spans[bi].depth + 1
				grow(newChild(spans[bi].id, d), d, cap)
			}
		}
	} else if c.shape == "slab" {
		// STRESS instrument: 2 orthogonal dials — per-node fan-out and absolute
		// --depth. Each node's fan-out is drawn from the --fanout-dist sampler
		// (zipf/geometric/uber — heavy-tailed: mostly small chains, occasionally
		// huge) floored at --fanout-min and capped at --fanout-max, giving genuine
		// (and occasionally very wide) windows -> cgprb O(W^2). Grown to the absolute
		// depth. width^depth would explode, so the CONTINUATION frontier is capped at
		// C = maxSpans/(depth*meanWidth): every node still gets its full fan-out (all
		// those children are real spans / real wide windows), but only up to C per
		// level spawn the next level — the rest are real leaves. Fan-out and depth
		// stay honest; C only sizes how many lineages run full-depth, so spans ~=
		// maxSpans (the size dial). fanout-min=0 lets lineages die early (smaller,
		// variable-size traces); >=1 guarantees reaching depth.
		// Per-depth continuation-cap caps[d] = how many depth-d nodes spawn depth d+1.
		// Flat -> brick (constant width). Spindle -> right-skewed arch f(d)=d*(D-d)^7
		// (peak ~ D/8, thin deep tail), scaled so sum(caps)*meanW ~= maxSpans, i.e.
		// the width profile rises then tapers like a real trace. Either way each node
		// still draws its full heavy-tailed fan-out (fat windows), only the count that
		// CONTINUES is shaped.
		caps := make([]int, maxDepth+1)
		budgetNodes := float64(c.maxSpans) / c.slabMeanW
		if c.spindle {
			// Periodic spindle: within each lobe of L levels the cap follows an arch
			// x*(L-x)^3 (rise -> peak -> taper), then re-spindles. The neck (phase 0)
			// is floored to 1 so the lineage never dies — it pinches and re-widens,
			// stacking lobes to reach any --depth. L = --spindle-period (0 = one lobe
			// over the whole depth). Scaled so sum(caps)*meanW ~= maxSpans.
			L := c.spindlePeriod
			if L < 2 || L > maxDepth {
				L = maxDepth
			}
			Lf := float64(L)
			f := make([]float64, maxDepth+1)
			fsum := 0.0
			for d := 1; d <= maxDepth; d++ {
				x := float64(d % L) // phase within the current lobe
				p := Lf - x
				f[d] = x * p * p * p // x*(L-x)^3 arch, repeating every L
				fsum += f[d]
			}
			for d := 1; d <= maxDepth && fsum > 0; d++ {
				caps[d] = int(budgetNodes * f[d] / fsum)
				if caps[d] < 1 {
					caps[d] = 1 // neck floor: pinch, don't die -> re-spindle
				}
			}
		} else {
			flat := int(budgetNodes / float64(maxDepth))
			if flat < 1 {
				flat = 1
			}
			for d := range caps {
				caps[d] = flat
			}
		}
		frontier := []int{0}
		for d := 1; d <= maxDepth && len(frontier) > 0 && len(spans) < c.maxSpans; d++ {
			var next []int
			for _, fi := range frontier {
				if len(spans) >= c.maxSpans {
					break
				}
				// per-node fan-out from the (heavy-tailed) --fanout-dist sampler,
				// floored at --fanout-min: mostly small (chains), occasionally huge.
				w := fanoutOf()
				if w < c.fanoutMin {
					w = c.fanoutMin
				}
				for j := 0; j < w && len(spans) < c.maxSpans; j++ {
					ch := newChild(spans[fi].id, d) // full fan-out: real span, real wide window
					if len(next) < caps[d] {
						next = append(next, ch) // only caps[d] per level continue; rest are leaves
					}
				}
			}
			frontier = next
		}
	} else {
		grow(0, 0, c.maxSpans)
	}

	kids := map[uint64][]int{}
	for i := range spans {
		if spans[i].parent != 0 {
			kids[spans[i].parent] = append(kids[spans[i].parent], i)
		}
	}
	assignTimes(spans, kids, c.concurrency)
	return spans
}

// offspring returns the child count for a node at the given depth, per shape.
func offspring(c *config, depth int, rng *rand.Rand, fanoutOf func() int) int {
	switch c.shape {
	case "chain":
		return 1
	case "star":
		if depth == 0 {
			return c.fanout
		}
		return 0
	case "kary":
		return c.fanout
	case "skewed": // legacy crude heavy-tail; prefer `branch --fanout-dist zipf`
		if rng.Float64() < 0.15 {
			return 1 + rng.Intn(2*c.fanout+1)
		}
		return rng.Intn(c.fanout + 1)
	case "branch", "spine": // spine's off-backbone subtrees branch by the real law
		return fanoutOf()
	}
	return c.fanout
}

// makeFanoutSampler returns the offspring-count sampler for --shape branch.
// zipf is the heavy-tail workhorse (P(k) ∝ 1/(k+1)^s, k∈[0,max], 0 = leaf).
func makeFanoutSampler(c *config, rng *rand.Rand) func() int {
	clamp := func(k int) int {
		if k < 0 {
			return 0
		}
		if k > c.fanoutMax {
			return c.fanoutMax
		}
		return k
	}
	switch c.fanoutDist {
	case "zipf":
		s := c.fanoutS
		if s <= 1 {
			fmt.Fprintf(os.Stderr, "warn: --fanout-s must be >1, clamping to 1.001\n")
			s = 1.001
		}
		max := c.fanoutMax
		if max < 1 {
			max = 1
		}
		z := rand.NewZipf(rng, s, 1.0, uint64(max))
		return func() int { return int(z.Uint64()) }
	case "poisson":
		return func() int { return clamp(poisson(rng, float64(c.fanout))) }
	case "geometric":
		p := 1.0 / (float64(c.fanout) + 1.0) // mean fan-out = c.fanout
		return func() int { return clamp(geometric(rng, p)) }
	case "uber":
		// Empirical offspring law from the unfiltered Uber day1 scan (50k traces,
		// 46M nodes): chain-dominated with a power-law branch tail.
		//   P(0=leaf)=39.2%  P(1=chain)=54.2%  P(>=2=branch)=6.6%
		// Branch counts (k>=2) are heavy-tailed (measured mean~6.9, p50=3, p90=11,
		// p99=82, max in the thousands) — modeled as 2 + Zipf(uberBranchS), capped
		// at --fanout-max. NOT Poisson: real branching has a power-law tail, not a
		// bell. uberBranchS is calibrated to reproduce the measured branch pctls.
		const (
			pLeaf       = 0.392
			pChainEnd   = 0.934 // pLeaf + 0.542
			uberBranchS = 1.8 // calibrated at fanout-max~256 to branch-mean~6.7 AND total offspring mean~1.0 (critical, like real)
		)
		zmax := c.fanoutMax - 2
		if zmax < 1 {
			zmax = 1
		}
		z := rand.NewZipf(rng, uberBranchS, 1.0, uint64(zmax))
		return func() int {
			u := rng.Float64()
			switch {
			case u < pLeaf:
				return 0
			case u < pChainEnd:
				return 1
			default:
				return 2 + int(z.Uint64())
			}
		}
	default: // fixed
		return func() int { return c.fanout }
	}
}

// makeDepthSampler returns the per-trace max-depth sampler. zipf gives
// mostly-shallow, rare-deep traces capped at --depth (depth = 1 + Zipf, so ≥1).
func makeDepthSampler(c *config, rng *rand.Rand) func() int {
	switch c.depthDist {
	case "zipf":
		s := c.depthS
		if s <= 1 {
			fmt.Fprintf(os.Stderr, "warn: --depth-s must be >1, clamping to 1.001\n")
			s = 1.001
		}
		imax := c.depth - 1
		if imax < 1 {
			imax = 1
		}
		z := rand.NewZipf(rng, s, 1.0, uint64(imax))
		return func() int { return 1 + int(z.Uint64()) }
	default: // fixed
		return func() int { return c.depth }
	}
}

// poisson samples Poisson(lambda) via Knuth (fine for the small means here).
func poisson(rng *rand.Rand, lambda float64) int {
	L := math.Exp(-lambda)
	k, p := 0, 1.0
	for {
		k++
		p *= rng.Float64()
		if p <= L {
			return k - 1
		}
	}
}

// geometric samples #failures before the first success (mean (1-p)/p).
func geometric(rng *rand.Rand, p float64) int {
	if p >= 1 {
		return 0
	}
	return int(math.Floor(math.Log(1-rng.Float64()) / math.Log(1-p)))
}

// assignTimes assigns timestamps by DFS pre/post-order tick numbering, in µs:
// each node gets start=enter-tick, end=exit-tick, so every child is STRICTLY
// contained in its parent and siblings are sequential — for ANY shape, depth, or
// fan-out, with no window to collapse. (The earlier interval-subdivision model
// was unsound: a node whose time window was smaller than ~2×fan-out placed
// children OUTSIDE their parent, yielding a non-containment tree with no
// well-defined order. This is valid by construction.)
//
// Concurrency is supported and validity-preserving: each sibling keeps its
// need-sized block (so its subtree always fits), but its START is slid toward
// the parent's open by (1-conc). conc=0 tiles siblings sequentially (end-order =
// start-order); conc=1 starts them all together so their ENDS spread by subtree
// size (end-order ≠ start-order — the reordering case EE/DEE must recover).
// Sliding a start only EARLIER never pushes its end past the parent, so every
// child stays strictly contained for all conc∈[0,1] (overlapping cousins just
// share ticks, which the corpus total-order tie-breaks).
func assignTimes(spans []span, kids map[uint64][]int, conc float64) {
	need := map[uint64]int64{} // ticks the subtree consumes (2 + children's)
	var computeNeed func(i int) int64
	computeNeed = func(i int) int64 {
		n := int64(2)
		for _, ci := range kids[spans[i].id] {
			n += computeNeed(ci)
		}
		need[spans[i].id] = n
		return n
	}
	computeNeed(0)

	var place func(i int, lo int64)
	place = func(i int, lo int64) {
		id := spans[i].id
		spans[i].start = lo
		spans[i].end = lo + need[id] - 1 // exit tick: strictly after every descendant
		off := int64(1)                  // sequential cursor within the parent
		for _, ci := range kids[id] {
			seqStart := lo + off
			start := lo + 1 + int64(float64(seqStart-(lo+1))*(1-conc)) // slide toward lo+1
			place(ci, start)
			off += need[spans[ci].id]
		}
	}
	place(0, 0)
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

func traceEvents(tid uint64, spans []span) []ev {
	out := make([]ev, 0, 2*len(spans))
	for _, s := range spans {
		out = append(out,
			ev{s.start, corpus.KindStart, s.depth, tid, s.id, s.parent, s.svc},
			ev{s.end, corpus.KindEnd, s.depth, tid, s.id, s.parent, s.svc})
	}
	return out
}

// ---- emitters ----

func emitCorpus(out string, tids []uint64, traces [][]span, svc []string) error {
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

func emitStore(out string, tids []uint64, traces [][]span) error {
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
func emitJaeger(out string, tids []uint64, traces [][]span, svc []string) error {
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
			pid := fmt.Sprintf("p%d", s.svc)
			jt.Processes[pid] = proc{ServiceName: svc[s.svc]}
			var refs []ref
			if s.parent != 0 {
				refs = []ref{{RefType: "CHILD_OF", TraceID: thex, SpanID: fmt.Sprintf("%016x", s.parent)}}
			}
			jt.Spans = append(jt.Spans, jspan{
				TraceID: thex, SpanID: fmt.Sprintf("%016x", s.id), OperationName: "op",
				References: refs, StartTime: s.start, Duration: s.end - s.start, // already µs (Jaeger native)
				ProcessID: pid,
			})
		}
		data = append(data, jt)
	}
	return writeJSON(filepath.Join(out, "jaeger.json"), map[string]any{"data": data})
}

// OTLP/JSON: resourceSpans grouped by service.name, each with one scopeSpans.
func emitOTel(out string, tids []uint64, traces [][]span, svc []string) error {
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
			if s.parent != 0 {
				parent = fmt.Sprintf("%016x", s.parent)
			}
			bySvc[s.svc] = append(bySvc[s.svc], ospan{
				TraceID: thex, SpanID: fmt.Sprintf("%016x", s.id), ParentSpanID: parent,
				Name: "op", Kind: 2,
				StartTimeUnixNano: fmt.Sprintf("%d", s.start*1000), EndTimeUnixNano: fmt.Sprintf("%d", s.end*1000), // µs->ns
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
