// Package gen produces synthetic traces with controlled structure (shape,
// fan-out law, depth, width, concurrency) entirely in memory, so callers can
// either emit them to disk (cmd/trace_gen) or stream them straight through
// analysis (cmd/stream_eval) without ever materializing a corpus. Generation is
// a pure function of the supplied *rand.Rand, so a seeded rng yields fully
// reproducible traces.
package gen

import (
	"fmt"
	"math"
	"math/rand"
	"os"
)

// Span is one generated span. Span ids are uniform-random (uniform fingerprint);
// timestamps are in microseconds and strictly nested by construction.
type Span struct {
	ID, Parent uint64
	Svc        uint16
	Start, End int64
	Depth      int
}

// Config holds the generation parameters. cmd/trace_gen and cmd/stream_eval each
// map their CLI flags onto these fields.
type Config struct {
	Shape         string
	Depth         int
	Fanout        int
	FanoutDist    string
	FanoutS       float64
	FanoutMin     int
	FanoutMax     int
	SlabMeanW     float64 // estimated mean per-node fan-out (floored at FanoutMin); set via EstimateMeanFanout before generating slab traces
	Spindle       bool
	SpindlePeriod int
	DepthDist     string
	DepthS        float64
	MaxSpans      int
	Concurrency   float64
	Services      int
	BaseDurUS     int64
	Seed          int64
}

// EstimateMeanFanout estimates the mean per-node fan-out (floored at FanoutMin)
// for the slab continuation-cap budget, regardless of which heavy-tailed
// --fanout-dist feeds it. Uses a THROWAWAY rng (derived from c.Seed) so the
// caller's generation stream is unperturbed. Callers set c.SlabMeanW from this
// once, before generating slab traces.
func EstimateMeanFanout(c *Config) float64 {
	er := MakeFanoutSampler(c, rand.New(rand.NewSource(c.Seed^0x9e3779b9)))
	sum := 0
	for i := 0; i < 8192; i++ {
		w := er()
		if w < c.FanoutMin {
			w = c.FanoutMin
		}
		sum += w
	}
	m := float64(sum) / 8192.0
	if m < 1 {
		m = 1
	}
	return m
}

// Trace builds one trace: a tree of the requested shape with valid contained
// timestamps and random (uniform-fingerprint) span ids. Per-trace max depth is
// drawn from depthOf; offspring counts from fanoutOf. (For slab, c.SlabMeanW
// must be set — see EstimateMeanFanout.)
func Trace(c *Config, rng *rand.Rand, fanoutOf, depthOf func() int) []Span {
	maxDepth := depthOf()
	spans := []Span{{ID: rng.Uint64(), Svc: uint16(rng.Intn(c.Services)), Depth: 0}}
	newChild := func(pid uint64, depth int) int {
		spans = append(spans, Span{ID: rng.Uint64(), Parent: pid, Svc: uint16(rng.Intn(c.Services)), Depth: depth})
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
				q = append(q, frame{newChild(spans[f.idx].ID, f.depth+1), f.depth + 1})
			}
		}
	}
	if c.Shape == "spine" {
		// Branching process CONDITIONED to reach maxDepth (Kesten spine
		// decomposition). Build the backbone FIRST (guarantees depth), THEN hang
		// ordinary realistic subtrees off each backbone node under a per-node span
		// budget. Realistic per-node fan-out, arbitrary depth, bounded width — even
		// when the fan-out law is supercritical (the budget caps the bushiness
		// instead of letting a near-root subtree starve the backbone).
		prev := 0
		backbone := []int{0}
		for d := 1; d <= maxDepth && len(spans) < c.MaxSpans; d++ {
			prev = newChild(spans[prev].ID, d)
			backbone = append(backbone, prev)
		}
		perNode := (c.MaxSpans - len(spans)) / len(backbone)
		for _, bi := range backbone {
			cap := len(spans) + perNode
			if cap > c.MaxSpans {
				cap = c.MaxSpans
			}
			for j := 1; j < fanoutOf() && len(spans) < cap; j++ {
				d := spans[bi].Depth + 1
				grow(newChild(spans[bi].ID, d), d, cap)
			}
		}
	} else if c.Shape == "deepwide" {
		// K PARALLEL deep paths (generalized Kesten spine) + realistic bushes. Real
		// Uber traces are wide AND deep — ~38 leaves reach the deep frontier — which
		// the single spine (1 deep path) can't represent. Here K = --fanout sets the
		// number of deep paths and --depth their length, so depth x width are two
		// independent sweep knobs (the 2D flame-plot axes). Bushes off the backbone
		// nodes follow the offspring law (use --fanout-dist uber) and supply the
		// many shallow leaves real traces also have.
		K := c.Fanout
		if K < 1 {
			K = 1
		}
		backbone := []int{0}
		for k := 0; k < K && len(spans) < c.MaxSpans; k++ {
			prev := 0 // each deep path is a fresh chain off the root
			for d := 1; d <= maxDepth && len(spans) < c.MaxSpans; d++ {
				prev = newChild(spans[prev].ID, d)
				backbone = append(backbone, prev)
			}
		}
		perNode := 0
		if len(backbone) > 0 {
			perNode = (c.MaxSpans - len(spans)) / len(backbone)
		}
		for _, bi := range backbone {
			cap := len(spans) + perNode
			if cap > c.MaxSpans {
				cap = c.MaxSpans
			}
			for j := 1; j < fanoutOf() && len(spans) < cap; j++ {
				d := spans[bi].Depth + 1
				grow(newChild(spans[bi].ID, d), d, cap)
			}
		}
	} else if c.Shape == "slab" {
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
		// Flat -> brick (constant width). Spindle -> right-skewed arch repeating every
		// --spindle-period, scaled so sum(caps)*meanW ~= maxSpans.
		caps := make([]int, maxDepth+1)
		budgetNodes := float64(c.MaxSpans) / c.SlabMeanW
		if c.Spindle {
			// Periodic spindle: within each lobe of L levels the cap follows an arch
			// x*(L-x)^3 (rise -> peak -> taper), then re-spindles. The neck (phase 0)
			// is floored to 1 so the lineage never dies — it pinches and re-widens,
			// stacking lobes to reach any --depth. L = --spindle-period (0 = one lobe
			// over the whole depth). Scaled so sum(caps)*meanW ~= maxSpans.
			L := c.SpindlePeriod
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
		for d := 1; d <= maxDepth && len(frontier) > 0 && len(spans) < c.MaxSpans; d++ {
			var next []int
			for _, fi := range frontier {
				if len(spans) >= c.MaxSpans {
					break
				}
				// per-node fan-out from the (heavy-tailed) --fanout-dist sampler,
				// floored at --fanout-min: mostly small (chains), occasionally huge.
				w := fanoutOf()
				if w < c.FanoutMin {
					w = c.FanoutMin
				}
				for j := 0; j < w && len(spans) < c.MaxSpans; j++ {
					ch := newChild(spans[fi].ID, d) // full fan-out: real span, real wide window
					if len(next) < caps[d] {
						next = append(next, ch) // only caps[d] per level continue; rest are leaves
					}
				}
			}
			frontier = next
		}
	} else {
		grow(0, 0, c.MaxSpans)
	}

	kids := map[uint64][]int{}
	for i := range spans {
		if spans[i].Parent != 0 {
			kids[spans[i].Parent] = append(kids[spans[i].Parent], i)
		}
	}
	assignTimes(spans, kids, c.Concurrency)
	return spans
}

// offspring returns the child count for a node at the given depth, per shape.
func offspring(c *Config, depth int, rng *rand.Rand, fanoutOf func() int) int {
	switch c.Shape {
	case "chain":
		return 1
	case "star":
		if depth == 0 {
			return c.Fanout
		}
		return 0
	case "kary":
		return c.Fanout
	case "skewed": // legacy crude heavy-tail; prefer `branch --fanout-dist zipf`
		if rng.Float64() < 0.15 {
			return 1 + rng.Intn(2*c.Fanout+1)
		}
		return rng.Intn(c.Fanout + 1)
	case "branch", "spine": // spine's off-backbone subtrees branch by the real law
		return fanoutOf()
	}
	return c.Fanout
}

// MakeFanoutSampler returns the offspring-count sampler for the offspring law.
// zipf is the heavy-tail workhorse (P(k) ∝ 1/(k+1)^s, k∈[0,max], 0 = leaf).
func MakeFanoutSampler(c *Config, rng *rand.Rand) func() int {
	clamp := func(k int) int {
		if k < 0 {
			return 0
		}
		if k > c.FanoutMax {
			return c.FanoutMax
		}
		return k
	}
	switch c.FanoutDist {
	case "zipf":
		s := c.FanoutS
		if s <= 1 {
			fmt.Fprintf(os.Stderr, "warn: --fanout-s must be >1, clamping to 1.001\n")
			s = 1.001
		}
		max := c.FanoutMax
		if max < 1 {
			max = 1
		}
		z := rand.NewZipf(rng, s, 1.0, uint64(max))
		return func() int { return int(z.Uint64()) }
	case "poisson":
		return func() int { return clamp(poisson(rng, float64(c.Fanout))) }
	case "geometric":
		p := 1.0 / (float64(c.Fanout) + 1.0) // mean fan-out = c.Fanout
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
			uberBranchS = 1.8   // calibrated at fanout-max~256 to branch-mean~6.7 AND total offspring mean~1.0 (critical, like real)
		)
		zmax := c.FanoutMax - 2
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
		return func() int { return c.Fanout }
	}
}

// MakeDepthSampler returns the per-trace max-depth sampler. zipf gives
// mostly-shallow, rare-deep traces capped at --depth (depth = 1 + Zipf, so ≥1).
func MakeDepthSampler(c *Config, rng *rand.Rand) func() int {
	switch c.DepthDist {
	case "zipf":
		s := c.DepthS
		if s <= 1 {
			fmt.Fprintf(os.Stderr, "warn: --depth-s must be >1, clamping to 1.001\n")
			s = 1.001
		}
		imax := c.Depth - 1
		if imax < 1 {
			imax = 1
		}
		z := rand.NewZipf(rng, s, 1.0, uint64(imax))
		return func() int { return 1 + int(z.Uint64()) }
	default: // fixed
		return func() int { return c.Depth }
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
// fan-out, with no window to collapse.
//
// Concurrency is supported and validity-preserving: each sibling keeps its
// need-sized block, but its START is slid toward the parent's open by (1-conc).
// conc=0 tiles siblings sequentially (end-order = start-order); conc=1 starts
// them all together so their ENDS spread by subtree size (end-order ≠
// start-order). Sliding a start only EARLIER never pushes its end past the
// parent, so every child stays strictly contained for all conc∈[0,1].
func assignTimes(spans []Span, kids map[uint64][]int, conc float64) {
	need := map[uint64]int64{} // ticks the subtree consumes (2 + children's)
	var computeNeed func(i int) int64
	computeNeed = func(i int) int64 {
		n := int64(2)
		for _, ci := range kids[spans[i].ID] {
			n += computeNeed(ci)
		}
		need[spans[i].ID] = n
		return n
	}
	computeNeed(0)

	var place func(i int, lo int64)
	place = func(i int, lo int64) {
		id := spans[i].ID
		spans[i].Start = lo
		spans[i].End = lo + need[id] - 1 // exit tick: strictly after every descendant
		off := int64(1)                  // sequential cursor within the parent
		for _, ci := range kids[id] {
			seqStart := lo + off
			start := lo + 1 + int64(float64(seqStart-(lo+1))*(1-conc)) // slide toward lo+1
			place(ci, start)
			off += need[spans[ci].ID]
		}
	}
	place(0, 0)
}
