package recon

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"

	"bridges/bloom"
	"bridges/bridge"
)

// Ambiguity instrumentation (TRACE_RECON_DEBUG=1): process-global tallies
// of the actual reconstruction decision points, to measure the real
// error-source rate per cpd rather than infer it from static structure.
//   - thread decisions = interior window levels with >=1 bloom-positive
//     candidate; AMBIGUOUS = those with >=2 (same-depth multiplicity the
//     threading walk must resolve, the genuine threading-error source).
//   - orphan placements = demand orphans; MULTI-WINDOW = those gated in
//     >1 window (the genuine placement-error source).
//
// Counted once per trace at candidate-fill / pre-solve, so rounds never
// double-count. DumpPCRSAmbiguity prints rates at drain.
var (
	ambigThreadLevels int64
	ambigThreadCands  int64
	ambigThreadGe2    int64
	ambigOrphans      int64
	ambigOrphanMultiW int64
)

// DumpPCRSAmbiguity writes the accumulated ambiguity rates and resets.
func DumpPCRSAmbiguity() {
	lv := atomic.LoadInt64(&ambigThreadLevels)
	ge2 := atomic.LoadInt64(&ambigThreadGe2)
	cs := atomic.LoadInt64(&ambigThreadCands)
	orph := atomic.LoadInt64(&ambigOrphans)
	mw := atomic.LoadInt64(&ambigOrphanMultiW)
	pct := func(a, b int64) float64 {
		if b == 0 {
			return 0
		}
		return 100 * float64(a) / float64(b)
	}
	mean := 0.0
	if lv > 0 {
		mean = float64(cs) / float64(lv)
	}
	fmt.Fprintf(os.Stderr, "AMBIG thread_levels=%d ambiguous_ge2=%d (%.4f%%) mean_cands=%.4f | orphans=%d multi_window=%d (%.4f%%)\n",
		lv, ge2, pct(ge2, lv), mean, orph, mw, pct(mw, orph))
}

// ReconstructPCRS is the v8 "pure solver" engine for PCRB payloads: the same
// wire format and pass-1 prefix anchoring as ReconstructPCRB, but resolution
// is propagate-then-search instead of greedy-rules-then-repair.
//
//   - PROPAGATION: the sound rules (unique-candidate collapse, strict orphan
//     unification, gated open-end multi-writes) run to fixpoint. These are
//     not heuristics here: each is a theorem that its assignment appears in
//     every satisfying configuration (arc consistency). Ties are never
//     broken by propagation.
//   - SEARCH: remaining invariant demands — unplaced orphans, unmatched
//     open ends — are solved JOINTLY per window-cluster (windows coupled by
//     shared orphan gating) with branch-and-bound. Orphan options range
//     over ALL gated windows natively ("placed somewhere", not "placed
//     here"); open ends choose among gated chains; existing single-test
//     writes are soft (displaceable at objective cost); unification-backed
//     placements and structural walls are hard. Objective: MAXIMIZE
//     EXPLAINED BLOOM POSITIVES — the maximum-likelihood assignment under
//     no-false-negatives + small-fp noise. The first optimum is applied
//     atomically; every search-applied assignment is flagged Forced.
//   - The outer loop alternates propagation and search until stability:
//     search placements enrich the structure, propagation collapses chains
//     through it, possibly exposing new unique decisions.
func ReconstructPCRS(survivors []Span, cfg Config) Result {
	byID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		byID[survivors[i].SpanID] = &survivors[i]
	}
	children := make(map[uint64][]*Span, len(survivors))
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 {
			if _, ok := byID[s.ParentID]; ok {
				children[s.ParentID] = append(children[s.ParentID], s)
			}
		}
	}
	byDepth := make(map[int][]*Span)
	for i := range survivors {
		s := &survivors[i]
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}

	var orphanRootsAll []*Span
	for i := range survivors {
		o := &survivors[i]
		if o.ParentID == 0 {
			continue
		}
		if _, ok := byID[o.ParentID]; ok {
			continue
		}
		orphanRootsAll = append(orphanRootsAll, o)
	}

	// --- Pass 1: prefix anchoring (identical to ReconstructPCRB) ---
	type anchored struct {
		o          *Span
		anchor     *Span
		threadBits []byte
		ambiguous  bool
		viaCarrier uint64
	}
	type fragState struct {
		p    anchored
		cand map[int][]*Span
		// bf is the window bloom deserialized ONCE (immutable thereafter);
		// nil when the window has no payload. Deserialize allocates and
		// copies the whole bit array — re-deserializing inside per-round
		// loops was a dominant, semantics-free cost.
		bf *bloom.Filter
	}
	var placed []anchored
	fragRoots := make(map[uint64][]*Span)

	var res Result
	res.Orphans = len(orphanRootsAll)
	for _, o := range orphanRootsAll {
		carrierDepth, prefix, threadBits, viaCarrier := coveringPCRBPayload(o, children, cfg)
		if prefix == nil {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		var ckptDepth int
		if carrierDepth%cfg.CPD == 0 {
			ckptDepth = carrierDepth - cfg.CPD
		} else {
			ckptDepth = (carrierDepth / cfg.CPD) * cfg.CPD
		}
		if ckptDepth < 0 {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		var hits []*Span
		for _, c := range byDepth[ckptDepth] {
			id8 := bridge.BigEndian8(c.SpanID)
			match := true
			for i := 0; i < cfg.PrefixLen; i++ {
				if id8[i] != prefix[i] {
					match = false
					break
				}
			}
			if match {
				hits = append(hits, c)
			}
		}
		if len(hits) == 0 {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		sort.Slice(hits, func(i, j int) bool { return hits[i].SpanID < hits[j].SpanID })
		placed = append(placed, anchored{o: o, anchor: hits[0], threadBits: threadBits, ambiguous: len(hits) > 1, viaCarrier: viaCarrier})
		fragRoots[hits[0].SpanID] = append(fragRoots[hits[0].SpanID], o)
	}

	// --- Candidate filling (registration-keyed doors; see note below) ---
	states := make([]*fragState, 0, len(placed))
	for i := range placed {
		p := placed[i]
		fs := &fragState{p: p}
		if p.threadBits != nil {
			fs.bf = bloom.Deserialize(p.threadBits, cfg.BloomM, cfg.BloomK)
		}
		maxD := p.o.Depth - 2
		if p.threadBits != nil && maxD > p.anchor.Depth {
			bf := fs.bf
			fs.cand = make(map[int][]*Span)
			var dfs func(s *Span)
			dfs = func(s *Span) {
				if s == p.o || s.Depth > maxD {
					return
				}
				if s.LeafCarrier {
					return
				}
				hex := bridge.HexOf(s.SpanID)
				if !bf.Test(hex[:]) {
					return
				}
				fs.cand[s.Depth] = append(fs.cand[s.Depth], s)
				for _, c := range children[s.SpanID] {
					dfs(c)
				}
			}
			for _, c := range children[p.anchor.SpanID] {
				dfs(c)
			}
			// Registration-keyed doors are sufficient: carrier-less gapped
			// fragments are ORPHANS (placed via pathOn, which is already
			// evidence-keyed over ALL windows); carrier-bearing fragments
			// register to the window their material belongs to (the prefix
			// names that window's anchor), and multi-window fragments'
			// deeper material is reachable from deeper anchors via real
			// edges. The evidence-keyed "door-complete" variant covered a
			// structurally vacant case (CANDCHECK: zero missing candidates
			// ever observed) at real combinatorial cost. CANDCHECK stays
			// armed as the tripwire if benign ever reappears cand=false.
			for _, r := range fragRoots[p.anchor.SpanID] {
				if r == p.o {
					continue
				}
				ph := bridge.HexOf(r.ParentID)
				if !bf.Test(ph[:]) {
					continue
				}
				dfs(r)
			}
			if debugScore {
				// one-shot threading-ambiguity tally: bloom-positive
				// candidate multiplicity per interior window level
				for d := p.anchor.Depth + 1; d <= maxD; d++ {
					n := int64(len(fs.cand[d]))
					if n >= 1 {
						atomic.AddInt64(&ambigThreadLevels, 1)
						atomic.AddInt64(&ambigThreadCands, n)
						if n >= 2 {
							atomic.AddInt64(&ambigThreadGe2, 1)
						}
					}
				}
			}
		}
		states = append(states, fs)
	}
	// Deterministic window index for symmetry fingerprints (pointer values
	// are not stable across runs; states order is).
	fsIdx := make(map[*fragState]int, len(states))
	for i, fs := range states {
		fsIdx[fs] = i
	}

	// --- Ledger ---
	type chainLedger struct {
		occ map[int]*Span
		rsv map[int]bool
	}
	led := make(map[*fragState]*chainLedger, len(states))
	prov := make(map[*fragState]map[int]byte)
	rsvOwner := make(map[*fragState]map[int]*Span)
	for _, fs := range states {
		led[fs] = &chainLedger{occ: make(map[int]*Span), rsv: map[int]bool{fs.p.o.Depth - 1: true}}
		prov[fs] = make(map[int]byte)
		rsvOwner[fs] = make(map[int]*Span)
	}
	open := func(fs *fragState, d int) bool {
		if d <= fs.p.anchor.Depth || d >= fs.p.o.Depth {
			return false
		}
		l := led[fs]
		return l.occ[d] == nil && !l.rsv[d]
	}
	write := func(fs *fragState, s *Span, kind byte) bool {
		// Idempotent same-span write: re-writing the span already at this
		// level is the same assignment, not a conflict. Provenance may
		// strengthen but a 'B' placement is never downgraded.
		if led[fs].occ[s.Depth] == s {
			if prov[fs][s.Depth] != 'B' {
				prov[fs][s.Depth] = kind
			}
			return true
		}
		if !open(fs, s.Depth) {
			return false
		}
		led[fs].occ[s.Depth] = s
		prov[fs][s.Depth] = kind
		return true
	}
	chainsByW := make(map[uint64][]*fragState)
	for _, fs := range states {
		chainsByW[fs.p.anchor.SpanID] = append(chainsByW[fs.p.anchor.SpanID], fs)
	}
	orphanSpansByW := make(map[uint64]map[int][]*Span)
	orphanRootSet := make(map[uint64]bool)
	type oFit struct {
		fs   *fragState
		path []*Span
	}
	occUpper := make(map[*fragState][]*oFit)
	orphanChains := make(map[uint64][]*oFit)
	placedRootOf := make(map[uint64]uint64) // any placed orphan's span -> its root
	commitFits := func(r *Span, fits []*oFit, forced bool) {
		w := fits[0].fs.p.anchor.SpanID
		pool := orphanSpansByW[w]
		if pool == nil {
			pool = make(map[int][]*Span)
			orphanSpansByW[w] = pool
		}
		seen := make(map[uint64]bool)
		for _, f := range fits {
			led[f.fs].rsv[r.Depth-1] = true
			rsvOwner[f.fs][r.Depth-1] = r
			for _, s := range f.path {
				write(f.fs, s, 'B')
				if !seen[s.SpanID] {
					seen[s.SpanID] = true
					pool[s.Depth] = append(pool[s.Depth], s)
				}
			}
			occUpper[f.fs] = append(occUpper[f.fs], f)
		}
		queue := []*Span{r}
		for len(queue) > 0 {
			s := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			placedRootOf[s.SpanID] = r.SpanID
			if !seen[s.SpanID] {
				seen[s.SpanID] = true
				pool[s.Depth] = append(pool[s.Depth], s)
			}
			queue = append(queue, children[s.SpanID]...)
		}
		orphanRootSet[r.SpanID] = true
		orphanChains[r.SpanID] = fits
		res.OrphansPlaced++
		if forced {
			res.ForcedMatches++
		}
	}
	var unplaced []*Span
	for _, id := range res.Unanchored {
		unplaced = append(unplaced, byID[id])
	}
	dbgTag := uint64(0)
	if len(survivors) > 0 {
		dbgTag = survivors[0].SpanID
	}
	placedSet := make(map[uint64]bool)
	forcedRoots := make(map[uint64]bool)
	isOpenEnd := func(s *Span) bool {
		return s.CkptPrefix == nil && len(children[s.SpanID]) == 0
	}
	pathOn := func(r *Span, fs *fragState) []*Span {
		if fs.p.threadBits == nil || r == fs.p.o {
			return nil
		}
		if r.Depth > fs.p.o.Depth-2 || r.Depth <= fs.p.anchor.Depth+1 {
			return nil
		}
		bf := fs.bf
		ph := bridge.HexOf(r.ParentID)
		rh := bridge.HexOf(r.SpanID)
		if !bf.Test(ph[:]) || !bf.Test(rh[:]) {
			return nil
		}
		path := []*Span{r}
		cur := r
		for {
			var next *Span
			n := 0
			for _, c := range children[cur.SpanID] {
				if c.Depth > fs.p.o.Depth-2 {
					continue
				}
				hex := bridge.HexOf(c.SpanID)
				if bf.Test(hex[:]) {
					n++
					next = c
				}
			}
			if n != 1 {
				break
			}
			path = append(path, next)
			cur = next
		}
		return path
	}
	gate := func(s *Span, fs *fragState) bool {
		if fs.p.threadBits == nil || s == fs.p.o || s.LeafCarrier {
			return false
		}
		if s.Depth <= fs.p.anchor.Depth || s.Depth >= fs.p.o.Depth-1 {
			return false
		}
		bf := fs.bf
		hex := bridge.HexOf(s.SpanID)
		return bf.Test(hex[:]) && chainConsistent(bf, s, byID, fs.p.anchor.Depth+1)
	}
	appearances := func(s *Span) int {
		n := 0
		for _, fs := range states {
			if led[fs].occ[s.Depth] == s {
				n++
			}
		}
		return n
	}

	// --- Propagation (arc consistency; ties never broken) ---
	propagate := func() {
		for {
			changed := false
			// Rule C: open-end multi-writes wherever gated and open.
			for i := range survivors {
				e := &survivors[i]
				if !isOpenEnd(e) {
					continue
				}
				for _, fs := range states {
					if !open(fs, e.Depth) || fs.p.threadBits == nil {
						continue
					}
					inCand := false
					for _, c := range fs.cand[e.Depth] {
						if c == e {
							inCand = true
							break
						}
					}
					if !inCand {
						if pool := orphanSpansByW[fs.p.anchor.SpanID]; pool != nil {
							for _, c := range pool[e.Depth] {
								if c == e {
									inCand = true
									break
								}
							}
						}
					}
					if !inCand {
						continue
					}
					bf := fs.bf
					hex := bridge.HexOf(e.SpanID)
					if !bf.Test(hex[:]) || !chainConsistent(bf, e, byID, fs.p.anchor.Depth+1) {
						continue
					}
					if write(fs, e, 'C') {
						changed = true
					}
				}
			}
			// Rule A: unique-claimant collapse.
			for _, fs := range states {
				if fs.p.threadBits == nil {
					continue
				}
				bf := fs.bf
				w := fs.p.anchor.SpanID
				for d := fs.p.anchor.Depth + 1; d < fs.p.o.Depth; d++ {
					if !open(fs, d) {
						continue
					}
					var hit *Span
					n := 0
					consider := func(c *Span) {
						if c.LeafCarrier || c == fs.p.o {
							return
						}
						if orphanRootSet[c.SpanID] && !open(fs, c.Depth-1) && !led[fs].rsv[c.Depth-1] {
							return
						}
						hex := bridge.HexOf(c.SpanID)
						if !bf.Test(hex[:]) {
							return
						}
						if !chainConsistent(bf, c, byID, fs.p.anchor.Depth+1) {
							return
						}
						n++
						hit = c
					}
					for _, c := range fs.cand[d] {
						consider(c)
					}
					if pool := orphanSpansByW[w]; pool != nil {
						for _, c := range pool[d] {
							already := false
							for _, cc := range fs.cand[d] {
								if cc == c {
									already = true
									break
								}
							}
							if !already {
								consider(c)
							}
						}
					}
					if n == 1 && hit != nil {
						if orphanRootSet[hit.SpanID] {
							led[fs].rsv[hit.Depth-1] = true
						}
						if write(fs, hit, 'A') {
							changed = true
						}
					}
				}
			}
			// Rule B (demoted to TRUE arc consistency): commit an orphan
			// only when the fit is forced in every optimal configuration —
			// exactly one window, exactly one chain, and every needed level
			// claimed by no other unplaced orphan's options. Anything less
			// (single window but contested levels, multiple chains) is a
			// CHOICE and belongs to the search, where alternatives are
			// weighed by the objective and the pick is flagged. This closes
			// the eager-commit FP channels: wrong-window commits via masked
			// bindings, timing-lottery order dependence, and the
			// contamination cascade through pools.
			type claimKey struct {
				fs *fragState
				d  int
			}
			claims := make(map[claimKey]int)
			type bOpt struct {
				fs   *fragState
				path []*Span
			}
			orphanOpts := make(map[uint64][]bOpt)
			for _, r := range unplaced {
				if placedSet[r.SpanID] {
					continue
				}
				for _, fs := range states {
					p := pathOn(r, fs)
					if p == nil {
						continue
					}
					orphanOpts[r.SpanID] = append(orphanOpts[r.SpanID], bOpt{fs: fs, path: p})
					claims[claimKey{fs, r.Depth - 1}]++
					for _, s := range p {
						claims[claimKey{fs, s.Depth}]++
					}
				}
			}
			for _, r := range unplaced {
				if placedSet[r.SpanID] {
					continue
				}
				opts := orphanOpts[r.SpanID]
				if len(opts) != 1 {
					continue // multiple chains or none: search territory
				}
				o := opts[0]
				// strict slot conditions (carrier-undecided, open) + sole claim
				ok := open(o.fs, r.Depth-1) || led[o.fs].rsv[r.Depth-1]
				if claims[claimKey{o.fs, r.Depth - 1}] != 1 {
					ok = false
				}
				for _, s := range o.path {
					if !ok {
						break
					}
					if !open(o.fs, s.Depth) || claims[claimKey{o.fs, s.Depth}] != 1 {
						ok = false
						break
					}
					if len(o.fs.cand[s.Depth]) > 0 {
						inCand := false
						for _, cc := range o.fs.cand[s.Depth] {
							if cc == s {
								inCand = true
								break
							}
						}
						if !inCand {
							ok = false
							break
						}
					}
				}
				if ok {
					commitFits(r, []*oFit{{fs: o.fs, path: o.path}}, false)
					placedSet[r.SpanID] = true
					changed = true
				}
			}
			if !changed {
				return
			}
		}
	}

	// --- Cluster search: joint MAP branch-and-bound over demands ---
	undoPlaced := func(root *Span) {
		set := make(map[uint64]bool)
		queue := []*Span{root}
		for len(queue) > 0 {
			s := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			set[s.SpanID] = true
			queue = append(queue, children[s.SpanID]...)
		}
		for _, fs := range states {
			for d, s := range led[fs].occ {
				if s != nil && set[s.SpanID] {
					led[fs].occ[d] = nil
					delete(prov[fs], d)
				}
			}
			for d, owner := range rsvOwner[fs] {
				if owner == root {
					shared := false
					for _, of := range occUpper[fs] {
						if of.path[0] != root && of.path[0].Depth == root.Depth {
							shared = true
							break
						}
					}
					if !shared && d != fs.p.o.Depth-1 {
						delete(led[fs].rsv, d)
					}
					delete(rsvOwner[fs], d)
				}
			}
			fits := occUpper[fs][:0]
			for _, of := range occUpper[fs] {
				if of.path[0] != root {
					fits = append(fits, of)
				}
			}
			occUpper[fs] = fits
		}
		for _, f := range orphanChains[root.SpanID] {
			if pool := orphanSpansByW[f.fs.p.anchor.SpanID]; pool != nil {
				for d := range pool {
					lst := pool[d][:0]
					for _, c := range pool[d] {
						if !set[c.SpanID] {
							lst = append(lst, c)
						}
					}
					pool[d] = lst
				}
			}
		}
		delete(orphanChains, root.SpanID)
		res.OrphansPlaced--
	}

	type sOpt struct {
		fs    *fragState
		spans []*Span // occupancy levels
		rsvD  int     // -1 for open ends
		gain  int     // explained positives
	}
	type sItem struct {
		span     *Span
		isOrphan bool
		reseat   bool // a placed orphan being reassigned (must keep a seat)
		thread   bool // an ambiguous threading level (likelihood-only: free skip)
		opts     []sOpt
	}
	solveDemands := func() bool {
		var demOrphans []*Span
		for _, r := range unplaced {
			if !placedSet[r.SpanID] {
				demOrphans = append(demOrphans, r)
			}
		}
		var demEnds []*Span
		for i := range survivors {
			e := &survivors[i]
			if isOpenEnd(e) && appearances(e) == 0 {
				demEnds = append(demEnds, e)
			}
		}
		// NOTE: no early-out when demands are empty — thread items (the
		// likelihood half of the solver) must be built even in traces with
		// zero invariant demands, or ambiguous levels in quiet traces are
		// never resolved (this exact gap kept benign alive at low drop
		// rates). The len(items)==0 check below is the sole empty-exit.
		// Build items with options across ALL gated chains (soft window
		// membership is native: the domain spans windows).
		var items []sItem
		// orphanSpine[root] = spans written by EVERY option of that orphan's
		// item (intersection of option paths). An end whose span is on the
		// spine has its coverage IMPLIED by the placement invariant — its
		// separate demand item would double-count one obligation (and the
		// alter-ego coupling it creates is what disabled symmetry grouping
		// in the monster clusters).
		orphanSpine := make(map[uint64]map[uint64]bool)
		for _, r := range demOrphans {
			var opts []sOpt
			for _, fs := range states {
				if p := pathOn(r, fs); p != nil {
					opts = append(opts, sOpt{fs: fs, spans: p, rsvD: r.Depth - 1, gain: len(p) + 1})
				}
			}
			if len(opts) > 0 {
				items = append(items, sItem{span: r, isOrphan: true, opts: opts})
				spine := make(map[uint64]bool)
				for _, s := range opts[0].spans {
					spine[s.SpanID] = true
				}
				for _, o := range opts[1:] {
					keep := make(map[uint64]bool)
					for _, s := range o.spans {
						if spine[s.SpanID] {
							keep[s.SpanID] = true
						}
					}
					spine = keep
				}
				orphanSpine[r.SpanID] = spine
			}
		}
		for _, e := range demEnds {
			// Coverage implied by a mandatory placement? Walk to e's
			// fragment root: if the root is a demand orphan and e is on its
			// spine, every possible seating of that orphan writes e — a
			// separate hard item would double-count the obligation. Branch
			// ends and partial-spine ends keep their independent demand.
			root := e
			for {
				p, ok := byID[root.ParentID]
				if !ok {
					break
				}
				root = p
			}
			if sp, ok := orphanSpine[root.SpanID]; ok && sp[e.SpanID] {
				continue
			}
			var opts []sOpt
			for _, fs := range states {
				if gate(e, fs) {
					opts = append(opts, sOpt{fs: fs, spans: []*Span{e}, rsvD: -1, gain: 1})
				}
			}
			if len(opts) > 0 {
				items = append(items, sItem{span: e, isOrphan: false, opts: opts})
			}
		}
		// THREADING AS SEARCH (v8h): a level with >=2 surviving candidates
		// is not a wall — it is a decision. Each candidate's unique-descent
		// continuation becomes an option scored by explained bloom
		// positives. The item is created only when a unique argmax exists:
		// an exact posterior tie keeps the risk-averse shallow stop, since
		// MAP is indifferent and a guess converts depth loss into false
		// ancestry. Skips are free — no invariant demands threading — so
		// demand items always dominate in shared clusters.
		for _, fs := range states {
			if fs.p.threadBits == nil {
				continue
			}
			bf := fs.bf
			for d := fs.p.anchor.Depth + 1; d < fs.p.o.Depth; d++ {
				// STICKY THREADING: only OPEN levels are itemized. A seated
				// thread decision is re-litigated solely when a demand's
				// seating displaces it (its soft write is cleared, the level
				// reopens, and the next round re-asks the question) — never
				// spontaneously. Spontaneous re-adjudication was measured to
				// cause cross-round churn (ROUNDCAP, stranded ends) while
				// defending a channel never observed in data (CANDCHECK).
				if !open(fs, d) {
					continue
				}
				var cands []*Span
				consider := func(c *Span) {
					if c.LeafCarrier || c == fs.p.o {
						return
					}
					if orphanRootSet[c.SpanID] && !open(fs, c.Depth-1) && !led[fs].rsv[c.Depth-1] {
						return
					}
					hex := bridge.HexOf(c.SpanID)
					if !bf.Test(hex[:]) {
						return
					}
					if !chainConsistent(bf, c, byID, fs.p.anchor.Depth+1) {
						return
					}
					cands = append(cands, c)
				}
				for _, c := range fs.cand[d] {
					consider(c)
				}
				if pool := orphanSpansByW[fs.p.anchor.SpanID]; pool != nil {
					for _, c := range pool[d] {
						dup := false
						for _, cc := range fs.cand[d] {
							if cc == c {
								dup = true
								break
							}
						}
						if !dup {
							consider(c)
						}
					}
				}
				if len(cands) < 2 {
					continue // empty or unique: rule A's domain
				}
				var opts []sOpt
				for _, c := range cands {
					path := []*Span{c}
					cur := c
					for {
						var next *Span
						n := 0
						for _, ch := range children[cur.SpanID] {
							if ch.Depth >= fs.p.o.Depth || ch.LeafCarrier || ch == fs.p.o {
								continue
							}
							hex := bridge.HexOf(ch.SpanID)
							if bf.Test(hex[:]) {
								n++
								next = ch
							}
						}
						if n != 1 || !open(fs, next.Depth) {
							break
						}
						path = append(path, next)
						cur = next
					}
					opts = append(opts, sOpt{fs: fs, spans: path, rsvD: -1, gain: len(path)})
				}
				// Exact posterior ties: the shallow stop is NOT a MAP argmax
				// (it leaves a true member's bit unexplained), so the
				// default forces a pick. cfg.TiePolicy selects the rule:
				//   aware — prefer the candidate whose bit is not already
				//           explained by another chain (global
				//           bit-accounting: same MAP objective, applied
				//           across windows), span ID as final fallback
				//   id    — span ID only (blind deterministic coin)
				//   stop  — abstain: drop the item, keep the synthetic
				// Tie sites are counted for disclosure.
				explainedElsewhere := func(c *Span) bool {
					for _, ofs := range states {
						if ofs != fs && led[ofs].occ[c.Depth] == c {
							return true
						}
					}
					return false
				}
				aware := cfg.TiePolicy != "id" && cfg.TiePolicy != "stop"
				sort.Slice(opts, func(i, j int) bool {
					if opts[i].gain != opts[j].gain {
						return opts[i].gain > opts[j].gain
					}
					if aware {
						ei, ej := explainedElsewhere(opts[i].spans[0]), explainedElsewhere(opts[j].spans[0])
						if ei != ej {
							return !ei // unclaimed candidate first
						}
					}
					return opts[i].spans[0].SpanID < opts[j].spans[0].SpanID
				})
				tied := len(opts) > 1 && opts[0].gain == opts[1].gain
				if tied && debugScore {
					fmt.Fprintf(os.Stderr, "THREADTIE t=%x level=%d cands=%d gain=%d\n",
						dbgTag, d, len(cands), opts[0].gain)
				}
				if tied && cfg.TiePolicy == "stop" {
					continue // abstain: risk-averse shallow stop
				}
				items = append(items, sItem{span: cands[0], thread: true, opts: opts})
			}
		}
		if debugScore {
			orphItems := 0
			for _, it := range items {
				if it.isOrphan {
					orphItems++
				}
			}
			if orphItems < len(demOrphans) {
				fmt.Fprintf(os.Stderr, "ITEMZERO t=%x n=%d\n", dbgTag, len(demOrphans)-orphItems)
			}
		}
		if len(items) == 0 {
			return false
		}
		// PURITY: contested placed orphans become reassignable items — a
		// rule-B placement blocking a demand's only options is a CHOICE the
		// search must own, not a wall. Their full pathOn domains are the
		// option set (current seat included), skip is effectively
		// forbidden (they must remain placed).
		// TRANSITIVE blocker closure: itemize every placed orphan whose
		// levels any item's options depend on, recursively — the cluster
		// must contain everything its own feasibility requires, or coverage
		// becomes structurally infeasible for search-order reasons.
		reseatRoots := make(map[uint64]bool)
		collectBlockers := func(opts []sOpt) []uint64 {
			var out []uint64
			for _, o := range opts {
				checkLevel := func(d int) {
					if x := led[o.fs].occ[d]; x != nil && prov[o.fs][d] == 'B' {
						if rid := placedRootOf[x.SpanID]; rid != 0 && !reseatRoots[rid] {
							out = append(out, rid)
						}
					}
					if owner := rsvOwner[o.fs][d]; owner != nil && !reseatRoots[owner.SpanID] {
						out = append(out, owner.SpanID)
					}
				}
				checkLevel(o.rsvD)
				for _, s := range o.spans {
					checkLevel(s.Depth)
				}
			}
			return out
		}
		// Coverage closure for ENDS: a demand end whose gated levels are
		// held by other ends' C/D writes pulls THOSE ends into the cluster
		// (their gate-set as options, current seat included) — same-depth
		// contention is assigned jointly, never by displacement roulette.
		endItem := make(map[uint64]bool)
		for _, it := range items {
			endItem[it.span.SpanID] = true
		}
		var endFrontier []*Span
		// Seed from EVERY item class and EVERY level its options touch: a
		// sitting end-write on an orphan path's level or a thread item's
		// continuation is displaceable by the assignment, so the displaced
		// end must be a first-class member of the same solve (its skip
		// penalty then owns its re-seating — churn cannot strand it).
		for _, it := range items {
			for _, o := range it.opts {
				probe := func(d int, w *Span) {
					if x := led[o.fs].occ[d]; x != nil && x != w && isOpenEnd(x) && !endItem[x.SpanID] {
						p := prov[o.fs][d]
						// Conscript ONLY sole-appearance writes: an end with
						// other live writes survives any single displacement
						// (its invariant is not at stake), so itemizing it
						// buys nothing but search width.
						if (p == 'C' || p == 'D') && appearances(x) <= 1 {
							endFrontier = append(endFrontier, x)
							endItem[x.SpanID] = true
						}
					}
				}
				for _, s := range o.spans {
					probe(s.Depth, s)
				}
				if o.rsvD >= 0 {
					probe(o.rsvD, nil)
				}
			}
		}
		for len(endFrontier) > 0 {
			e := endFrontier[len(endFrontier)-1]
			endFrontier = endFrontier[:len(endFrontier)-1]
			var opts []sOpt
			for _, fs := range states {
				if gate(e, fs) {
					opts = append(opts, sOpt{fs: fs, spans: []*Span{e}, rsvD: -1, gain: 1})
				}
			}
			if len(opts) == 0 {
				continue
			}
			items = append(items, sItem{span: e, isOrphan: false, opts: opts})
			for _, o := range opts {
				if x := led[o.fs].occ[e.Depth]; x != nil && x != e && isOpenEnd(x) && !endItem[x.SpanID] {
					p := prov[o.fs][e.Depth]
					if p == 'C' || p == 'D' {
						endFrontier = append(endFrontier, x)
						endItem[x.SpanID] = true
					}
				}
			}
		}
		// Vacate itemized ends' current writes at APPLY time only; during
		// search their old seats read as soft via the usual C/D path.
		var frontier []uint64
		for _, it := range items {
			// Thread items conscript blockers too: a likelihood item whose
			// every option is hard-blocked by un-itemized placed material
			// would otherwise be silently unseatable (a benign channel).
			// Cluster growth from this is bounded by the cap fallback.
			frontier = append(frontier, collectBlockers(it.opts)...)
		}
		for len(frontier) > 0 {
			rid := frontier[len(frontier)-1]
			frontier = frontier[:len(frontier)-1]
			if reseatRoots[rid] {
				continue
			}
			root := byID[rid]
			var opts []sOpt
			for _, fs := range states {
				if p := pathOn(root, fs); p != nil {
					opts = append(opts, sOpt{fs: fs, spans: p, rsvD: root.Depth - 1, gain: len(p) + 1})
				}
			}
			if len(opts) == 0 {
				continue // immovable in practice; its levels stay hard
			}
			reseatRoots[rid] = true
			items = append(items, sItem{span: root, isOrphan: true, reseat: true, opts: opts})
			frontier = append(frontier, collectBlockers(opts)...)
		}
		// Cluster items by shared chains (union-find over chain sets).
		parent := make(map[int]int)
		var find func(int) int
		find = func(x int) int {
			if parent[x] != x {
				parent[x] = find(parent[x])
			}
			return parent[x]
		}
		for i := range items {
			parent[i] = i
		}
		chainItem := make(map[*fragState]int)
		for i, it := range items {
			for _, o := range it.opts {
				if j, ok := chainItem[o.fs]; ok {
					parent[find(i)] = find(j)
				} else {
					chainItem[o.fs] = i
				}
			}
		}
		clusters := make(map[int][]int)
		for i := range items {
			clusters[find(i)] = append(clusters[find(i)], i)
		}
		any := false
		// solveCluster: branch-and-bound over one cluster. Returns the best
		// assignment found and whether the node budget capped (best-found
		// but uncertified). No size guard: B&B is anytime (items sorted
		// fewest-options-first, options explored before skips), so a capped
		// solve is strictly better than abandoning the cluster — and capped
		// clusters are re-solved decomposed by objective band (see loop).
		// lk keys one chain level: the search's unit of exclusivity.
		type lk struct {
			fs *fragState
			d  int
		}
		solveCluster := func(cl []sItem) []int {
			sort.Slice(cl, func(i, j int) bool { return len(cl[i].opts) < len(cl[j].opts) })
			for ci := range cl {
				if cl[ci].thread {
					continue // likelihood-only: no coverage band
				}
				for oj := range cl[ci].opts {
					cl[ci].opts[oj].gain += 1000000 // lexicographic coverage bonus
				}
			}
			// Branch-and-bound: maximize gain - displacement cost. Hard:
			// walls, rsv, B-writes; soft: A/C/D occupants (cost 1 each,
			// once per level).
			usedOcc := make(map[lk]*Span)
			usedRsv := make(map[lk]bool)
			softHit := make(map[lk]bool)
			// -inf sentinel: the optimum must ALWAYS be returned, even when
			// it carries multiple skip penalties — a nil bestAssign silently
			// freezes every item in the cluster.
			bestScore := -1 << 62
			var bestAssign []int
			cur := make([]int, len(cl))
			// DISCLOSED certification budget: 10M nodes. A handful of
			// clusters per 10k traces (measured: 3, all tight matching
			// cores — see CLUSTERDUMP) exceed any practical budget while
			// certifying among near-tied all-seated arrangements; they ship
			// their best-found COMPLETE seating (first descent finishes in
			// ~|items| nodes), are counted (capped=true) and anatomized
			// (CLUSTERDUMP fires at the cap). Invariants are enforced by
			// the same penalties and re-verified by the census; capped
			// best-found seatings have never moved a metric. Full
			// certification path (Régin-style matching filter) documented
			// as future work.
			nodes := 0
			// Simple admissible bound: sum of each remaining item's best
			// gain. (The mutual-exclusion/pigeonhole bound and the search-
			// space quotient techniques — dedup, symmetry — are documented
			// in docs/pcrs_map_solver.md as future work: they attacked the
			// capped matching cores but destroyed re-solve idempotence,
			// which is what makes capped re-roll churn self-extinguishing.)
			optimistic := make([]int, len(cl)+1)
			for i := len(cl) - 1; i >= 0; i-- {
				mx := 0
				for _, o := range cl[i].opts {
					if o.gain > mx {
						mx = o.gain
					}
				}
				optimistic[i] = optimistic[i+1] + mx
			}
			// One-shot anatomy dump for clusters that hit the cap.
			dumpComp := func() {
				no, nr, ne, nt := 0, 0, 0, 0
				minO, maxO, sumO := 1<<30, 0, 0
				for i := range cl {
					switch {
					case cl[i].reseat:
						nr++
					case cl[i].isOrphan:
						no++
					case cl[i].thread:
						nt++
					default:
						ne++
					}
					n := len(cl[i].opts)
					sumO += n
					if n < minO {
						minO = n
					}
					if n > maxO {
						maxO = n
					}
				}
				claims := make(map[lk]int)
				for _, it := range cl {
					for _, o := range it.opts {
						for _, s := range o.spans {
							claims[lk{o.fs, s.Depth}]++
						}
					}
				}
				maxC, sumC := 0, 0
				for _, c := range claims {
					if c > maxC {
						maxC = c
					}
					sumC += c
				}
				avgC := 0.0
				if len(claims) > 0 {
					avgC = float64(sumC) / float64(len(claims))
				}
				fmt.Fprintf(os.Stderr, "CLUSTERDUMP t=%x items=%d(o=%d r=%d e=%d t=%d) opts=%d/%d/%d levels=%d claims=%d/%.1f\n",
					dbgTag, len(cl), no, nr, ne, nt, minO, sumO/len(cl), maxO, len(claims), maxC, avgC)
			}
			var bt func(i, score int)
			bt = func(i, score int) {
				nodes++
				if nodes == 10000000 && debugScore {
					dumpComp()
				}
				if nodes > 10000000 {
					return // disclosed cap: keep best-found
				}
				if score+optimistic[i] <= bestScore {
					return
				}
				if i == len(cl) {
					if score > bestScore {
						bestScore = score
						bestAssign = append([]int(nil), cur...)
					}
					return
				}
				it := cl[i]
				for oi := 0; oi < len(it.opts); oi++ {
					opt := it.opts[oi]
					ok := true
					cost := 0
					var setOcc, newSoft []lk
					var rsvKey *lk
					for _, s := range opt.spans {
						k := lk{opt.fs, s.Depth}
						// Same span at the same level is the SAME assignment
						// (orphan/end alter-egos of one span), never a conflict.
						if w := usedOcc[k]; (w != nil && w != s) || usedRsv[k] {
							ok = false
							break
						}
						if led[opt.fs].rsv[s.Depth] {
							owner := rsvOwner[opt.fs][s.Depth]
							if owner == nil || !reseatRoots[owner.SpanID] {
								ok = false // wall, or non-itemized reservation
								break
							}
							// itemized owner: level vacates on apply
						}
						if x := led[opt.fs].occ[s.Depth]; x != nil && x != s {
							if prov[opt.fs][s.Depth] == 'B' {
								rid := placedRootOf[x.SpanID]
								if rid == 0 || !reseatRoots[rid] {
									ok = false
									break
								}
								// itemized: vacates on apply, no cost
							} else if !softHit[k] {
								// Lexicographic displacement: stealing an
								// end's SOLE coverage write costs at the
								// coverage band — likelihood gains can never
								// out-bid an invariant, so churn cannot
								// strand an open end.
								p := prov[opt.fs][s.Depth]
								if (p == 'C' || p == 'D') && isOpenEnd(x) && appearances(x) <= 1 {
									cost += 1000000
								} else {
									cost++
								}
								newSoft = append(newSoft, k)
							}
						}
						if usedOcc[k] == nil {
							usedOcc[k] = s
							setOcc = append(setOcc, k)
						}
					}
					if ok && opt.rsvD >= 0 {
						k := lk{opt.fs, opt.rsvD}
						if usedOcc[k] != nil {
							ok = false
						} else {
							if x := led[opt.fs].occ[opt.rsvD]; x != nil {
								if prov[opt.fs][opt.rsvD] == 'B' {
									rid := placedRootOf[x.SpanID]
									if rid == 0 || !reseatRoots[rid] {
										ok = false
									}
								} else if !softHit[k] {
									p := prov[opt.fs][opt.rsvD]
									if (p == 'C' || p == 'D') && isOpenEnd(x) && appearances(x) <= 1 {
										cost += 1000000
									} else {
										cost++
									}
									newSoft = append(newSoft, k)
								}
							}
							if ok {
								rsvKey = &k
							}
						}
					}
					if !ok {
						for _, k := range setOcc {
							delete(usedOcc, k)
						}
						continue
					}
					if rsvKey != nil {
						usedRsv[*rsvKey] = true
					}
					for _, k := range newSoft {
						softHit[k] = true
					}
					cur[i] = oi
					bt(i+1, score+opt.gain-cost)
					for _, k := range setOcc {
						delete(usedOcc, k)
					}
					if rsvKey != nil {
						delete(usedRsv, *rsvKey)
					}
					for _, k := range newSoft {
						delete(softHit, k)
					}
				}
				// Skip is explored last with a heavy penalty; reseated
				// placements must keep a seat (placement invariant), so
				// their skip is effectively forbidden.
				// Lexicographic: covering a demand dominates any possible
				// MAP delta; skip is only ever taken when literally no
				// option is consistent. Reseats must never be dropped.
				pen := 1000000000
				if cl[i].reseat {
					pen = 2000000000
				}
				if cl[i].thread {
					pen = 0 // no invariant demands threading
				}
				cur[i] = -1
				bt(i+1, score-pen)
			}
			bt(0, 0)
			// revert the coverage bonus: gains live on shared option state,
			// and any future re-solve of these items must not see it
			// double-applied
			for ci := range cl {
				if cl[ci].thread {
					continue
				}
				for oj := range cl[ci].opts {
					cl[ci].opts[oj].gain -= 1000000
				}
			}
			if debugScore {
				fmt.Fprintf(os.Stderr, "CLUSTER nodes=%d capped=%t found=%t\n", nodes, nodes > 10000000, bestAssign != nil)
			}
			return bestAssign
		}
		// applyAssign: commit one cluster's assignment to the ledger.
		// Returns whether the ledger actually changed (progress).
		applyAssign := func(cl []sItem, bestAssign []int) bool {
			progress := false
			if debugScore {
				for i, oi := range bestAssign {
					if oi >= 0 || cl[i].thread {
						continue // thread skips are free and routine
					}
					kind := "end"
					if cl[i].isOrphan {
						kind = "orphan"
					}
					if cl[i].reseat {
						kind = "reseat"
					}
					var dump strings.Builder
					fmt.Fprintf(&dump, "SEARCHSKIP t=%x %s depth=%d opts=%d cluster=%d id=%x parent=%x\n",
						dbgTag, kind, cl[i].span.Depth, len(cl[i].opts), len(cl),
						cl[i].span.SpanID, cl[i].span.ParentID)
					for _, opt := range cl[i].opts {
						hard := ""
						for _, s := range opt.spans {
							d := s.Depth
							if led[opt.fs].rsv[d] {
								owner := rsvOwner[opt.fs][d]
								if owner == nil {
									hard += fmt.Sprintf(" L%d=WALL", d)
								} else if !reseatRoots[owner.SpanID] {
									hard += fmt.Sprintf(" L%d=RSV(owner=%x)", d, owner.SpanID)
								}
							}
							if x := led[opt.fs].occ[d]; x != nil && prov[opt.fs][d] == 'B' {
								rid := placedRootOf[x.SpanID]
								if rid == 0 {
									hard += fmt.Sprintf(" L%d=B(noroot)", d)
								} else if !reseatRoots[rid] {
									hard += fmt.Sprintf(" L%d=B(root=%x)", d, rid)
								}
							}
						}
						if opt.rsvD >= 0 {
							if x := led[opt.fs].occ[opt.rsvD]; x != nil && prov[opt.fs][opt.rsvD] == 'B' {
								rid := placedRootOf[x.SpanID]
								if rid == 0 || !reseatRoots[rid] {
									hard += fmt.Sprintf(" R%d=B", opt.rsvD)
								}
							}
						}
						if hard != "" {
							fmt.Fprintf(&dump, "  SKIPOPT hard:%s\n", hard)
							continue
						}
						// base-feasible: lost to in-cluster contention — name
						// the seated competitors whose winning option overlaps
						// (same-span same-level writes are NOT conflicts)
						comp := ""
						need := make(map[int]*Span)
						for _, s := range opt.spans {
							need[s.Depth] = s
						}
						for j, oj := range bestAssign {
							if oj < 0 || j == i {
								continue
							}
							w := cl[j].opts[oj]
							if w.fs != opt.fs {
								continue
							}
							hit := false
							for _, s := range w.spans {
								if (need[s.Depth] != nil && need[s.Depth] != s) || s.Depth == opt.rsvD {
									hit = true
								}
							}
							if w.rsvD >= 0 && need[w.rsvD] != nil {
								hit = true
							}
							if hit {
								ck := "end"
								if cl[j].isOrphan {
									ck = "orphan"
								}
								if cl[j].reseat {
									ck = "reseat"
								}
								comp += fmt.Sprintf(" %s(depth=%d,opts=%d,id=%x", ck, cl[j].span.Depth, len(cl[j].opts), cl[j].span.SpanID)
								if cl[j].isOrphan {
									comp += ",path="
									for pi, s := range w.spans {
										if pi > 0 {
											comp += "+"
										}
										comp += fmt.Sprintf("%x@%d", s.SpanID, s.Depth)
									}
								}
								comp += ")"
							}
						}
						fmt.Fprintf(&dump, "  SKIPOPT contention:%s\n", comp)
					}
					os.Stderr.WriteString(dump.String())
				}
			}
			// Apply atomically: vacate reseated placements and itemized
			// end-writes first (they re-write per the assignment).
			for _, it := range cl {
				if it.isOrphan || it.thread {
					continue
				}
				for _, fs := range states {
					if led[fs].occ[it.span.Depth] == it.span {
						p := prov[fs][it.span.Depth]
						if p == 'C' || p == 'D' {
							led[fs].occ[it.span.Depth] = nil
							delete(prov[fs], it.span.Depth)
						}
					}
				}
			}
			for i, oi := range bestAssign {
				it := cl[i]
				if it.reseat && oi >= 0 {
					undoPlaced(it.span)
					placedSet[it.span.SpanID] = false
				}
			}
			for i, oi := range bestAssign {
				if oi < 0 {
					continue
				}
				it := cl[i]
				opt := it.opts[oi]
				// Change detection BEFORE clearing: a thread item re-seating
				// its incumbent verbatim is not progress (else the round
				// loop spins re-solving stable clusters to its cap).
				changed := false
				for _, s := range opt.spans {
					if led[opt.fs].occ[s.Depth] != s {
						changed = true
					}
				}
				// clear soft occupants on needed levels
				clear := func(d int) {
					if x := led[opt.fs].occ[d]; x != nil && prov[opt.fs][d] != 'B' {
						led[opt.fs].occ[d] = nil
						delete(prov[opt.fs], d)
					}
				}
				for _, s := range opt.spans {
					clear(s.Depth)
				}
				if opt.rsvD >= 0 {
					clear(opt.rsvD)
				}
				if it.isOrphan {
					commitFits(it.span, []*oFit{{fs: opt.fs, path: opt.spans}}, true)
					placedSet[it.span.SpanID] = true
					forcedRoots[it.span.SpanID] = true
					progress = true
				} else if it.thread {
					for _, s := range opt.spans {
						write(opt.fs, s, 'T') // soft: later rounds may displace
					}
					// Thread fills on open levels are monotone (absorbing);
					// verbatim re-seats are no-ops via `changed`, so counting
					// real fills as progress cannot spin the loop.
					if changed {
						progress = true
					}
				} else {
					write(opt.fs, it.span, 'D')
					res.ForcedMatches++
					progress = true
				}
			}
			return progress
		}
		for _, member := range clusters {
			cl := make([]sItem, 0, len(member))
			for _, i := range member {
				cl = append(cl, items[i])
			}
			if debugScore {
				fmt.Fprintf(os.Stderr, "CLUSTER solve items=%d\n", len(cl))
			}
			assign := solveCluster(cl)
			if assign == nil {
				continue
			}
			if applyAssign(cl, assign) {
				any = true
			}
		}
		if debugScore {
			fmt.Fprintf(os.Stderr, "SOLVE t=%x orph=%d ends=%d items=%d any=%t\n",
				dbgTag, len(demOrphans), len(demEnds), len(items), any)
		}
		return any
	}

	if debugScore {
		// one-shot orphan placement-ambiguity tally: viable windows per
		// unplaced orphan (gated in >1 window = placement the search must
		// disambiguate)
		for _, r := range unplaced {
			if placedSet[r.SpanID] {
				continue
			}
			wins := 0
			for _, fs := range states {
				if pathOn(r, fs) != nil {
					wins++
				}
			}
			if wins >= 1 {
				atomic.AddInt64(&ambigOrphans, 1)
				if wins >= 2 {
					atomic.AddInt64(&ambigOrphanMultiW, 1)
				}
			}
		}
	}

	// Plain fixpoint loop (v8h2 semantics): the deterministic search is
	// idempotent per ledger state, so capped re-solves reproduce their
	// previous assignment once demands stop changing — applies become
	// no-ops and the loop self-converges. ROUNDCAP discloses the rare
	// exit-by-exhaustion.
	converged := false
	for round := 0; round < len(states)+len(unplaced)+8; round++ {
		propagate()
		if !solveDemands() {
			converged = true
			break
		}
	}
	if debugScore && !converged {
		// Exit by round exhaustion, not stability: any state left mid-churn
		// is disclosed here rather than silently shipped.
		fmt.Fprintf(os.Stderr, "ROUNDCAP t=%x\n", dbgTag)
	}

	// --- Bookkeeping, emission, census (as ReconstructPCRB) ---
	var stillUn []uint64
	for _, r := range unplaced {
		if !placedSet[r.SpanID] {
			stillUn = append(stillUn, r.SpanID)
			if debugScore {
				// Classify why this orphan has no viable assignment:
				// ZEROOPT = no pathOn option on any chain;
				// BBLOCK  = options exist, every one collides with a 'B'
				//           write or orphan reservation (rule-B eager-commit
				//           pathology);
				// OTHER   = options exist with non-B collisions only.
				opts, bblocked := 0, 0
				for _, fs := range states {
					p := pathOn(r, fs)
					if p == nil {
						continue
					}
					opts++
					blocked := false
					check := func(d int) {
						if led[fs].rsv[d] && rsvOwner[fs][d] != nil {
							blocked = true
						}
						if x := led[fs].occ[d]; x != nil && prov[fs][d] == 'B' {
							blocked = true
						}
					}
					check(r.Depth - 1)
					for _, s := range p {
						check(s.Depth)
					}
					if blocked {
						bblocked++
					}
				}
				cls := "OTHER"
				if opts == 0 {
					cls = "ZEROOPT"
				} else if bblocked == opts {
					cls = "BBLOCK"
				}
				fmt.Fprintf(os.Stderr, "UNPLACED t=%x %s depth=%d opts=%d bblocked=%d\n", dbgTag, cls, r.Depth, opts, bblocked)
			}
		}
	}
	res.Unanchored = stillUn

	for _, fs := range states {
		anchor := fs.p.anchor
		for d := fs.p.o.Depth - 1; d > anchor.Depth; d-- {
			if s := led[fs].occ[d]; s != nil {
				anchor = s
				break
			}
		}
		res.Reconnected++
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:   fs.p.o.SpanID,
			AnchorID:   anchor.SpanID,
			Synthetic:  fs.p.o.Depth - anchor.Depth - 1,
			Ambiguous:  fs.p.ambiguous,
			ViaCarrier: fs.p.viaCarrier,
		})
	}
	for _, r := range unplaced {
		if !placedSet[r.SpanID] {
			continue
		}
		fits := orphanChains[r.SpanID]
		primary := fits[0]
		for _, f := range fits[1:] {
			if len(f.path) > len(primary.path) {
				primary = f
			}
		}
		top := primary.fs.p.anchor
		for d := r.Depth - 1; d > top.Depth; d-- {
			if s := led[primary.fs].occ[d]; s != nil && s != r {
				top = s
				break
			}
		}
		// Evidence pointer for the independent verifier: the placement
		// window's payload owner (its inherited carrier, else the
		// window-defining root itself).
		via := primary.fs.p.viaCarrier
		if via == 0 {
			via = primary.fs.p.o.SpanID
		}
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:   r.SpanID,
			AnchorID:   top.SpanID,
			Synthetic:  r.Depth - top.Depth - 1,
			Forced:     forcedRoots[r.SpanID],
			ViaCarrier: via,
		})
		res.Reconnected++
	}

	unplacedSet := make(map[uint64]bool)
	for _, id := range stillUn {
		queue := []*Span{byID[id]}
		for len(queue) > 0 {
			s := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			unplacedSet[s.SpanID] = true
			queue = append(queue, children[s.SpanID]...)
		}
	}
	// CANDCHECK: for spans named in TRACE_RECON_DEBUG_ENDS, report their
	// evidence vs candidacy vs occupancy in every depth-eligible window.
	if len(debugEnds) > 0 {
		for id := range debugEnds {
			s, ok := byID[id]
			if !ok {
				continue
			}
			for _, fs := range states {
				if fs.p.threadBits == nil || s.Depth <= fs.p.anchor.Depth || s.Depth >= fs.p.o.Depth {
					continue
				}
				bf := fs.bf
				hex := bridge.HexOf(s.SpanID)
				inC := false
				for _, c := range fs.cand[s.Depth] {
					if c == s {
						inC = true
						break
					}
				}
				x := led[fs].occ[s.Depth]
				occ := "nil"
				if x == s {
					occ = "me"
				} else if x != nil {
					occ = fmt.Sprintf("%x/%c", x.SpanID, prov[fs][s.Depth])
				}
				nconsider := 0
				meConsider := false
				for _, c := range fs.cand[s.Depth] {
					if c.LeafCarrier || c == fs.p.o {
						continue
					}
					ch := bridge.HexOf(c.SpanID)
					if !bf.Test(ch[:]) || !chainConsistent(bf, c, byID, fs.p.anchor.Depth+1) {
						continue
					}
					nconsider++
					if c == s {
						meConsider = true
					}
				}
				fmt.Fprintf(os.Stderr, "CANDCHECK t=%x id=%x depth=%d wanchor=%x adepth=%d pos=%t cand=%t occ=%s rsv=%t ncand=%d nconsider=%d meConsider=%t\n",
					dbgTag, s.SpanID, s.Depth, fs.p.anchor.SpanID, fs.p.anchor.Depth, bf.Test(hex[:]), inC, occ,
					led[fs].rsv[s.Depth], len(fs.cand[s.Depth]), nconsider, meConsider)
			}
		}
	}
	inLedger := make(map[uint64]bool)
	for _, fs := range states {
		for d, s := range led[fs].occ {
			if s != nil {
				inLedger[s.SpanID] = true
				if debugEnds[s.SpanID] {
					fmt.Fprintf(os.Stderr, "ENDMATCH t=%x id=%x level=%d prov=%c window-anchor=%x adepth=%d carrier=%x cdepth=%d\n",
						dbgTag, s.SpanID, d, prov[fs][d], fs.p.anchor.SpanID, fs.p.anchor.Depth, fs.p.o.SpanID, fs.p.o.Depth)
				}
			}
		}
		inLedger[fs.p.o.SpanID] = true
	}
	for i := range survivors {
		e := &survivors[i]
		if !isOpenEnd(e) || unplacedSet[e.SpanID] {
			continue
		}
		isOrphanMember := false
		if placedSet[e.SpanID] || orphanRootSet[e.SpanID] {
			isOrphanMember = true
		} else {
			cur := e
			for {
				p, ok := byID[cur.ParentID]
				if !ok {
					break
				}
				cur = p
			}
			if placedSet[cur.SpanID] {
				isOrphanMember = true
			}
		}
		res.OpenEnds++
		if inLedger[e.SpanID] {
			res.OpenEndsMatched++
		} else if isOrphanMember {
			res.OpenEnds--
			res.OrphanOpenEnds++
		} else if debugScore {
			// DRYEND forensics: why did no chain take this open end?
			gates, openLv, occA, occB, occCD, inCand := 0, 0, 0, 0, 0, 0
			for _, fs := range states {
				for _, c := range fs.cand[e.Depth] {
					if c == e {
						inCand++
						break
					}
				}
				if !gate(e, fs) {
					continue
				}
				gates++
				if open(fs, e.Depth) {
					openLv++
				} else if led[fs].rsv[e.Depth] {
					occB++ // reservation blocks
				} else {
					switch prov[fs][e.Depth] {
					case 'A':
						occA++
					case 'B':
						occB++
					default:
						occCD++
					}
				}
			}
			fmt.Fprintf(os.Stderr, "DRYEND depth=%d gates=%d openLv=%d occA=%d occB=%d occCD=%d inCand=%d\n",
				e.Depth, gates, openLv, occA, occB, occCD, inCand)
		}
	}
	return res
}
