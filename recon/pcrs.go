package recon

import (
	"fmt"
	"os"
	"sort"

	"bridges/bloom"
	"bridges/bridge"
)

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

	// --- Candidate filling (identical eliminations to ReconstructPCRB) ---
	states := make([]*fragState, 0, len(placed))
	for i := range placed {
		p := placed[i]
		fs := &fragState{p: p}
		maxD := p.o.Depth - 2
		if p.threadBits != nil && maxD > p.anchor.Depth {
			bf := bloom.Deserialize(p.threadBits, cfg.BloomM, cfg.BloomK)
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
		}
		states = append(states, fs)
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
		bf := bloom.Deserialize(fs.p.threadBits, cfg.BloomM, cfg.BloomK)
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
		bf := bloom.Deserialize(fs.p.threadBits, cfg.BloomM, cfg.BloomK)
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
					bf := bloom.Deserialize(fs.p.threadBits, cfg.BloomM, cfg.BloomK)
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
				bf := bloom.Deserialize(fs.p.threadBits, cfg.BloomM, cfg.BloomK)
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
		if len(demOrphans) == 0 && len(demEnds) == 0 {
			return false
		}
		// Build items with options across ALL gated chains (soft window
		// membership is native: the domain spans windows).
		var items []sItem
		for _, r := range demOrphans {
			var opts []sOpt
			for _, fs := range states {
				if p := pathOn(r, fs); p != nil {
					opts = append(opts, sOpt{fs: fs, spans: p, rsvD: r.Depth - 1, gain: len(p) + 1})
				}
			}
			if len(opts) > 0 {
				items = append(items, sItem{span: r, isOrphan: true, opts: opts})
			}
		}
		for _, e := range demEnds {
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
		for _, it := range items {
			if it.isOrphan {
				continue
			}
			for _, o := range it.opts {
				if x := led[o.fs].occ[it.span.Depth]; x != nil && x != it.span && isOpenEnd(x) && !endItem[x.SpanID] {
					p := prov[o.fs][it.span.Depth]
					if p == 'C' || p == 'D' {
						endFrontier = append(endFrontier, x)
						endItem[x.SpanID] = true
					}
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
		for _, member := range clusters {
			cl := make([]sItem, 0, len(member))
			for _, i := range member {
				cl = append(cl, items[i])
			}
			if len(cl) > 256 {
				if debugScore {
					fmt.Fprintf(os.Stderr, "CLUSTER skip items=%d\n", len(cl))
				}
				continue // combinatorial guard
			}
			if debugScore {
				fmt.Fprintf(os.Stderr, "CLUSTER solve items=%d\n", len(cl))
			}
			sort.Slice(cl, func(i, j int) bool { return len(cl[i].opts) < len(cl[j].opts) })
			for ci := range cl {
				for oj := range cl[ci].opts {
					cl[ci].opts[oj].gain += 1000000 // lexicographic coverage bonus
				}
			}
			// Branch-and-bound: maximize gain - displacement cost. Hard:
			// walls, rsv, B-writes; soft: A/C/D occupants (cost 1 each,
			// once per level).
			type lk struct {
				fs *fragState
				d  int
			}
			usedOcc := make(map[lk]bool)
			usedRsv := make(map[lk]bool)
			softHit := make(map[lk]bool)
			bestScore := -1 << 30
			var bestAssign []int
			cur := make([]int, len(cl))
			budget := 5000000
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
			var bt func(i, score int)
			bt = func(i, score int) {
				if budget <= 0 {
					return
				}
				budget--
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
				for oi, opt := range it.opts {
					ok := true
					cost := 0
					var occKeys, newSoft []lk
					var rsvKey *lk
					for _, s := range opt.spans {
						k := lk{opt.fs, s.Depth}
						if usedOcc[k] || usedRsv[k] {
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
						if x := led[opt.fs].occ[s.Depth]; x != nil {
							if prov[opt.fs][s.Depth] == 'B' {
								rid := placedRootOf[x.SpanID]
								if rid == 0 || !reseatRoots[rid] {
									ok = false
									break
								}
								// itemized: vacates on apply, no cost
							} else if !softHit[k] {
								cost++
								newSoft = append(newSoft, k)
							}
						}
						occKeys = append(occKeys, k)
					}
					if ok && opt.rsvD >= 0 {
						k := lk{opt.fs, opt.rsvD}
						if usedOcc[k] {
							ok = false
						} else {
							if x := led[opt.fs].occ[opt.rsvD]; x != nil {
								if prov[opt.fs][opt.rsvD] == 'B' {
									rid := placedRootOf[x.SpanID]
									if rid == 0 || !reseatRoots[rid] {
										ok = false
									}
								} else if !softHit[k] {
									cost++
									newSoft = append(newSoft, k)
								}
							}
							if ok {
								rsvKey = &k
							}
						}
					}
					if !ok {
						continue
					}
					for _, k := range occKeys {
						usedOcc[k] = true
					}
					if rsvKey != nil {
						usedRsv[*rsvKey] = true
					}
					for _, k := range newSoft {
						softHit[k] = true
					}
					cur[i] = oi
					bt(i+1, score+opt.gain-cost)
					for _, k := range occKeys {
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
				cur[i] = -1
				bt(i+1, score-pen)
			}
			bt(0, 0)
			if debugScore {
				fmt.Fprintf(os.Stderr, "CLUSTER nodes=%d capped=%t found=%t\n", 5000000-budget, budget <= 0, bestAssign != nil)
			}
			if bestAssign == nil {
				continue
			}
			if debugScore {
				for i, oi := range bestAssign {
					if oi < 0 && cl[i].isOrphan && !cl[i].reseat {
						fmt.Fprintf(os.Stderr, "SEARCHSKIP orphan depth=%d opts=%d cluster=%d\n",
							cl[i].span.Depth, len(cl[i].opts), len(cl))
					}
				}
			}
			// Apply atomically: vacate reseated placements and itemized
			// end-writes first (they re-write per the assignment).
			for _, it := range cl {
				if it.isOrphan {
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
				} else {
					write(opt.fs, it.span, 'D')
					res.ForcedMatches++
				}
				any = true
			}
		}
		return any
	}

	for round := 0; round < len(states)+len(unplaced)+8; round++ {
		propagate()
		if !solveDemands() {
			break
		}
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
				fmt.Fprintf(os.Stderr, "UNPLACED %s opts=%d bblocked=%d\n", cls, opts, bblocked)
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
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:  r.SpanID,
			AnchorID:  top.SpanID,
			Synthetic: r.Depth - top.Depth - 1,
			Forced:    forcedRoots[r.SpanID],
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
	inLedger := make(map[uint64]bool)
	for _, fs := range states {
		for _, s := range led[fs].occ {
			if s != nil {
				inLedger[s.SpanID] = true
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
