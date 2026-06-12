package recon

import (
	"encoding/binary"
	"errors"
	"sort"

	"bridges/bloom"
	"bridges/bridge"
)

// PCRB reconstruction = PCR anchoring + bloom-scoped threading.
//
// Pass 1 anchors every fragment exactly as PCR does: the truncated
// checkpoint-root prefix names the anchor; the bloom plays no part. All of
// PCR's guarantees carry over (wrong-checkpoint rate = prefix collisions,
// carrier-less fragments recorded lost).
//
// Pass 2 (threading v4) recovers intra-window nesting: a negative-pruned
// DFS from the anchored checkpoint enumerates every bloom-consistent
// candidate per window level (see the implementation comment at the pass-2
// block), then a top-down level walk attaches the fragment under the
// deepest uniquely-supported candidate, stopping at any ambiguity. Wrong
// edges require an FP that is simultaneously unique at its level, deeper
// than every true surviving candidate, and (for fragment entries) passes
// the dropped-parent gate.
//
// NewPCRBConfig builds a Config with PCRB bloom geometry: sized for
// PCRBBloomCapacity(cpd) = cpd-1 entries, NOT cpd as PB uses — the handler
// guarantees the loaded population never exceeds that (no checkpoint entry,
// inherited/pre-self emission). Keep both sides on this constructor.
func NewPCRBConfig(cpd, prefixLen int, bloomFPRate float64) Config {
	if cpd < 1 {
		cpd = 1
	}
	m, k := bloom.EstimateParameters(bridge.PCRBBloomCapacity(cpd), bloomFPRate)
	return Config{CPD: cpd, BloomM: m, BloomK: k, PrefixLen: prefixLen}
}

func ReconstructPCRB(survivors []Span, cfg Config) Result {
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

	var orphans []*Span
	for i := range survivors {
		o := &survivors[i]
		if o.ParentID == 0 {
			continue
		}
		if _, ok := byID[o.ParentID]; ok {
			continue
		}
		orphans = append(orphans, o)
	}

	// --- Pass 1: PCR anchoring (identical logic to ReconstructPCR) ---
	type anchored struct {
		o          *Span
		anchor     *Span
		threadBits []byte // bloom usable for threading: o's own, or the carrier's (see coveringPCRBPayload)
		ambiguous  bool
		viaCarrier uint64
	}
	var placed []anchored
	// fragRoots: co-window fragments anchored at each checkpoint — pass-2
	// threading candidates alongside real children.
	fragRoots := make(map[uint64][]*Span)

	var res Result
	res.Orphans = len(orphans)
	for _, o := range orphans {
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

	// --- Pass 2: threading v6 — candidate filling, elimination, fixpoint ---
	//
	// Phase A (fill + eliminate): per fragment, a negative-pruned DFS from
	// its anchored checkpoint enumerates every bloom-consistent candidate
	// per window level. Eliminations happen during filling: bloom-negative
	// branches are cut (ancestors form a chain; no false negatives), leaf
	// carriers are excluded (payload at a non-checkpoint depth = provably a
	// leaf = nothing can attach at it), co-window fragment entries are gated
	// by their nameable dropped-parent ID, and levels are bounded to
	// [W+1, root.depth-2] (root.depth-1 holds only chain-immune siblings of
	// the dropped parent).
	//
	// Phase B (forward walk): per fragment, walk levels top-down; unique
	// candidate -> accept; zero -> continue (or stop, cfg.StopOnGap); >1 ->
	// AMBIGUOUS: the fragment is left uncommitted at its deepest certain
	// position. Unambiguous walks commit immediately.
	//
	// Phase C (must-receive fixpoint): an open end — surviving non-carrier
	// with zero surviving children — provably lost all its children, and at
	// least one fragment must attach EXACTLY at it (nothing survives below
	// it, so any fragment whose path passes through it has it as nearest
	// surviving ancestor). Its true receiver always appears among its
	// claimants (no false negatives), so an unsatisfied open end with
	// exactly ONE uncommitted claimant commits that claimant to it — even
	// past the claimant's forward ambiguity. Each commitment removes the
	// fragment from other claimant pools, which can make further open ends
	// uniquely claimed: iterate to fixpoint (commitments are monotone;
	// terminates in <= #fragments rounds). Commitments are per-fragment;
	// one open end may legitimately receive several fragments (it lost
	// several children). Fragments still uncommitted at the fixpoint keep
	// their deepest certain position: a flagged benign skip, never a guess.
	type fragState struct {
		p         anchored
		cand      map[int][]*Span // level -> eliminated candidate sets
		pos       *Span           // deepest certain position so far
		committed bool
		forced    bool // committed by the forced-resolution tier
	}
	states := make([]*fragState, 0, len(placed))
	for i := range placed {
		p := placed[i]
		fs := &fragState{p: p, pos: p.anchor, committed: true}
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
					return // provably a leaf: structurally impossible attachment
				}
				hex := bridge.HexOf(s.SpanID)
				if !bf.Test(hex[:]) {
					return // negative: provably not an ancestor; cut the branch
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
					continue // dropped-parent gate failed
				}
				dfs(r)
			}
			for d := p.anchor.Depth + 1; d <= maxD; d++ {
				cands := fs.cand[d]
				if len(cands) == 0 {
					if cfg.StopOnGap {
						break
					}
					continue
				}
				if len(cands) > 1 {
					fs.committed = false // ambiguous: eligible for must-receive resolution
					break
				}
				fs.pos = cands[0]
			}
		}
		states = append(states, fs)
	}

	// --- v7.3: level-ledger joint fixpoint ---
	//
	// One shared structure, every constraint talking to every other
	// (Tomislav's thesis: simultaneous constraints multiply FP suppression
	// and guarantee mutual success — the true configuration satisfies all
	// invariants at once, so invariant blockage proves a wrong commitment).
	//
	// Each anchored fragment owns a CHAIN: levels (W.depth, root.depth-1]
	// from its pass-1 window checkpoint down to its root. Every level holds
	// exactly one occupant: a real span, a reservation (the level above any
	// fragment/orphan root — its dropped parent), or remains an open
	// synthetic. A span may legitimately occupy the same level in MANY
	// chains (every fragment below it threads through it); orphan rigidity
	// constrains placements to a single window.
	//
	// Move rules, iterated to fixpoint:
	//   A (collapse): a level's unique viable claimant is written in.
	//     Claimants = carrier candidates (pass-2 DFS: bloom-positive,
	//     chain-consistent, leaf-excluded, reachability-gated) plus spans
	//     of orphans already placed in the same window. No-false-negatives
	//     means a true occupant always volunteers (A->B->C collapses).
	//   B (orphan binding): v7.2 unification — strict bindings into
	//     ledger-open carrier-undecided levels, same-window coalescing,
	//     cross-window contradiction, corroboration for contests, widening
	//     only in the must-place ladder.
	//   C (coverage): an unmatched open end is written into every chain
	//     where it is a gated candidate at an open level (multi-write is
	//     correct: an open end is an ancestor of every fragment below it).
	//
	// Invariants: every open end appears in >=1 ledger; every orphan is
	// placed. Residual blockage is counted, flagged, and reported.
	type chainLedger struct {
		occ map[int]*Span // depth -> occupant span
		rsv map[int]bool  // depth -> reserved synthetic (dropped parent slots)
	}
	led := make(map[*fragState]*chainLedger, len(states))
	prov := make(map[*fragState]map[int]byte) // write provenance: 'A' collapse, 'B' orphan placement, 'C'/'D' coverage
	for _, fs := range states {
		l := &chainLedger{occ: make(map[int]*Span), rsv: map[int]bool{fs.p.o.Depth - 1: true}}
		led[fs] = l
		prov[fs] = make(map[int]byte)
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

	// Window partition of chains, and the dynamic same-window orphan-span
	// candidate pool (rule A sources).
	chainsByW := make(map[uint64][]*fragState)
	for _, fs := range states {
		chainsByW[fs.p.anchor.SpanID] = append(chainsByW[fs.p.anchor.SpanID], fs)
	}
	orphanSpansByW := make(map[uint64]map[int][]*Span) // W -> depth -> placed orphan spans
	orphanRoots := make(map[uint64]bool)               // orphan ROOT span IDs (need d-1 reservation)

	// Orphan machinery (rule B), adapted from v7.2 to ledger occupancy.
	type oFit struct {
		fs   *fragState
		path []*Span
	}
	occUpper := make(map[*fragState][]*oFit)
	orphanChains := make(map[uint64][]*oFit)
	slotOK := func(fs *fragState, d int, wide bool) bool {
		if !open(fs, d) {
			return false
		}
		if wide {
			return true
		}
		return len(fs.cand[d]) == 0 // strict: carrier-undecided levels only
	}
	tryFit := func(r *Span, fs *fragState, wide bool) *oFit {
		if fs.p.threadBits == nil || r == fs.p.o {
			return nil
		}
		if r.Depth > fs.p.o.Depth-2 {
			return nil
		}
		if !slotOK(fs, r.Depth-1, wide) || !slotOK(fs, r.Depth, wide) {
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
				if c.Depth > fs.p.o.Depth-2 || !slotOK(fs, c.Depth, wide) {
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
		return &oFit{fs: fs, path: path}
	}
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
			for _, s := range f.path {
				write(f.fs, s, 'B')
				if !seen[s.SpanID] {
					seen[s.SpanID] = true
					pool[s.Depth] = append(pool[s.Depth], s)
				}
			}
			occUpper[f.fs] = append(occUpper[f.fs], f)
		}
		// Every span of the orphan (path or side branch) joins the window
		// pool so rule A can collapse other chains through it.
		queue := []*Span{r}
		for len(queue) > 0 {
			s := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if !seen[s.SpanID] {
				seen[s.SpanID] = true
				pool[s.Depth] = append(pool[s.Depth], s)
			}
			queue = append(queue, children[s.SpanID]...)
		}
		orphanRoots[r.SpanID] = true
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
	resolveOrphans := func(wide bool) bool {
		any := false
		for changed := true; changed; {
			changed = false
			for _, r := range unplaced {
				if placedSet[r.SpanID] {
					continue
				}
				byW := make(map[uint64][]*oFit)
				for _, fs := range states {
					if f := tryFit(r, fs, wide); f != nil {
						byW[fs.p.anchor.SpanID] = append(byW[fs.p.anchor.SpanID], f)
					}
				}
				commit := func(fits []*oFit, forced bool) {
					commitFits(r, fits, forced)
					placedSet[r.SpanID] = true
					changed, any = true, true
				}
				if len(byW) == 1 {
					for _, fits := range byW {
						commit(fits, wide)
					}
				} else if len(byW) > 1 {
					// Contradiction: corroboration decides (witnesses, then
					// total path testimony). Flagged.
					var bestW uint64
					bestN, bestSpans := -1, -1
					for w, fits := range byW {
						spans := 0
						for _, f := range fits {
							spans += len(f.path)
						}
						if len(fits) > bestN ||
							(len(fits) == bestN && (spans > bestSpans || (spans == bestSpans && w < bestW))) {
							bestW, bestN, bestSpans = w, len(fits), spans
						}
					}
					commit(byW[bestW], true)
				}
			}
		}
		return any
	}

	// Open-end census over carrier-connected material; placed-orphan
	// internal open ends join dynamically (rule C checks them each round).
	isOpenEnd := func(s *Span) bool {
		return s.CkptPrefix == nil && len(children[s.SpanID]) == 0
	}
	// --- The joint fixpoint ---
	maxRounds := len(states) + len(unplaced) + 8
	for round := 0; round < maxRounds; round++ {
		changed := false
		// Rule A: unique-claimant collapse, per chain per level.
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
					if orphanRoots[c.SpanID] && !open(fs, c.Depth-1) && !led[fs].rsv[c.Depth-1] {
						return // orphan root needs its parent slot synthetic
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
					if orphanRoots[hit.SpanID] {
						led[fs].rsv[hit.Depth-1] = true
					}
					if write(fs, hit, 'A') {
						changed = true
					}
				}
			}
		}
		// Rule B: strict orphan binding + unification.
		if resolveOrphans(false) {
			changed = true
		}
		// Rule C: coverage — write each unmatched open end into every chain
		// where it is a gated candidate at an open level. Open ends include
		// placed-orphan internals (they are in the candidate pools).
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
		if changed {
			continue
		}
		// Escalation 1: must-place ladder (widened orphan bindings).
		if resolveOrphans(true) {
			continue
		}
		// Escalation 2 (v7.3.2, rule D): generalized recursive displacement
		// over the JOINT constraint graph. Demands = unmatched open ends AND
		// unplaced orphans; supply = ledger levels; moves = drop (when the
		// occupant's coverage is not solely demanded here) or re-home
		// (recursively displacing the destination). Provenance-legal:
		// 'B' orphan-placement writes and reservations are immovable.
		// Dropped A/C evidence is self-healing — rule A re-collapses it on
		// the next round wherever it is still uniquely supported. Every
		// displacement is flagged; empirically displacement CORRECTS FPs
		// (must-receive/must-place are theorem-backed; the displaced write
		// rests on a single membership test).
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
		type lvlKey struct {
			fs *fragState
			d  int
		}
		var displace func(fs *fragState, d int, visited map[lvlKey]bool) bool
		displace = func(fs *fragState, d int, visited map[lvlKey]bool) bool {
			if d <= fs.p.anchor.Depth || d >= fs.p.o.Depth || led[fs].rsv[d] {
				return false
			}
			x := led[fs].occ[d]
			if x == nil {
				return true
			}
			if prov[fs][d] == 'B' {
				return false // unification-backed: immovable here
			}
			k := lvlKey{fs, d}
			if visited[k] {
				return false
			}
			visited[k] = true
			// Free drop: the occupant's coverage is not solely demanded here.
			if !(isOpenEnd(x) && appearances(x) == 1) {
				led[fs].occ[d] = nil
				delete(prov[fs], d)
				return true
			}
			// Sole-coverage open end: re-home it first (recursively).
			for _, fs2 := range states {
				if fs2 == fs || !gate(x, fs2) {
					continue
				}
				if led[fs2].occ[x.Depth] == nil && open(fs2, x.Depth) || displace(fs2, x.Depth, visited) {
					write(fs2, x, 'D')
					led[fs].occ[d] = nil
					delete(prov[fs], d)
					res.ForcedMatches++
					return true
				}
			}
			return false
		}
		ladder := false
		// Demand class 1: unmatched open ends.
		for i := range survivors {
			e := &survivors[i]
			if !isOpenEnd(e) || appearances(e) > 0 {
				continue
			}
			for _, fs := range states {
				if !gate(e, fs) {
					continue
				}
				if open(fs, e.Depth) || displace(fs, e.Depth, make(map[lvlKey]bool)) {
					write(fs, e, 'D')
					res.ForcedMatches++
					ladder = true
					break
				}
			}
		}
		// Demand class 2: unplaced orphans — force-fit with displacement.
		// Path walk treats displaceable levels as viable, then clears them.
		slotForce := func(fs *fragState, d int) bool {
			if d <= fs.p.anchor.Depth || d >= fs.p.o.Depth || led[fs].rsv[d] {
				return false
			}
			return led[fs].occ[d] == nil || prov[fs][d] != 'B'
		}
		for _, r := range unplaced {
			if placedSet[r.SpanID] {
				continue
			}
			for _, fs := range states {
				if fs.p.threadBits == nil || r == fs.p.o || r.Depth > fs.p.o.Depth-2 {
					continue
				}
				if !slotForce(fs, r.Depth-1) || !slotForce(fs, r.Depth) {
					continue
				}
				bf := bloom.Deserialize(fs.p.threadBits, cfg.BloomM, cfg.BloomK)
				ph := bridge.HexOf(r.ParentID)
				rh := bridge.HexOf(r.SpanID)
				if !bf.Test(ph[:]) || !bf.Test(rh[:]) {
					continue
				}
				path := []*Span{r}
				cur := r
				for {
					var next *Span
					n := 0
					for _, c := range children[cur.SpanID] {
						if c.Depth > fs.p.o.Depth-2 || !slotForce(fs, c.Depth) {
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
				// Clear every needed level (reservation slot + path levels).
				ok := true
				visited := make(map[lvlKey]bool)
				if led[fs].occ[r.Depth-1] != nil && !displace(fs, r.Depth-1, visited) {
					ok = false
				}
				for _, s := range path {
					if !ok {
						break
					}
					if led[fs].occ[s.Depth] != nil && !displace(fs, s.Depth, visited) {
						ok = false
					}
				}
				if !ok {
					continue
				}
				commitFits(r, []*oFit{{fs: fs, path: path}}, true)
				placedSet[r.SpanID] = true
				ladder = true
				break
			}
		}
		if ladder {
			continue
		}
		break
	}

	// Bookkeeping: unplaced residue, discarded set for scoring.
	var stillUn []uint64
	for _, r := range unplaced {
		if !placedSet[r.SpanID] {
			stillUn = append(stillUn, r.SpanID)
		}
	}
	res.Unanchored = stillUn

	// Emission: a fragment's anchor is the deepest occupant of its chain;
	// each placed orphan root bridges to the deepest occupant above it on
	// its primary (longest-path) chain.
	for _, fs := range states {
		anchor := fs.p.anchor
		for d := fs.p.o.Depth - 1; d > anchor.Depth; d-- {
			if s := led[fs].occ[d]; s != nil {
				anchor = s
				break
			}
		}
		fs.pos = anchor
		res.Reconnected++
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:   fs.p.o.SpanID,
			AnchorID:   anchor.SpanID,
			Synthetic:  fs.p.o.Depth - anchor.Depth - 1,
			Ambiguous:  fs.p.ambiguous,
			ViaCarrier: fs.p.viaCarrier,
			Forced:     fs.forced,
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
		})
		res.Reconnected++
	}

	// Census: open ends across carrier material AND placed orphans; an open
	// end is matched iff it occupies a ledger level somewhere. Orphan-
	// internal open ends of UNPLACED orphans stay out (their fragments are
	// discarded); OrphanOpenEnds reports placed-orphan internals that no
	// chain collapsed through (still pending work, now rare).
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
		if placedSet[e.SpanID] || orphanRoots[e.SpanID] {
			isOrphanMember = true
		} else {
			// member of a placed orphan iff its fragment root is a placed root
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
			res.OpenEnds-- // tracked separately, pending
			res.OrphanOpenEnds++
		}
	}
	return res
}

// coveringPCRBPayload mirrors coveringPrefix, returning both halves of the
// covering payload. The bloom is usable for threading even when borrowed:
// an in-band carrier c descends from o within the same window, so at depths
// above o the true members of c's window bloom are EXACTLY o's ancestors
// (the shared window path). A checkpoint carrier at the band edge
// contributes its pre-reset bloom, which covers the same window. The
// borrowed bloom has more entries below o's depth, but those sit outside
// the threading range and only matter through the (unchanged) FP rate.
func coveringPCRBPayload(o *Span, children map[uint64][]*Span, cfg Config) (int, []byte, []byte, uint64) {
	if o.CkptPrefix != nil {
		return o.Depth, o.CkptPrefix, o.BloomBits, 0
	}
	band := (o.Depth/cfg.CPD + 1) * cfg.CPD
	queue := children[o.SpanID]
	for len(queue) > 0 {
		var next []*Span
		for _, s := range queue {
			if s.Depth > band {
				continue
			}
			if s.CkptPrefix != nil {
				return s.Depth, s.CkptPrefix, s.BloomBits, s.SpanID
			}
			next = append(next, children[s.SpanID]...)
		}
		queue = next
	}
	return 0, nil, nil, 0
}

// DecodePCRBPayload parses a PCRB _br value:
// type(1) || varint(depth) || ckptK || bloom bits.
func DecodePCRBPayload(p []byte, cfg Config) (depth int, prefix, bloomBits []byte, err error) {
	if len(p) < 2 || p[0] != byte(bridge.PCRBBridgeTypeID) {
		return 0, nil, nil, errors.New("recon: not a PCRB payload")
	}
	d, n := binary.Uvarint(p[1:])
	if n <= 0 {
		return 0, nil, nil, errors.New("recon: bad depth varint")
	}
	rest := p[1+n:]
	bloomLen := int((cfg.BloomM + 7) / 8)
	if len(rest) != cfg.PrefixLen+bloomLen {
		return 0, nil, nil, errors.New("recon: pcrb payload length mismatch")
	}
	return int(d), rest[:cfg.PrefixLen], rest[cfg.PrefixLen:], nil
}
