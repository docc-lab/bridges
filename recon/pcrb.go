package recon

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
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
	prov := make(map[*fragState]map[int]byte)      // write provenance: 'A' collapse, 'B' orphan placement, 'C'/'D' coverage
	rsvOwner := make(map[*fragState]map[int]*Span) // reserving orphan root per level; nil/absent = the chain's own dropped-parent wall
	for _, fs := range states {
		l := &chainLedger{occ: make(map[int]*Span), rsv: map[int]bool{fs.p.o.Depth - 1: true}}
		led[fs] = l
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
	rebindCount := make(map[uint64]int) // anti-ping-pong cap across rounds
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
		// Escalation 2 (v7.3.4, rule D): FULL mutual movability to fixpoint.
		// Demands (unmatched open ends, unplaced orphans) displace through
		// any blocker: A/C/D writes drop or re-home; B-writes AND orphan
		// reservations move by RE-BINDING the owning orphan — the whole
		// coalesced placement, rigidity preserved, with rollback if no
		// alternative fit exists. The only absolute wall is a chain's own
		// dropped-parent slot (truth-backed). A per-orphan rebind cap
		// prevents cross-round ping-pong. Spacing theorem note: a demand
		// colliding with a reservation means a surviving span and a
		// dropped parent claim one slot — impossible in truth, so one
		// placement is FP and SHOULD be contested.
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
		ownerRoot := func(s *Span) *Span {
			cur := s
			for {
				p, ok := byID[cur.ParentID]
				if !ok {
					break
				}
				cur = p
			}
			if placedSet[cur.SpanID] {
				return cur
			}
			return nil
		}
		orphanSpanSet := func(root *Span) map[uint64]bool {
			set := make(map[uint64]bool)
			queue := []*Span{root}
			for len(queue) > 0 {
				s := queue[len(queue)-1]
				queue = queue[:len(queue)-1]
				set[s.SpanID] = true
				queue = append(queue, children[s.SpanID]...)
			}
			return set
		}
		// undoOrphan removes a placement completely: every ledger write of
		// any of the orphan's spans on ANY chain (cross-chain rule-A
		// collapses included), its reservations, occupancy records, and
		// pool entries.
		undoOrphan := func(root *Span) []*oFit {
			old := orphanChains[root.SpanID]
			set := orphanSpanSet(root)
			for _, fs2 := range states {
				for d, s := range led[fs2].occ {
					if s != nil && set[s.SpanID] {
						led[fs2].occ[d] = nil
						delete(prov[fs2], d)
					}
				}
				for d, owner := range rsvOwner[fs2] {
					if owner == root {
						shared := false
						for _, of := range occUpper[fs2] {
							if of.path[0] != root && of.path[0].Depth == root.Depth {
								shared = true
								break
							}
						}
						if !shared {
							delete(led[fs2].rsv, d)
						}
						delete(rsvOwner[fs2], d)
					}
				}
				fits := occUpper[fs2][:0]
				for _, of := range occUpper[fs2] {
					if of.path[0] != root {
						fits = append(fits, of)
					}
				}
				occUpper[fs2] = fits
			}
			for _, f := range old {
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
			return old
		}
		type lvlKey struct {
			fs *fragState
			d  int
		}
		var displace func(fs *fragState, d int, visited map[lvlKey]bool) bool
		var rebindOrphan func(root *Span, visited map[lvlKey]bool) bool
		slotForce := func(fs *fragState, d int) bool {
			return d > fs.p.anchor.Depth && d < fs.p.o.Depth
		}
		forceFitOn := func(r *Span, fs *fragState, visited map[lvlKey]bool) *oFit {
			if fs.p.threadBits == nil || r == fs.p.o || r.Depth > fs.p.o.Depth-2 {
				return nil
			}
			if !slotForce(fs, r.Depth-1) || !slotForce(fs, r.Depth) {
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
			// Clear the reservation slot and every path level.
			need := []int{r.Depth - 1}
			for _, s := range path {
				need = append(need, s.Depth)
			}
			for _, d := range need {
				if led[fs].rsv[d] || led[fs].occ[d] != nil {
					if !displace(fs, d, visited) {
						return nil
					}
				}
			}
			return &oFit{fs: fs, path: path}
		}
		rebindOrphan = func(root *Span, visited map[lvlKey]bool) bool {
			if rebindCount[root.SpanID] >= 3 {
				return false // anti-ping-pong cap
			}
			oldChains := make(map[*fragState]bool)
			for _, f := range orphanChains[root.SpanID] {
				oldChains[f.fs] = true
			}
			old := undoOrphan(root)
			rebindCount[root.SpanID]++
			for _, fs2 := range states {
				if oldChains[fs2] {
					continue // fixed depths: same chain = same conflict
				}
				if f := forceFitOn(root, fs2, visited); f != nil {
					commitFits(root, []*oFit{f}, true)
					return true
				}
			}
			// No alternative: roll the old placement back.
			commitFits(root, old, true)
			return false
		}
		displace = func(fs *fragState, d int, visited map[lvlKey]bool) bool {
			if d <= fs.p.anchor.Depth || d >= fs.p.o.Depth {
				return false
			}
			k := lvlKey{fs, d}
			if visited[k] {
				return false
			}
			visited[k] = true
			if led[fs].rsv[d] {
				owner := rsvOwner[fs][d]
				if owner == nil {
					return false // the chain's own dropped-parent slot: truth-backed wall
				}
				if rebindOrphan(owner, visited) && !led[fs].rsv[d] {
					return led[fs].occ[d] == nil
				}
				return false
			}
			x := led[fs].occ[d]
			if x == nil {
				return true
			}
			if prov[fs][d] == 'B' {
				root := ownerRoot(x)
				if root != nil && rebindOrphan(root, visited) && led[fs].occ[d] == nil {
					return true
				}
				return false
			}
			if !(isOpenEnd(x) && appearances(x) == 1) {
				led[fs].occ[d] = nil
				delete(prov[fs], d)
				return true
			}
			for _, fs2 := range states {
				if fs2 == fs || !gate(x, fs2) {
					continue
				}
				if open(fs2, x.Depth) || displace(fs2, x.Depth, visited) {
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
		// Demand class 2: unplaced orphans.
		for _, r := range unplaced {
			if placedSet[r.SpanID] {
				continue
			}
			fitted := false
			for _, fs := range states {
				if f := forceFitOn(r, fs, make(map[lvlKey]bool)); f != nil {
					commitFits(r, []*oFit{f}, true)
					placedSet[r.SpanID] = true
					ladder, fitted = true, true
					break
				}
			}
			if !fitted && debugScore {
				// Classify the best failure stage reached across all chains:
				// 0=no chains at all, 1=depth-infeasible everywhere,
				// 2=gates never passed, 3=gates passed but displacement failed.
				best := 0
				for _, fs := range states {
					if fs.p.threadBits == nil || r == fs.p.o {
						continue
					}
					stage := 1
					if r.Depth <= fs.p.o.Depth-2 {
						stage = 2
						bf := bloom.Deserialize(fs.p.threadBits, cfg.BloomM, cfg.BloomK)
						ph := bridge.HexOf(r.ParentID)
						rh := bridge.HexOf(r.SpanID)
						if bf.Test(ph[:]) && bf.Test(rh[:]) {
							stage = 3
						}
					}
					if stage > best {
						best = stage
					}
				}
				fmt.Fprintf(os.Stderr, "STUCKORPHAN stage=%d root=%016x depth=%d chains=%d\n",
					best, r.SpanID, r.Depth, len(states))
			}
		}
		if ladder {
			continue
		}
		// Escalation 3 (v7.3.5): window-local COMPLETE solver. Greedy
		// displacement is approximate — order-dependent, local — and its
		// deadlocks (A needs B's slot, B needs A's) require joint
		// reassignment. Every constraint is window-local, so the global
		// problem shatters into micro-CSPs small enough for exhaustive
		// backtracking: variables = this window's unplaced orphans,
		// unmatched open ends, already-placed orphans, and existing
		// coverage writes; constraints = chain walls, level exclusivity,
		// rigidity (whole-orphan options), spacing (root-1 reservations),
		// and both invariants. The search is PURE (no ledger mutation);
		// the first satisfying assignment is applied atomically. The
		// mutual-success theorem guarantees satisfiability whenever the
		// true receivers survive — a cap-out or unsat is counted, never
		// papered over.
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
		type wOpt struct {
			fs    *fragState
			spans []*Span // occupancy levels (path for orphans, self for open ends)
			rsvD  int     // reservation level (orphans), -1 for open ends
		}
		type wItem struct {
			span     *Span
			isOrphan bool
			opts     []wOpt
		}
		solveWindow := func(w uint64, chains []*fragState, unmatchedEnds []*Span) bool {
			inW := make(map[*fragState]bool, len(chains))
			for _, fs := range chains {
				inW[fs] = true
			}
			// Demands: unplaced orphans gated here, unmatched open ends
			// gated here. Reassignables: placed-in-W orphans, existing C/D
			// coverage writes on W's chains.
			var items []wItem
			demand := 0
			seenOrphan := make(map[uint64]bool)
			addOrphan := func(r *Span) {
				if seenOrphan[r.SpanID] {
					return
				}
				seenOrphan[r.SpanID] = true
				var opts []wOpt
				for _, fs := range chains {
					if p := pathOn(r, fs); p != nil {
						opts = append(opts, wOpt{fs: fs, spans: p, rsvD: r.Depth - 1})
					}
				}
				if len(opts) > 0 {
					items = append(items, wItem{span: r, isOrphan: true, opts: opts})
				}
			}
			for _, r := range unplaced {
				if placedSet[r.SpanID] {
					continue
				}
				before := len(items)
				addOrphan(r)
				if len(items) > before {
					demand++
				}
			}
			for _, e := range unmatchedEnds {
				var opts []wOpt
				for _, fs := range chains {
					if gate(e, fs) {
						opts = append(opts, wOpt{fs: fs, spans: []*Span{e}, rsvD: -1})
					}
				}
				if len(opts) > 0 {
					items = append(items, wItem{span: e, isOrphan: false, opts: opts})
					demand++
				}
			}
			if demand == 0 {
				return false
			}
			for _, fs := range chains {
				for _, of := range occUpper[fs] {
					addOrphan(of.path[0])
				}
				for d, p := range prov[fs] {
					if p == 'C' || p == 'D' {
						if s := led[fs].occ[d]; s != nil && isOpenEnd(s) {
							var opts []wOpt
							for _, fs2 := range chains {
								if gate(s, fs2) {
									opts = append(opts, wOpt{fs: fs2, spans: []*Span{s}, rsvD: -1})
								}
							}
							if len(opts) > 0 {
								items = append(items, wItem{span: s, isOrphan: false, opts: opts})
							}
						}
					}
				}
			}
			if len(items) > 24 {
				return false // combinatorial guard (counted via debug)
			}
			sort.Slice(items, func(i, j int) bool { return len(items[i].opts) < len(items[j].opts) })
			// Pure backtracking over joint assignments.
			type lk struct {
				fs *fragState
				d  int
			}
			usedOcc := make(map[lk]bool)
			usedRsv := make(map[lk]bool)
			assign := make([]int, len(items))
			budget := 20000
			var bt func(i int) bool
			bt = func(i int) bool {
				if budget <= 0 {
					return false
				}
				budget--
				if i == len(items) {
					return true
				}
				it := items[i]
				for oi, opt := range it.opts {
					ok := true
					var occKeys []lk
					var rsvKey *lk
					for _, s := range opt.spans {
						k := lk{opt.fs, s.Depth}
						if usedOcc[k] || usedRsv[k] || led[opt.fs].rsv[s.Depth] && rsvOwner[opt.fs][s.Depth] == nil {
							ok = false
							break
						}
						occKeys = append(occKeys, k)
					}
					if ok && opt.rsvD >= 0 {
						k := lk{opt.fs, opt.rsvD}
						if usedOcc[k] {
							ok = false
						} else {
							rsvKey = &k
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
					assign[i] = oi
					if bt(i + 1) {
						return true
					}
					for _, k := range occKeys {
						delete(usedOcc, k)
					}
					if rsvKey != nil {
						delete(usedRsv, *rsvKey)
					}
				}
				return false
			}
			if !bt(0) {
				if debugScore {
					fmt.Fprintf(os.Stderr, "WSOLVER unsat/cap window=%016x items=%d budget=%d\n", w, len(items), budget)
				}
				return false
			}
			// Apply atomically: tear down the window's reassignable state,
			// then write the satisfying assignment.
			for _, it := range items {
				if it.isOrphan && placedSet[it.span.SpanID] {
					undoOrphan(it.span)
					placedSet[it.span.SpanID] = false
				}
			}
			for _, fs := range chains {
				for d, p := range prov[fs] {
					if p == 'C' || p == 'D' || p == 'A' {
						led[fs].occ[d] = nil
						delete(prov[fs], d)
					}
				}
			}
			for i, it := range items {
				opt := it.opts[assign[i]]
				if it.isOrphan {
					commitFits(it.span, []*oFit{{fs: opt.fs, path: opt.spans}}, true)
					placedSet[it.span.SpanID] = true
				} else {
					if s := led[opt.fs].occ[it.span.Depth]; s != nil {
						led[opt.fs].occ[it.span.Depth] = nil
						delete(prov[opt.fs], it.span.Depth)
					}
					write(opt.fs, it.span, 'D')
					res.ForcedMatches++
				}
			}
			return true
		}
		// Demand lists once per round; skip the solver entirely when both
		// invariants are already satisfied (the overwhelmingly common case).
		var unmatchedEnds []*Span
		for i := range survivors {
			e := &survivors[i]
			if isOpenEnd(e) && appearances(e) == 0 {
				unmatchedEnds = append(unmatchedEnds, e)
			}
		}
		hasUnplaced := false
		for _, r := range unplaced {
			if !placedSet[r.SpanID] {
				hasUnplaced = true
				break
			}
		}
		if len(unmatchedEnds) == 0 && !hasUnplaced {
			break
		}
		solved := false
		for w, chains := range chainsByW {
			if solveWindow(w, chains, unmatchedEnds) {
				solved = true
			}
		}
		if solved {
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
