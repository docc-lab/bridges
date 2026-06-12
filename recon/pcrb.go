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

	// --- Pass 3: open-end matching to fixpoint (v6.5) ---
	//
	// Discarded fragments (unanchored roots + everything connected below
	// them) are not part of the reconstruction: they are excluded from the
	// open-end census, and a span whose surviving children are ALL
	// discarded is itself open.
	//
	// Reduced-set theorem: no checkpoint can sit strictly between an open
	// end and the topmost carrier fragments below it (checkpoints never
	// drop and never orphan), so those fragments share the open end's
	// window and their blooms contain it with no false negatives. Every
	// open end therefore has >=1 claimant present — an unmatched open end
	// after this pass is PROVABLY a deepest-site ambiguity, never a
	// satisfied end state.
	//
	// Two commitment rules, iterated to fixpoint:
	//  (a) deepest-site claim: a claimant with NO candidate deeper than the
	//      open end has it as its deepest viable site -> commit. Several
	//      claimants may commit to the SAME open end (it lost several
	//      children); multi-claimant is not ambiguity.
	//  (b) sole claimant: an unsatisfied open end with exactly one
	//      uncommitted claimant commits it (must-receive), even past
	//      deeper FP candidates.
	discarded := make(map[uint64]struct{})
	{
		queue := make([]*Span, 0, len(res.Unanchored))
		for _, id := range res.Unanchored {
			queue = append(queue, byID[id])
		}
		for len(queue) > 0 {
			s := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if _, ok := discarded[s.SpanID]; ok {
				continue
			}
			discarded[s.SpanID] = struct{}{}
			queue = append(queue, children[s.SpanID]...)
		}
	}
	var openEnds []*Span
	for i := range survivors {
		s := &survivors[i]
		if s.CkptPrefix != nil {
			continue // carrier: closed
		}
		if _, gone := discarded[s.SpanID]; gone {
			continue // not part of the reconstruction
		}
		open := true
		for _, c := range children[s.SpanID] {
			if _, gone := discarded[c.SpanID]; !gone {
				open = false
				break
			}
		}
		if open {
			openEnds = append(openEnds, s)
		}
	}

	satisfied := make(map[uint64]bool)
	for _, fs := range states {
		if fs.committed {
			satisfied[fs.pos.SpanID] = true
		}
	}
	claims := func(fs *fragState, e *Span) bool {
		for _, c := range fs.cand[e.Depth] {
			if c == e {
				return true
			}
		}
		return false
	}
	deepestCand := func(fs *fragState) int {
		dmax := -1
		for d, c := range fs.cand {
			if len(c) > 0 && d > dmax {
				dmax = d
			}
		}
		return dmax
	}
	soundFixpoint := func() {
		for changed := true; changed; {
			changed = false
			// (a) deepest-site claims: all claimants whose deepest viable site
			// is this open end commit here (multi-attach allowed).
		for _, e := range openEnds {
			for _, fs := range states {
				if fs.committed || fs.cand == nil || !claims(fs, e) {
					continue
				}
				if deepestCand(fs) == e.Depth {
					fs.pos = e
					fs.committed = true
					satisfied[e.SpanID] = true
					changed = true
				}
			}
		}
			// (b) sole-claimant must-receive.
			for _, e := range openEnds {
				if satisfied[e.SpanID] {
					continue
				}
				var sole *fragState
				n := 0
				for _, fs := range states {
					if fs.committed || fs.cand == nil || !claims(fs, e) {
						continue
					}
					n++
					sole = fs
					if n > 1 {
						break
					}
				}
				if n == 1 {
					sole.pos = e
					sole.committed = true
					satisfied[e.SpanID] = true
					changed = true
				}
			}
		}
	}
	soundFixpoint() // tier 1: carrier-certain commits, final

	// --- Tier 2 (v7.2): orphan binding with unification ---
	//
	// Invariants tighten the viable path space (Tomislav's framing): open
	// ends MUST be matched, orphans MUST be placed. Tier 1's carrier-
	// certain commits stand by default; orphans bind to chains where
	// carrier evidence left levels undecided; the must-place ladder at the
	// bottom may widen to ambiguity-contested levels because an orphan
	// with zero bindings would contradict the recoverability theorem
	// (descendant blooms testify with no false negatives) and therefore
	// signals an upstream wrong commit.
	//
	// A binding = a chain whose viable slots cover a descending root-path
	// of the orphan, gated by the dropped-parent test plus one bloom test
	// PER PATH SPAN. Orphans are RIGID: bindings in the same pass-1 window
	// are mutually consistent (paths coalesce above branch points: X
	// calling Y and Z), bindings across windows contradict.
	type oFit struct {
		fs       *fragState
		path     []*Span
		reanchor bool // fragment re-anchors through this fit (primary or >=2-span testimony)
	}
	consumed := make(map[*fragState]map[int]bool) // orphan-occupied + reserved levels per chain
	occUpper := make(map[*fragState][]*oFit)      // committed fits per chain
	orphanChains := make(map[uint64][]*oFit)      // orphan root -> committed fits
	slotOK := func(fs *fragState, d int, wide bool) bool {
		if d <= fs.p.anchor.Depth || d >= fs.p.o.Depth {
			return false
		}
		if m := consumed[fs]; m != nil && m[d] {
			return false
		}
		if wide {
			return true // must-place ladder: compete at ambiguity-contested levels
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
		// The slot above the orphan root stays synthetic (its parent is
		// dropped), and the root's own level must be viable.
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
				break // zero: path ends; >1: ambiguous, end at last certain span
			}
			path = append(path, next)
			cur = next
		}
		return &oFit{fs: fs, path: path}
	}
	commitFits := func(r *Span, fits []*oFit, forced bool) {
		// Coalesced commit: every bound chain consumes slots; only fits
		// with >=2-span testimony (or the primary) re-anchor their
		// fragment — single-test claimants in the right window may be FPs
		// and keep their current position.
		primary := fits[0]
		for _, f := range fits[1:] {
			if len(f.path) > len(primary.path) {
				primary = f
			}
		}
		for _, f := range fits {
			f.reanchor = f == primary || len(f.path) >= 2
			m := consumed[f.fs]
			if m == nil {
				m = make(map[int]bool)
				consumed[f.fs] = m
			}
			m[r.Depth-1] = true // reserved synthetic above the orphan root
			for _, s := range f.path {
				m[s.Depth] = true
			}
			occUpper[f.fs] = append(occUpper[f.fs], f)
		}
		orphanChains[r.SpanID] = fits
		res.OrphansPlaced++
		if forced {
			res.ForcedMatches++
		}
		// Anchor: deepest occupied-or-accepted span above the root on the
		// primary chain, else the window checkpoint.
		top := primary.fs.p.anchor
		for d := r.Depth - 1; d > top.Depth; d-- {
			var occ *Span
			for _, of := range occUpper[primary.fs] {
				for _, s := range of.path {
					if s.Depth == d && s != r {
						occ = s
					}
				}
			}
			if occ == nil && len(primary.fs.cand[d]) == 1 {
				occ = primary.fs.cand[d][0]
			}
			if occ != nil {
				top = occ
				break
			}
		}
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:  r.SpanID,
			AnchorID:  top.SpanID,
			Synthetic: r.Depth - top.Depth - 1,
			Forced:    forced,
		})
		res.Reconnected++
	}
	var unplaced []*Span
	for _, id := range res.Unanchored {
		unplaced = append(unplaced, byID[id])
	}
	bindings := func(r *Span, wide bool) map[uint64][]*oFit {
		byW := make(map[uint64][]*oFit)
		for _, fs := range states {
			if f := tryFit(r, fs, wide); f != nil {
				byW[fs.p.anchor.SpanID] = append(byW[fs.p.anchor.SpanID], f)
			}
		}
		return byW
	}
	placedSet := make(map[uint64]bool)
	resolveOrphans := func(wide bool) {
		// Unification fixpoint: single-window orphans commit coalesced;
		// placements consume slots and can collapse another orphan's
		// contested bindings to one window -> iterate.
		for changed := true; changed; {
			changed = false
			for _, r := range unplaced {
				if placedSet[r.SpanID] {
					continue
				}
				byW := bindings(r, wide)
				if len(byW) == 1 {
					for _, fits := range byW {
						commitFits(r, fits, wide)
					}
					placedSet[r.SpanID] = true
					changed = true
				}
			}
		}
		// Contested residue: corroboration decides the window — most
		// independent witnesses, then most total path testimony. Flagged.
		for _, r := range unplaced {
			if placedSet[r.SpanID] {
				continue
			}
			byW := bindings(r, wide)
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
			if bestN > 0 {
				commitFits(r, byW[bestW], true)
				placedSet[r.SpanID] = true
			}
		}
	}
	reanchorAll := func() {
		for fs, fits := range occUpper {
			deepest := fs.pos
			for _, f := range fits {
				if !f.reanchor {
					continue
				}
				bot := f.path[len(f.path)-1]
				if bot.Depth > deepest.Depth {
					deepest = bot
				}
			}
			fs.pos = deepest
		}
	}
	resolveOrphans(false)
	reanchorAll()

	// --- Tier 3: coverage repair (augmenting) over the placed structure ---
	// Saturate every open end via augmenting paths (total coverage is the
	// invariant). A chain carrying placed orphans is movable only if every
	// such orphan retains another bound chain.
	openEndSet := make(map[uint64]bool, len(openEnds))
	byIDOpen := make(map[uint64]*Span, len(openEnds))
	for _, e := range openEnds {
		openEndSet[e.SpanID] = true
		byIDOpen[e.SpanID] = e
	}
	posCount := make(map[uint64]int)
	for _, fs := range states {
		posCount[fs.pos.SpanID]++
	}
	assign := func(fs *fragState, e *Span) {
		posCount[fs.pos.SpanID]--
		fs.pos = e
		fs.committed = true
		fs.forced = true
		posCount[e.SpanID]++
		satisfied[e.SpanID] = true
		res.ForcedMatches++
	}
	var augment func(e *Span, visited map[uint64]bool) bool
	augment = func(e *Span, visited map[uint64]bool) bool {
		for _, fs := range states {
			if fs.cand == nil || fs.pos == e || !claims(fs, e) {
				continue
			}
			if fits, bound := occUpper[fs]; bound {
				movable := true
				for _, f := range fits {
					if len(orphanChains[f.path[0].SpanID]) < 2 {
						movable = false
						break
					}
				}
				if !movable {
					continue // sole host of a placed orphan: not movable
				}
			}
			if visited[fs.p.o.SpanID] {
				continue
			}
			visited[fs.p.o.SpanID] = true
			cur := fs.pos.SpanID
			if !openEndSet[cur] || posCount[cur] >= 2 {
				assign(fs, e)
				return true
			}
			if augment(byIDOpen[cur], visited) {
				assign(fs, e)
				return true
			}
		}
		return false
	}
	for _, e := range openEnds {
		if !satisfied[e.SpanID] {
			augment(e, make(map[uint64]bool))
		}
	}

	// --- Must-place ladder: no unplaced orphans, as a rule. Rebind the
	// residue against post-repair chains (strict), then widen to
	// ambiguity-contested levels. Anything still unplaced contradicts the
	// recoverability theorem -> reported in Unanchored as a correctness
	// signal, not an accepted outcome.
	resolveOrphans(false)
	resolveOrphans(true)
	reanchorAll()

	var stillUn []uint64
	for _, r := range unplaced {
		if !placedSet[r.SpanID] {
			stillUn = append(stillUn, r.SpanID)
		}
	}
	res.Unanchored = stillUn
	// Open ends INSIDE placed orphans: reported separately pending their
	// integration as claimable census material.
	for _, r := range unplaced {
		if !placedSet[r.SpanID] {
			continue
		}
		queue := []*Span{r}
		for len(queue) > 0 {
			s := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if len(children[s.SpanID]) == 0 {
				covered := false
				for _, fs := range states {
					if fs.pos == s {
						covered = true
						break
					}
				}
				if !covered {
					res.OrphanOpenEnds++
				}
			}
			queue = append(queue, children[s.SpanID]...)
		}
	}

	for _, fs := range states {
		res.Reconnected++
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:   fs.p.o.SpanID,
			AnchorID:   fs.pos.SpanID,
			Synthetic:  fs.p.o.Depth - fs.pos.Depth - 1,
			Ambiguous:  fs.p.ambiguous,
			ViaCarrier: fs.p.viaCarrier,
			Forced:     fs.forced,
		})
	}

	// Census: every open end must be matched; unmatched = deepest-site
	// ambiguity, reported as a correctness deficit.
	res.OpenEnds = len(openEnds)
	for _, e := range openEnds {
		if satisfied[e.SpanID] {
			res.OpenEndsMatched++
			continue
		}
		for _, fs := range states {
			if fs.pos == e {
				res.OpenEndsMatched++
				break
			}
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
