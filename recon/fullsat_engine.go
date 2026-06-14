package recon

import (
	"sort"
	"strconv"
	"strings"

	"bridges/bloom"
	"bridges/bridge"
)

// ReconstructFullSAT is the single-pass CP-SAT reconstruction engine (design:
// docs/cgp_cpsat_model.md). The principle: establish ALL rules as constraints,
// find clusters, solve each cluster once, read the assignment — nothing
// imperative before (only candidate enumeration + clustering) and nothing after
// (only reading). Every reconstruction decision is a variable; the invariants
// (cover every open end, place every fragment/orphan, named-node exclusivity,
// structural consistency, carrier-hard HA) are hard constraints; the objective
// is explained bloom positives (MAP) under those invariants.
//
// Enumeration model (the bloom IS the path enumerator; checkpoints are PRESERVED):
//   - Backbone: each checkpoint-bearing fragment is located to a window by
//     prefix-matching its shallow checkpoint(s) against the (always-present)
//     upstream checkpoint. Never rootless.
//   - Filter-set: a fragment owns every carrier bloom within CPD of its window
//     root (the band). Any connection point must satisfy them ALL (AND) — by
//     no-false-negatives a real common ancestor is in every one, so the
//     conjunction only tightens admissibility and never rejects a true ancestor.
//   - Path fan: each admissible reconnection point (non-checkpoint, non-leaf,
//     all-filters, chain-consistent) is a distinct candidate path; a deeper
//     reconnection threads one more real survivor and uses fewer synthetics.
//   - Everything is a POSSIBILITY until the one solve commits; the bloom gives
//     completeness, the solve gives the choice.
//
// Not yet built: phase-2 orphan placement (checkpoint-less fragments via the
// downward DFS), the two-sided no-open-ends coverage constraint, fan-out
// consensus from the window root.
func ReconstructFullSAT(survivors []Span, cfg Config) Result {
	var res Result
	byID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		byID[survivors[i].SpanID] = &survivors[i]
	}
	children := make(map[uint64][]*Span, len(survivors))
	byDepth := make(map[int][]*Span)
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 {
			if _, ok := byID[s.ParentID]; ok {
				children[s.ParentID] = append(children[s.ParentID], s)
			}
		}
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}

	// Fragment roots: surviving spans whose parent dropped.
	var roots []*Span
	for i := range survivors {
		o := &survivors[i]
		if o.ParentID == 0 {
			continue
		}
		if _, ok := byID[o.ParentID]; ok {
			continue
		}
		roots = append(roots, o)
	}
	res.Orphans = len(roots)

	type frag struct {
		o     *Span
		cands []*Span
	}
	var frags []frag
	for _, o := range roots {
		carrierDepth, prefix, fbits := fragmentPayloads(o, children, cfg)
		if prefix == nil {
			// Non-checkpoint-bearing fragment = ORPHAN: no checkpoint to
			// self-locate, so the backbone phase legitimately does not place it.
			// Reconnecting it is the job of the downward-DFS path enumeration
			// (phase 2, not yet built). Recorded as pending; never root-slammed.
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
			ckptDepth = 0 // first window: the trace root is the checkpoint
		}
		// Deserialize the fragment's filter-set (carriers within the band only).
		filters := make([]*bloom.Filter, 0, len(fbits))
		for _, b := range fbits {
			filters = append(filters, bloom.Deserialize(b, cfg.BloomM, cfg.BloomK))
		}
		confirmedByAll := func(s *Span) bool {
			hex := bridge.HexOf(s.SpanID)
			for _, f := range filters {
				if !f.Test(hex[:]) {
					return false
				}
			}
			return true
		}
		// Window-root candidate(s): the upstream checkpoint matching the prefix.
		// Checkpoints are PRESERVED, so this always yields at least one; this is
		// the shallowest candidate path (every dropped level becomes synthetic).
		var cands []*Span
		for _, c := range byDepth[ckptDepth] {
			id8 := bridge.BigEndian8(c.SpanID)
			match := true
			for k := 0; k < cfg.PrefixLen; k++ {
				if id8[k] != prefix[k] {
					match = false
					break
				}
			}
			if match {
				cands = append(cands, c)
			}
		}
		// Reconnection points: NON-checkpoint, NON-leaf survivors strictly inside
		// the window that satisfy ALL filters and are chain-consistent. Each is a
		// distinct candidate path; deeper reconnection => fewer synthetics. A
		// checkpoint-depth span is skipped: it is a window boundary or (leaves emit
		// checkpoint payloads) a leaf, and neither can host a reconnection.
		//
		// The deepest possible reconnection is the GRANDPARENT at o.Depth-2: o's
		// parent (o.Depth-1) is dropped by definition (that is why o is a fragment
		// root), and there is exactly one ancestor per depth, so nothing surviving
		// at o.Depth-1 can be an ancestor — a bloom hit there is necessarily a
		// false positive. Hence d < o.Depth-1.
		for d := ckptDepth + 1; d < o.Depth-1; d++ {
			if d%cfg.CPD == 0 {
				continue
			}
			for _, s := range byDepth[d] {
				if s.LeafCarrier {
					continue
				}
				if confirmedByAll(s) && chainConsistent(filters[0], s, byID, ckptDepth+1) {
					cands = append(cands, s)
				}
			}
		}
		sort.Slice(cands, func(i, j int) bool { return cands[i].SpanID < cands[j].SpanID })
		frags = append(frags, frag{o: o, cands: cands})
	}

	// NO-OPEN-ENDS scope diagnostic (not yet a hard constraint). An open end is an
	// interior node (emitted nothing => CkptPrefix == nil) whose children ALL
	// dropped — the real surviving skeleton is cut there, so a fragment MUST
	// continue it. OpenEndsMatched counts those that at least one fragment we
	// already enumerate could claim (the open end is in some fragment's candidate
	// set); the shortfall is what strictly requires an orphan claimant.
	candSet := make(map[uint64]bool)
	for i := range frags {
		for _, c := range frags[i].cands {
			candSet[c.SpanID] = true
		}
	}
	var openEndIDs []uint64
	for i := range survivors {
		x := &survivors[i]
		if x.CkptPrefix != nil || len(children[x.SpanID]) > 0 {
			continue // checkpoint/leaf terminal, or still connected below
		}
		res.OpenEnds++
		if candSet[x.SpanID] {
			res.OpenEndsMatched++
			openEndIDs = append(openEndIDs, x.SpanID)
		}
	}

	emit := func(o, anchor *Span) {
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:  o.SpanID,
			AnchorID:  anchor.SpanID,
			Synthetic: o.Depth - anchor.Depth - 1,
			Ambiguous: false,
		})
		res.Reconnected++
	}

	// STRUCTURAL: fragments that share a dropped parent (ParentID) are direct
	// children of one fan-out, so they share a nearest survivor and must share ONE
	// anchor decision. Group them and let the model pick each group's anchor by
	// CORROBORATION (how many members' filters confirm the candidate) then depth
	// (nearest). One anchor variable per (group, candidate); ExactlyOne per group.
	groups := map[uint64][]int{} // ParentID -> frag indices
	var gkeys []uint64
	for i := range frags {
		p := frags[i].o.ParentID
		if _, ok := groups[p]; !ok {
			gkeys = append(gkeys, p)
		}
		groups[p] = append(groups[p], i)
	}
	sort.Slice(gkeys, func(i, j int) bool { return gkeys[i] < gkeys[j] })

	type gcand struct {
		s      *Span
		corrob int
	}
	gCands := map[uint64][]gcand{}
	nv := 0
	gVar := map[uint64][]int{}       // ParentID -> var per gcand
	varsBySpan := map[uint64][]int{} // anchor spanID -> every var that selects it
	for _, p := range gkeys {
		corrob := map[uint64]int{}
		anchByID := map[uint64]*Span{}
		for _, mi := range groups[p] {
			for _, c := range frags[mi].cands {
				corrob[c.SpanID]++
				anchByID[c.SpanID] = c
			}
		}
		var cs []gcand
		for id := range anchByID {
			cs = append(cs, gcand{s: anchByID[id], corrob: corrob[id]})
		}
		sort.Slice(cs, func(i, j int) bool { return cs[i].s.SpanID < cs[j].s.SpanID })
		gCands[p] = cs
		gVar[p] = make([]int, len(cs))
		for k := range cs {
			gVar[p][k] = nv
			varsBySpan[cs[k].s.SpanID] = append(varsBySpan[cs[k].s.SpanID], nv)
			nv++
		}
	}

	// Best candidate for a group when no solver is linked (or as the read-back
	// default): max corroboration, then deepest, then lowest id (the sort above
	// makes the lowest-id win deterministic on a full tie).
	bestOf := func(cs []gcand) *Span {
		b := cs[0]
		for _, c := range cs[1:] {
			if c.corrob > b.corrob || (c.corrob == b.corrob && c.s.Depth > b.s.Depth) {
				b = c
			}
		}
		return b.s
	}
	emitGroup := func(p uint64, anchor *Span) {
		for _, mi := range groups[p] {
			emit(frags[mi].o, anchor)
		}
	}
	if nv == 0 || fullsatSolveFn == nil {
		for _, p := range gkeys {
			emitGroup(p, bestOf(gCands[p]))
		}
		return res
	}

	// corroboration dominates depth in the objective; depths are O(tens), groups
	// are O(tens), so a 1000x multiplier keeps corroboration strictly primary.
	const corrobW = 1000
	var b strings.Builder
	b.WriteString("MODEL ")
	b.WriteString(strconv.Itoa(nv))
	b.WriteByte('\n')
	for _, p := range gkeys {
		cs := gCands[p]
		b.WriteString("EO ")
		b.WriteString(strconv.Itoa(len(cs)))
		for k := range cs {
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(gVar[p][k] + 1))
		}
		b.WriteByte('\n')
	}
	// HARD NO-OPEN-ENDS coverage: every open end (interior node whose children all
	// dropped) MUST be claimed — at least one group anchors there. This forces the
	// solve to cover real cuts in the surviving skeleton instead of letting
	// fragments drift to deeper false-positive points, and is what pulls orphans
	// (once enumerated) in as claimants of otherwise-dangling terminals.
	for _, x := range openEndIDs {
		vs := varsBySpan[x]
		if len(vs) == 0 {
			continue
		}
		b.WriteString("ALO ")
		b.WriteString(strconv.Itoa(len(vs)))
		for _, v := range vs {
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(v + 1))
		}
		b.WriteByte('\n')
	}
	b.WriteString("OBJ ")
	b.WriteString(strconv.Itoa(nv))
	for _, p := range gkeys {
		cs := gCands[p]
		for k := range cs {
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(cs[k].corrob*corrobW + cs[k].s.Depth)) // corroboration, then nearest
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(gVar[p][k]))
		}
	}
	b.WriteByte('\n')

	asg, _, status := fullsatSolveFn(b.String(), nv, fullsatDetTime, fullsatSeed)
	for _, p := range gkeys {
		cs := gCands[p]
		chosen := bestOf(cs)
		if status != 0 {
			for k := range cs {
				if asg[gVar[p][k]] {
					chosen = cs[k].s
					break
				}
			}
		}
		emitGroup(p, chosen)
	}
	return res
}

// fragmentPayloads collects every PCRB carrier within one CPD band below the
// fragment root o (BFS over surviving children, bounded by band = the next
// checkpoint depth). A branching fragment yields a SET of shallow checkpoints;
// their prefixes name the shared upstream checkpoint and their blooms form the
// filter-set that any connection must satisfy together. Carriers deeper than the
// band live in a deeper window — their blooms reset at the band checkpoint and
// do NOT contain o's trunk, so they are excluded. carrierDepth/prefix come from
// the shallowest carrier (used for the window assignment).
func fragmentPayloads(o *Span, children map[uint64][]*Span, cfg Config) (carrierDepth int, prefix []byte, filters [][]byte) {
	band := (o.Depth/cfg.CPD + 1) * cfg.CPD
	queue := []*Span{o}
	for len(queue) > 0 {
		var next []*Span
		for _, s := range queue {
			if s.Depth > band {
				continue
			}
			if s.CkptPrefix != nil {
				filters = append(filters, s.BloomBits)
				if prefix == nil {
					prefix = s.CkptPrefix
					carrierDepth = s.Depth
				}
			}
			next = append(next, children[s.SpanID]...)
		}
		queue = next
	}
	return
}
