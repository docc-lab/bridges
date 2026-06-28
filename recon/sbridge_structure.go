package recon

import "sort"

// S-Bridge Phase 2: ordering/timestamp reconstruction (the "structure" pass).
//
// After topology (Phase 1) we have a tree where surviving spans carry their
// real (fixed) timestamps and reconstructed spans carry none. A single
// bottom-up sweep assigns timestamps to the reconstructed spans:
//
//   For each node (post-order — children finalized first), resolve the node's
//   whole SIBLING SET in one shot:
//     1. reorder the children: push reconstructed starts EARLIER / ends LATER
//        (only-widening, ε apart) so starts come out in start-ordinal order and
//        ends in the EE/DEE-derived end order; survivors never move.
//     2. the node (if reconstructed) encompasses its now-positioned children.
//
// Because children are finalized before their parent, a child pushed past its
// parent's window simply widens the parent when the parent is processed — the
// widening propagates upward in the same sweep. No per-parent revisiting, no
// reconcile pass; each sibling set is touched exactly once.
//
// Ordering is later judged against the corpus total-order, not raw ms-tied
// timestamps, so ε only needs to keep distinct events strictly separated.

// SBChild is one child of a parent, for gathering the orderings before any
// timestamp work: its start-ordinal and the EE block it carried (the earlier-
// sibling end-ordinals it witnessed before it started).
type SBChild struct {
	Ord int
	EE  []int
}

// GatherStartOrder returns a parent's children's start-ordinals in start order.
func GatherStartOrder(children []SBChild) []int {
	cs := append([]SBChild(nil), children...)
	sort.Slice(cs, func(i, j int) bool { return cs[i].Ord < cs[j].Ord })
	out := make([]int, len(cs))
	for i, c := range cs {
		out[i] = c.Ord
	}
	return out
}

// GatherEndOrder reassembles a parent's children's end-event order — the child
// start-ordinals in the order their END events occurred — from the EE blocks the
// children carried plus the parent's DEE leftover ends. The handler lays these
// down so that, in start-ordinal order, each child's EE is the ends witnessed
// since the previous child started; the DEE is the ends after the last child
// started minus one; the very last end is implicit (the one child appearing
// nowhere). So: EE blocks in start order ++ DEE leftovers ++ the implicit last.
func GatherEndOrder(children []SBChild, dee []int) []int {
	cs := append([]SBChild(nil), children...)
	sort.Slice(cs, func(i, j int) bool { return cs[i].Ord < cs[j].Ord })
	order := make([]int, 0, len(cs))
	for _, c := range cs {
		order = append(order, c.EE...)
	}
	order = append(order, dee...)
	seen := make(map[int]bool, len(order))
	for _, o := range order {
		seen[o] = true
	}
	for _, c := range cs { // the single implicit-last end: not witnessed, not in DEE
		if !seen[c.Ord] {
			order = append(order, c.Ord)
			break
		}
	}
	return order
}

// DEEStatus is the outcome of attributing a DEE batch to a parent.
type DEEStatus int

const (
	DEEPlaced    DEEStatus = iota // unique parent
	DEEAmbiguous                  // >=2 parents survive fingerprint + content pruning -> wrong
	DEENoPlace                    // 0 parents survive -> no valid owner
)

// DEECandidate is a possible owner of a DEE batch: its depth, fingerprint
// (real-id top4 if it survived, else the 2-byte recovered fp), the set of valid
// child ordinals, and the end-ordinals already witnessed in its EE.
type DEECandidate struct {
	ID        uint64
	Survived  bool
	RealID    uint64 // top-fpBits used for the match when Survived
	FP        uint64 // recovered fp (top fpBits of the span id, right-aligned) when lost
	Depth     int
	ChildOrds map[int]bool
	EE        map[int]bool // ends already witnessed (a child cannot also be a DEE leftover)
}

// AttributeDEE finds the unique parent a DEE batch (ownerFP=top4 of the parent
// span id, depth, seqs=leftover end-ordinals) belongs to. Candidates must match
// the owner fingerprint at that depth; then content-pruning rejects any whose
// existing EE can't coexist with the DEE (a seq already witnessed, or a seq that
// isn't one of the candidate's children). >=2 survivors -> ambiguous (wrong).
func AttributeDEE(ownerFP uint64, depth int, seqs []int, cands []DEECandidate, fpBits int) (idx int, st DEEStatus) {
	if fpBits <= 0 {
		fpBits = 16
	}
	matched, n := -1, 0
	for i := range cands {
		c := &cands[i]
		if c.Depth != depth {
			continue
		}
		if c.Survived {
			if c.RealID>>uint(64-fpBits) != ownerFP { // top-fpBits of the real id vs owner fp
				continue
			}
		} else if c.FP != ownerFP { // recovered fp vs owner fp (both top-fpBits, right-aligned)
			continue
		}
		ok := true
		for _, s := range seqs {
			if !c.ChildOrds[s] || c.EE[s] { // not a child, or already ended -> can't be this parent
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		matched = i
		n++
	}
	switch {
	case n == 1:
		return matched, DEEPlaced
	case n == 0:
		return -1, DEENoPlace
	default:
		return -1, DEEAmbiguous
	}
}

// STNode is one span for the structure pass.
type STNode struct {
	ID       uint64 // span id (for event-order / critical-path scoring)
	Real     bool   // survivor: Start/End are fixed and never move
	Start    int64  // reconstructed: assigned by the sweep
	End      int64
	Ord      int             // start-ordinal under its parent (start order)
	Children map[int]*STNode // keyed by child start-ordinal
	EndOrder []int           // this node's children's ordinals, in end-event order (EE+DEE)
}

// criticalPath returns the chain of last-finishing spans from the root: at each
// node it descends into the child that ENDS LAST under endOf. It's the
// bottleneck chain — the path whose timings gate the root's makespan. Siblings
// share a depth, so equal-end ties resolve to the larger id (which sorts later
// in the corpus total-order, i.e. ends last). Used for the TRUE path.
func criticalPath(root *STNode, endOf func(*STNode) int64) []uint64 {
	var path []uint64
	for n := root; n != nil; {
		path = append(path, n.ID)
		var best *STNode
		for _, c := range n.Children {
			switch {
			case best == nil, endOf(c) > endOf(best), endOf(c) == endOf(best) && c.ID > best.ID:
				best = c
			}
		}
		n = best
	}
	return path
}

// criticalPathByOrder follows the RECOVERED end-order — the last child in each
// parent's gathered EndOrder is its last-finisher. We read the bottleneck off
// the recovered ordering, never off the ε-nudged synthetic end VALUES: those
// only-widen, so at a millisecond tie a reconstructed end can overshoot a
// survivor sibling by a nanosecond and flip the pick. The recovered order
// already respects the corpus total-order, so it can't be flipped that way.
func criticalPathByOrder(root *STNode) []uint64 {
	var path []uint64
	for n := root; n != nil; {
		path = append(path, n.ID)
		if len(n.Children) == 0 || len(n.EndOrder) == 0 {
			break
		}
		n = n.Children[n.EndOrder[len(n.EndOrder)-1]]
	}
	return path
}

// CriticalPathMatch reports whether the reconstructed bottleneck chain (read off
// the recovered end-order) equals the true one (last-finisher by END-event
// position in the corpus total-order — endPos, not raw timestamps).
func CriticalPathMatch(root *STNode, endPos map[uint64]int64) bool {
	rec := criticalPathByOrder(root)
	tru := criticalPath(root, func(n *STNode) int64 { return endPos[n.ID] })
	if len(rec) != len(tru) {
		return false
	}
	for i := range rec {
		if rec[i] != tru[i] {
			return false
		}
	}
	return true
}

// ReconstructStructure runs the single bottom-up sweep on the tree.
func ReconstructStructure(root *STNode, eps int64) { resolve(root, eps) }

// resolve finalizes n's subtree: children first, then n's sibling set in one shot.
func resolve(n *STNode, eps int64) {
	if n == nil {
		return
	}
	for _, c := range n.Children {
		resolve(c, eps)
	}
	reorderSiblings(n, eps)
	encompass(n)
}

// reorderSiblings resolves a node's whole child set at once: ends pushed later in
// end-event order, starts pushed earlier in ordinal order. Reconstructed spans
// only widen; survivors are fixed anchors.
func reorderSiblings(n *STNode, eps int64) {
	// Ends: walk children in end-event order; each end >= the previous (+eps for a
	// strict gap). Only reconstructed ends move, and only later.
	var prev int64
	have := false
	for _, ord := range n.EndOrder {
		c := n.Children[ord]
		if c == nil {
			continue
		}
		if have && !c.Real {
			if need := prev + eps; c.End < need {
				c.End = need
			}
		}
		prev = c.End
		have = true
	}
	// Starts: walk children in ordinal order; each start <= the next (-eps). Only
	// reconstructed starts move, and only earlier — sweep backward so an
	// out-of-order start is pulled under its successor.
	ords := sortedOrds(n.Children)
	var next int64
	haveN := false
	for i := len(ords) - 1; i >= 0; i-- {
		c := n.Children[ords[i]]
		if haveN && !c.Real {
			if lim := next - eps; c.Start > lim {
				c.Start = lim
			}
		}
		next = c.Start
		haveN = true
	}
}

// encompass widens a reconstructed node to cover its (already positioned)
// children. Survivors and childless nodes are left as-is.
func encompass(n *STNode) {
	if n.Real || len(n.Children) == 0 {
		return
	}
	var mn, mx int64
	first := true
	for _, c := range n.Children {
		if first || c.Start < mn {
			mn = c.Start
		}
		if first || c.End > mx {
			mx = c.End
		}
		first = false
	}
	n.Start, n.End = mn, mx
}

func sortedOrds(children map[int]*STNode) []int {
	ords := make([]int, 0, len(children))
	for o := range children {
		ords = append(ords, o)
	}
	sort.Ints(ords)
	return ords
}
