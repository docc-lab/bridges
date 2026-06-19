package recon

import (
	"sort"

	"bridges/bridge"
)

// StructureResult is the Phase-2 (ordering) outcome for one trace.
type StructureResult struct {
	EventOrderOK bool // reconstructed event total-order == corpus total-order
	CriticalPath bool // reconstructed bottleneck chain == true bottleneck chain
	DEEAmbiguous bool // a DEE matched >1 parent after content-pruning -> wrong

	// Per-parent end-order recovery (the thing EE+DEE actually buys): over
	// multi-child parents, how many had their children's END order recovered
	// exactly, judged against the corpus total-order.
	NParents   int
	EndOrderOK int
}

// trueEndOrder is a parent's children's ordinals in true end order, taken from
// each child's END-event position in the corpus total-order (endPos). Those
// positions are already a tie-broken total order, so this is the authoritative
// truth — no order is reconstructed from raw (tie-prone) timestamps.
func trueEndOrder(st *STNode, endPos map[uint64]int64) []int {
	ords := make([]int, 0, len(st.Children))
	for ord := range st.Children {
		ords = append(ords, ord)
	}
	sort.Slice(ords, func(i, j int) bool {
		return endPos[st.Children[ords[i]].ID] < endPos[st.Children[ords[j]].ID]
	})
	return ords
}

func sameOrder(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ScoreStructure reconstructs the event ORDERING for one trace and scores it,
// order against order. It assumes the topology (res) is already correct: it
// mirrors res's tree into STNodes (resolving each node's true span id via truth
// so lost spans still have an identity), gathers each parent's end-order from
// its children's EE blocks + the attributed DEEs, and compares that recovered
// order to the true order, which is read off each END event's position in the
// corpus total-order (endPos) — a tie-broken total order, NOT raw timestamps —
// so order among tied/concurrent siblings is well-defined, not invented. endPos
// covers EVERY span.
func ScoreStructure(res SBResult, truth SBTruth, endPos map[uint64]int64, deeQuads [][]byte) StructureResult {
	if res.Root == nil {
		return StructureResult{}
	}

	var cands []DEECandidate
	stByID := map[uint64]*STNode{}
	childrenByID := map[uint64][]SBChild{}

	var build func(n *SBNode, trueID uint64, depth int) *STNode
	build = func(n *SBNode, trueID uint64, depth int) *STNode {
		st := &STNode{ID: trueID, Ord: n.Ord, Real: n.RealID != 0, Children: map[int]*STNode{}}
		var kids []SBChild
		childOrds := map[int]bool{}
		witnessed := map[int]bool{} // ends already accounted in children's EE
		for ord, c := range n.Children {
			childTrueID := truth.ChildByOrd[trueID][ord]
			st.Children[ord] = build(c, childTrueID, depth+1)
			kids = append(kids, SBChild{Ord: ord, EE: c.EE})
			childOrds[ord] = true
			for _, e := range c.EE {
				witnessed[e] = true
			}
		}
		stByID[trueID] = st
		childrenByID[trueID] = kids
		if len(n.Children) > 0 { // a possible DEE owner
			cand := DEECandidate{ID: trueID, Survived: n.RealID != 0, RealID: n.RealID,
				Depth: depth, ChildOrds: childOrds, EE: witnessed}
			if !cand.Survived {
				cand.FP = n.FP // recovered fpBits-wide fp for a dropped parent
			}
			cands = append(cands, cand)
		}
		return st
	}
	stRoot := build(res.Root, truth.RootID, 0)

	// Attribute the trace's DEEs to their owning parents; >1 match -> ambiguous.
	deeByParent := map[uint64][]int{}
	ambiguous := false
	for _, q := range deeQuads {
		quads, err := bridge.DecodeDEEQuads(q)
		if err != nil {
			continue
		}
		for _, dq := range quads {
			idx, st := AttributeDEE(dq.OwnerFP, dq.Depth, dq.Seqs, cands, res.FPBits)
			switch st {
			case DEEAmbiguous:
				ambiguous = true
			case DEEPlaced:
				id := cands[idx].ID
				deeByParent[id] = append(deeByParent[id], dq.Seqs...)
			}
		}
	}

	// Unresolved DEE ambiguity -> REJECT the trace: we refuse to reconstruct an
	// ordering we can't attribute, rather than guess. It's reported as rejected,
	// not folded into the correctness rates.
	if ambiguous {
		return StructureResult{DEEAmbiguous: true}
	}

	// Each parent's end-order = its children's EE blocks ++ its DEE leftovers.
	for id, st := range stByID {
		st.EndOrder = GatherEndOrder(childrenByID[id], deeByParent[id])
	}

	// Order against order: each parent's recovered EndOrder vs its true end order.
	// The trace's ordering is fully recovered iff EVERY multi-child parent matches.
	var nParents, endOK int
	var walk func(st *STNode)
	walk = func(st *STNode) {
		if len(st.Children) > 1 {
			nParents++
			if sameOrder(st.EndOrder, trueEndOrder(st, endPos)) {
				endOK++
			}
		}
		for _, c := range st.Children {
			walk(c)
		}
	}
	walk(stRoot)

	return StructureResult{
		EventOrderOK: nParents == endOK, // every parent's end-order recovered
		CriticalPath: CriticalPathMatch(stRoot, endPos),
		NParents:     nParents,
		EndOrderOK:   endOK,
	}
}
