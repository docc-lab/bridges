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
	DEEUnplaced  bool // a DEE matched no structurally valid parent -> reject
	Incomplete   bool // EE/DEE did not yield one complete order per parent
	Reason       string

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
	var dees []bridge.DEEQuad
	for _, q := range deeQuads {
		var decoded []bridge.DEEQuad
		var err error
		if res.LehmerEE {
			decoded, err = bridge.DecodeDEEQuadsLehmer(q, res.FPBits)
		} else {
			decoded, err = bridge.DecodeDEEQuads(q, res.FPBits)
		}
		if err != nil {
			return StructureResult{Incomplete: true, Reason: err.Error()}
		}
		dees = append(dees, decoded...)
	}
	return ScoreStructureQuads(res, truth, endPos, dees)
}

// ScoreStructureQuads scores already-decoded, origin-trace-grouped DEE
// evidence. It invokes the same production structure materializer used by SB3
// before consulting truth; truth is used only for the final accuracy verdict.
func ScoreStructureQuads(res SBResult, truth SBTruth, endPos map[uint64]int64, dees []bridge.DEEQuad) StructureResult {
	if res.Root == nil {
		return StructureResult{}
	}
	status := ApplyStructureEvidence(&res, dees)
	if !status.Complete {
		return StructureResult{
			DEEAmbiguous: status.DEEAmbiguous > 0,
			DEEUnplaced:  status.DEENoPlace > 0,
			Incomplete:   true,
			Reason:       status.Reason,
		}
	}

	stByID := map[uint64]*STNode{}

	var build func(n *SBNode, trueID uint64, depth int) *STNode
	build = func(n *SBNode, trueID uint64, depth int) *STNode {
		st := &STNode{ID: trueID, Ord: n.Ord, Real: n.RealID != 0,
			Children: map[int]*STNode{}, EE: append([]int(nil), n.EE...),
			DEE: append([]int(nil), n.DEE...), EndOrder: append([]int(nil), n.EndOrder...)}
		for ord, c := range n.Children {
			childTrueID := truth.ChildByOrd[trueID][ord]
			st.Children[ord] = build(c, childTrueID, depth+1)
		}
		stByID[trueID] = st
		return st
	}
	stRoot := build(res.Root, truth.RootID, 0)

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
