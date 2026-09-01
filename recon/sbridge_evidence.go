package recon

import (
	"fmt"
	"sort"

	"bridges/bridge"
)

// SBStructureStatus describes the topology-independent EE/DEE reconstruction
// performed after a tree has received a complete start-ordinal labeling.
// Complete means every parent has one reconstructed end order. Ambiguous and
// unplaced DEEs are never guessed through.
type SBStructureStatus struct {
	Complete        bool
	Omitted         bool
	Reason          string
	Parents         int
	ParentsComplete int
	DEEPlaced       int
	DEEAmbiguous    int
	DEENoPlace      int
}

func (s *SBStructureStatus) fail(format string, args ...any) {
	if s.Reason == "" {
		s.Reason = fmt.Sprintf(format, args...)
	}
}

type sbStructureCandidate struct {
	node *SBNode
	cand DEECandidate
}

// ApplyStructureEvidence completes the lateral S-Bridge layer on an already
// reconstructed and ordinal-labeled topology. It validates that each sibling
// set is exactly labeled 1..k, validates inline EE blocks in that namespace,
// attributes DEE records without guessing, and materializes DEE and EndOrder
// directly on each parent SBNode.
//
// Synthetic nodes with a known topology identity use its fingerprint. A truly
// anonymous synthetic node starts with an unknown fingerprint and can be
// selected only when depth and EE/DEE content make it the unique owner; that
// successful placement also learns the node's fingerprint.
func ApplyStructureEvidence(res *SBResult, dees []bridge.DEEQuad) SBStructureStatus {
	var out SBStructureStatus
	if res == nil || res.Root == nil {
		out.fail("nil structural root")
		return out
	}
	if res.Root.Ord != 0 {
		out.fail("root has ordinal %d, want 0", res.Root.Ord)
		return out
	}
	fpBits := res.FPBits
	if fpBits <= 0 {
		fpBits = 16
	}

	var cands []sbStructureCandidate
	var parents []*SBNode
	var walk func(*SBNode, int) bool
	walk = func(n *SBNode, depth int) bool {
		if n == nil {
			out.fail("nil node at depth %d", depth)
			return false
		}
		n.DEE = nil
		n.EndOrder = nil
		k := len(n.Children)
		if k > 0 {
			out.Parents++
			parents = append(parents, n)
			childOrds := make(map[int]bool, k)
			witnessed := make(map[int]bool, k)
			for ord := 1; ord <= k; ord++ {
				child := n.Children[ord]
				if child == nil {
					out.fail("depth %d parent is missing child ordinal %d of 1..%d", depth, ord, k)
					return false
				}
				if child.Ord != ord {
					out.fail("depth %d child key %d carries ordinal %d", depth, ord, child.Ord)
					return false
				}
				childOrds[ord] = true
				for _, e := range child.EE {
					// Before child ord starts, only strictly earlier start ordinals
					// can already have ended. Each end can be witnessed once.
					if e < 1 || e >= ord || witnessed[e] {
						out.fail("depth %d child ordinal %d has invalid/duplicate EE ordinal %d", depth, ord, e)
						return false
					}
					witnessed[e] = true
				}
			}
			knownFP, fpKnown := nodeFP(n, fpBits)
			c := DEECandidate{Survived: n.RealID != 0, RealID: n.RealID,
				FP: knownFP, FPKnown: fpKnown, Depth: depth,
				ChildOrds: childOrds, EE: witnessed}
			cands = append(cands, sbStructureCandidate{node: n, cand: c})
		}
		ords := make([]int, 0, k)
		for ord := range n.Children {
			ords = append(ords, ord)
		}
		sort.Ints(ords)
		for _, ord := range ords {
			if !walk(n.Children[ord], depth+1) {
				return false
			}
		}
		return true
	}
	if !walk(res.Root, 0) {
		return out
	}

	// Repeatedly place singleton DEE domains. This is deterministic constraint
	// propagation, not a guessed matching: a uniquely identified anonymous
	// owner learns its fingerprint, which can make another record unique on the
	// next pass. Records that remain multi-owner after the fixpoint are genuine
	// ambiguities and are rejected together.
	pending := append([]bridge.DEEQuad(nil), dees...)
	assigned := make([]bool, len(cands)) // one parent emits at most one DEE quad
	for len(pending) > 0 {
		progress := false
		next := make([]bridge.DEEQuad, 0, len(pending))
		for _, dq := range pending {
			flat := make([]DEECandidate, len(cands))
			for i := range cands {
				flat[i] = cands[i].cand
				if assigned[i] {
					flat[i].ChildOrds = nil // already consumed by its sole DEE
				}
			}
			idx, status := AttributeDEE(dq.OwnerFP, dq.Depth, dq.Seqs, flat, fpBits)
			switch status {
			case DEEAmbiguous:
				next = append(next, dq)
			case DEENoPlace:
				out.DEENoPlace++
				out.fail("DEE owner fp %x at depth %d has no valid parent", dq.OwnerFP, dq.Depth)
			case DEEPlaced:
				progress = true
				assigned[idx] = true
				out.DEEPlaced++
				sc := &cands[idx]
				sc.node.DEE = append(sc.node.DEE, dq.Seqs...)
				for _, seq := range dq.Seqs {
					sc.cand.EE[seq] = true
				}
				if !sc.cand.Survived && !sc.cand.FPKnown && sc.cand.FP == 0 {
					sc.cand.FP = dq.OwnerFP
					sc.cand.FPKnown = true
					sc.node.FP = dq.OwnerFP
					sc.node.FPBits = fpBits
				}
			}
		}
		pending = next
		if !progress {
			break
		}
	}
	if len(pending) > 0 {
		out.DEEAmbiguous += len(pending)
		out.fail("%d DEE record(s) remain owner-ambiguous after constraint propagation", len(pending))
	}

	// Materialize a total end order only when EE+DEE account for exactly k-1
	// distinct child ends; the one remaining child end is the implicit last.
	for _, n := range parents {
		k := len(n.Children)
		kids := make([]SBChild, 0, k)
		seen := make(map[int]bool, k)
		valid := true
		for ord := 1; ord <= k; ord++ {
			child := n.Children[ord]
			kids = append(kids, SBChild{Ord: ord, EE: child.EE})
			for _, e := range child.EE {
				if seen[e] {
					valid = false
				}
				seen[e] = true
			}
		}
		for _, e := range n.DEE {
			if e < 1 || e > k || seen[e] {
				valid = false
			}
			seen[e] = true
		}
		if !valid || len(seen) != k-1 {
			out.fail("parent at ordinal %d has evidence for %d of %d required explicit child ends", n.Ord, len(seen), k-1)
			continue
		}
		order := GatherEndOrder(kids, n.DEE)
		if len(order) != k {
			out.fail("parent at ordinal %d reconstructed %d of %d child ends", n.Ord, len(order), k)
			continue
		}
		n.EndOrder = order
		out.ParentsComplete++
	}
	out.Complete = out.Reason == "" && out.ParentsComplete == out.Parents
	return out
}
