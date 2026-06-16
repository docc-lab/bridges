package recon

import (
	"encoding/binary"
	"fmt"

	"bridges/bridge"
)

// S-Bridge reconstruction (recon/sbridge.go).
//
// Each emitting span (checkpoint at OnStart, leaf at OnEnd) persists an _br
// payload: the 4-byte window anchor (ckpt4), an explicit breadcrumb chain of
// (start-ordinal, parent-fingerprint) per level from the window root down to
// the span, per-level EE sub-lists, and trailing DEE quads (see
// bridge/sbridge_decode.go). Topology reconstruction rebuilds the tree from
// those chains: within a window the chain ordinals form a trie keyed by the
// ordinal-path from the root, and interior non-emitting spans fall out as
// inferred nodes identified by the fingerprint their children carry.
//
// MILESTONE: no-drop, multi-window. Windows are stitched by ckpt4 — each
// checkpoint span anchors its own window AND sits as a terminal node in its
// parent window's chains, and those two are the same node object, so child
// windows hang under their anchor automatically. Ambiguous ckpt4 (two distinct
// checkpoints sharing a 4-byte truncation) -> Unsolvable. Drop handling is the
// next increment.

// SBInput is one surviving emitting span: its known span id plus the raw _br
// payload it persisted.
type SBInput struct {
	SpanID  uint64
	Payload []byte
}

// SBNode is a reconstructed tree node. Emitting spans land on a node with a
// known RealID; interior non-emitting spans are inferred nodes (RealID 0)
// identified by the fingerprint recovered from their children's chain entries.
type SBNode struct {
	Ord      int    // start ordinal under its parent (0 for the window root)
	RealID   uint64 // known span id if an emitting span landed here, else 0
	FP       uint32 // recovered fingerprint
	FPBits   int    // 32 = window-root ckpt4, 16 = interior 2-byte fp, 0 = none (bare leaf)
	Children map[int]*SBNode
}

func newSBNode(ord int) *SBNode { return &SBNode{Ord: ord, Children: map[int]*SBNode{}} }

// SBResult is the reconstructed tree for one trace.
type SBResult struct {
	Root       *SBNode
	Unsolvable bool
	Reason     string

	// Orphan-match accounting (see placeOrphans).
	OrphanPlaced    int // surviving non-checkpoint spans uniquely matched to a placeholder
	OrphanAmbiguous int // matched >1 placeholder (truncated-key collision) -> failure
	OrphanNoPlace   int // matched 0 placeholders (only if a checkpoint was lost; not exercised)
}

// SBOrphan is a surviving non-checkpoint span carrying real data. own-fp and
// parent-fp come for free from the real span/parent ids; the span only emits
// its ordinal + depth. Reconstruction matches it to a unique synthetic
// placeholder by all four (own-fp, parent-fp, depth, ordinal).
type SBOrphan struct {
	SpanID   uint64
	ParentID uint64
	Depth    int
	Ordinal  int
}

func unsolvable(format string, a ...any) SBResult {
	return SBResult{Unsolvable: true, Reason: fmt.Sprintf(format, a...)}
}

// ReconstructSBridge rebuilds the topology of one trace from its emitting spans
// (checkpoint + leaf chains -> the coalesced placeholder graph), then matches
// each surviving non-checkpoint orphan into a unique placeholder.
func ReconstructSBridge(inputs []SBInput, orphans []SBOrphan, cfg Config) SBResult {
	type decoded struct {
		id uint64
		br bridge.SBridgeBR
	}
	ds := make([]decoded, 0, len(inputs))
	for _, in := range inputs {
		br, err := bridge.DecodeSBridgeBR(in.Payload, cfg.CPD)
		if err != nil {
			return unsolvable("decode span %016x: %v", in.SpanID, err)
		}
		ds = append(ds, decoded{id: in.SpanID, br: br})
	}

	// Ambiguous ckpt4: two DISTINCT checkpoints whose ids share a 4-byte
	// truncation can't be told apart as window anchors -> the trace is
	// unsolvable (practically never happens, but guard it rather than misbuild).
	ckptByTop4 := map[uint32]uint64{}
	for _, d := range ds {
		if d.br.Depth%cfg.CPD == 0 {
			t4 := uint32(d.id >> 32)
			if prev, ok := ckptByTop4[t4]; ok && prev != d.id {
				return unsolvable("ambiguous ckpt4 %08x: checkpoints %016x and %016x", t4, prev, d.id)
			}
			ckptByTop4[t4] = d.id
		}
	}

	// One node per window anchor (its 4-byte ckpt4 identity). A checkpoint span
	// is BOTH a terminal in its parent window's chains and the root of its own
	// window, so its parent-window placement reuses the very same node object
	// (getWindow by top4(id)) — that unification IS the stitch.
	windowRoot := map[uint32]*SBNode{}
	getWindow := func(x uint32) *SBNode {
		if n, ok := windowRoot[x]; ok {
			return n
		}
		n := newSBNode(0)
		n.FP, n.FPBits = x, 32
		windowRoot[x] = n
		return n
	}

	var root *SBNode
	for _, d := range ds {
		isCkpt := d.br.Depth%cfg.CPD == 0
		if isCkpt && d.br.Depth == 0 {
			// Trace root: its own window anchor, no parent window to place into.
			n := getWindow(uint32(d.id >> 32))
			if n.RealID != 0 && n.RealID != d.id {
				return unsolvable("root collision: %016x vs %016x", n.RealID, d.id)
			}
			n.RealID = d.id
			root = n
			continue
		}
		// Place this span in its window (its payload ckpt4) by walking the chain.
		cur := getWindow(binary.BigEndian.Uint32(d.br.Ckpt4[:]))
		for j, lv := range d.br.Chain {
			// lv.FP is cur's (the parent's) fingerprint when present; never
			// overwrite a window root's 4-byte ckpt4 (FPBits 32).
			if lv.HasFP && cur.FPBits != 32 {
				if cur.FPBits == 16 && uint16(cur.FP) != lv.FP {
					return unsolvable("span %016x: conflicting fp (%04x vs %04x)", d.id, uint16(cur.FP), lv.FP)
				}
				cur.FP, cur.FPBits = uint32(lv.FP), 16
			}
			var child *SBNode
			if j == len(d.br.Chain)-1 && isCkpt {
				// Terminal checkpoint: its node IS the root of its own window.
				child = getWindow(uint32(d.id >> 32))
			}
			if existing, ok := cur.Children[lv.Ord]; ok {
				if child != nil && existing != child {
					return unsolvable("span %016x: ordinal %d already taken at a chain node", d.id, lv.Ord)
				}
				child = existing
			}
			if child == nil {
				child = newSBNode(lv.Ord)
			}
			child.Ord = lv.Ord
			cur.Children[lv.Ord] = child
			cur = child
		}
		if cur.RealID != 0 && cur.RealID != d.id {
			return unsolvable("span %016x collides with %016x at the same chain position", d.id, cur.RealID)
		}
		cur.RealID = d.id
	}
	if root == nil {
		return unsolvable("no depth-0 root checkpoint among %d emitting spans", len(inputs))
	}
	res := SBResult{Root: root}
	res.placeOrphans(orphans)
	return res
}

// node16fp returns a node's 2-byte fingerprint for orphan matching: the low 2
// bytes of a 16-bit interior fp, or the top 2 bytes of a 32-bit window-anchor
// (ckpt4). Bare leaves (no recovered fp) return ok=false.
func node16fp(n *SBNode) (uint16, bool) {
	switch n.FPBits {
	case 16:
		return uint16(n.FP), true
	case 32:
		return uint16(n.FP >> 16), true // top 2 of the 4-byte anchor
	}
	return 0, false
}

// placeOrphans matches each surviving non-checkpoint orphan to a synthetic
// placeholder by the 4-tuple (own-fp, parent-fp, depth, ordinal). The
// placeholder graph is already coalesced (one position-keyed trie), so a clash
// is a genuine truncated-key collision, not an un-merged duplicate. Outcomes:
// unique match -> identify the placeholder with the orphan's id; >=2 -> ambiguity
// (failure); 0 -> no placeholder (only if a checkpoint was lost). Orphan
// subgraphs are placed member-by-member, which is equivalent.
func (res *SBResult) placeOrphans(orphans []SBOrphan) {
	if res.Root == nil || len(orphans) == 0 {
		return
	}
	type key struct {
		depth, ord int
		fp, pfp    uint16
	}
	idx := map[key][]*SBNode{}
	var walk func(n *SBNode, depth int, pfp uint16, pfpOK bool)
	walk = func(n *SBNode, depth int, pfp uint16, pfpOK bool) {
		nfp, nfpOK := node16fp(n)
		// Only unidentified placeholders with a recoverable fp and a fingerprinted
		// parent are orphan-match candidates (checkpoints/leaves are already known).
		if n.RealID == 0 && nfpOK && pfpOK {
			idx[key{depth, n.Ord, nfp, pfp}] = append(idx[key{depth, n.Ord, nfp, pfp}], n)
		}
		for _, c := range n.Children {
			walk(c, depth+1, nfp, nfpOK)
		}
	}
	walk(res.Root, 0, 0, false)

	for _, o := range orphans {
		k := key{o.Depth, o.Ordinal, uint16(o.SpanID >> 48), uint16(o.ParentID >> 48)}
		cands := idx[k]
		switch len(cands) {
		case 0:
			res.OrphanNoPlace++
		case 1:
			cands[0].RealID = o.SpanID
			res.OrphanPlaced++
		default:
			res.OrphanAmbiguous++
		}
	}
}

// ---- scoring against ground truth ----

// SBTruth is the pre-drop tree: the root id, each span's children indexed by
// their start ordinal (1-based rank among siblings by start order), and the
// full id set. Fingerprints are derived from the ids (top 4/2 big-endian bytes).
type SBTruth struct {
	RootID     uint64
	ChildByOrd map[uint64]map[int]uint64 // parent id -> ordinal -> child id
}

// SBVerdict is the per-trace reconstruction outcome.
type SBVerdict struct {
	Correct    bool
	Unsolvable bool
	Reason     string
}

// ScoreSBridge checks the reconstructed tree against ground truth: exact
// call-graph shape AND a fingerprint match at every node that carries one
// (window root 4-byte ckpt4, interior 2-byte fp). Leaves are matched by
// position + known id.
func ScoreSBridge(res SBResult, truth SBTruth) SBVerdict {
	if res.Unsolvable {
		return SBVerdict{Unsolvable: true, Reason: res.Reason}
	}
	if res.Root == nil {
		return SBVerdict{Reason: "nil root"}
	}
	if res.Root.RealID != truth.RootID {
		return SBVerdict{Reason: fmt.Sprintf("root id %016x != truth %016x", res.Root.RealID, truth.RootID)}
	}
	if ok, why := sbWalk(res.Root, truth.RootID, truth); !ok {
		return SBVerdict{Reason: why}
	}
	return SBVerdict{Correct: true}
}

// ScoreSBridgeUnderDrop is the drop-tolerant verdict. The reconstructed tree
// must EMBED correctly into ground truth — every reconstructed node sits at a
// true position (true ancestry) with a matching fingerprint and id — but truth
// children that were dropped (hence not reconstructed) are not required, and a
// dropped span's node may have RealID 0 (matched by fp/position instead).
// Correct iff there are no wrong edges and every recovered fp/id matches.
func ScoreSBridgeUnderDrop(res SBResult, truth SBTruth) SBVerdict {
	if res.Unsolvable {
		return SBVerdict{Unsolvable: true, Reason: res.Reason}
	}
	if res.Root == nil {
		return SBVerdict{Reason: "nil root"}
	}
	if res.Root.RealID != 0 && res.Root.RealID != truth.RootID {
		return SBVerdict{Reason: fmt.Sprintf("root id %016x != truth %016x", res.Root.RealID, truth.RootID)}
	}
	if ok, why := sbWalkPartial(res.Root, truth.RootID, truth); !ok {
		return SBVerdict{Reason: why}
	}
	return SBVerdict{Correct: true}
}

// sbWalkPartial is sbWalk without the child-count equality requirement: it
// rejects wrong edges and fp/id mismatches but tolerates truth children that
// were dropped and so weren't reconstructed.
func sbWalkPartial(node *SBNode, realID uint64, truth SBTruth) (bool, string) {
	switch node.FPBits {
	case 32:
		if node.FP != uint32(realID>>32) {
			return false, fmt.Sprintf("node %016x ckpt4 %08x != %08x", realID, node.FP, uint32(realID>>32))
		}
	case 16:
		if uint16(node.FP) != uint16(realID>>48) {
			return false, fmt.Sprintf("node %016x fp %04x != %04x", realID, uint16(node.FP), uint16(realID>>48))
		}
	}
	kids := truth.ChildByOrd[realID]
	for ord, child := range node.Children {
		realChild, ok := kids[ord]
		if !ok {
			return false, fmt.Sprintf("wrong edge: node %016x has no truth child at ordinal %d", realID, ord)
		}
		if child.RealID != 0 && child.RealID != realChild {
			return false, fmt.Sprintf("node %016x ord %d: id %016x != truth %016x", realID, ord, child.RealID, realChild)
		}
		if ok, why := sbWalkPartial(child, realChild, truth); !ok {
			return false, why
		}
	}
	return true, ""
}

// CountReal returns the number of reconstructed nodes carrying a known span id
// (reachable from the root) — used to report coverage vs surviving emitters.
func (r SBResult) CountReal() int {
	if r.Root == nil {
		return 0
	}
	n := 0
	var rec func(*SBNode)
	rec = func(nd *SBNode) {
		if nd.RealID != 0 {
			n++
		}
		for _, c := range nd.Children {
			rec(c)
		}
	}
	rec(r.Root)
	return n
}

func sbWalk(node *SBNode, realID uint64, truth SBTruth) (bool, string) {
	switch node.FPBits {
	case 32:
		if node.FP != uint32(realID>>32) {
			return false, fmt.Sprintf("node %016x ckpt4 %08x != %08x", realID, node.FP, uint32(realID>>32))
		}
	case 16:
		if uint16(node.FP) != uint16(realID>>48) {
			return false, fmt.Sprintf("node %016x fp %04x != %04x", realID, uint16(node.FP), uint16(realID>>48))
		}
	}
	kids := truth.ChildByOrd[realID]
	if len(kids) != len(node.Children) {
		return false, fmt.Sprintf("node %016x: %d children, truth has %d", realID, len(node.Children), len(kids))
	}
	for ord, child := range node.Children {
		realChild, ok := kids[ord]
		if !ok {
			return false, fmt.Sprintf("node %016x: no truth child at ordinal %d", realID, ord)
		}
		if child.RealID != 0 && child.RealID != realChild {
			return false, fmt.Sprintf("node %016x ord %d: id %016x != truth %016x", realID, ord, child.RealID, realChild)
		}
		if ok, why := sbWalk(child, realChild, truth); !ok {
			return false, why
		}
	}
	return true, ""
}
