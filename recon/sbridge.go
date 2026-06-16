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
}

func unsolvable(format string, a ...any) SBResult {
	return SBResult{Unsolvable: true, Reason: fmt.Sprintf(format, a...)}
}

// ReconstructSBridge rebuilds the topology of one trace from its emitting spans.
func ReconstructSBridge(inputs []SBInput, cfg Config) SBResult {
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
	return SBResult{Root: root}
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
