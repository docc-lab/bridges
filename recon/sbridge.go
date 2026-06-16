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
// MILESTONE: this is the no-drop, single-window pass (one checkpoint = the
// depth-0 root; every other emitting span carries that root's ckpt4). Multi-
// window ckpt4 stitching is the next increment; until then a trace that
// presents more than one window is reported Unsolvable rather than mis-built.

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
	var checkpoints, others []decoded
	for _, in := range inputs {
		br, err := bridge.DecodeSBridgeBR(in.Payload, cfg.CPD)
		if err != nil {
			return unsolvable("decode span %016x: %v", in.SpanID, err)
		}
		d := decoded{id: in.SpanID, br: br}
		if br.Depth%cfg.CPD == 0 {
			checkpoints = append(checkpoints, d)
		} else {
			others = append(others, d)
		}
	}

	// Single-window milestone: exactly one checkpoint, the depth-0 root.
	var roots []decoded
	for _, c := range checkpoints {
		if c.br.Depth == 0 {
			roots = append(roots, c)
		}
	}
	if len(roots) == 0 {
		return unsolvable("no depth-0 root checkpoint among %d emitting spans", len(inputs))
	}
	if len(roots) > 1 {
		return unsolvable("ambiguous root: %d depth-0 checkpoints", len(roots))
	}
	if len(checkpoints) > 1 {
		return unsolvable("multi-window stitching not yet implemented (%d checkpoints)", len(checkpoints))
	}

	root := roots[0]
	rootCkpt4 := uint32(root.id >> 32)
	tree := newSBNode(0)
	tree.RealID = root.id
	tree.FP = rootCkpt4
	tree.FPBits = 32

	for _, o := range others {
		// Every non-checkpoint emitting span must anchor to the one window.
		if got := binary.BigEndian.Uint32(o.br.Ckpt4[:]); got != rootCkpt4 {
			return unsolvable("multi-window stitching not yet implemented (span %016x ckpt4 %08x != root %08x)", o.id, got, rootCkpt4)
		}
		cur := tree
		for _, lv := range o.br.Chain {
			// lv.FP is the PARENT's fingerprint (cur), present iff cur is not a
			// checkpoint. Record it and check all children agree.
			if lv.HasFP {
				if cur.FPBits == 16 && uint16(cur.FP) != lv.FP {
					return unsolvable("span %016x: conflicting fp at a chain node (%04x vs %04x)", o.id, uint16(cur.FP), lv.FP)
				}
				if cur.FPBits != 32 { // never overwrite the window root's 4-byte id
					cur.FP, cur.FPBits = uint32(lv.FP), 16
				}
			}
			child, ok := cur.Children[lv.Ord]
			if !ok {
				child = newSBNode(lv.Ord)
				cur.Children[lv.Ord] = child
			}
			cur = child
		}
		// cur is this emitting span's own node.
		if cur.RealID != 0 && cur.RealID != o.id {
			return unsolvable("span %016x collides with %016x at the same chain position", o.id, cur.RealID)
		}
		cur.RealID = o.id
	}
	return SBResult{Root: tree}
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
