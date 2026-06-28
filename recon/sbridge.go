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
	FP       uint64 // recovered fingerprint, right-aligned to its top FPBits of the span id
	FPBits   int    // width in bits of FP: window root = ckpt-anchor bits (CkptBytes*8), interior fp width (e.g. 16), or 0 = none (bare leaf)
	IsRoot   bool   // this node is a window anchor (its FP is the checkpoint-root prefix, never overwritten by an interior fp)
	EE       []int  // this node's witnessed end-block (earlier-sibling ends before it started)
	Children map[int]*SBNode
}

func newSBNode(ord int) *SBNode { return &SBNode{Ord: ord, Children: map[int]*SBNode{}} }

// SBResult is the reconstructed tree for one trace.
type SBResult struct {
	Root       *SBNode
	Unsolvable bool
	Reason     string
	FPBits     int  // non-checkpoint fp width used (for severed matching)
	NoOrdinal  bool // severed placement ignores the ordinal (own-fp distinguishes siblings)

	// Attachment accounting.
	Identified      int // survivors attached by their real parent edge (connected)
	SeveredPlaced   int // severed survivors (parent dropped) uniquely matched by 4-tuple
	SeveredAmbiguous int // severed survivor matched >1 slot (truncated-key collision) -> fail
	SeveredNoPlace  int // severed survivor matched 0 slots (shouldn't happen: leaves never drop)
}

// SBSurvivor is one surviving span's real topology: id, real parent, 1-based
// start ordinal under that parent, and absolute depth. Connected survivors
// attach by their real edge; severed ones (parent dropped) by the 4-tuple
// (depth, ordinal, own-fp, parent-fp).
type SBSurvivor struct {
	SpanID   uint64
	ParentID uint64
	Ordinal  int
	Depth    int
}

func unsolvable(format string, a ...any) SBResult {
	return SBResult{Unsolvable: true, Reason: fmt.Sprintf(format, a...)}
}

// ReconstructSBridge rebuilds the topology of one trace. The emitting spans'
// chains build the skeleton (positioning dropped interior spans by ckpt4 + the
// breadcrumb shift); then connected survivors are identified on that skeleton by
// walking their REAL parent->child edges from the root — no placeholder search.
func ReconstructSBridge(inputs []SBInput, survivors []SBSurvivor, cfg Config) SBResult {
	fpBits := cfg.FPBits
	if fpBits <= 0 {
		fpBits = 16 // legacy 2-byte fp
	}
	ckptBytes := cfg.PrefixLen
	if ckptBytes <= 0 {
		ckptBytes = 4 // legacy 4-byte checkpoint-root anchor
	}
	ckptBits := ckptBytes * 8
	type decoded struct {
		id uint64
		br bridge.SBridgeBR
	}
	ds := make([]decoded, 0, len(inputs))
	for _, in := range inputs {
		br, err := bridge.DecodeSBridgeBR(in.Payload, cfg.CPD, fpBits, ckptBytes)
		if err != nil {
			return unsolvable("decode span %016x: %v", in.SpanID, err)
		}
		ds = append(ds, decoded{id: in.SpanID, br: br})
	}

	// ckptKey is a span id truncated to the checkpoint-root anchor width (top
	// ckptBits), the value used to key windows. ckptKeyOf reads the same value
	// back from a decoded anchor (left-aligned in an [8]byte).
	ckptKey := func(id uint64) uint64 { return id >> uint(64-ckptBits) }
	ckptKeyOf := func(c [8]byte) uint64 { return binary.BigEndian.Uint64(c[:]) >> uint(64-ckptBits) }

	// Ambiguous anchor: two DISTINCT checkpoints whose ids share a ckptBytes-byte
	// truncation can't be told apart as window anchors -> the trace is
	// unsolvable (vanishes at full width; guard it rather than misbuild).
	ckptByTop := map[uint64]uint64{}
	for _, d := range ds {
		if d.br.Depth%cfg.CPD == 0 {
			t := ckptKey(d.id)
			if prev, ok := ckptByTop[t]; ok && prev != d.id {
				return unsolvable("ambiguous ckpt anchor %0*x: checkpoints %016x and %016x", ckptBytes*2, t, prev, d.id)
			}
			ckptByTop[t] = d.id
		}
	}

	// One node per window anchor (its ckptBytes-byte truncated identity). A
	// checkpoint span is BOTH a terminal in its parent window's chains and the
	// root of its own window, so its parent-window placement reuses the very same
	// node object (getWindow by ckptKey(id)) — that unification IS the stitch.
	windowRoot := map[uint64]*SBNode{}
	getWindow := func(x uint64) *SBNode {
		if n, ok := windowRoot[x]; ok {
			return n
		}
		n := newSBNode(0)
		n.FP, n.FPBits, n.IsRoot = x, ckptBits, true
		windowRoot[x] = n
		return n
	}

	var root *SBNode
	for _, d := range ds {
		isCkpt := d.br.Depth%cfg.CPD == 0
		if isCkpt && d.br.Depth == 0 {
			// Trace root: its own window anchor, no parent window to place into.
			n := getWindow(ckptKey(d.id))
			if n.RealID != 0 && n.RealID != d.id {
				return unsolvable("root collision: %016x vs %016x", n.RealID, d.id)
			}
			n.RealID = d.id
			root = n
			continue
		}
		// Place this span in its window (its payload anchor) by walking the chain.
		cur := getWindow(ckptKeyOf(d.br.Ckpt))
		for j, lv := range d.br.Chain {
			// lv.FP is cur's (the parent's) fingerprint when present; never
			// overwrite a window root's anchor prefix.
			if lv.HasFP && !cur.IsRoot {
				if cur.FPBits == fpBits && cur.FP != lv.FP {
					return unsolvable("span %016x: conflicting fp (%x vs %x)", d.id, cur.FP, lv.FP)
				}
				cur.FP, cur.FPBits = lv.FP, fpBits
			}
			var child *SBNode
			if j == len(d.br.Chain)-1 && isCkpt {
				// Terminal checkpoint: its node IS the root of its own window.
				child = getWindow(ckptKey(d.id))
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
			if len(lv.EE) > 0 { // the ends this child witnessed before it started
				child.EE = lv.EE
			}
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
	res := SBResult{Root: root, FPBits: fpBits, NoOrdinal: cfg.NoOrdinal}
	labeled := map[uint64]bool{}
	collectRealIDs(root, labeled) // checkpoints + leaves placed by their own chains (incl. severed)
	res.identifyByEdges(survivors, labeled)
	res.placeSevered(survivors, labeled)
	return res
}

// collectRealIDs records every span id already placed on the skeleton by a chain
// (the checkpoints and leaves, wherever they sit — connected or severed).
func collectRealIDs(n *SBNode, into map[uint64]bool) {
	if n.RealID != 0 {
		into[n.RealID] = true
	}
	for _, c := range n.Children {
		collectRealIDs(c, into)
	}
}

// identifyByEdges attaches CONNECTED interior survivors to the skeleton top-down
// by their REAL parent->child edges — no fingerprint matching. From the root, a
// surviving child at ordinal o under an identified node takes the skeleton child
// at that ordinal. Adds each attached id to labeled; Identified counts the ones
// it newly placed (previously-unlabeled interior nodes). Survivors whose parent
// dropped are never reached here — placeSevered handles them.
func (res *SBResult) identifyByEdges(survivors []SBSurvivor, labeled map[uint64]bool) {
	if res.Root == nil {
		return
	}
	kids := make(map[uint64]map[int]uint64) // real parent id -> ordinal -> child id
	for _, s := range survivors {
		if s.ParentID == 0 {
			continue
		}
		m := kids[s.ParentID]
		if m == nil {
			m = make(map[int]uint64)
			kids[s.ParentID] = m
		}
		m[s.Ordinal] = s.SpanID
	}
	visited := map[*SBNode]bool{}
	queue := []*SBNode{res.Root}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if visited[n] || n.RealID == 0 {
			continue
		}
		visited[n] = true
		for ord, childID := range kids[n.RealID] {
			c := n.Children[ord]
			if c == nil {
				continue
			}
			if c.RealID == 0 {
				c.RealID = childID // newly attached interior span
				res.Identified++
			}
			labeled[childID] = true
			queue = append(queue, c)
		}
	}
}

// nodeFP returns a skeleton node's fingerprint truncated to w bits: an interior
// fp (already w bits) directly, or the top w bits of a window anchor. Used to
// build the severed-placement key at a uniform width.
func nodeFP(n *SBNode, w int) (uint64, bool) {
	switch {
	case n.FPBits == 0:
		return 0, false
	case n.IsRoot: // window anchor (ckptBits wide): project to the top w bits
		if n.FPBits >= w {
			return n.FP >> uint(n.FPBits-w), true
		}
		return n.FP, true // anchor narrower than w: best effort (degenerate config)
	default: // interior fp, stored at w bits
		return n.FP, true
	}
}

// placeSevered re-attaches survivors whose parent dropped (so edges couldn't
// reach them) by matching their (depth, ordinal, own-fp, parent-fp) 4-tuple to a
// unique unlabeled skeleton slot. Because leaves never drop, every dropped
// interior span is witnessed by a surviving leaf below it, so the slot always
// exists. >=2 matches -> ambiguous (a truncated-key collision); 0 -> no slot
// (shouldn't happen).
func (res *SBResult) placeSevered(survivors []SBSurvivor, labeled map[uint64]bool) {
	w := res.FPBits
	if w <= 0 {
		w = 16
	}
	// ordOf zeroes the ordinal when NoOrdinal: severed placement then keys on
	// (depth, own-fp, parent-fp) only, letting own-fp distinguish siblings.
	ordOf := func(o int) int {
		if res.NoOrdinal {
			return 0
		}
		return o
	}
	type key struct {
		depth, ord int
		fp, pfp    uint64
	}
	idx := map[key][]*SBNode{}
	var walk func(n *SBNode, depth int, pfp uint64, pfpOK bool)
	walk = func(n *SBNode, depth int, pfp uint64, pfpOK bool) {
		nfp, nfpOK := nodeFP(n, w)
		if n.RealID == 0 && nfpOK && pfpOK {
			k := key{depth, ordOf(n.Ord), nfp, pfp}
			idx[k] = append(idx[k], n)
		}
		for _, c := range n.Children {
			walk(c, depth+1, nfp, nfpOK)
		}
	}
	walk(res.Root, 0, 0, false)

	topW := func(id uint64) uint64 { return id >> uint(64-w) }
	for _, s := range survivors {
		if labeled[s.SpanID] {
			continue // attached by edge
		}
		k := key{s.Depth, ordOf(s.Ordinal), topW(s.SpanID), topW(s.ParentID)}
		switch cands := idx[k]; len(cands) {
		case 0:
			res.SeveredNoPlace++
		case 1:
			cands[0].RealID = s.SpanID
			res.SeveredPlaced++
		default:
			res.SeveredAmbiguous++
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
	if node.FPBits > 0 { // window anchor (ckptBits) or interior fp — both right-aligned at FPBits
		if want := realID >> uint(64-node.FPBits); node.FP != want {
			return false, fmt.Sprintf("node %016x fp %x != %x (%db)", realID, node.FP, want, node.FPBits)
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
	if node.FPBits > 0 { // window anchor (ckptBits) or interior fp — both right-aligned at FPBits
		if want := realID >> uint(64-node.FPBits); node.FP != want {
			return false, fmt.Sprintf("node %016x fp %x != %x (%db)", realID, node.FP, want, node.FPBits)
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
