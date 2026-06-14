package recon

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	"bridges/bloom"
	"bridges/bridge"
)

// cgpDiag, when set, logs why a witnessed fan-out's direct child failed to be
// reunited under it (recall-miss anatomy). Off by default.
var cgpDiag = os.Getenv("TRACE_RECON_CGPDIAG") == "1"

// cgpPerf, when set, logs per-trace structural counts (survivors, synthetics,
// fan-outs) and resolve() call/hop totals — to distinguish a call-count blowup
// from a chain-length blowup in the merge passes.
var cgpPerf = os.Getenv("TRACE_RECON_CGPPERF") == "1"

// topoDiag, when set, logs each over-merged reconstructed fan-out group (a
// precision violation): the recovered fan-out fingerprint and, per member, its
// orphan id, TRUE dropped parent, and depths — to reveal which distinct
// dropped fan-outs are being collapsed and how they sit structurally.
var topoDiag = os.Getenv("TRACE_RECON_TOPODIAG") == "1"

// connMeasure, when set, quantifies the connectivity-gain ceiling of a sibling-
// pinning rule in phase 1: among wrong-anchored orphans, how many have a
// same-true-parent sibling that IS correctly anchored (so pinning to it would
// fix them) vs. are only-children (no sibling to borrow an anchor from).
var connMeasure = os.Getenv("TRACE_RECON_CONNMEASURE") == "1"

// stealMeasure quantifies the ceiling of "fan-out stealing": a wrong-anchored
// fragment O is rescuable if its true dropped ancestor chain (O..nearestSurv)
// contains a branch-point F (>=2 true children -> HA-named fan-out) that has
// some OTHER correctly-anchored descendant fragment. Then F is well-anchored,
// O's bloom confirms F, and re-anchoring O under F yields the correct anchor.
var stealMeasure = os.Getenv("TRACE_RECON_STEALMEASURE") == "1"

// DecodeCGPRBPayload parses a CGPRB _br value:
//
//	type(1) || varint(depth) || ckptK || bloomBits || haBytes
//
// The bloom has the fixed PCRB geometry length, so the HA is the trailing
// remainder: a sequence of entries, each big-endian 8-byte branch-parent id
// followed by varint(child depth).
func DecodeCGPRBPayload(p []byte, cfg Config) (depth int, prefix, bloomBits []byte, ha []HAEntry, err error) {
	if len(p) < 2 || p[0] != byte(bridge.CGPRBBridgeTypeID) {
		return 0, nil, nil, nil, errors.New("recon: not a CGPRB payload")
	}
	d, n := binary.Uvarint(p[1:])
	if n <= 0 {
		return 0, nil, nil, nil, errors.New("recon: bad depth varint")
	}
	rest := p[1+n:]
	bloomLen := int((cfg.BloomM + 7) / 8)
	if len(rest) < cfg.PrefixLen+bloomLen {
		return 0, nil, nil, nil, errors.New("recon: cgprb payload too short")
	}
	prefix = rest[:cfg.PrefixLen]
	bloomBits = rest[cfg.PrefixLen : cfg.PrefixLen+bloomLen]
	haRaw := rest[cfg.PrefixLen+bloomLen:]
	for len(haRaw) > 0 {
		if len(haRaw) < 8 {
			return 0, nil, nil, nil, errors.New("recon: truncated cgprb ha entry")
		}
		pid := binary.BigEndian.Uint64(haRaw[:8])
		haRaw = haRaw[8:]
		hd, m := binary.Uvarint(haRaw)
		if m <= 0 {
			return 0, nil, nil, nil, errors.New("recon: bad cgprb ha depth varint")
		}
		haRaw = haRaw[m:]
		ha = append(ha, HAEntry{ParentID: pid, Depth: int(hd)})
	}
	return int(d), prefix, bloomBits, ha, nil
}

// rnode is one node in the explicit reconstructed forest: a real surviving span
// or a phase-1 synthetic. Synthetics carry the covering bloom of the fragment
// they were inserted for, so fan-out membership can be tested downstream.
type rnode struct {
	id     uint64
	depth  int
	surv   bool
	parent uint64        // 0 = none (root / detached)
	fanout uint64        // known fan-out fingerprint marked on this node, else 0
	bf     *bloom.Filter // downstream covering bloom (synthetics only)
}

// ReconstructCGPRB reconstructs a call-graph-preserving trace via the paper's
// two-phase CGP-Bridge algorithm (§3.3).
//
//	Phase 1 (establish_connectivity): PCRS path reattachment — each fragment
//	  anchors to its nearest surviving ancestor by bloom, with a synthetic gap.
//	Phase 2 (merge_fanouts): synthetics that represent the same lost fan-out,
//	  duplicated across the fan-out's separate descendant subtrees, are merged
//	  into one. We materialize phase-1's implicit synthetics as explicit nodes,
//	  mark the known fan-outs from the hash arrays, then BFS top-down: for each
//	  known fan-out k, candidates are the SAME-PARENT synthetics at k's depth
//	  whose downstream bloom contains k's fingerprint; those collapse into k.
//	  The same-parent constraint (resolved through earlier merges) and the
//	  bloom verification are what keep the collapse from over-merging.
//
// Exact-edge head start: a fragment whose ParentID is a dropped fan-out M is a
// VERIFIED child of M (no bloom needed, never a false positive), so those
// direct edges are merged first to pin M's canonical node and anchor; the bloom
// collapse then conforms the deeper descendants to it.
func ReconstructCGPRB(survivors []Span, cfg Config) Result {
	res := ReconstructPCRS(survivors, cfg)

	surv := make(map[uint64]bool, len(survivors))
	byID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		surv[survivors[i].SpanID] = true
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

	// Known dropped fan-outs from the hash arrays: fingerprint -> depth.
	fanouts := make(map[uint64]int)
	for i := range survivors {
		for _, e := range survivors[i].HA {
			if !surv[e.ParentID] {
				fanouts[e.ParentID] = e.Depth - 1
			}
		}
	}
	if len(fanouts) == 0 {
		return res // no witnessed fan-outs to recover
	}

	// Build the explicit forest: real survivors + per-bridge synthetic chains.
	nodes := make(map[uint64]*rnode, len(survivors)*2)
	for i := range survivors {
		s := &survivors[i]
		p := uint64(0)
		if surv[s.ParentID] {
			p = s.ParentID
		}
		nodes[s.SpanID] = &rnode{id: s.SpanID, depth: s.Depth, surv: true, parent: p}
	}
	// Bottom-up covering carrier (memoized): each span's nearest descendant
	// carrier within its window. Replaces the per-bridge subtree BFS in
	// coveringPCRBPayload — at high drop, many carrier-less fragment roots per
	// window each re-walked the same overlapping subtree, O(fragments x window)
	// ~ O(W^2) per window, which ground for hours on full-corpus monster traces.
	// Memoized, each span's carrier is computed once: O(W). Blooms are
	// deserialized once per carrier and cached.
	coverMemo := make(map[uint64]*Span, len(survivors))
	var cover func(s *Span) *Span
	cover = func(s *Span) *Span {
		if v, ok := coverMemo[s.SpanID]; ok {
			return v // also the cycle guard (trees won't, but be safe)
		}
		coverMemo[s.SpanID] = nil
		var best *Span
		if s.CkptPrefix != nil {
			best = s // a carrier covers itself; stop (don't cross into its window)
		} else {
			for _, c := range children[s.SpanID] {
				if cc := cover(c); cc != nil && (best == nil || cc.Depth < best.Depth) {
					best = cc
				}
			}
		}
		coverMemo[s.SpanID] = best
		return best
	}
	bfCache := make(map[uint64]*bloom.Filter)
	coverBloom := func(o *Span) *bloom.Filter {
		cc := cover(o)
		if cc == nil || cc.BloomBits == nil {
			return nil
		}
		if bf, ok := bfCache[cc.SpanID]; ok {
			return bf
		}
		bf := bloom.Deserialize(cc.BloomBits, cfg.BloomM, cfg.BloomK)
		bfCache[cc.SpanID] = bf
		return bf
	}

	synSeq := uint64(1) << 62 // synthetic id space, disjoint from real span ids
	synOwner := make(map[uint64]int)   // synthetic id -> owning bridge index
	phase1Anchor := make([]uint64, len(res.Bridges))
	for bi := range res.Bridges {
		phase1Anchor[bi] = res.Bridges[bi].AnchorID
		o := byID[res.Bridges[bi].OrphanID]
		a := byID[res.Bridges[bi].AnchorID]
		if o == nil || a == nil {
			continue
		}
		bf := coverBloom(o)
		child := o.SpanID
		for d := o.Depth - 1; d > a.Depth; d-- {
			sid := synSeq
			synSeq++
			nodes[sid] = &rnode{id: sid, depth: d, parent: 0, bf: bf}
			synOwner[sid] = bi
			nodes[child].parent = sid
			child = sid
		}
		nodes[child].parent = a.SpanID
	}

	merged := make(map[uint64]uint64) // node id -> kept node id
	// resolve is a union-find find with PATH COMPRESSION: it follows the merge
	// chain to its root, then rewrites every link on the path to point straight
	// at the root. Without compression, chained merges (M1->M2->M3...) made each
	// call O(chain) and the merge passes O(W^2) on mega-fan-out tail traces. The
	// returned root is identical either way; only the hop count changes.
	var resolveCalls, resolveHops int64
	cycleHit := false
	var resolve func(uint64) uint64
	resolve = func(id uint64) uint64 {
		resolveCalls++
		root := id
		hops := 0
		for {
			m, ok := merged[root]
			if !ok {
				break
			}
			root = m
			resolveHops++
			hops++
			if hops > len(merged)+8 { // a forest chain can't exceed |merged|: cycle
				if !cycleHit {
					cycleHit = true
					fmt.Fprintf(os.Stderr, "RESOLVE CYCLE id=%016x root=%016x merged=%d\n", id, root, len(merged))
				}
				break
			}
		}
		for id != root {
			next, ok := merged[id]
			if !ok {
				break
			}
			merged[id] = root
			id = next
		}
		return root
	}

	// union merges root node `cid` (a confirmed root: callers gate on
	// resolve(cid)==cid) into the tree containing `target`. It links cid to
	// target's ROOT and refuses the merge when that root is cid itself —
	// otherwise two fan-out nodes that claim each other (cid->target and later
	// target->cid) form a cycle in `merged`, and resolve()'s find-loop spins
	// forever (observed: a 701-span trace grinding 30+ min). Linking to the
	// resolved root (not the raw target) is semantically identical for resolve
	// and keeps chains short.
	union := func(cid, target uint64) {
		rt := resolve(target)
		if rt == cid {
			return // would create a cycle; leave cid as its own root
		}
		// INVARIANT: a node carrying an HA fan-out name is a UNIQUE branch point.
		// Two differently-named nodes must never merge — the HA names them as
		// distinct, so collapsing them is provably wrong (the bloom, a
		// probabilistic hint, cannot override an exact identity record). An
		// unnamed synthetic may still merge into a named node (that's how deeper
		// descendants attach to their recovered fan-out), and same-name merges
		// are fine.
		if cn, tn := nodes[cid], nodes[rt]; cn != nil && tn != nil &&
			cn.fanout != 0 && tn.fanout != 0 && cn.fanout != tn.fanout {
			return
		}
		merged[cid] = rt
	}

	// Mark known fan-outs onto the synthetic at the fan-out's depth on each
	// witnessing span's reconstructed path.
	mark := func(start, mfp uint64, d int) uint64 {
		cur := start
		for cur != 0 {
			n := nodes[resolve(cur)]
			if n == nil {
				return 0
			}
			if n.depth == d {
				if !n.surv {
					n.fanout = mfp
					return n.id
				}
				return 0
			}
			if n.depth < d {
				return 0
			}
			cur = n.parent
		}
		return 0
	}
	for i := range survivors {
		s := &survivors[i]
		for _, e := range s.HA {
			if d, ok := fanouts[e.ParentID]; ok {
				mark(s.SpanID, e.ParentID, d)
			}
		}
	}

	// Exact-edge head start: merge the verified direct-child synthetics of each
	// dropped fan-out by id, pinning M's canonical node before the bloom pass.
	directOf := make(map[uint64][]uint64) // fan-out fp -> its direct-child synthetic ids (at depth d)
	var gDropParent, gNotWitnessed, gDepthMismatch, gMarkFail, gNamed int
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID == 0 || surv[s.ParentID] {
			continue
		}
		if topoDiag { // gate-failure accounting: why a dropped-parent survivor isn't named
			gDropParent++
			if d, ok := fanouts[s.ParentID]; !ok {
				gNotWitnessed++
			} else if s.Depth != d+1 {
				gDepthMismatch++
			}
		}
		// Recover the exact direct parent only when the HA witnessed it as a
		// real fan-out. The HA witnessing is essentially complete (gated and
		// ungated recall are identical), and gating on it acts as a useful
		// branch-point filter: recovering EVERY dropped parent (incl. linear
		// single-child ones) and group-consensus-reassigning them tripled the
		// wrong-anchor rate for no recall gain.
		// A surviving span's immediate parent sits at s.Depth-1 and its EXACT id
		// is s.ParentID — known regardless of HA witnessing. Name the synthetic
		// there by that id so direct children of the SAME dropped parent group
		// together and children of DIFFERENT dropped parents can never merge (the
		// union invariant). Previously this gated on HA witnessing, which skipped
		// LINEAR (non-branch) dropped parents and left their lone children
		// unnamed — prey to bloom misattribution into a sibling fan-out.
		sid := mark(s.SpanID, s.ParentID, s.Depth-1)
		if sid != 0 {
			directOf[s.ParentID] = append(directOf[s.ParentID], sid)
			if topoDiag {
				gNamed++
			}
		} else if topoDiag {
			gMarkFail++
		}
	}
	if topoDiag {
		fmt.Fprintf(os.Stderr, "CGPGATE dropParentSurvivors=%d notWitnessed=%d depthMismatch=%d markFail=%d named=%d\n",
			gDropParent, gNotWitnessed, gDepthMismatch, gMarkFail, gNamed)
	}
	for _, ids := range directOf {
		keep := resolve(ids[0])
		for _, id := range ids[1:] {
			if r := resolve(id); r != keep {
				merged[r] = keep
			}
		}
	}

	// Index nodes by depth and find max depth.
	byDepth := make(map[int][]uint64)
	maxDepth := 0
	for id, n := range nodes {
		byDepth[n.depth] = append(byDepth[n.depth], id)
		if n.depth > maxDepth {
			maxDepth = n.depth
		}
	}

	// Canonical node per fan-out fingerprint: the exact-edge pre-merge already
	// collapsed each fan-out's direct children into one node; otherwise take
	// the first marked synthetic.
	foNode := make(map[uint64]uint64)
	for fp, ids := range directOf {
		foNode[fp] = resolve(ids[0])
	}
	for _, n := range nodes {
		if !n.surv && n.fanout != 0 {
			if _, ok := foNode[n.fanout]; !ok {
				foNode[n.fanout] = resolve(n.id)
			}
		}
	}

	// merge_fanouts with an FP guard and the CP-SAT contested re-solve. BFS
	// top-down so same-parent tests resolve through earlier merges. Per depth:
	// gather each fan-out's same-parent bloom-confirmed candidate synthetics;
	// a fan-out is CONFIRMED iff it has a verified exact-edge child or >=2 bloom
	// supporters (a lone bloom hit into an otherwise-unsupported fan-out is a
	// likely false positive and is left un-merged). A synthetic claimed by one
	// confirmed fan-out merges into it; a synthetic claimed by >=2 confirmed
	// fan-outs is contested (only possible on a bloom FP) and the assignment is
	// handed to CP-SAT to maximize corroboration.
	// Index HA-witnessed fan-outs by depth. Only these need the bloom merge:
	// direct children are already reunited exactly by ParentID (above), so the
	// bloom pass is purely for DEEPER descendants of witnessed fan-outs. (Before,
	// the pass ran over *every* recovered parent and scanned all spans per
	// fan-out — O(fan-outs x spans), which exploded on wide cpd6 traces.)
	foByDepth := make(map[int][]uint64)
	for fp := range fanouts {
		if nid, ok := foNode[fp]; ok && nodes[nid] != nil {
			foByDepth[nodes[nid].depth] = append(foByDepth[nodes[nid].depth], fp)
		}
	}

	// Pin confirmed (exact-edge) fan-out nodes: a node proven to be a distinct
	// branch point by its direct ParentID-match children must never be merged
	// INTO another fan-out (it can still receive merges). Without this, the
	// bloom pass swept a recovered fan-out up as a candidate of a same-parent
	// fan-out — the sole cause of the recall misses (diagnosed: 100% were
	// "M_node_merged_into_fanout").
	pinned := make(map[uint64]bool)
	for fp := range directOf {
		pinned[foNode[fp]] = true
	}
	for d := 0; d <= maxDepth; d++ {
		fps := foByDepth[d]
		if len(fps) == 0 {
			continue
		}
		// Group depth-d candidate synthetics by resolved parent ONCE, so each
		// fan-out inspects only its own same-parent group, not all spans.
		byParent := make(map[uint64][]uint64)
		for _, cid := range byDepth[d] {
			if resolve(cid) != cid || pinned[cid] {
				continue // pinned = confirmed fan-out, never a merge victim
			}
			c := nodes[cid]
			if c == nil || c.surv || c.bf == nil {
				continue
			}
			byParent[resolve(c.parent)] = append(byParent[resolve(c.parent)], cid)
		}
		fpsAtD := make([]uint64, 0, len(fps))
		bloomCands := make(map[uint64][]uint64)
		for _, fp := range fps {
			nid := foNode[fp]
			if resolve(nid) != nid {
				continue // fan-out node already merged away
			}
			fpsAtD = append(fpsAtD, fp)
			key := bridge.HexOf(fp)
			for _, cid := range byParent[resolve(nodes[nid].parent)] {
				if cid == nid {
					continue
				}
				if nodes[cid].bf.Test(key[:]) { // downstream bloom confirms M
					bloomCands[fp] = append(bloomCands[fp], cid)
				}
			}
		}
		// claimants per candidate, restricted to CONFIRMED fan-outs.
		claim := make(map[uint64][]uint64)
		for _, fp := range fpsAtD {
			if len(directOf[fp]) < 1 && len(bloomCands[fp]) < 2 {
				continue // unconfirmed fan-out: skip (treat its lone hit as FP)
			}
			for _, cid := range bloomCands[fp] {
				claim[cid] = append(claim[cid], fp)
			}
		}
		corrob := func(fp uint64) int { return len(directOf[fp])*1000000 + len(bloomCands[fp]) }
		var contCids []uint64
		var contOpts [][]uint64
		for cid, fps := range claim {
			if resolve(cid) != cid {
				continue
			}
			if len(fps) == 1 {
				union(cid, foNode[fps[0]])
			} else {
				contCids = append(contCids, cid)
				contOpts = append(contOpts, fps)
			}
		}
		if len(contCids) == 0 {
			continue
		}
		// Contested: maximize corroboration. Route through the CP-SAT shim we
		// already hook into (one item per contested synthetic, one option per
		// claiming fan-out); fall back to argmax if the solver isn't linked.
		if cpsatSolveFn != nil {
			var b strings.Builder
			fmt.Fprintf(&b, "C 0 0 %d\n", len(contCids))
			for i := range contCids {
				fmt.Fprintf(&b, "I 0 %d\n", len(contOpts[i]))
				for _, fp := range contOpts[i] {
					fmt.Fprintf(&b, "O %d 0 -1 0\n", corrob(fp))
				}
			}
			if assign, ok := cpsatSolveFn(b.String(), len(contCids)); ok {
				for i, cid := range contCids {
					if assign[i] >= 0 && assign[i] < len(contOpts[i]) {
						union(cid, foNode[contOpts[i][assign[i]]])
					}
				}
				continue
			}
		}
		for i, cid := range contCids {
			best, bestG := contOpts[i][0], -1
			for _, fp := range contOpts[i] {
				if g := corrob(fp); g > bestG {
					best, bestG = fp, g
				}
			}
			union(cid, foNode[best])
		}
	}

	// Group the bridges whose synthetics merged into a common fan-out node, and
	// choose each group's shared anchor by SUPPORT across the members' incumbent
	// (phase-1) connections — not the arbitrary kept node's chain. This is the
	// anti-stealing fix: a well-supported incumbent anchor wins and mis-threaded
	// outliers conform to it, instead of an outlier's anchor being forced on the
	// whole group. Bridges not in any merge keep their phase-1 connection.
	groups := make(map[uint64][]int)
	seenMember := make(map[[2]uint64]bool)
	addMember := func(canon uint64, bi int) {
		key := [2]uint64{canon, uint64(bi)}
		if seenMember[key] {
			return
		}
		seenMember[key] = true
		groups[canon] = append(groups[canon], bi)
	}
	for sid, bi := range synOwner {
		r := resolve(sid)
		if r == sid {
			continue // not merged
		}
		addMember(r, bi)
		if ob, ok := synOwner[r]; ok {
			addMember(r, ob) // the kept node's owning fragment is a member too
		}
	}

	type grp struct {
		members []int
		anchors []uint64
	}
	var gs []grp
	for _, members := range groups {
		if len(members) < 2 {
			continue
		}
		ac := make(map[uint64]int)
		for _, bi := range members {
			ac[phase1Anchor[bi]]++
		}
		anchors := make([]uint64, 0, len(ac))
		for a := range ac {
			anchors = append(anchors, a)
		}
		gs = append(gs, grp{members: members, anchors: anchors})
	}

	// Group-consensus anchor: each merge group independently takes its
	// max-support incumbent anchor (tie-break deepest = nearest true ancestor).
	// This is per-item argmax with NO inter-group coupling, so it's a trivial
	// O(groups) pick — NOT a solver job. (Routing it through CP-SAT built one
	// giant single-threaded model per trace, which ground for minutes on the
	// mega-fan-out tail traces. The genuine solver use is the contested
	// multi-fan-out case handled above.)
	for i := range gs {
		ac := make(map[uint64]int)
		for _, bi := range gs[i].members {
			ac[phase1Anchor[bi]]++
		}
		var best uint64
		bc, bbd := -1, -1
		for a, c := range ac {
			d := 0
			if s := byID[a]; s != nil {
				d = s.Depth
			}
			if c > bc || (c == bc && d > bbd) {
				best, bc, bbd = a, c, d
			}
		}
		bd := 0
		if s := byID[best]; s != nil {
			bd = s.Depth
		}
		for _, bi := range gs[i].members {
			o := byID[res.Bridges[bi].OrphanID]
			if o == nil {
				continue
			}
			res.Bridges[bi].AnchorID = best
			res.Bridges[bi].Synthetic = o.Depth - bd - 1
		}
	}

	// Expose each fragment's immediate reconstructed parent identity: if the
	// node directly above it (depth orphan-1) is a recovered fan-out, record its
	// true fingerprint. Two fragments sharing this are reconstructed siblings
	// under that recovered fan-out — the topology signal CGP exists to provide.
	for bi := range res.Bridges {
		o := byID[res.Bridges[bi].OrphanID]
		if o == nil || nodes[o.SpanID] == nil {
			continue
		}
		p := nodes[o.SpanID].parent
		if p == 0 {
			continue
		}
		if n := nodes[resolve(p)]; n != nil && !n.surv && n.depth == o.Depth-1 && n.fanout != 0 {
			res.Bridges[bi].ReconFanout = n.fanout
		}
	}

	if cgpDiag {
		for bi := range res.Bridges {
			o := byID[res.Bridges[bi].OrphanID]
			if o == nil {
				continue
			}
			m := o.ParentID // fragment root's exact (dropped) true parent
			if m == 0 || surv[m] {
				continue
			}
			if _, ok := fanouts[m]; !ok {
				continue // M was not HA-witnessed
			}
			if res.Bridges[bi].ReconFanout == m {
				continue // correctly reunited under M
			}
			// A witnessed fan-out's direct child that did NOT land under M.
			reason := "unknown"
			if nid, ok := foNode[m]; !ok {
				reason = "no_canonical_node"
			} else if r := resolve(nid); r != nid {
				reason = fmt.Sprintf("M_node_merged_into_fanout=%016x", nodes[r].fanout)
			} else if nodes[nid].fanout != m {
				reason = "M_node_unmarked"
			} else {
				pr := resolve(nodes[o.SpanID].parent)
				reason = fmt.Sprintf("child_parent_resolves_to=%016x depth=%d fanout=%016x",
					pr, nodes[pr].depth, nodes[pr].fanout)
			}
			fmt.Fprintf(os.Stderr, "CGPDIAG F=%016x M=%016x reconfanout=%016x %s\n",
				o.SpanID, m, res.Bridges[bi].ReconFanout, reason)
		}
	}
	if cgpPerf {
		fmt.Fprintf(os.Stderr, "CGPPERF survivors=%d bridges=%d synthetics=%d fanouts=%d merges=%d resolve_calls=%d resolve_hops=%d\n",
			len(survivors), len(res.Bridges), len(synOwner), len(fanouts), len(merged), resolveCalls, resolveHops)
	}
	return res
}

// TopoScore is the call-graph-topology verdict for one trace: connectivity
// (right ancestors) AND fan-out structure (right siblings under the right
// recovered branch identity). Pairs are scoped to DROPPED-parent sibling
// groups — surviving-parent siblings are trivially reunited by both bridges.
type TopoScore struct {
	TrueSibPairs   int  // true sibling pairs whose common parent was dropped
	RecallPairs    int  // of those, reconstructed as siblings under the correct fan-out identity
	ReconSibPairs  int  // pairs the reconstruction calls siblings (shared recovered fan-out)
	PrecisionPairs int  // of those, actually true siblings under that identity
	HasDropFanout  bool // trace has >=1 dropped-parent sibling group (a fan-out to recover)
	ConnCorrect    bool // every bridge anchored to the true nearest surviving ancestor, none lost
	TopoCorrect    bool // ConnCorrect AND the fan-out structure exactly matches truth
}

// ScoreCGPTopology grades reconstructed call-graph topology against the original
// trace: a trace is topology-correct iff it is connectivity-correct AND its
// reconstructed fan-out structure (sibling grouping + recovered identity) equals
// the truth. P-Bridge leaves ReconFanout=0 everywhere, so it scores ~0 recall on
// any trace with a dropped fan-out — by design.
func ScoreCGPTopology(res Result, truth []TruthSpan, dropped map[uint64]struct{}, tid uint64) TopoScore {
	trueParent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		trueParent[t.SpanID] = t.ParentID
	}
	nearestSurv := func(id uint64) uint64 {
		for p := trueParent[id]; p != 0; p = trueParent[p] {
			if _, gone := dropped[p]; !gone {
				return p
			}
		}
		return 0
	}

	var truthDepth map[uint64]int
	if topoDiag {
		truthDepth = make(map[uint64]int, len(truth))
		for _, t := range truth {
			truthDepth[t.SpanID] = t.Depth
		}
	}

	var ts TopoScore
	ts.ConnCorrect = len(res.Unanchored) == 0
	recFan := make(map[uint64]uint64, len(res.Bridges))
	for _, b := range res.Bridges {
		recFan[b.OrphanID] = b.ReconFanout
		if ts.ConnCorrect && b.AnchorID != nearestSurv(b.OrphanID) {
			ts.ConnCorrect = false
		}
	}

	if connMeasure {
		// Group orphans by their true direct parent; the correct shared anchor of
		// a sibling group is nearestSurv of any member. For each wrong-anchored
		// orphan, can a sibling rescue it (some sibling holds the correct anchor)?
		byTrueParent := make(map[uint64][]int, len(res.Bridges))
		for i, b := range res.Bridges {
			byTrueParent[trueParent[b.OrphanID]] = append(byTrueParent[trueParent[b.OrphanID]], i)
		}
		var wrong, onlyChild, sibCorrect, sibAllWrong int
		// cross-tab: ambiguous(prefix-collision) x has-sibling
		var ambSib, ambOnly, threadSib, threadOnly int
		for k := range res.Bridges {
			b := res.Bridges[k]
			correct := nearestSurv(b.OrphanID)
			if b.AnchorID == correct {
				continue
			}
			wrong++
			hasSib := len(byTrueParent[trueParent[b.OrphanID]]) >= 2
			switch {
			case b.Ambiguous && hasSib:
				ambSib++
			case b.Ambiguous && !hasSib:
				ambOnly++
			case !b.Ambiguous && hasSib:
				threadSib++
			default:
				threadOnly++
			}
			if !hasSib {
				onlyChild++
				continue
			}
			rescuable := false
			for _, j := range byTrueParent[trueParent[b.OrphanID]] {
				if res.Bridges[j].AnchorID == correct {
					rescuable = true
					break
				}
			}
			if rescuable {
				sibCorrect++
			} else {
				sibAllWrong++
			}
		}
		if wrong > 0 {
			fmt.Fprintf(os.Stderr, "CONNMEASURE wrong=%d onlyChild=%d sibCorrect=%d sibAllWrong=%d | ambSib=%d ambOnly=%d threadSib=%d threadOnly=%d\n",
				wrong, onlyChild, sibCorrect, sibAllWrong, ambSib, ambOnly, threadSib, threadOnly)
		}
	}

	if stealMeasure {
		childCount := make(map[uint64]int, len(truth))
		for _, t := range truth {
			if t.ParentID != 0 {
				childCount[t.ParentID]++
			}
		}
		isCorrect := make(map[uint64]bool, len(res.Bridges))
		correctOf := make(map[uint64]uint64, len(res.Bridges))
		for _, b := range res.Bridges {
			c := nearestSurv(b.OrphanID)
			correctOf[b.OrphanID] = c
			isCorrect[b.OrphanID] = b.AnchorID == c
		}
		// Mark branch-point ancestors that have a correctly-anchored descendant.
		hasCorrectDesc := make(map[uint64]bool)
		for _, b := range res.Bridges {
			if !isCorrect[b.OrphanID] {
				continue
			}
			c := correctOf[b.OrphanID]
			for p := trueParent[b.OrphanID]; p != 0 && p != c; p = trueParent[p] {
				if _, gone := dropped[p]; !gone {
					break
				}
				if childCount[p] >= 2 {
					hasCorrectDesc[p] = true
				}
			}
		}
		var wrong, rescuable, fanoutNoCorrect, noFanout int
		for _, b := range res.Bridges {
			if isCorrect[b.OrphanID] {
				continue
			}
			wrong++
			c := correctOf[b.OrphanID]
			hasFanout, resc := false, false
			for p := trueParent[b.OrphanID]; p != 0 && p != c; p = trueParent[p] {
				if _, gone := dropped[p]; !gone {
					break
				}
				if childCount[p] >= 2 {
					hasFanout = true
					if hasCorrectDesc[p] {
						resc = true
						break
					}
				}
			}
			switch {
			case resc:
				rescuable++
			case hasFanout:
				fanoutNoCorrect++
			default:
				noFanout++
			}
		}
		if wrong > 0 {
			fmt.Fprintf(os.Stderr, "STEALMEASURE wrong=%d rescuable=%d fanoutNoCorrect=%d noFanout=%d\n",
				wrong, rescuable, fanoutNoCorrect, noFanout)
		}
	}

	fanoutOK := true

	// True dropped-parent sibling groups: fragment roots by their dropped parent.
	trueGroups := make(map[uint64][]uint64)
	for _, b := range res.Bridges {
		p := trueParent[b.OrphanID]
		if p == 0 {
			continue
		}
		if _, gone := dropped[p]; !gone {
			continue
		}
		trueGroups[p] = append(trueGroups[p], b.OrphanID)
	}
	for parent, members := range trueGroups {
		if len(members) < 2 {
			continue
		}
		ts.HasDropFanout = true
		n := len(members)
		ts.TrueSibPairs += n * (n - 1) / 2
		c := 0 // members reunited under the CORRECT recovered identity
		for _, m := range members {
			if recFan[m] == parent {
				c++
			}
		}
		ts.RecallPairs += c * (c - 1) / 2
		if c != n {
			fanoutOK = false // a true sibling not correctly reunited
		}
	}

	// Reconstructed sibling groups: fragment roots by recovered fan-out id.
	reconGroups := make(map[uint64][]uint64)
	for _, b := range res.Bridges {
		if b.ReconFanout != 0 {
			reconGroups[b.ReconFanout] = append(reconGroups[b.ReconFanout], b.OrphanID)
		}
	}
	for fp, members := range reconGroups {
		if len(members) < 2 {
			continue
		}
		n := len(members)
		ts.ReconSibPairs += n * (n - 1) / 2
		c := 0
		for _, m := range members {
			if trueParent[m] == fp {
				c++
			}
		}
		ts.PrecisionPairs += c * (c - 1) / 2
		if c != n {
			fanoutOK = false // a reconstructed sibling pair that isn't a true sibling
			if topoDiag {
				var b strings.Builder
				fmt.Fprintf(&b, "TOPODIAG tid=%016x over-merge fp=%016x members=%d:", tid, fp, n)
				for _, m := range members {
					tp := trueParent[m]
					witnessed := "-"
					if _, ok := dropped[tp]; ok {
						witnessed = "drop"
					}
					fmt.Fprintf(&b, " [o=%016x od=%d tp=%016x tpd=%d %s]",
						m, truthDepth[m], tp, truthDepth[tp], witnessed)
				}
				fmt.Fprintln(os.Stderr, b.String())
			}
		}
	}

	ts.TopoCorrect = ts.ConnCorrect && fanoutOK
	return ts
}
