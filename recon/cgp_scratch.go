package recon

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"bridges/bloom"
	"bridges/bridge"
)

// cgp2Diag gates the from-scratch reconstructor's structural diagnostics.
var cgp2Diag = os.Getenv("TRACE_RECON_CGP2DIAG") == "1"

// cpsatBoolSolveFn solves a general 0/1 model (VARS/OBJ/CON text) and returns
// the per-var assignment. Set by the //go:build cpsat cgo file; nil otherwise.
var cpsatBoolSolveFn func(model string, nvars int, tlim float64) ([]int, bool)

// ============================================================================
// From-scratch principled CGP reconstructor (cgp2).
//
// Built bottom-up from certainty, as a feasibility-constrained optimization —
// NOT a chain of patches over a fan-out-blind solve.
//
// Hard feasibility constraints (a configuration satisfies ALL or is rejected):
//   - Admissibility: no bloom NEGATIVE on any reconnected in-window path.
//   - Every HA-witnessed fan-out has >=2 children wired through it.
//   - Every non-HA node (named-once parent / orphan) has <=1 child.
//   - Each HA fan-out's witnessing carrier is wired as its descendant.
//   - One upstream path per node; all reasoning is window-local.
//
// Single objective (tie-break among feasible configs only):
//   - Maximize aggregate depth-based bloom match (deepest bloom-confirmed
//     attachments win).
//
// Phases (each layered in and validated before the next):
//   1. Parse  -> fragments, classify, HA fan-out skeleton.        [this commit]
//   2. Certain skeleton: exact-named parents, exact-id coalescing.
//   3. Candidates: per-fragment confirmed-ancestor chain; per-fan-out members.
//   4. Solve: feasibility + objective over the admissible space.
//   5. Emit: Bridges from the chosen configuration.
// ============================================================================

// cgpFragment is a maximal connected component of surviving spans (connected
// through surviving parent<->child edges). The drop policy disconnects the tree
// into these.
type cgpFragment struct {
	root    *Span   // shallowest survivor of the component (its true parent dropped, or it is the trace root)
	spans   []*Span // every survivor in the component
	carrier *Span   // a span in the fragment carrying a _br payload — checkpoint OR leaf-carrier; both give own window evidence (prefix+bloom). nil => orphan.

	// Filled in later phases:
	bf         *bloom.Filter // window bloom (own carrier's, or borrowed for an orphan)
	prefix     []byte        // checkpoint prefix (own, or borrowed)
	viaCarrier uint64        // span the prefix/bloom was borrowed from (0 = own)
	anchorCkpt *Span         // prefix-matched window-top checkpoint (the routing ceiling); nil if unresolved
	anchorAmbig bool         // >1 checkpoint matched the prefix
}

// isOrphan reports whether the fragment is carrier-less: no checkpoint and no
// leaf-carrier, hence no window evidence of its own. It can only be placed by
// borrowing a descendant carrier's window evidence. (A leaf-carrier counts as a
// carrier — evidentially it is a checkpoint.)
func (f *cgpFragment) isOrphan() bool { return f.carrier == nil }

// cgpFanout is an HA-witnessed dropped fan-out: a NAMED branch point (its id is
// known exactly from the HA entry) that MUST have >=2 children wired through it.
// Membership — which paths are its children — is what the solve determines.
type cgpFanout struct {
	id      uint64 // M: the dropped parent's true id (named, exact)
	depth   int    // M's depth
	witness *Span  // a survivor carrying M's HA entry; MUST be wired as M's descendant
}

// cgpSkeleton is the output of Phase 1: the structural facts derived purely from
// the survivors and their carried evidence, before any bloom guessing.
type cgpSkeleton struct {
	byID      map[uint64]*Span     // SpanID -> survivor
	childrenS map[uint64][]*Span   // surviving-edge children (parent must survive)
	frags     []*cgpFragment       // connected components
	fanouts   map[uint64]*cgpFanout // HA-witnessed dropped fan-outs, by id
}

// cgpParse performs Phase 1: it partitions the survivors into fragments,
// classifies each as orphan or carrier-bearing, and extracts the HA-witnessed
// fan-out skeleton. Pure structure, no blooms touched.
func cgpParse(survivors []Span, cfg Config) *cgpSkeleton {
	sk := &cgpSkeleton{
		byID:      make(map[uint64]*Span, len(survivors)),
		childrenS: make(map[uint64][]*Span, len(survivors)),
		fanouts:   make(map[uint64]*cgpFanout),
	}
	for i := range survivors {
		sk.byID[survivors[i].SpanID] = &survivors[i]
	}
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID == 0 {
			continue
		}
		if _, ok := sk.byID[s.ParentID]; ok {
			sk.childrenS[s.ParentID] = append(sk.childrenS[s.ParentID], s)
		}
	}

	// Fragments = connected components. A component's root is a survivor whose
	// true parent is NOT a surviving span (parent dropped, or it is the trace
	// root). Because the tree is partitioned by drops, walking surviving-edge
	// children down from each root visits exactly that component.
	claimed := make(map[uint64]bool, len(survivors))
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 {
			if _, parentSurv := sk.byID[s.ParentID]; parentSurv {
				continue // has a surviving parent: not a component root
			}
		}
		f := &cgpFragment{root: s}
		stack := []*Span{s}
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if claimed[n.SpanID] {
				continue
			}
			claimed[n.SpanID] = true
			f.spans = append(f.spans, n)
			// A carrier is any span with a _br payload — checkpoint or leaf-carrier;
			// both anchor a window. Prefer the shallowest (closest to the window top).
			if n.BloomBits != nil && (f.carrier == nil || n.Depth < f.carrier.Depth) {
				f.carrier = n
			}
			stack = append(stack, sk.childrenS[n.SpanID]...)
		}
		sk.frags = append(sk.frags, f)
	}

	// HA-witnessed fan-outs. An HA entry (ParentID=M, Depth=child depth) means M
	// branched (>=2 children) and the carrying survivor is in M's 2nd-child
	// lineage, hence a descendant of M. M is at child depth - 1. We keep it only
	// if M itself is dropped (a recovered fan-out); a surviving M is just a real
	// node. Witness = shallowest carrier of the entry (closest to M), the
	// concrete descendant whose existence vouches for M.
	for i := range survivors {
		s := &survivors[i]
		for _, e := range s.HA {
			if _, ok := sk.byID[e.ParentID]; ok {
				continue // M survived: not a recovered fan-out
			}
			fo := sk.fanouts[e.ParentID]
			if fo == nil {
				sk.fanouts[e.ParentID] = &cgpFanout{id: e.ParentID, depth: e.Depth - 1, witness: s}
			} else if s.Depth < fo.witness.Depth {
				fo.witness = s
			}
		}
	}
	return sk
}

// ============================================================================
// Phase 2 — the explicit node forest (certain structure, still no bloom).
//
// A node is either a surviving span or a NAMED synthetic for a dropped parent
// that >=1 fragment exactly references. Exact children draw certain edges to
// their shared named node (coalescing). Named nodes' downward edges are fixed;
// their upward attachment stays open for HA routing.
// ============================================================================

type cgpNode struct {
	id    uint64
	depth int
	surv  bool // a surviving span
	ha    bool // HA-witnessed fan-out: must end with >=2 children wired through it

	// Certain downward structure (no guessing):
	exactKids []uint64 // ids of fragment roots that exactly named this node (root.ParentID == id)

	// Upward attachment + routed children are resolved in later phases.
}

// cgpForest is the Phase-2 node set: every survivor plus every exactly-named
// dropped parent, with certain child-edges and HA tags.
type cgpForest struct {
	nodes map[uint64]*cgpNode
}

// cgpBuildForest performs Phase 2: materialize survivor nodes and named-parent
// nodes, coalesce same-id exact children, and tag HA fan-outs. Pure certainty.
func cgpBuildForest(sk *cgpSkeleton) *cgpForest {
	fr := &cgpForest{nodes: make(map[uint64]*cgpNode, len(sk.byID)+len(sk.fanouts))}
	for _, s := range sk.byID {
		fr.nodes[s.SpanID] = &cgpNode{id: s.SpanID, depth: s.Depth, surv: true}
	}
	// Exact-name coalescing: each fragment root whose true parent dropped draws a
	// certain edge to the one named node for that parent id.
	for _, f := range sk.frags {
		r := f.root
		if r.ParentID == 0 {
			continue // trace root: no parent to name
		}
		if _, surv := sk.byID[r.ParentID]; surv {
			continue // parent survived: a real edge, not a recovered parent
		}
		n := fr.nodes[r.ParentID]
		if n == nil {
			n = &cgpNode{id: r.ParentID, depth: r.Depth - 1}
			fr.nodes[r.ParentID] = n
		}
		n.exactKids = append(n.exactKids, r.SpanID)
	}
	// Tag HA-witnessed fan-outs; materialize any witnessed fan-out that no
	// fragment named exactly (its direct children all dropped — it will recruit
	// routed children to reach >=2).
	for id, fo := range sk.fanouts {
		n := fr.nodes[id]
		if n == nil {
			n = &cgpNode{id: id, depth: fo.depth}
			fr.nodes[id] = n
		}
		n.ha = true
	}
	return fr
}

// ============================================================================
// Phase 3a — resolve each fragment's window evidence (the bloom it routes
// against) and its anchor prefix. Carrier-bearing fragments use their own
// shallowest carrier's payload. Orphans (carrier-less, ~5-15/trace) must borrow
// a descendant carrier's window and are handled separately (deferred here, NOT
// dropped — marked unresolved so nothing silently guesses for them yet).
//
// This is the first phase that touches the bloom, so it is validated directly:
// a carrier-bearing fragment's bloom MUST confirm its own exactly-known dropped
// parent (a true in-window ancestor — blooms have no false negatives).
// ============================================================================

func cgpResolveEvidence(sk *cgpSkeleton, cfg Config) {
	var orphans []*cgpFragment
	for _, f := range sk.frags {
		if f.carrier == nil {
			orphans = append(orphans, f)
			continue
		}
		if f.carrier.BloomBits != nil {
			f.bf = bloom.Deserialize(f.carrier.BloomBits, cfg.BloomM, cfg.BloomK)
		}
		f.prefix = f.carrier.CkptPrefix
		f.viaCarrier = 0
	}
	if len(orphans) == 0 {
		return
	}
	// An orphan fragment carries no _br payload of its own, but its ancestors are
	// also the ancestors of any surviving span BELOW it on the same path. So it
	// borrows a descendant carrier in its OWN window: a bloom-bearing span deeper
	// than the orphan root but no further than the window-bottom checkpoint, whose
	// bloom confirms BOTH the orphan root and its named parent (two-hit
	// corroboration to suppress a false-positive borrow). That carrier's bloom IS
	// the orphan's window bloom, and its prefix the orphan's window checkpoint, so
	// the orphan then anchors with the identical machinery as any other fragment.
	carriersByDepth := make(map[int][]*Span)
	for _, s := range sk.byID {
		if s.BloomBits != nil {
			carriersByDepth[s.Depth] = append(carriersByDepth[s.Depth], s)
		}
	}
	for _, f := range orphans {
		r := f.root
		rk := bridge.HexOf(r.SpanID)
		mk := bridge.HexOf(r.ParentID)
		haveMF := r.ParentID != 0
		wbot := (r.Depth/cfg.CPD + 1) * cfg.CPD // window-bottom checkpoint depth
		for d := r.Depth + 1; d <= wbot && f.carrier == nil; d++ {
			for _, c := range carriersByDepth[d] {
				bf := bloom.Deserialize(c.BloomBits, cfg.BloomM, cfg.BloomK)
				if !bf.Test(rk[:]) {
					continue
				}
				if haveMF && !bf.Test(mk[:]) {
					continue
				}
				f.carrier = c
				f.bf = bf
				f.prefix = c.CkptPrefix
				f.viaCarrier = c.SpanID
				break
			}
		}
	}
}

// ============================================================================
// Phase 3b — anchor ceiling. For each carrier-bearing fragment, prefix-match its
// checkpoint prefix against surviving checkpoints at the window-top depth. This
// is the certain (structural, no bloom) upper bound on its upward routing.
// ============================================================================

func cgpResolveAnchors(sk *cgpSkeleton, cfg Config) {
	// Index surviving checkpoints by depth (depth % cpd == 0).
	ckptByDepth := make(map[int][]*Span)
	for _, s := range sk.byID {
		if s.Depth%cfg.CPD == 0 {
			ckptByDepth[s.Depth] = append(ckptByDepth[s.Depth], s)
		}
	}
	for _, f := range sk.frags {
		if f.carrier == nil || f.prefix == nil {
			continue
		}
		cd := f.carrier.Depth
		var ckd int
		if cd%cfg.CPD == 0 {
			ckd = cd - cfg.CPD
		} else {
			ckd = (cd / cfg.CPD) * cfg.CPD
		}
		if ckd < 0 {
			continue // root window: nothing above
		}
		var hit *Span
		n := 0
		for _, c := range ckptByDepth[ckd] {
			id8 := bridge.BigEndian8(c.SpanID)
			match := true
			for i := 0; i < cfg.PrefixLen && i < len(f.prefix); i++ {
				if id8[i] != f.prefix[i] {
					match = false
					break
				}
			}
			if match {
				n++
				if hit == nil || c.SpanID < hit.SpanID {
					hit = c
				}
			}
		}
		f.anchorCkpt = hit
		f.anchorAmbig = n > 1
	}
}

// ============================================================================
// Phase 3c — permissive candidate space. For each carrier-bearing fragment, the
// HA fan-outs its bloom confirms within its window (between its exact parent and
// its anchor ceiling) are the joins it COULD route through. FPs are admitted by
// design; all pruning is deferred to the solve.
// ============================================================================

type cgpCandidates struct {
	members   map[uint64][]*cgpFragment // HA fan-out id -> fragments whose bloom confirms it
	fragJoins map[uint64][]uint64       // fragment root id -> confirmed HA fan-out ids
	// survAnc: fragment root id -> confirmed SURVIVING ancestors (in-window, by
	// depth descending = nearest first). The deepest is the candidate nearest
	// surviving ancestor; choosing it nests this fragment under that survivor.
	survAnc map[uint64][]*Span
}

func cgpGenCandidates(sk *cgpSkeleton, cfg Config) *cgpCandidates {
	// Index HA fan-outs by depth so each fragment only tests the ones inside its
	// own window (bounded, not a scan over everything).
	haByDepth := make(map[int][]uint64)
	for id, fo := range sk.fanouts {
		haByDepth[fo.depth] = append(haByDepth[fo.depth], id)
	}
	survByDepth := make(map[int][]*Span)
	for _, s := range sk.byID {
		survByDepth[s.Depth] = append(survByDepth[s.Depth], s)
	}
	// Determinism: sk.fanouts and sk.byID are maps, so the per-depth lists above
	// come out in random iteration order, which would feed a randomly-permuted
	// model to the solver (and randomly-ordered equal-depth options, whose ties
	// the solver then breaks differently run-to-run). Sort by id so identical
	// input yields an identical candidate set and option order.
	for d := range haByDepth {
		sort.Slice(haByDepth[d], func(i, j int) bool { return haByDepth[d][i] < haByDepth[d][j] })
	}
	for d := range survByDepth {
		sort.Slice(survByDepth[d], func(i, j int) bool { return survByDepth[d][i].SpanID < survByDepth[d][j].SpanID })
	}
	wtop := func(d int) int { // window-top checkpoint depth for a span at depth d
		if d%cfg.CPD == 0 {
			return d - cfg.CPD
		}
		return (d / cfg.CPD) * cfg.CPD
	}
	c := &cgpCandidates{
		members:   make(map[uint64][]*cgpFragment),
		fragJoins: make(map[uint64][]uint64),
		survAnc:   make(map[uint64][]*Span),
	}
	dbgBlooms := os.Getenv("TRACE_RECON_CGP2BLOOMS") != ""
	var dbgFrags, dbgMulti, dbgBloomSum int
	dbgEfpr := os.Getenv("TRACE_RECON_CGP2EFPR") != ""
	var efprHits, efprProbes, thN int
	var thFprSum float64
	seenEfpr := make(map[uint64]bool)
	for _, f := range sk.frags {
		if f.bf == nil || f.anchorCkpt == nil {
			continue
		}
		lo := f.anchorCkpt.Depth
		hi := f.root.Depth
		// ADMISSIBILITY — confirm against EVERY reachable in-window bloom, not just
		// the shallowest carrier. Gather all bloom-bearing spans of this fragment
		// that sit in the root's window (window-top == lo). A true ancestor is an
		// ancestor of every one of them and so (no false negatives) is present in
		// all their blooms; a false positive must coincide in EVERY bloom, so a
		// single negative rejects it. Orphans (no own bloom) fall back to their one
		// borrowed window bloom.
		type winBloom struct {
			depth int
			bf    *bloom.Filter
		}
		var blooms []winBloom
		for _, s := range f.spans {
			if s.BloomBits != nil && wtop(s.Depth) == lo {
				blooms = append(blooms, winBloom{s.Depth, bloom.Deserialize(s.BloomBits, cfg.BloomM, cfg.BloomK)})
			}
		}
		if len(blooms) == 0 {
			blooms = append(blooms, winBloom{f.carrier.Depth, f.bf})
		}
		if dbgBlooms {
			dbgFrags++
			dbgBloomSum += len(blooms)
			if len(blooms) > 1 {
				dbgMulti++
			}
		}
		// Empirical per-test FPR: probe each distinct carrier bloom with fixed
		// NON-member keys (exercises the real hash schedule, so it catches
		// small-m clustering that the theoretical fill^k would miss).
		if dbgEfpr && f.carrier != nil && !seenEfpr[f.carrier.SpanID] {
			seenEfpr[f.carrier.SpanID] = true
			const probes = 256
			for i := 0; i < probes; i++ {
				key := bridge.HexOf(0xCAFE000000000000 + uint64(i)*0x9E3779B97F4A7C15)
				if f.bf.Test(key[:]) {
					efprHits++
				}
				efprProbes++
			}
			x, m, k := f.bf.PopCount(), int(f.bf.M()), int(f.bf.K())
			thFprSum += math.Pow(float64(x)/float64(m), float64(k))
			thN++
		}
		// A candidate at depth d is an ancestor of every fragment span below it, so
		// require its fingerprint in every in-window bloom whose window contains d
		// (carrier strictly deeper than d). A single miss is a bloom-negative and
		// rejects the candidate.
		confirmedByAll := func(key []byte, d int) bool {
			for _, wb := range blooms {
				if wb.depth > d && !wb.bf.Test(key) {
					return false
				}
			}
			return true
		}
		// Confirmed dropped fan-outs this fragment routes through. d-2 rule: the
		// node at root.Depth-1 is the certain dropped parent M_F (not a routing
		// decision); any other node there is a cousin.
		for d := hi - 2; d > lo; d-- {
			for _, m := range haByDepth[d] {
				k := bridge.HexOf(m)
				if confirmedByAll(k[:], d) {
					c.members[m] = append(c.members[m], f)
					c.fragJoins[f.root.SpanID] = append(c.fragJoins[f.root.SpanID], m)
				}
			}
		}
		// Surviving-ancestor candidates, nearest-first. Keep S only if S AND its own
		// parent are confirmed by ALL in-window blooms — the parent check prunes a
		// same-depth cousin whose lineage diverges above it.
		for d := hi - 2; d > lo; d-- {
			for _, s := range survByDepth[d] {
				k := bridge.HexOf(s.SpanID)
				if !confirmedByAll(k[:], d) {
					continue
				}
				if s.ParentID != f.anchorCkpt.SpanID {
					pk := bridge.HexOf(s.ParentID)
					if !confirmedByAll(pk[:], d-1) {
						continue
					}
				}
				c.survAnc[f.root.SpanID] = append(c.survAnc[f.root.SpanID], s)
			}
		}
	}
	if dbgBlooms && dbgFrags > 0 {
		fmt.Fprintf(os.Stderr, "CGP2BLOOMS frags=%d multiBloom=%d (%.1f%%) avgBlooms=%.3f\n",
			dbgFrags, dbgMulti, 100*float64(dbgMulti)/float64(dbgFrags), float64(dbgBloomSum)/float64(dbgFrags))
	}
	if dbgEfpr && efprProbes > 0 {
		fmt.Fprintf(os.Stderr, "CGP2EFPR empiricalFPR=%.6f theoreticalFPR=%.6f blooms=%d m=%d k=%d\n",
			float64(efprHits)/float64(efprProbes), thFprSum/float64(thN), thN, cfg.BloomM, cfg.BloomK)
	}
	return c
}

// ============================================================================
// Phase 4a — enumerate each fragment's one-hot attachment OPTIONS.
//
// A fragment's confirmed fan-outs form a nested chain (all on its one root
// path, ordered by depth). An option = "the deepest recovered ancestor I route
// through": either the bare anchor (no fan-out recovered) or one of the
// confirmed fan-outs M_i, which implies routing through M_i and every shallower
// confirmed fan-out up to the anchor. Distinct options, distinct depth scores
// (deeper recovered point = higher score). Exactly one option per fragment.
// ============================================================================

// cgpOption is one candidate attachment: the surviving ancestor this fragment
// connects to. score = anchor depth (deeper = nearer = better). The recovered
// fan-outs on the path are those confirmed fan-outs strictly between the anchor
// and the fragment, materialized as named synthetics when the tree is built.
type cgpOption struct {
	anchor *Span // surviving ancestor to attach under
	score  int   // = anchor.Depth
}

type cgpFragOpts struct {
	frag      *cgpFragment
	fanouts   []uint64 // confirmed fan-out ids on this fragment's chain (for routing/≥2)
	options   []cgpOption
}

// cgpGenOptions builds each fragment's one-hot anchor options: its confirmed
// surviving ancestors (nearest first) plus the window checkpoint as the always-
// valid fallback. Deeper anchor scores higher (nearest surviving ancestor wins).
func cgpGenOptions(sk *cgpSkeleton, cand *cgpCandidates) []*cgpFragOpts {
	var out []*cgpFragOpts
	for _, f := range sk.frags {
		if f.bf == nil || f.anchorCkpt == nil {
			continue
		}
		fo := &cgpFragOpts{frag: f, fanouts: cand.fragJoins[f.root.SpanID]}
		// nearest-first surviving ancestors (survAnc is already depth-descending)
		for _, s := range cand.survAnc[f.root.SpanID] {
			fo.options = append(fo.options, cgpOption{anchor: s, score: s.Depth})
		}
		// always-valid fallback: the window checkpoint (shallowest)
		fo.options = append(fo.options, cgpOption{anchor: f.anchorCkpt, score: f.anchorCkpt.Depth})
		out = append(out, fo)
	}
	return out
}

// ============================================================================
// Phase 4b — assemble the CP-SAT model from the options and solve.
//
//   vars       : one boolean per (fragment, option).
//   objective  : maximize sum score(option)*var   (deepest attachment wins).
//   one-hot    : per fragment, sum(its option vars) == 1.
//   HA >=2     : per HA fan-out M, sum(option vars routing through M)
//                >= max(0, 2 - exactChildren(M)). Options route through M iff M
//                is on that fragment's chain and the option's deepest point is at
//                or below M (nested chain). Bare-anchor options route through no
//                fan-out. Non-HA nodes are never routing targets, so their <=1
//                cap holds structurally.
//
// (All-downstream exclusions are layered in next; this commit lands the core
// model + solve + emit so the pipeline runs end-to-end.)
// ============================================================================

func cgpSolveAndEmit(sk *cgpSkeleton, fr *cgpForest, opts []*cgpFragOpts, cfg Config) Result {
	type vref struct{ fi, oi int }
	var vrefs []vref
	varOf := make([][]int, len(opts))
	for fi := range opts {
		varOf[fi] = make([]int, len(opts[fi].options))
		for oi := range opts[fi].options {
			varOf[fi][oi] = len(vrefs)
			vrefs = append(vrefs, vref{fi, oi})
		}
	}
	n := len(vrefs)
	if n == 0 || cpsatBoolSolveFn == nil {
		return Result{}
	}
	haDepth := make(map[uint64]int, len(sk.fanouts))
	for id, fo := range sk.fanouts {
		haDepth[id] = fo.depth
	}
	// Deterministic fan-out order for any loop that EMITS into the model (map
	// range order would otherwise permute the constraints run-to-run). Map-fill
	// loops below (witnessWindow/realizedWindow) are keyed lookups and need no order.
	fanoutIDs := make([]uint64, 0, len(sk.fanouts))
	for id := range sk.fanouts {
		fanoutIDs = append(fanoutIDs, id)
	}
	sort.Slice(fanoutIDs, func(i, j int) bool { return fanoutIDs[i] < fanoutIDs[j] })

	// A fragment routes through fan-out M iff its chosen anchor sits ABOVE M
	// (anchor.Depth < M.depth) and it confirms M. routesByWindow groups those
	// routing options by the routing fragment's WINDOW (its anchorCkpt id). A
	// fan-out carries no window identity of its own, so the all-downstream rule
	// is: M may be realized through AT MOST ONE window — routing fragments from
	// two different windows through one fan-out is inadmissible.
	routes := make(map[uint64][]int)
	routesByWindow := make(map[uint64]map[uint64][]int)
	for fi, foo := range opts {
		win := foo.frag.anchorCkpt.SpanID
		for oi, o := range foo.options {
			for _, m := range foo.fanouts {
				if o.anchor.Depth < haDepth[m] {
					routes[m] = append(routes[m], varOf[fi][oi])
					if routesByWindow[m] == nil {
						routesByWindow[m] = make(map[uint64][]int)
					}
					routesByWindow[m][win] = append(routesByWindow[m][win], varOf[fi][oi])
				}
			}
		}
	}
	// A fan-out's exact children (fragments that named it as their exact parent)
	// are CERTAIN children and pin its window: M lives where its exact kids live.
	exactWindow := make(map[uint64]uint64)
	for _, foo := range opts {
		r := foo.frag.root
		if r.ParentID == 0 {
			continue
		}
		if _, surv := sk.byID[r.ParentID]; surv {
			continue // real edge, not a recovered fan-out
		}
		exactWindow[r.ParentID] = foo.frag.anchorCkpt.SpanID
	}
	// The checkpoint witnessing an HA pins M to a single window (it emits M's
	// bloom from the bottom of M's window). Resolve each fan-out's window from its
	// witness, the SAME way fragments resolve their anchorCkpt (depth -> window-top
	// checkpoint depth, prefix-match a surviving checkpoint). witnessWindow[m] is
	// M's true window id, independent of how many routers any window happens to
	// have.
	ckptByDepth := make(map[int][]*Span)
	for _, s := range sk.byID {
		if s.Depth%cfg.CPD == 0 {
			ckptByDepth[s.Depth] = append(ckptByDepth[s.Depth], s)
		}
	}
	resolveWindow := func(depth int, prefix []byte) uint64 {
		if prefix == nil {
			return 0
		}
		var ckd int
		if depth%cfg.CPD == 0 {
			ckd = depth - cfg.CPD
		} else {
			ckd = (depth / cfg.CPD) * cfg.CPD
		}
		if ckd < 0 {
			return 0
		}
		var hit uint64
		for _, c := range ckptByDepth[ckd] {
			id8 := bridge.BigEndian8(c.SpanID)
			match := true
			for i := 0; i < cfg.PrefixLen && i < len(prefix); i++ {
				if id8[i] != prefix[i] {
					match = false
					break
				}
			}
			if match && (hit == 0 || c.SpanID < hit) {
				hit = c.SpanID
			}
		}
		return hit
	}
	witnessWindow := make(map[uint64]uint64)
	for m, fo := range sk.fanouts {
		if fo.witness != nil {
			witnessWindow[m] = resolveWindow(fo.witness.Depth, fo.witness.CkptPrefix)
		}
	}
	if os.Getenv("TRACE_RECON_CGP2WIT") != "" {
		var haveWit, resolved, agreeExact, disagreeExact, noPrefix int
		for m, fo := range sk.fanouts {
			if fo.witness == nil {
				continue
			}
			haveWit++
			if fo.witness.CkptPrefix == nil {
				noPrefix++
			}
			ww := witnessWindow[m]
			if ww != 0 {
				resolved++
			}
			if ew, ok := exactWindow[m]; ok && ww != 0 {
				if ww == ew {
					agreeExact++
				} else {
					disagreeExact++
				}
			}
		}
		fmt.Fprintf(os.Stderr, "CGP2WIT fanouts-with-witness=%d witness-noPrefix=%d witnessWindow-resolved=%d vsExact[agree=%d disagree=%d]\n",
			haveWit, noPrefix, resolved, agreeExact, disagreeExact)
	}
	// All-downstream is DETERMINED, not chosen: each fan-out's window is pinned by
	// its witness (validated to match the exact-children window in every case), so
	// the solver does not guess it. M is named only for fragments in this window;
	// fragments from every other window pass that depth as an anonymous synthetic,
	// so a cross-window FP confirmer can never wrongly claim M. No aux vars.
	realizedWindow := make(map[uint64]uint64)
	for m := range sk.fanouts {
		if w := witnessWindow[m]; w != 0 {
			realizedWindow[m] = w
		} else if w, ok := exactWindow[m]; ok {
			realizedWindow[m] = w
		}
	}
	nTotal := n

	var b strings.Builder
	fmt.Fprintf(&b, "VARS %d\n", nTotal)
	fmt.Fprintf(&b, "OBJ %d", n) // objective ranges over option vars only; aux carry no score
	for _, vr := range vrefs {
		fmt.Fprintf(&b, " %d %d", opts[vr.fi].options[vr.oi].score, varOf[vr.fi][vr.oi])
	}
	b.WriteByte('\n')
	for fi := range opts { // one-hot anchor per fragment
		k := len(opts[fi].options)
		fmt.Fprintf(&b, "CON 2 1 %d", k)
		for oi := 0; oi < k; oi++ {
			fmt.Fprintf(&b, " 1 %d", varOf[fi][oi])
		}
		b.WriteByte('\n')
	}
	// HA >=2 corroboration: every HA fan-out must end with >=2 children, drawn from
	// its single pinned window (cross-window routing is inadmissible). Its EXACT
	// children (direct, certain, at d-1) are credited up front — routing members
	// now exclude d-1, so they no longer appear among routers — and the solver only
	// needs to supply the shortfall: need = 2 - exactKids. A pinned window that
	// cannot meet the shortfall is a candidate-generation gap (counted, not hidden).
	thinWindow := 0
	for _, m := range fanoutIDs { // deterministic constraint-emission order
		w := realizedWindow[m]
		if w == 0 {
			continue
		}
		need := 2
		if nd := fr.nodes[m]; nd != nil {
			need -= len(nd.exactKids)
		}
		if need <= 0 {
			continue // realized by its direct (exact) children alone
		}
		ovs := routesByWindow[m][w]
		if len(ovs) < need {
			thinWindow++
			if len(ovs) == 0 {
				continue
			}
		}
		fmt.Fprintf(&b, "CON 0 %d %d", need, len(ovs))
		for _, ov := range ovs {
			fmt.Fprintf(&b, " 1 %d", ov)
		}
		b.WriteByte('\n')
	}
	if os.Getenv("TRACE_RECON_CGP2SOLVE") != "" {
		fmt.Fprintf(os.Stderr, "CGP2PIN fanouts=%d pinned-windows-with-<2-routers=%d\n", len(sk.fanouts), thinWindow)
	}

	tlim := 5.0
	if v := os.Getenv("TRACE_RECON_CGP2TLIM"); v != "" {
		if t, err := strconv.ParseFloat(v, 64); err == nil {
			tlim = t
		}
	}
	if os.Getenv("TRACE_RECON_CGP2SOLVE") != "" {
		fmt.Fprintf(os.Stderr, "CGP2MODEL optionVars=%d auxVars=%d fragments=%d fanouts=%d\n",
			n, nTotal-n, len(opts), len(sk.fanouts))
	}
	vals, ok := cpsatBoolSolveFn(b.String(), nTotal, tlim)
	if !ok {
		if os.Getenv("TRACE_RECON_CGP2SOLVE") != "" {
			// Infeasibility now can only mean a pinned window cannot supply >=2
			// routers for some fan-out (a candidate-generation gap), since the window
			// is no longer a free choice.
			thin := 0
			for m := range sk.fanouts {
				w := realizedWindow[m]
				if w != 0 && len(routesByWindow[m][w]) < 2 {
					thin++
				}
			}
			fmt.Fprintf(os.Stderr, "CGP2INFEAS pinned-windows-with-<2-routers=%d\n", thin)
		}
		return Result{}
	}

	// chosen anchor per fragment
	chosen := make([]*Span, len(opts))
	for fi, foo := range opts {
		for oi := range foo.options {
			if vals[varOf[fi][oi]] == 1 {
				chosen[fi] = foo.options[oi].anchor
				break
			}
		}
	}
	// per-fragment depth -> confirmed fan-out (for naming nodes during threading)
	fanAt := make([]map[int]uint64, len(opts))
	for fi, foo := range opts {
		m := make(map[int]uint64, len(foo.fanouts))
		for _, id := range foo.fanouts {
			m[haDepth[id]] = id
		}
		fanAt[fi] = m
	}

	// === build the full reconstructed tree ===
	parent := make(map[uint64]uint64)
	for _, s := range sk.byID { // intra-fragment real edges (certain)
		if s.ParentID != 0 {
			if _, surv := sk.byID[s.ParentID]; surv {
				parent[s.SpanID] = s.ParentID
			}
		}
	}
	// Anonymous synthetic ids: a counter that skips every real id that can appear
	// in the tree (survivors + every fragment's exact parent + every fan-out), so
	// gap-fillers can never collide with a real span id. Tracked explicitly.
	used := make(map[uint64]bool, len(sk.byID))
	for id := range sk.byID {
		used[id] = true
	}
	for _, fo := range sk.frags {
		if fo.root.ParentID != 0 {
			used[fo.root.ParentID] = true
		}
	}
	for id := range sk.fanouts {
		used[id] = true
	}
	anon := make(map[uint64]bool)
	nextAnon := uint64(1)
	allocAnon := func() uint64 {
		for used[nextAnon] || anon[nextAnon] {
			nextAnon++
		}
		id := nextAnon
		nextAnon++
		anon[id] = true
		return id
	}

	var res Result
	for fi, foo := range opts {
		a := chosen[fi]
		f := foo.frag
		if a == nil || f.root.ParentID == 0 {
			continue
		}
		cur := f.root.SpanID
		done := false
		for d := f.root.Depth - 1; d > a.Depth; d-- {
			if _, exists := parent[cur]; exists {
				done = true // cur already has an upward path (shared via a coalesced node)
				break
			}
			var node uint64
			if d == f.root.Depth-1 {
				node = f.root.ParentID // exact parent M_F (named synthetic)
			} else if m := fanAt[fi][d]; m != 0 && realizedWindow[m] == f.anchorCkpt.SpanID {
				node = m // recovered fan-out realized in THIS fragment's window (named, shared)
			} else {
				node = allocAnon() // anonymous gap-filler (private, collision-free id)
			}
			parent[cur] = node
			cur = node
		}
		if !done {
			if _, exists := parent[cur]; !exists {
				parent[cur] = a.SpanID // top of the new chain attaches to the anchor survivor
			}
		}
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:    f.root.SpanID,
			AnchorID:    a.SpanID,
			Synthetic:   f.root.Depth - a.Depth - 1,
			ViaCarrier:  f.viaCarrier,
			ReconFanout: f.root.ParentID,
		})
		res.Reconnected++
	}
	res.ReconParent = parent
	res.ReconAnon = anon
	ha := make(map[uint64]bool, len(sk.fanouts))
	for id := range sk.fanouts {
		ha[id] = true
	}
	res.ReconHAFanouts = ha
	return res
}

// CheckSurvivorSyntheticEdges answers one question directly, against each
// survivor's OWN record (not the truth list): is any survivor connected in the
// result graph to a synthetic node that is NOT its literal named parent?
//
// A survivor's parent id is exactly known (s.ParentID). Its only legitimate
// reconstructed parent is the node with that id — whether that resolves to a
// surviving span or to a named synthetic materialized for a dropped parent.
// badSynthetic counts survivors whose reconstructed parent is a synthetic
// (anonymous, or a named synthetic with a different id) other than s.ParentID;
// wrongSurvivor counts survivors attached to a different real survivor; missing
// counts survivors with no reconstructed parent edge at all.
func CheckSurvivorSyntheticEdges(survivors []Span, res Result) (badSynthetic, holeInvented, wrongSurvivor, missing, missingParentSurvived, total int, examples []string) {
	if res.ReconParent == nil {
		return
	}
	survSet := make(map[uint64]bool, len(survivors))
	for i := range survivors {
		survSet[survivors[i].SpanID] = true
	}
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID == 0 {
			continue // trace root: no parent
		}
		total++
		p, ok := res.ReconParent[s.SpanID]
		if !ok {
			missing++
			if survSet[s.ParentID] {
				missingParentSurvived++ // parent SURVIVED yet child has no edge: intra-fragment copy failed
			}
			continue
		}
		if p == s.ParentID {
			continue // connected to its literal named parent (real span or named synthetic)
		}
		if survSet[p] {
			wrongSurvivor++ // wired to a DIFFERENT survivor: a non-fragmented edge corrupted
			continue
		}
		badSynthetic++
		parentSurvived := survSet[s.ParentID]
		if parentSurvived {
			holeInvented++ // parent survived (no fragmentation here) yet wired to a synthetic
		}
		if len(examples) < 12 {
			kind := "named-syn"
			if res.ReconAnon[p] {
				kind = "anon"
			}
			pk := "parentDropped"
			if parentSurvived {
				pk = "parentSURVIVED"
			}
			examples = append(examples, fmt.Sprintf("surv=%016x d=%d parentID=%016x(%s) reconParent=%016x(%s)",
				s.SpanID, s.Depth, s.ParentID, pk, p, kind))
		}
	}
	return
}

// GrandparentDecomp decomposes the bloom-path failure into its two factors, for
// the d1 / grandparent slot. Over each fragment root R whose grandparent slot is
// INTERIOR (bloom-exposed, R.Depth-2 not a checkpoint depth), it asks: did the
// true grandparent G actually drop, and — when it dropped — did a WRONG real node
// (an FP-cousin survivor) fill M_F's parent slot instead of an anonymous filler?
// This isolates whether the CPD-parity zigzag lives in P(grandparent dropped) or
// in P(FP fill | dropped). Counts each named parent M_F once.
func GrandparentDecomp(survivors []Span, truth []TruthSpan, res Result, dropped map[uint64]struct{}, cpd int) (exposed, gDrop, gDropWrong, gSurv, gSurvWrong, cousinSum, cousinN int) {
	if res.ReconParent == nil || cpd <= 0 {
		return
	}
	tparent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		tparent[t.SpanID] = t.ParentID
	}
	survSet := make(map[uint64]bool, len(survivors))
	for i := range survivors {
		survSet[survivors[i].SpanID] = true
	}
	// surviving children per true parent: the surviving siblings available at a
	// slot. (A grandparent GP's surviving children all sit at the slot depth.)
	survChildren := make(map[uint64]int, len(survivors))
	for i := range survivors {
		survChildren[survivors[i].ParentID]++
	}
	isDropped := func(id uint64) bool { _, d := dropped[id]; return d }
	seenMF := make(map[uint64]bool)
	for i := range survivors {
		R := &survivors[i]
		mf := R.ParentID
		if mf == 0 || survSet[mf] {
			continue // not a fragment root: trace root or surviving parent
		}
		if seenMF[mf] {
			continue
		}
		seenMF[mf] = true
		gpDepth := R.Depth - 2
		if gpDepth < 0 || gpDepth%cpd == 0 {
			continue // checkpoint-aligned grandparent (clean/prefix) — not bloom-exposed
		}
		exposed++
		g := tparent[mf]
		rp, ok := res.ReconParent[mf]
		if isDropped(g) {
			gDrop++
			cousinSum += survChildren[tparent[g]] // surviving siblings of G (the FP-candidate pool)
			cousinN++
			if ok && !res.ReconAnon[rp] && rp != g {
				gDropWrong++ // a wrong real node (FP cousin) filled the slot
			}
		} else {
			gSurv++
			if !ok || rp != g {
				gSurvWrong++
			}
		}
	}
	return
}

// CGP2Iso is the edge-accuracy verdict on the FULL reconstructed tree
// (Result.ReconParent) vs ground truth: for each real node (survivor or
// recovered named synthetic), is its reconstructed parent's IDENTITY the true
// parent? This is the isomorphism measure — a faithful reconstruction has every
// edge correct, with synthetics standing in for dropped spans.
type CGP2Iso struct {
	RealNodes int // nodes with a real span id (survivor or named synthetic) that have a reconstructed parent
	EdgeExact int // reconstructed parent identity == true parent (CORRECT edge)
	EdgeAnonOK int // reconstructed parent is an anonymous synthetic AND the true parent is dropped (right shape, identity not recovered)
	EdgeWrong int // wrong parent identity, or anonymous parent where the true parent actually survived

	SurvNodes int // of RealNodes, survivors
	SurvExact int // of survivor edges, exactly correct
	NamedSyn  int // recovered named-synthetic (dropped-span) nodes with a parent
	NamedExact int // ... with the correct parent identity
}

// ScoreCGP2Iso scores the full reconstructed tree against truth by per-edge
// identity correctness. Anonymous synthetic nodes (no recovered identity) are
// not scored as sources; their use as a parent is judged by whether the true
// parent was indeed dropped.
func ScoreCGP2Iso(res Result, truth []TruthSpan, dropped map[uint64]struct{}) CGP2Iso {
	var iso CGP2Iso
	if res.ReconParent == nil {
		return iso
	}
	tparent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		tparent[t.SpanID] = t.ParentID
	}
	isDropped := func(id uint64) bool { _, d := dropped[id]; return d }
	for x, r := range res.ReconParent {
		if res.ReconAnon[x] {
			continue // anonymous node: no true identity to score
		}
		iso.RealNodes++
		surv := !isDropped(x)
		if surv {
			iso.SurvNodes++
		} else {
			iso.NamedSyn++
		}
		tp := tparent[x]
		switch {
		case res.ReconAnon[r]: // reconstructed parent is anonymous
			if isDropped(tp) {
				iso.EdgeAnonOK++
			} else {
				iso.EdgeWrong++
			}
		case r == tp: // exact identity match
			iso.EdgeExact++
			if surv {
				iso.SurvExact++
			} else {
				iso.NamedExact++
			}
		default:
			iso.EdgeWrong++
		}
	}
	return iso
}

// DumpCGP2WrongDepths prints, per trace, a histogram of how far BELOW each wrong
// node its nearest surviving descendant sits in the reconstructed tree. Distance
// 1 = the wrong node directly parents a fragment root (the exact parent M_F);
// larger = deeper up the synthetic chain toward the anchor. It also splits by
// whether the wrong node's TRUE parent survived (so an anonymous gap-filler was
// wrongly placed where a real survivor belonged) or was itself dropped.
func DumpCGP2WrongDepths(truth []TruthSpan, res Result, dropped map[uint64]struct{}, cpd int) {
	if res.ReconParent == nil {
		return
	}
	tparent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		tparent[t.SpanID] = t.ParentID
	}
	isDropped := func(id uint64) bool { _, d := dropped[id]; return d }
	isSurv := func(id uint64) bool { return !res.ReconAnon[id] && !isDropped(id) }
	children := make(map[uint64][]uint64, len(res.ReconParent))
	for c, p := range res.ReconParent {
		children[p] = append(children[p], c)
	}
	// nearest surviving descendant distance (BFS down the recon tree)
	distToSurv := func(x uint64) int {
		frontier := append([]uint64(nil), children[x]...)
		d := 1
		for len(frontier) > 0 {
			var next []uint64
			for _, n := range frontier {
				if isSurv(n) {
					return d
				}
				next = append(next, children[n]...)
			}
			frontier = next
			d++
			if d > 64 {
				break
			}
		}
		return 0 // no surviving descendant found
	}
	tdepth := make(map[uint64]int, len(truth))
	for _, t := range truth {
		tdepth[t.SpanID] = t.Depth
	}
	// every node id present in the recon tree (as child or parent)
	reconNode := make(map[uint64]bool, len(res.ReconParent)*2)
	for c, p := range res.ReconParent {
		reconNode[c] = true
		reconNode[p] = true
	}
	// count surviving descendants of x (capped at 2) = corroboration breadth
	survDescCount := func(x uint64) int {
		frontier := append([]uint64(nil), children[x]...)
		n := 0
		for len(frontier) > 0 && n < 2 {
			var next []uint64
			for _, c := range frontier {
				if isSurv(c) {
					n++
					if n >= 2 {
						break
					}
				} else {
					next = append(next, children[c]...)
				}
			}
			frontier = next
		}
		return n
	}
	var hist [8]int // index = min(distance,7); 0 bucket = unknown
	tpSurv, tpDrop := 0, 0
	// distance-1 breakdown of WHAT the exact-parent node got attached to vs its true parent
	var d1, rpAnon, rpSurvSameDepth, rpSurvOtherDepth, rpNamedSyn int
	var tpNamedElsewhere, tpSameDepthAsRp int
	// HA-witnessed vs plain exact-parent, and corroboration reach (>=2 surviving descendants)
	var d1HA, d1Plain, d1HAMulti, d1PlainMulti int
	// For the WRONG cases where the true (surviving) parent got an anonymous filler:
	// was that true parent at/above the root's window top (excluded by the d-2 loop
	// bound) or strictly inside the window (should have been found — a real miss)?
	var anonSurvExclBound, anonSurvInWindow int
	// checkpoint-alignment of the d1 wrong edge: is the TRUE grandparent on a
	// checkpoint depth (should be prefix-reconnected, clean), and is the CHOSEN
	// wrong parent a checkpoint (prefix-ambiguity) vs interior survivor (bloom FP)?
	var d1TpCkpt, d1RpCkpt, d1RpInterior int
	for x, r := range res.ReconParent {
		if res.ReconAnon[x] {
			continue
		}
		tp := tparent[x]
		wrong := false
		if res.ReconAnon[r] {
			wrong = !isDropped(tp) // anon parent where the true parent survived
		} else {
			wrong = r != tp
		}
		if !wrong {
			continue
		}
		d := distToSurv(x)
		if d > 7 {
			d = 7
		}
		hist[d]++
		if isDropped(tp) {
			tpDrop++
		} else {
			tpSurv++
		}
		if d != 1 {
			continue
		}
		d1++
		switch {
		case res.ReconAnon[r]:
			rpAnon++
		case !isDropped(r): // r is a survivor
			if tdepth[r] == tdepth[tp] {
				rpSurvSameDepth++
			} else {
				rpSurvOtherDepth++
			}
		default: // r is a named synthetic (dropped, recovered)
			rpNamedSyn++
		}
		if reconNode[tp] { // the TRUE parent exists as a node elsewhere in the tree
			tpNamedElsewhere++
		}
		if tdepth[r] == tdepth[tp] {
			tpSameDepthAsRp++
		}
		// isolate the wrong "true parent survived, but we placed anon" cases
		if res.ReconAnon[r] && !isDropped(tp) && cpd > 0 {
			dx := tdepth[x]            // x = M_F, at R.Depth-1
			lo := (dx / cpd) * cpd     // root's window top (R.Depth = dx+1)
			gDepth := dx - 1           // true parent G depth = R.Depth-2
			if gDepth <= lo {
				anonSurvExclBound++
			} else {
				anonSurvInWindow++
			}
		}
		if cpd > 0 {
			if tdepth[tp]%cpd == 0 {
				d1TpCkpt++ // true grandparent IS a checkpoint: should have been prefix-reconnected
			}
			if !res.ReconAnon[r] && !isDropped(r) { // chosen wrong parent is a real survivor
				if tdepth[r]%cpd == 0 {
					d1RpCkpt++ // wired to a CHECKPOINT cousin (prefix ambiguity)
				} else {
					d1RpInterior++ // wired to an INTERIOR cousin (bloom false positive)
				}
			}
		}
		multi := survDescCount(x) >= 2
		if res.ReconHAFanouts[x] {
			d1HA++
			if multi {
				d1HAMulti++
			}
		} else {
			d1Plain++
			if multi {
				d1PlainMulti++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "CGP2WD d1=%d d2=%d d3=%d d4+=%d dUnk=%d | tpSurvived=%d tpDropped=%d\n",
		hist[1], hist[2], hist[3], hist[4]+hist[5]+hist[6]+hist[7], hist[0], tpSurv, tpDrop)
	fmt.Fprintf(os.Stderr, "CGP2WD-D1 n=%d | rp:anon=%d survSameDepth=%d survOtherDepth=%d namedSyn=%d | tpExistsAsNode=%d rpTpSameDepth=%d\n",
		d1, rpAnon, rpSurvSameDepth, rpSurvOtherDepth, rpNamedSyn, tpNamedElsewhere, tpSameDepthAsRp)
	fmt.Fprintf(os.Stderr, "CGP2WD-HA d1HA=%d d1HAwithMultiDesc=%d | d1Plain=%d d1PlainWithMultiDesc=%d\n",
		d1HA, d1HAMulti, d1Plain, d1PlainMulti)
	fmt.Fprintf(os.Stderr, "CGP2WD-ANON survParentGotAnon: exclBound=%d inWindow=%d\n",
		anonSurvExclBound, anonSurvInWindow)
	fmt.Fprintf(os.Stderr, "CGP2WD-CKPT d1 trueGrandparentIsCkpt=%d | chosenWrong: ckptCousin=%d interiorCousin=%d\n",
		d1TpCkpt, d1RpCkpt, d1RpInterior)
}

// DumpCGP2Edges prints, for one (feasible) trace, the per-survivor comparison of
// reconstructed parent vs true parent, and aggregate buckets — to localize where
// survivor edges go wrong (or whether the measurement itself is the problem).
func DumpCGP2Edges(survivors []Span, truth []TruthSpan, res Result, dropped map[uint64]struct{}) {
	tparent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		tparent[t.SpanID] = t.ParentID
	}
	isDropped := func(id uint64) bool { _, d := dropped[id]; return d }
	var total, spTpMismatch, reconMissing, exact, anonForDropped, anonForSurv, wrongReal int
	fmt.Fprintf(os.Stderr, "=== CGP2DUMP feasible trace: survivors=%d ReconParent=%d ===\n", len(survivors), len(res.ReconParent))
	sampled := 0
	for i := range survivors {
		s := &survivors[i]
		total++
		sp := s.ParentID
		tp := tparent[s.SpanID]
		if sp != tp {
			spTpMismatch++
		}
		R, ok := res.ReconParent[s.SpanID]
		switch {
		case !ok:
			reconMissing++
		case res.ReconAnon[R]:
			if isDropped(tp) {
				anonForDropped++
			} else {
				anonForSurv++
			}
		case R == tp:
			exact++
		default:
			wrongReal++
		}
		if sampled < 24 {
			recon := "MISSING"
			if ok {
				if res.ReconAnon[R] {
					recon = "anon"
				} else {
					recon = fmt.Sprintf("%016x", R)
				}
			}
			fmt.Fprintf(os.Stderr, "  s=%016x d=%d spParent=%016x truthParent=%016x spEQtruth=%t parentDropped=%t recon=%s reconEQtruth=%t\n",
				s.SpanID, s.Depth, sp, tp, sp == tp, isDropped(sp), recon, ok && R == tp)
			sampled++
		}
	}
	fmt.Fprintf(os.Stderr, "CGP2DUMP-AGG survivors=%d spTpMismatch=%d reconMissing=%d exact=%d anonForDropped=%d anonForSurv=%d wrongReal=%d\n",
		total, spTpMismatch, reconMissing, exact, anonForDropped, anonForSurv, wrongReal)
}

// cgpDiagCandidates reports anchor resolution and the permissive candidate
// space: how many fragments got an anchor, how many were ambiguous, total
// candidate (fragment->fan-out) edges, and the average routing options per
// fragment (a proxy for how much the solve must disambiguate).
func cgpDiagCandidates(sk *cgpSkeleton, c *cgpCandidates) {
	var anchored, ambig, carrierFrags int
	for _, f := range sk.frags {
		if f.carrier == nil {
			continue
		}
		carrierFrags++
		if f.anchorCkpt != nil {
			anchored++
		}
		if f.anchorAmbig {
			ambig++
		}
	}
	totalEdges := 0
	for _, ms := range c.members {
		totalEdges += len(ms)
	}
	fragsWithJoins := len(c.fragJoins)
	fmt.Fprintf(os.Stderr,
		"CGP2CAND carrierFrags=%d anchored=%d anchorAmbig=%d fanoutsWithMembers=%d candEdges=%d fragsWithJoins=%d\n",
		carrierFrags, anchored, ambig, len(c.members), totalEdges, fragsWithJoins)
}

// cgpDiagOptions reports the one-hot option space: fragments with options, total
// options, the distribution of options-per-fragment, and how many fragments have
// >1 option (i.e. a genuine attachment-depth choice the solve must make).
func cgpDiagOptions(opts []*cgpFragOpts) {
	var totalOpts, multiOpt, maxOpts int
	for _, fo := range opts {
		totalOpts += len(fo.options)
		if len(fo.options) > 1 {
			multiOpt++
		}
		if len(fo.options) > maxOpts {
			maxOpts = len(fo.options)
		}
	}
	avg := 0.0
	if len(opts) > 0 {
		avg = float64(totalOpts) / float64(len(opts))
	}
	fmt.Fprintf(os.Stderr,
		"CGP2OPTS fragsWithOptions=%d totalOptions=%d avgOptsPerFrag=%.2f fragsWithChoice(>1)=%d maxOpts=%d\n",
		len(opts), totalOpts, avg, multiOpt, maxOpts)
}

// cgp2DiagDump prints structural counts and checks the invariants that Phase 1
// must uphold, so the foundation is validated before later phases build on it.
func (sk *cgpSkeleton) diag() {
	var orphans, carrierFrags int
	for _, f := range sk.frags {
		if f.isOrphan() {
			orphans++
		} else {
			carrierFrags++
		}
	}
	// Invariant check: every witnessed fan-out's witness must be strictly
	// deeper than the fan-out (a descendant), and the fan-out id must be dropped.
	badWitness, badDropped := 0, 0
	for _, fo := range sk.fanouts {
		if fo.witness.Depth <= fo.depth {
			badWitness++
		}
		if _, surv := sk.byID[fo.id]; surv {
			badDropped++
		}
	}
	fmt.Fprintf(os.Stderr,
		"CGP2PARSE survivors=%d frags=%d carrierFrags=%d orphans(carrierless)=%d fanouts=%d badWitnessDepth=%d badDropped=%d\n",
		len(sk.byID), len(sk.frags), carrierFrags, orphans, len(sk.fanouts), badWitness, badDropped)
}

// diag validates the Phase-2 forest: the airtight invariant is that no node with
// >=2 exact children is non-HA (a real >=2 fan-out is always witnessed). Also
// reports how many HA fan-outs have <2 exact children (must recruit routed
// children) and how many named nodes are linear (1 exact child, non-HA).
func (fr *cgpForest) diag() {
	var named, namedLinear, badNonHaMultiKid, haNeedRouted, haExactGe2 int
	for _, n := range fr.nodes {
		if n.surv {
			continue
		}
		named++
		k := len(n.exactKids)
		if !n.ha && k >= 2 {
			badNonHaMultiKid++ // INVARIANT VIOLATION if >0
		}
		if !n.ha && k == 1 {
			namedLinear++
		}
		if n.ha {
			if k >= 2 {
				haExactGe2++
			} else {
				haNeedRouted++ // <2 exact children: must recruit routed children
			}
		}
	}
	fmt.Fprintf(os.Stderr,
		"CGP2FOREST nodes=%d namedSynthetics=%d linearNamed=%d haFanouts(exact>=2)=%d haFanouts(needRouted)=%d INVARIANT_badNonHaMultiKid=%d\n",
		len(fr.nodes), named, namedLinear, haExactGe2, haNeedRouted, badNonHaMultiKid)
}

// diagEvidence validates Phase 3a: a carrier-bearing fragment whose immediate
// dropped parent M is known must have M in its resolved bloom (true in-window
// ancestor, no false negatives). mNotInBloom must be 0 (modulo M being above the
// carrier's window — counted separately as mOutOfWindow, the legitimate case
// where the fragment's own carrier sits in a deeper window than M).
func cgpDiagEvidence(sk *cgpSkeleton, cfg Config) {
	var carrierFrags, withBloom, mTestable, mInBloom, mNotInBloom, mOutOfWindow int
	for _, f := range sk.frags {
		if f.carrier == nil {
			continue
		}
		carrierFrags++
		if f.bf == nil {
			continue
		}
		withBloom++
		r := f.root
		if r.ParentID == 0 {
			continue
		}
		if _, surv := sk.byID[r.ParentID]; surv {
			continue // parent survived
		}
		mDepth := r.Depth - 1
		// M is in the carrier's window only if it's at/below the carrier's covering
		// checkpoint depth (the bloom resets there). carrier covering ckpt depth:
		cd := f.carrier.Depth
		var ckd int
		if cd%cfg.CPD == 0 {
			ckd = cd - cfg.CPD
		} else {
			ckd = (cd / cfg.CPD) * cfg.CPD
		}
		if mDepth <= ckd {
			mOutOfWindow++
			continue
		}
		mTestable++
		k := bridge.HexOf(r.ParentID)
		if f.bf.Test(k[:]) {
			mInBloom++
		} else {
			mNotInBloom++
		}
	}
	fmt.Fprintf(os.Stderr,
		"CGP2EVID carrierFrags=%d withBloom=%d mTestable=%d mInBloom=%d mNotInBloom=%d mOutOfWindow=%d\n",
		carrierFrags, withBloom, mTestable, mInBloom, mNotInBloom, mOutOfWindow)
}

// ReconstructCGP2 is the from-scratch reconstructor entry point. Phase 1 is in
// place (parse + skeleton); subsequent phases are layered on next. Until the
// solve lands it returns an empty Result (so callers see "nothing reconnected"
// rather than a wrong edge — soundness first), and emits diagnostics under
// TRACE_RECON_CGP2DIAG.
func ReconstructCGP2(survivors []Span, cfg Config) Result {
	sk := cgpParse(survivors, cfg)
	fr := cgpBuildForest(sk)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	cand := cgpGenCandidates(sk, cfg)
	opts := cgpGenOptions(sk, cand)
	if cgp2Diag {
		sk.diag()
		fr.diag()
		cgpDiagEvidence(sk, cfg)
		cgpDiagCandidates(sk, cand)
		cgpDiagOptions(opts)
	}
	return cgpSolveAndEmit(sk, fr, opts, cfg)
}
