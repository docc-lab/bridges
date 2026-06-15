package recon

import (
	"fmt"
	"os"

	"bridges/bloom"
	"bridges/bridge"
)

// ReconstructFullSAT is the single-pass CP-SAT reconstruction engine: it PORTS
// the PCRS MAP solver's item model into one declarative solve, with NO staging.
// The PCRS pipeline committed pass-1 anchors, propagated rules to fixpoint, then
// solved the residual; here every decision is enumerated as an option and dumped
// at once into solveClusterFullsat — anchoring/threading, orphan placement, and
// open-end coverage all decided simultaneously under the explained-positives
// (MAP) objective. Construction has an order; the SOLVE has none.
//
// The model (faithful to recon/pcrs.go's sItem/sOpt + recon/fullsat_cluster.go):
//   - fragState: a checkpoint-bearing fragment root, anchored at its window-root
//     checkpoint (prefix match; checkpoints are PRESERVED so it always exists).
//   - Level ids: (fragState, depth) -> dense int (idOf), the occupancy space.
//   - thread items: each interior level with gate-admissible survivors; options
//     = which survivor occupies it (or free skip); gain = explained positive.
//   - orphan items: a checkpoint-less fragment's pathOn chains across windows;
//     options carry occupancy + a reserved parent level; high skip penalty.
//   - open-end items: every interior node whose children all dropped must be
//     covered by an admissible writer; high skip penalty.
//   - occupancy: one span per (fragState, depth); same id may coexist.
//   - objective: maximize sum(gain) + placement (the no-open-ends / no-orphan
//     invariants enter as large skip penalties so placement dominates threading).
func ReconstructFullSAT(survivors []Span, cfg Config) Result {
	var res Result
	byID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		byID[survivors[i].SpanID] = &survivors[i]
	}
	children := make(map[uint64][]*Span, len(survivors))
	byDepth := make(map[int][]*Span)
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 {
			if _, ok := byID[s.ParentID]; ok {
				children[s.ParentID] = append(children[s.ParentID], s)
			}
		}
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}

	// Fragment roots: surviving spans whose parent dropped.
	var roots []*Span
	for i := range survivors {
		o := &survivors[i]
		if o.ParentID == 0 {
			continue
		}
		if _, ok := byID[o.ParentID]; ok {
			continue
		}
		roots = append(roots, o)
	}
	res.Orphans = len(roots)

	// --- fragStates: checkpoint-bearing fragments anchored at the window-root. ---
	type fragState struct {
		o       *Span
		anchor  *Span
		filters []*bloom.Filter
	}
	var states []*fragState
	var orphanRoots []*Span
	for _, o := range roots {
		carrierDepth, prefix, fbits := fragmentPayloads(o, children, cfg)
		if prefix == nil {
			orphanRoots = append(orphanRoots, o) // checkpoint-less => orphan
			continue
		}
		var ckptDepth int
		if carrierDepth%cfg.CPD == 0 {
			ckptDepth = carrierDepth - cfg.CPD
		} else {
			ckptDepth = (carrierDepth / cfg.CPD) * cfg.CPD
		}
		if ckptDepth < 0 {
			ckptDepth = 0
		}
		// Window-root anchor: the preserved checkpoint matching the prefix.
		var anchor *Span
		for _, c := range byDepth[ckptDepth] {
			id8 := bridge.BigEndian8(c.SpanID)
			match := true
			for k := 0; k < cfg.PrefixLen; k++ {
				if id8[k] != prefix[k] {
					match = false
					break
				}
			}
			if match && (anchor == nil || c.SpanID < anchor.SpanID) {
				anchor = c
			}
		}
		if anchor == nil {
			orphanRoots = append(orphanRoots, o) // prefix names a dropped checkpoint? treat as orphan
			continue
		}
		filters := make([]*bloom.Filter, 0, len(fbits))
		for _, bts := range fbits {
			filters = append(filters, bloom.Deserialize(bts, cfg.BloomM, cfg.BloomK))
		}
		states = append(states, &fragState{o: o, anchor: anchor, filters: filters})
	}

	// --- Level ids. THREE kinds share one dense space:
	//   'p' positional (fragState, depth): the per-window occupancy a thread/orphan
	//       path writes; used for read-back. Conflicts when two options claim it
	//       with different spans.
	//   'n' named-parent (ParentID, GLOBAL): a recovered dropped-parent NODE, shared
	//       by every path through that common parent in any window — same id
	//       coexists (coalescing). A named synthetic span.
	//   't' trunk (ParentID, depth): the SHARED, exclusive trunk slot above a fork.
	//       Fork siblings collide here, so a single span must satisfy ALL branches
	//       (each only offers candidates its own bloom admits) — the conjunction,
	//       enforced structurally instead of by pooling blooms.
	type lk struct {
		kind byte
		id   uint64 // ParentID for 'n'/'t'
		fs   int    // fragState for 'p'
		d    int    // depth for 'p'/'t'
	}
	levelID := map[lk]int{}
	mint := func(k lk) int32 {
		if id, ok := levelID[k]; ok {
			return int32(id)
		}
		id := len(levelID)
		levelID[k] = id
		return int32(id)
	}
	idOf := func(fsI, d int) int32 { return mint(lk{kind: 'p', fs: fsI, d: d}) }
	namedIdOf := func(p uint64) int32 { return mint(lk{kind: 'n', id: p}) }
	trunkIdOf := func(p uint64, d int) int32 { return mint(lk{kind: 't', id: p, d: d}) }

	// --- Items: enumerate every option, then dump into ONE solve. ---
	const mustPlace = 1 << 16 // skip penalty for invariant items (orphans, open ends)
	var items []fsItem
	type imeta struct {
		kind  byte  // 't' thread, 'e' open-end, 'o' orphan
		r     *Span // orphan root (kind 'o')
		optFs []int // fragState index per option
	}
	var metas []imeta

	// DIAGNOSTIC (TRACE_RECON_FULLSAT_DIAG): deepest gate-admissible thread candidate
	// per fragState, to tell a solve/read-back miss (picked < deepest enumerated)
	// from an enumeration/reach miss (deepest enumerated already short).
	diag := os.Getenv("TRACE_RECON_FULLSAT_DIAG") != ""
	maxCand := make([]int, len(states))
	for i := range states {
		maxCand[i] = states[i].anchor.Depth
	}

	// (1) thread items: per fragState, each interior level (anchor, o.Depth-1) with
	// >=1 gate-admissible survivor. Options = which survivor occupies; free skip.
	for fsI, fs := range states {
		for d := fs.anchor.Depth + 1; d < fs.o.Depth-1; d++ {
			var opts []fsOpt
			for _, s := range byDepth[d] {
				if fsGate(s, fs.o, fs.anchor.Depth, fs.filters, byID) {
					if d > maxCand[fsI] {
						maxCand[fsI] = d
					}
					opt := fsOpt{
						gain: 1, rsvID: -1,
						levels:      []int32{idOf(fsI, d)},
						spanIDs:     []uint64{s.SpanID},
						chainAnchor: fs.anchor.SpanID,
					}
					// Shared exclusive trunk slot: fork siblings collide here, so the
					// span chosen at this depth must satisfy every branch's bloom (each
					// only offers candidates it admits). This is the conjunction, made
					// structural. The positional level above stays for read-back.
					if !fullsatNoParents {
						opt.levels = append(opt.levels, trunkIdOf(fs.o.ParentID, d))
						opt.spanIDs = append(opt.spanIDs, s.SpanID)
					}
					opts = append(opts, opt)
				}
			}
			if len(opts) > 0 {
				ofs := make([]int, len(opts))
				for k := range ofs {
					ofs[k] = fsI
				}
				items = append(items, fsItem{skipPen: 0, opts: opts})
				metas = append(metas, imeta{kind: 't', optFs: ofs})
			}
		}
	}

	// (2) orphan items: a checkpoint-less fragment's pathOn chains across windows.
	for _, r := range orphanRoots {
		var opts []fsOpt
		var ofs []int
		for fsI, fs := range states {
			// Orphan admissibility tests against the host's SINGLE covering bloom
			// (filters[0], the shallowest carrier — widest window), NOT the AND of
			// all the host's carriers. The multi-filter conjunction is correct for a
			// fragment's OWN trunk (its checkpoints really do all sit above it), but
			// an orphan dropping into a host window is not under those checkpoints, so
			// ANDing them wrongly rejects admissible slots (the bug that dropped 19k).
			p := fsPathOn(r, fs.o, fs.anchor.Depth, fs.filters[:1], children)
			if p == nil {
				continue
			}
			levels := make([]int32, 0, len(p))
			sids := make([]uint64, 0, len(p))
			for _, s := range p {
				levels = append(levels, idOf(fsI, s.Depth))
				sids = append(sids, s.SpanID)
			}
			opt := fsOpt{
				gain: len(p) + 1, rsvID: idOf(fsI, r.Depth-1),
				levels: levels, spanIDs: sids, chainAnchor: fs.anchor.SpanID,
			}
			// Coalesce in-model: this orphan's dropped parent is a named synthetic
			// node, shared by every path through the same ParentID. The reserved
			// positional slot (rsvID) keeps threading out; the named node carries the
			// sibling identity. (The named node IS the orphan's required synthetic
			// parent above — it does not consume the synthetic-below obligation.)
			if !fullsatNoParents {
				opt.named = []fsNamed{{level: namedIdOf(r.ParentID), id: r.ParentID}}
			}
			opts = append(opts, opt)
			ofs = append(ofs, fsI)
		}
		if len(opts) > 0 {
			items = append(items, fsItem{skipPen: mustPlace, opts: opts})
			metas = append(metas, imeta{kind: 'o', r: r, optFs: ofs})
		}
		// No admissible window => no item; read-back is the SINGLE source of
		// res.Unanchored (an orphan absent from placedOrphan is counted there,
		// exactly once — avoiding the earlier double-count).
	}

	// (3) open-end items: every interior node whose children all dropped MUST be
	// covered by an admissible writer (no open ends).
	for i := range survivors {
		e := &survivors[i]
		if e.CkptPrefix != nil || len(children[e.SpanID]) > 0 {
			continue
		}
		res.OpenEnds++
		var opts []fsOpt
		var ofs []int
		for fsI, fs := range states {
			if fsGate(e, fs.o, fs.anchor.Depth, fs.filters, byID) {
				opts = append(opts, fsOpt{
					gain: 1, rsvID: -1,
					levels:      []int32{idOf(fsI, e.Depth)},
					spanIDs:     []uint64{e.SpanID},
					chainAnchor: fs.anchor.SpanID,
				})
				ofs = append(ofs, fsI)
			}
		}
		if len(opts) > 0 {
			res.OpenEndsMatched++
			items = append(items, fsItem{skipPen: mustPlace, opts: opts})
			metas = append(metas, imeta{kind: 'e', optFs: ofs})
		}
	}

	// (4) parent-naming items: every fragment root (checkpoint-bearing too) exposes
	// its dropped parent's exact ParentID. Each becomes a named occupancy on the
	// GLOBAL named node for that parent, so fragments and orphans sharing a dropped
	// parent coalesce onto one synthetic node — sibling identity decided in-model,
	// not stamped on the output. Ablated by --no-parents.
	if !fullsatNoParents {
		for _, fs := range states {
			items = append(items, fsItem{skipPen: mustPlace, opts: []fsOpt{{
				gain: 0, rsvID: -1,
				named:       []fsNamed{{level: namedIdOf(fs.o.ParentID), id: fs.o.ParentID}},
				chainAnchor: fs.anchor.SpanID,
			}}})
			metas = append(metas, imeta{kind: 'p', r: fs.o, optFs: []int{-1}})
		}
	}

	emit := func(o, anchor *Span, reconFanout uint64) {
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:    o.SpanID,
			AnchorID:    anchor.SpanID,
			Synthetic:   o.Depth - anchor.Depth - 1,
			ReconFanout: reconFanout, // read BACK from the solve's named occupancy
		})
		res.Reconnected++
	}

	// --- One dump into CP-SAT. ---
	var assign []int
	if len(items) > 0 {
		if a, ok := solveClusterFullsat(items); ok {
			assign = a
		}
	}
	if assign == nil {
		// No solver linked / no solution: emit each fragment at its window-root,
		// orphans unplaced. (The cpsat build always has the solver.)
		for _, fs := range states {
			rf := uint64(0)
			if !fullsatNoParents {
				rf = fs.o.ParentID
			}
			emit(fs.o, fs.anchor, rf)
		}
		for _, r := range orphanRoots {
			res.Unanchored = append(res.Unanchored, r.SpanID)
		}
		return res
	}

	// --- Read back the occupancy and emit bridges. ---
	levelInfo := make([][2]int, len(levelID))
	for k, id := range levelID {
		if k.kind == 'p' {
			levelInfo[id] = [2]int{k.fs, k.d}
		} else {
			levelInfo[id] = [2]int{-1, -1} // trunk/named coupling level — not positional
		}
	}
	occ := make([]map[int]uint64, len(states)) // fragState -> depth -> spanID
	for i := range occ {
		occ[i] = map[int]uint64{}
	}
	placedOrphan := map[uint64]int{} // orphan spanID -> fragState index it placed in
	namedOcc := map[int32]uint64{}   // realized named-parent node level -> ParentID
	for i := range items {
		ch := assign[i]
		if ch < 0 {
			continue
		}
		opt := items[i].opts[ch]
		for k, L := range opt.levels {
			info := levelInfo[L]
			if info[0] < 0 {
				continue // trunk/coupling level, not positional occupancy
			}
			occ[info[0]][info[1]] = opt.spanIDs[k]
		}
		for _, nm := range opt.named {
			namedOcc[nm.level] = nm.id
		}
		if metas[i].kind == 'o' {
			placedOrphan[metas[i].r.SpanID] = metas[i].optFs[ch]
		}
	}
	// reconFanoutOf reads the recovered parent identity BACK from the solve: the
	// coalesced named node this root landed on (0 if none / ablated).
	reconFanoutOf := func(root *Span) uint64 {
		if fullsatNoParents {
			return 0
		}
		return namedOcc[namedIdOf(root.ParentID)]
	}
	// orphanAnchor threads a placed orphan's chain to its nearest surviving
	// ancestor: the deepest survivor strictly above r that the host's covering
	// bloom admits (filters[:1] — the orphan placed under that single bloom). Since
	// r sits on the host's chain (pathOn confirmed r is the host's ancestor), any
	// host-admissible survivor shallower than r is r's ancestor; the deepest is its
	// nearest. This replicates PCRS's dense-ledger walk (deepest real occupant above
	// r) without relying on a sparse per-fragState occupancy map.
	orphanAnchor := func(host *fragState, r *Span) *Span {
		for d := r.Depth - 1; d > host.anchor.Depth; d-- {
			var best *Span
			for _, s := range byDepth[d] {
				if fsGate(s, host.o, host.anchor.Depth, host.filters[:1], byID) {
					if best == nil || s.SpanID < best.SpanID {
						best = s
					}
				}
			}
			if best != nil {
				return best // deepest depth carrying an admissible ancestor
			}
		}
		return host.anchor
	}
	// deepestAbove returns the deepest occupied survivor in fragState fsI strictly
	// above depth lim (lim exclusive), falling back to the window-root anchor.
	deepestAbove := func(fsI, lim int) *Span {
		anchor := states[fsI].anchor
		best := anchor.Depth
		for d, sid := range occ[fsI] {
			if d > best && d < lim {
				if sp := byID[sid]; sp != nil {
					best = d
					anchor = sp
				}
			}
		}
		return anchor
	}
	var nFrag, fragShort, nOrphan, orphanAtRoot int
	for fsI, fs := range states {
		a := deepestAbove(fsI, fs.o.Depth)
		if diag {
			nFrag++
			if a.Depth < maxCand[fsI] {
				fragShort++ // had a deeper admissible candidate but didn't thread it
			}
		}
		emit(fs.o, a, reconFanoutOf(fs.o))
	}
	for _, r := range orphanRoots {
		fsI, ok := placedOrphan[r.SpanID]
		if !ok {
			res.Unanchored = append(res.Unanchored, r.SpanID)
			continue
		}
		a := orphanAnchor(states[fsI], r)
		if diag {
			nOrphan++
			if a.Depth == states[fsI].anchor.Depth {
				orphanAtRoot++ // anchored at the bare window-root: no admissible ancestor
			}
		}
		emit(r, a, reconFanoutOf(r))
		res.OrphansPlaced++
	}
	if diag {
		fmt.Fprintf(os.Stderr, "FSDIAG frags=%d frag_not_deepest=%d orphans=%d orphan_at_root=%d\n",
			nFrag, fragShort, nOrphan, orphanAtRoot)
	}
	return res
}

// fragmentPayloads collects every PCRB carrier within one CPD band below the
// fragment root o (BFS over surviving children, bounded by band = the next
// checkpoint depth). A branching fragment yields a SET of shallow checkpoints;
// their prefixes name the shared upstream checkpoint and their blooms form the
// filter-set that any connection must satisfy together. Carriers deeper than the
// band live in a deeper window — their blooms reset at the band checkpoint and
// do NOT contain o's trunk, so they are excluded. carrierDepth/prefix come from
// the shallowest carrier (used for the window assignment).
func fragmentPayloads(o *Span, children map[uint64][]*Span, cfg Config) (carrierDepth int, prefix []byte, filters [][]byte) {
	band := (o.Depth/cfg.CPD + 1) * cfg.CPD
	queue := []*Span{o}
	for len(queue) > 0 {
		var next []*Span
		for _, s := range queue {
			if s.Depth > band {
				continue
			}
			if s.CkptPrefix != nil {
				filters = append(filters, s.BloomBits)
				if prefix == nil {
					prefix = s.CkptPrefix
					carrierDepth = s.Depth
				}
			}
			next = append(next, children[s.SpanID]...)
		}
		queue = next
	}
	return
}

// fsGate reports whether s is an admissible WRITER for a fragment rooted at o
// with the given anchor depth and filter-set: a non-leaf, non-root survivor
// strictly inside (anchorDepth, o.Depth-1) that every filter confirms and whose
// upward chain is consistent. (Ported from PCRS gate, generalized to a filter
// conjunction — a real common ancestor is in EVERY filter, no false negatives.)
func fsGate(s, o *Span, anchorDepth int, filters []*bloom.Filter, byID map[uint64]*Span) bool {
	if len(filters) == 0 || s == o || s.LeafCarrier {
		return false
	}
	if s.Depth <= anchorDepth || s.Depth >= o.Depth-1 {
		return false
	}
	hex := bridge.HexOf(s.SpanID)
	for _, f := range filters {
		if !f.Test(hex[:]) {
			return false
		}
	}
	return chainConsistent(filters[0], s, byID, anchorDepth+1)
}

// fsPathOn returns the candidate downward path from reconnection root r inside a
// fragment rooted at o (anchor at anchorDepth), following UNIQUE filter-confirmed
// children until the descent forks or terminates. nil if r is inadmissible. Each
// such path is one orphan/connection option; its length is the explained-positive
// gain. (Ported from PCRS pathOn, generalized to the filter conjunction.)
func fsPathOn(r, o *Span, anchorDepth int, filters []*bloom.Filter, children map[uint64][]*Span) []*Span {
	if len(filters) == 0 || r == o {
		return nil
	}
	if r.Depth > o.Depth-2 || r.Depth <= anchorDepth+1 {
		return nil
	}
	confirmed := func(id uint64) bool {
		h := bridge.HexOf(id)
		for _, f := range filters {
			if !f.Test(h[:]) {
				return false
			}
		}
		return true
	}
	if !confirmed(r.ParentID) || !confirmed(r.SpanID) {
		return nil
	}
	path := []*Span{r}
	cur := r
	for {
		var next *Span
		n := 0
		for _, c := range children[cur.SpanID] {
			if c.Depth > o.Depth-2 {
				continue
			}
			if confirmed(c.SpanID) {
				n++
				next = c
			}
		}
		if n != 1 {
			break
		}
		path = append(path, next)
		cur = next
	}
	return path
}
