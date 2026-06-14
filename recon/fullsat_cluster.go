package recon

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Full-SAT per-cluster solver (--fullsat-pb). Translates one PCRS cluster — the
// same items/options the C/I/O block carries — into the general declarative
// model (docs/fullsat_shim.md) and solves it with the CP-SAT model shim. The
// encoding is a faithful mirror of the legacy C/I/O CP-SAT path: one bool var
// per (item,option), AtMostOne per item (skip = none selected), a weighted
// objective (gain - cost + skipPenalty), and (level,spanID) exclusivity emitted
// as `ALO 2 -a -b` clauses — two options writing the same level coexist iff
// they place the SAME span id, else they conflict. So --fullsat-pb should solve
// to the same assignment as the C/I/O path; that parity validates the protocol
// before --fullsat-cgpb adds naming/channeling on top.

var fullsatEnabled = os.Getenv("TRACE_RECON_FULLSAT") == "1"

// fullsatNaming (--fullsat-cgpb) injects named-synthetic identities into the
// cluster occupancy: a dropped-parent slot is occupied by its ParentID, so
// same-parent children coalesce (a recovered fan-out) and different-parent ones
// conflict. Same-chain only (occupancy is keyed per (chain,depth)); cross-chain
// fan-out coalescing is a separate, name-keyed constraint (not yet built).
var fullsatNaming = os.Getenv("TRACE_RECON_FULLSAT_CGPB") == "1"

// EnableFullsatCGPB turns on named-synthetic identity injection (--fullsat-cgpb).
func EnableFullsatCGPB() { fullsatNaming = true }

// fullsatDiag (TRACE_RECON_FSDIAG=1) prints per-cluster join-node / vote counts,
// to check whether the HA mechanism is engaging at all.
var fullsatDiag = os.Getenv("TRACE_RECON_FSDIAG") == "1"


// fullsatHA gates the HA naming source (upstream fan-outs witnessed by the hash
// array) on top of the always-on direct-ParentID source. Default on; set
// TRACE_RECON_FULLSAT_HA=0 for the direct-parent-only ablation (the "PB" rung).
var fullsatHA = os.Getenv("TRACE_RECON_FULLSAT_HA") != "0"

// fullsatVote is the per-agreement consensus reward (objective weight on each
// confirmer vote). Sized above raw likelihood differences (~tens) so support
// can pull an outlier, but below the coverage band (1e6) so it never overrides
// an invariant. Tunable via TRACE_RECON_FULLSAT_VOTE.
var fullsatVote = func() int {
	if v := os.Getenv("TRACE_RECON_FULLSAT_VOTE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 1000
}()

// fsNamed is one extra named occupancy an option carries: a named dropped
// ancestor (HA-witnessed) at a chain level, modeled like rsvSpanID — same id
// coexists (coalesce), different id conflicts (exclusivity).
type fsNamed struct {
	level int32
	id    uint64
}
var fullsatSeed int64 = 42
var fullsatDetTime = func() float64 {
	if v := os.Getenv("TRACE_RECON_FULLSAT_DTIME"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return 8.0
}()

// EnableFullsatPB turns on the full-SAT per-cluster solver with a fixed seed
// (for machine-independent results) and an optional deterministic-time override.
// TRACE_RECON_FULLSAT_SEED overrides the CP-SAT seed independently of the drop
// seed (for tie-selection / seed-variance diagnostics).
func EnableFullsatPB(seed int64, detTime float64) {
	fullsatEnabled = true
	fullsatSeed = seed
	if v := os.Getenv("TRACE_RECON_FULLSAT_SEED"); v != "" {
		if s, err := strconv.ParseInt(v, 10, 64); err == nil {
			fullsatSeed = s
		}
	}
	if detTime > 0 {
		fullsatDetTime = detTime
	}
}

// fsOpt is one feasible option of a cluster item, in the same terms the C/I/O
// block carries: gain (coverage-bonus-included), displacement cost against the
// bare ledger, an optional reservation level (-1 = none), and the occupied
// (level, spanID) pairs.
type fsOpt struct {
	gain, cost int
	rsvID      int32
	rsvSpanID  uint64   // --fullsat-cgpb: named identity (ParentID) of the dropped-parent
	                     // slot. When set, that slot is modeled as an OCCUPANCY by this id
	                     // (same id coexists => fan-out coalescing; different id conflicts =>
	                     // exclusivity) rather than a bare reservation.
	levels      []int32   // dense level ids this option occupies
	spanIDs     []uint64  // span id placed at each level (same id => may coexist)
	named       []fsNamed // HA-named dropped ancestors on this option's route
	chainAnchor uint64    // the surviving anchor this option's chain attaches to
}

// fsItem is one cluster item (orphan / open-end / thread / reseat).
type fsItem struct {
	skipPen  int
	opts     []fsOpt
	carries  []uint64 // HA-named fan-out ids this fragment CARRIES (certain descendant)
	confirms []uint64 // named fan-out ids this fragment's BLOOM confirms (candidate descendant)
}

// solveClusterFullsat emits items as a declarative CP-SAT model and solves it,
// returning per item the chosen option index (-1 = skip), matching the shape
// solveCluster/cpsat return. ok=false when the shim is absent or finds no
// solution. Constraint emission is deterministic (levels sorted) so a fixed
// seed yields machine-independent results.
func solveClusterFullsat(items []fsItem) (assign []int, ok bool) {
	if fullsatSolveFn == nil {
		return nil, false
	}
	// dense var index per (item, option)
	nv := 0
	varOf := make([][]int, len(items))
	for i := range items {
		varOf[i] = make([]int, len(items[i].opts))
		for j := range items[i].opts {
			varOf[i][j] = nv
			nv++
		}
	}
	if nv == 0 {
		return make([]int, len(items)), true // nothing to assign (all -1)
	}

	// Carrier-hard join nodes: a fragment that CARRIES N's HA entry is a certain
	// descendant of N, so all carriers of N must route through N's SINGLE upstream
	// anchor. Model N's anchor as ExactlyOne(nAnc[N][·]) over the distinct chain
	// anchors its carriers' options offer, and tie each carrier's placement to it
	// (place -> nAnc[N][a]). Two carriers picking options with different anchors
	// then both pin nAnc[N], which ExactlyOne forbids — forcing them onto a common
	// anchor (group-consensus, decided by the MAP objective). Only fires for N with
	// >=2 carriers (a lone carrier is trivially consistent).
	nAncVar := make(map[uint64]map[uint64]int) // N -> anchor id -> var index
	var nList []uint64                          // deterministic N order
	// Confirmer votes: vote[F,N,a] = 1 iff F routes to anchor a AND nAnc[N]=a.
	// Rewarded (+fullsatVote) so the solver prefers the anchor the most attachers
	// agree on — the group-consensus, as a soft objective term (carriers are
	// hard-tied separately; confirmers vote).
	type voteRec struct {
		v      int   // vote var
		places []int // F's option vars whose chain anchor == a
		nAncV  int   // nAnc[N][a]
	}
	var votes []voteRec
	if fullsatHA {
		carrierCnt := make(map[uint64]int)  // hard descendants (HA emitters)
		attacherCnt := make(map[uint64]int) // carriers + bloom-confirmers
		anchorsOf := make(map[uint64]map[uint64]bool)
		for i := range items {
			cseen := make(map[uint64]bool)
			aseen := make(map[uint64]bool)
			note := func(n uint64) {
				if anchorsOf[n] == nil {
					anchorsOf[n] = make(map[uint64]bool)
				}
				for j := range items[i].opts {
					anchorsOf[n][items[i].opts[j].chainAnchor] = true
				}
				if !aseen[n] {
					aseen[n] = true
					attacherCnt[n]++
				}
			}
			for _, n := range items[i].carries {
				if !cseen[n] {
					cseen[n] = true
					carrierCnt[n]++
				}
				note(n)
			}
			for _, n := range items[i].confirms {
				note(n)
			}
		}
		// A join node forms with a hard carrier (it anchors N) OR >=2 attachers.
		for n, ac := range attacherCnt {
			if carrierCnt[n] >= 1 || ac >= 2 {
				nList = append(nList, n)
			}
		}
		sort.Slice(nList, func(a, b int) bool { return nList[a] < nList[b] })
		for _, n := range nList {
			as := make([]uint64, 0, len(anchorsOf[n]))
			for a := range anchorsOf[n] {
				as = append(as, a)
			}
			sort.Slice(as, func(x, y int) bool { return as[x] < as[y] })
			nAncVar[n] = make(map[uint64]int, len(as))
			for _, a := range as {
				nAncVar[n][a] = nv
				nv++
			}
		}
		// Allocate a vote var per (confirmer item, formed N it confirms but does
		// NOT carry, anchor a it has an option for).
		for i := range items {
			carried := make(map[uint64]bool)
			for _, n := range items[i].carries {
				carried[n] = true
			}
			for _, n := range items[i].confirms {
				if carried[n] {
					continue // carriers are hard-tied below, not soft voters
				}
				av, ok := nAncVar[n]
				if !ok {
					continue
				}
				byA := make(map[uint64][]int)
				for j := range items[i].opts {
					a := items[i].opts[j].chainAnchor
					if _, ok := av[a]; ok {
						byA[a] = append(byA[a], varOf[i][j])
					}
				}
				as := make([]uint64, 0, len(byA))
				for a := range byA {
					as = append(as, a)
				}
				sort.Slice(as, func(x, y int) bool { return as[x] < as[y] })
				for _, a := range as {
					votes = append(votes, voteRec{v: nv, places: byA[a], nAncV: av[a]})
					nv++
				}
			}
		}
	}

	if fullsatDiag && (len(nList) > 0 || len(votes) > 0) {
		fmt.Fprintf(os.Stderr, "FSVOTE items=%d nodes=%d votes=%d\n", len(items), len(nList), len(votes))
	}

	var b strings.Builder
	b.WriteString("MODEL ")
	b.WriteString(strconv.Itoa(nv))
	b.WriteByte('\n')

	// Carrier-hard: ExactlyOne anchor per N, and tie each carrier's placement to it.
	for _, n := range nList {
		b.WriteString("EO ")
		b.WriteString(strconv.Itoa(len(nAncVar[n])))
		// deterministic anchor order
		as := make([]uint64, 0, len(nAncVar[n]))
		for a := range nAncVar[n] {
			as = append(as, a)
		}
		sort.Slice(as, func(x, y int) bool { return as[x] < as[y] })
		for _, a := range as {
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(nAncVar[n][a] + 1))
		}
		b.WriteByte('\n')
	}
	for i := range items {
		tied := make(map[uint64]bool)
		tie := func(n uint64) {
			if tied[n] {
				return
			}
			tied[n] = true
			av, ok := nAncVar[n]
			if !ok {
				return
			}
			// If this item picks an option whose chain anchor is a candidate for
			// N, that pins N's single upstream anchor — so every attacher of N is
			// forced onto one shared anchor (carrier hard, confirmer competing).
			for j := range items[i].opts {
				if v, ok := av[items[i].opts[j].chainAnchor]; ok {
					b.WriteString("IMP ")
					b.WriteString(strconv.Itoa(varOf[i][j] + 1)) // place -> nAnc
					b.WriteByte(' ')
					b.WriteString(strconv.Itoa(v + 1))
					b.WriteByte('\n')
				}
			}
		}
		for _, n := range items[i].carries {
			tie(n) // carriers: HARD (certain descendant -> must share N's anchor)
		}
	}
	// Confirmer votes: vote -> (F routes to a) AND vote -> nAnc[N]=a. The reward on
	// vote (in OBJ) makes the solver set vote=1 whenever F agrees with N's chosen
	// anchor, so total reward = support for that anchor — the soft, competing vote.
	for _, vt := range votes {
		b.WriteString("ALO ") // ¬vote ∨ (some option of F at anchor a)
		b.WriteString(strconv.Itoa(1 + len(vt.places)))
		b.WriteByte(' ')
		b.WriteString(strconv.Itoa(-(vt.v + 1)))
		for _, p := range vt.places {
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(p + 1))
		}
		b.WriteByte('\n')
		b.WriteString("IMP ") // vote -> nAnc[N][a]
		b.WriteString(strconv.Itoa(vt.v + 1))
		b.WriteByte(' ')
		b.WriteString(strconv.Itoa(vt.nAncV + 1))
		b.WriteByte('\n')
	}

	// AtMostOne per item (skip = no option selected).
	for i := range items {
		if len(items[i].opts) == 0 {
			continue
		}
		b.WriteString("AMO ")
		b.WriteString(strconv.Itoa(len(items[i].opts)))
		for j := range items[i].opts {
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(varOf[i][j] + 1)) // positive literal
		}
		b.WriteByte('\n')
	}

	// Objective: gain - cost + skipPenalty per option var, plus +fullsatVote per
	// confirmer vote (consensus reward). Count the TERMS emitted (place + vote) —
	// NOT nv (which also counts the auxiliary nAnc/vote-as-var indices that don't
	// all appear here); a wrong count makes the shim read past the line.
	var objTerms strings.Builder
	objN := 0
	addTerm := func(w, v int) {
		objTerms.WriteByte(' ')
		objTerms.WriteString(strconv.Itoa(w))
		objTerms.WriteByte(' ')
		objTerms.WriteString(strconv.Itoa(v))
		objN++
	}
	for i := range items {
		for j := range items[i].opts {
			addTerm(items[i].opts[j].gain-items[i].opts[j].cost+items[i].skipPen, varOf[i][j])
		}
	}
	for _, vt := range votes {
		addTerm(fullsatVote, vt.v)
	}
	b.WriteString("OBJ ")
	b.WriteString(strconv.Itoa(objN))
	b.WriteString(objTerms.String())
	b.WriteByte('\n')

	// Occupancy / reservation conflicts, keyed by level. Two options writing the
	// same level with DIFFERENT span ids are mutually exclusive; same id may
	// coexist (shared ancestor). A reservation conflicts with any other item's
	// occupant of that level.
	type occ struct {
		v    int
		span uint64
		item int
	}
	type rsv struct{ v, item int }
	occAt := make(map[int32][]occ)
	rsvAt := make(map[int32][]rsv)
	for i := range items {
		for j := range items[i].opts {
			o := &items[i].opts[j]
			for k := range o.levels {
				L := o.levels[k]
				occAt[L] = append(occAt[L], occ{varOf[i][j], o.spanIDs[k], i})
			}
			for k := range o.named {
				occAt[o.named[k].level] = append(occAt[o.named[k].level], occ{varOf[i][j], o.named[k].id, i})
			}
			if o.rsvID >= 0 {
				if o.rsvSpanID != 0 {
					// named dropped-parent slot: model as an OCCUPANCY by the named
					// id — same id coexists (fan-out coalescing), different id
					// conflicts (exclusivity) — rather than a bare reservation.
					occAt[o.rsvID] = append(occAt[o.rsvID], occ{varOf[i][j], o.rsvSpanID, i})
				} else {
					rsvAt[o.rsvID] = append(rsvAt[o.rsvID], rsv{varOf[i][j], i})
				}
			}
		}
	}
	levels := make([]int32, 0, len(occAt))
	for L := range occAt {
		levels = append(levels, L)
	}
	sort.Slice(levels, func(a, c int) bool { return levels[a] < levels[c] })
	clause := func(va, vb int) {
		b.WriteString("ALO 2 ")
		b.WriteString(strconv.Itoa(-(va + 1)))
		b.WriteByte(' ')
		b.WriteString(strconv.Itoa(-(vb + 1)))
		b.WriteByte('\n')
	}
	for _, L := range levels {
		lst := occAt[L]
		for a := 0; a < len(lst); a++ {
			for c := a + 1; c < len(lst); c++ {
				if lst[a].item == lst[c].item || lst[a].span == lst[c].span {
					continue
				}
				clause(lst[a].v, lst[c].v)
			}
		}
		for _, oc := range lst {
			for _, rs := range rsvAt[L] {
				if oc.item == rs.item {
					continue
				}
				clause(oc.v, rs.v)
			}
		}
	}

	asg, _, status := fullsatSolveFn(b.String(), nv, fullsatDetTime, fullsatSeed)
	if status == 0 {
		return nil, false
	}
	assign = make([]int, len(items))
	for i := range items {
		assign[i] = -1
		for j := range items[i].opts {
			if asg[varOf[i][j]] {
				assign[i] = j
				break
			}
		}
	}
	return assign, true
}
