package recon

import (
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
func EnableFullsatPB(seed int64, detTime float64) {
	fullsatEnabled = true
	fullsatSeed = seed
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
	levels     []int32  // dense level ids this option occupies
	spanIDs    []uint64 // span id placed at each level (same id => may coexist)
}

// fsItem is one cluster item (orphan / open-end / thread / reseat).
type fsItem struct {
	skipPen int
	opts    []fsOpt
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

	var b strings.Builder
	b.WriteString("MODEL ")
	b.WriteString(strconv.Itoa(nv))
	b.WriteByte('\n')

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

	// Objective: gain - cost + skipPenalty per option var. The skip-penalty
	// constant (subtracted in the C/I/O shim) only shifts the objective, not the
	// argmax, so we fold it per-option and drop the constant.
	b.WriteString("OBJ ")
	b.WriteString(strconv.Itoa(nv))
	for i := range items {
		for j := range items[i].opts {
			w := items[i].opts[j].gain - items[i].opts[j].cost + items[i].skipPen
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(w))
			b.WriteByte(' ')
			b.WriteString(strconv.Itoa(varOf[i][j]))
		}
	}
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
			if o.rsvID >= 0 {
				rsvAt[o.rsvID] = append(rsvAt[o.rsvID], rsv{varOf[i][j], i})
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
