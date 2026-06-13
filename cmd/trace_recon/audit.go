package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"

	"bridges/recon"
)

// Fault-injection audit of the scoring subsystem (--score-audit).
//
// The danger direction for a scorer is leniency: an engine bug just makes
// reported numbers worse, but a lenient scorer silently reports
// better-than-actual results. This audit measures the scorer's detection
// sensitivity directly: for each trace it copies the reconstruction
// result, injects mutations whose correct grading is computed from an
// INDEPENDENT truth walk (reimplemented here, not shared with the
// scorer), re-scores with the real ScorePCR, and requires the counters
// to move by exactly the predicted delta. Any disagreement is a miss.
//
// Mutation classes:
//
//	lateral    anchor moved to a surviving non-ancestor elsewhere in the
//	           trace            -> exact-1, ancestor-1, misattached+1
//	samedepth  anchor moved to a surviving non-ancestor at the SAME depth
//	           as the true anchor (the near-miss a lenient scorer would
//	           wave through)    -> exact-1, ancestor-1, misattached+1
//	shallow    anchor moved UP to a surviving true ancestor above the
//	           nearest one (the "benign" class)
//	                            -> exact-1, ancestor+-0, misattached+1
//	descendant anchor moved to a descendant of the fragment root
//	                            -> exact-1, ancestor-1, misattached+1
//	delete     a placed fragment moved to the unanchored list
//	                            -> fragments-lost+1, spans-lost+size
type auditStats struct {
	injected map[string]int
	detected map[string]int
}

func newAuditStats() *auditStats {
	return &auditStats{injected: map[string]int{}, detected: map[string]int{}}
}

func (a *auditStats) add(b *auditStats) {
	for k, v := range b.injected {
		a.injected[k] += v
	}
	for k, v := range b.detected {
		a.detected[k] += v
	}
}

func (a *auditStats) report() {
	fmt.Fprintln(os.Stderr, "SCORE-AUDIT sensitivity (injected -> detected with exact predicted delta):")
	keys := make([]string, 0, len(a.injected))
	for k := range a.injected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		inj, det := a.injected[k], a.detected[k]
		rate := 100.0
		if inj > 0 {
			rate = 100 * float64(det) / float64(inj)
		}
		fmt.Fprintf(os.Stderr, "  %-10s injected=%-8d detected=%-8d sensitivity=%.4f%%\n", k, inj, det, rate)
	}
}

// auditScores runs the per-trace audit. Deterministic per trace (seeded
// by trace ID).
func auditScores(res recon.Result, truth []recon.TruthSpan, dropped map[uint64]struct{}, seed int64) *auditStats {
	st := newAuditStats()
	if len(res.Bridges) == 0 {
		return st
	}
	rng := rand.New(rand.NewSource(seed))

	// Independent truth walk: parent/depth maps, reduced-set lost
	// computation, ancestor predicate, nearest surviving ancestor.
	parent := make(map[uint64]uint64, len(truth))
	depth := make(map[uint64]int, len(truth))
	for _, t := range truth {
		parent[t.SpanID] = t.ParentID
		depth[t.SpanID] = t.Depth
	}
	surv := func(id uint64) bool {
		_, gone := dropped[id]
		return !gone
	}
	// Lost set: spans whose surviving-root walk lands on an unanchored
	// fragment root (mirrors the reduced-set definition from first
	// principles).
	unanch := make(map[uint64]bool, len(res.Unanchored))
	for _, id := range res.Unanchored {
		unanch[id] = true
	}
	rootOf := func(id uint64) uint64 {
		r := id
		for {
			p := parent[r]
			if p == 0 || !surv(p) {
				return r
			}
			r = p
		}
	}
	lost := make(map[uint64]bool)
	fragSize := make(map[uint64]int)
	for _, t := range truth {
		if !surv(t.SpanID) {
			continue
		}
		r := rootOf(t.SpanID)
		fragSize[r]++
		if unanch[r] {
			lost[t.SpanID] = true
		}
	}
	isAncestor := func(a, o uint64) bool {
		for p := parent[o]; p != 0; p = parent[p] {
			if p == a {
				return true
			}
		}
		return false
	}
	nearestSurv := func(o uint64) uint64 {
		for p := parent[o]; p != 0; p = parent[p] {
			if surv(p) && !lost[p] {
				return p
			}
		}
		return 0
	}

	// Sample a bridge that is definitionally exact (per OUR walk), so
	// every mutation's grade transition is known a priori.
	var exactIdx []int
	for i, b := range res.Bridges {
		if b.AnchorID != 0 && b.AnchorID == nearestSurv(b.OrphanID) {
			exactIdx = append(exactIdx, i)
		}
	}
	if len(exactIdx) == 0 {
		return st
	}

	// Survivor list for mutation-target sampling.
	var survivors []uint64
	for _, t := range truth {
		if surv(t.SpanID) && !lost[t.SpanID] {
			survivors = append(survivors, t.SpanID)
		}
	}

	cloneRes := func() recon.Result {
		c := res
		c.Bridges = append([]recon.Bridge(nil), res.Bridges...)
		c.Unanchored = append([]uint64(nil), res.Unanchored...)
		return c
	}
	base := recon.ScorePCR(cloneRes(), truth, dropped)
	score := func(mut recon.Result) recon.Score {
		return recon.ScorePCR(mut, truth, dropped)
	}

	// Anchor-mutation classes: expected deltas on (AnchorCorrect,
	// AnchorAncestor, Misattached).
	type anchorClass struct {
		name       string
		pick       func(b recon.Bridge) (uint64, bool)
		dC, dA, dW int
	}
	classes := []anchorClass{
		{"lateral", func(b recon.Bridge) (uint64, bool) {
			for tries := 0; tries < 64; tries++ {
				cand := survivors[rng.Intn(len(survivors))]
				if cand != b.OrphanID && cand != b.AnchorID &&
					!isAncestor(cand, b.OrphanID) && !isAncestor(b.OrphanID, cand) {
					return cand, true
				}
			}
			return 0, false
		}, -1, -1, +1},
		{"samedepth", func(b recon.Bridge) (uint64, bool) {
			d := depth[b.AnchorID]
			var pool []uint64
			for _, s := range survivors {
				if depth[s] == d && s != b.AnchorID && !isAncestor(s, b.OrphanID) {
					pool = append(pool, s)
				}
			}
			if len(pool) == 0 {
				return 0, false
			}
			return pool[rng.Intn(len(pool))], true
		}, -1, -1, +1},
		{"shallow", func(b recon.Bridge) (uint64, bool) {
			// first surviving non-lost TRUE ancestor strictly above the
			// nearest one
			for p := parent[b.AnchorID]; p != 0; p = parent[p] {
				if surv(p) && !lost[p] {
					return p, true
				}
			}
			return 0, false
		}, -1, 0, +1},
		{"descendant", func(b recon.Bridge) (uint64, bool) {
			var pool []uint64
			for _, s := range survivors {
				if isAncestor(b.OrphanID, s) {
					pool = append(pool, s)
				}
			}
			if len(pool) == 0 {
				return 0, false
			}
			return pool[rng.Intn(len(pool))], true
		}, -1, -1, +1},
	}

	for _, cl := range classes {
		bi := exactIdx[rng.Intn(len(exactIdx))]
		target, ok := cl.pick(res.Bridges[bi])
		if !ok {
			continue
		}
		mut := cloneRes()
		mut.Bridges[bi].AnchorID = target
		st.injected[cl.name]++
		got := score(mut)
		if got.AnchorCorrect-base.AnchorCorrect == cl.dC &&
			got.AnchorAncestor-base.AnchorAncestor == cl.dA &&
			got.Misattached-base.Misattached == cl.dW {
			st.detected[cl.name]++
		}
	}

	// delete: un-place a bridged fragment; lost accounting must grow by
	// exactly its fragment size.
	{
		bi := exactIdx[rng.Intn(len(exactIdx))]
		o := res.Bridges[bi].OrphanID
		mut := cloneRes()
		mut.Bridges = append(mut.Bridges[:bi:bi], mut.Bridges[bi+1:]...)
		mut.Unanchored = append(mut.Unanchored, o)
		st.injected["delete"]++
		got := score(mut)
		if got.FragmentsLost-base.FragmentsLost == 1 &&
			got.SpansLost-base.SpansLost == fragSize[o] {
			st.detected["delete"]++
		}
	}

	// MULTI-FAULT COCKTAIL: 1-8 mixed faults at once. Faults interact —
	// deletions change the reduced set and thereby other bridges' correct
	// grades — so the oracle is no longer a delta sum but a full
	// independent re-grading of the mutated result (reference grader
	// below). ScorePCR must agree with it exactly on the whole vector;
	// this catches interaction and cancellation bugs that single-fault
	// sensitivity cannot.
	{
		mut := cloneRes()
		k := 1 + rng.Intn(8)
		usedBridge := make(map[int]bool)
		faults := 0
		for f := 0; f < k && len(mut.Bridges) > 0; f++ {
			switch rng.Intn(5) {
			case 0, 1, 2, 3: // anchor mutation on an unused bridge
				bi := rng.Intn(len(mut.Bridges))
				if usedBridge[bi] {
					continue
				}
				b := mut.Bridges[bi]
				var target uint64
				var ok bool
				switch rng.Intn(4) {
				case 0:
					for tries := 0; tries < 32 && !ok; tries++ {
						cand := survivors[rng.Intn(len(survivors))]
						if cand != b.OrphanID && cand != b.AnchorID {
							target, ok = cand, true
						}
					}
				case 1: // ancestor above (may be exact's parent: any ancestor)
					for p := parent[b.OrphanID]; p != 0; p = parent[p] {
						if surv(p) {
							target, ok = p, true // may equal current: fine
							break
						}
					}
				case 2: // descendant
					for tries := 0; tries < 32 && !ok; tries++ {
						cand := survivors[rng.Intn(len(survivors))]
						if isAncestor(b.OrphanID, cand) {
							target, ok = cand, true
						}
					}
				case 3: // root span
					target, ok = trapRootOf(parent, b.OrphanID), true
				}
				if ok && target != 0 {
					mut.Bridges[bi].AnchorID = target
					usedBridge[bi] = true
					faults++
				}
			case 4: // delete a bridged fragment
				bi := rng.Intn(len(mut.Bridges))
				if usedBridge[bi] {
					continue
				}
				o := mut.Bridges[bi].OrphanID
				mut.Bridges = append(mut.Bridges[:bi:bi], mut.Bridges[bi+1:]...)
				mut.Unanchored = append(mut.Unanchored, o)
				// bridge indices shifted: old marks are invalid; clearing
				// them may let a bridge mutate twice, which is harmless —
				// the oracle grades the final state, not the fault list.
				for j := range usedBridge {
					delete(usedBridge, j)
				}
				faults++
			}
		}
		if faults > 0 {
			st.injected["multi"]++
			got := score(mut)
			want := refGrade(mut, truth, dropped)
			if got.AnchorCorrect == want.c && got.AnchorAncestor == want.a &&
				got.Misattached == want.w && got.FragmentsLost == want.fl &&
				got.SpansLost == want.sl {
				st.detected["multi"]++
			}
		}
	}
	return st
}

// trapRootOf walks truth parents to the root span.
func trapRootOf(parent map[uint64]uint64, id uint64) uint64 {
	r := id
	for parent[r] != 0 {
		r = parent[r]
	}
	return r
}

type refVector struct{ c, a, w, fl, sl int }

// refGrade independently grades a (possibly mutated) Result from first
// principles: reduced-set lost from the result's OWN unanchored list,
// then per bridge exact/ancestor/misattached against the truth walk.
// Shares no code with ScorePCR.
func refGrade(res recon.Result, truth []recon.TruthSpan, dropped map[uint64]struct{}) refVector {
	parent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		parent[t.SpanID] = t.ParentID
	}
	surv := func(id uint64) bool {
		_, gone := dropped[id]
		return !gone
	}
	unanch := make(map[uint64]bool, len(res.Unanchored))
	for _, id := range res.Unanchored {
		unanch[id] = true
	}
	rootOf := func(id uint64) uint64 {
		r := id
		for {
			p := parent[r]
			if p == 0 || !surv(p) {
				return r
			}
			r = p
		}
	}
	lost := make(map[uint64]bool)
	var v refVector
	for _, t := range truth {
		if surv(t.SpanID) && unanch[rootOf(t.SpanID)] {
			lost[t.SpanID] = true
			v.sl++
		}
	}
	v.fl = len(res.Unanchored)
	isAncestor := func(a, o uint64) bool {
		for p := parent[o]; p != 0; p = parent[p] {
			if p == a {
				return true
			}
		}
		return false
	}
	nearest := func(o uint64) uint64 {
		for p := parent[o]; p != 0; p = parent[p] {
			if surv(p) && !lost[p] {
				return p
			}
		}
		return 0
	}
	for _, b := range res.Bridges {
		if b.AnchorID == nearest(b.OrphanID) && b.AnchorID != 0 {
			v.c++
		} else {
			v.w++
		}
		if isAncestor(b.AnchorID, b.OrphanID) {
			v.a++
		}
	}
	return v
}
