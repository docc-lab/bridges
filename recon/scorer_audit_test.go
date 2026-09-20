package recon

import (
	"fmt"
	"path/filepath"
	"testing"
)

// Adversarially corrupt a correct reconstruction in the four ways an
// over-permissive scorer would wave through, and require each to be counted
// wrong. These are exactly the failure modes the certain-root fallback could
// in principle hide behind.
func TestStrictScorerRejectsBogusAnonymousChains(t *testing.T) {
	paths, _ := filepath.Glob("testdata/hardevidence_*.gob")
	for _, path := range paths {
		traces, err := LoadDumpedTraces(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range traces {
			res := ReconstructCGP0(tr.Survivors, tr.Cfg)
			base := ScoreCGP2Strict(res, tr.Survivors, tr.Truth, tr.DroppedSet())

			nameable := map[uint64]bool{}
			surv := map[uint64]bool{}
			for i := range tr.Survivors {
				s := &tr.Survivors[i]
				surv[s.SpanID] = true
				nameable[s.SpanID] = true
				if s.ParentID != 0 {
					nameable[s.ParentID] = true
				}
				for _, h := range s.HA {
					nameable[h.ParentID] = true
				}
			}
			// A source whose correct segment runs through >=1 anonymous node.
			var src, firstAnon, lastAnon, terminal uint64
			for s := range nameable {
				cur, ok := res.ReconParent[s]
				if !ok || !res.ReconAnon[cur] {
					continue
				}
				src, firstAnon = s, cur
				prev := cur
				for res.ReconAnon[cur] {
					prev = cur
					nxt, ok := res.ReconParent[cur]
					if !ok {
						break
					}
					cur = nxt
				}
				lastAnon, terminal = prev, cur
				break
			}
			if src == 0 {
				t.Skip("no anonymous segment in this fixture")
			}
			var other uint64
			for s := range surv {
				if s != terminal && s != src {
					other = s
					break
				}
			}

			clone := func() Result {
				r := res
				r.ReconParent = make(map[uint64]uint64, len(res.ReconParent))
				for k, v := range res.ReconParent {
					r.ReconParent[k] = v
				}
				r.ReconAnon = make(map[uint64]bool, len(res.ReconAnon))
				for k, v := range res.ReconAnon {
					r.ReconAnon[k] = v
				}
				return r
			}

			cases := []struct {
				name string
				mut  func(*Result)
			}{
				{"wrong terminal ancestor", func(r *Result) { r.ReconParent[lastAnon] = other }},
				{"named node in an unnameable slot", func(r *Result) { r.ReconAnon[firstAnon] = false }},
				{"chain too short", func(r *Result) { r.ReconParent[src] = terminal }},
				{"chain too long", func(r *Result) {
					extra := uint64(1)
					for r.ReconParent[extra] != 0 || r.ReconAnon[extra] {
						extra++
					}
					r.ReconAnon[extra] = true
					r.ReconParent[extra] = firstAnon
					r.ReconParent[src] = extra
				}},
			}
			fmt.Printf("\n%016x baseline: exact=%d anonOK=%d wrong=%d\n",
				tr.TID, base.EdgeExact, base.EdgeAnonOK, base.EdgeWrong)
			for _, c := range cases {
				r := clone()
				c.mut(&r)
				got := ScoreCGP2Strict(r, tr.Survivors, tr.Truth, tr.DroppedSet())
				status := "DETECTED"
				if got.EdgeWrong <= base.EdgeWrong {
					status = "MISSED -- SCORER IS PERMISSIVE"
					t.Errorf("%016x: %s not counted wrong (wrong %d -> %d)",
						tr.TID, c.name, base.EdgeWrong, got.EdgeWrong)
				}
				fmt.Printf("  %-34s wrong %d -> %-4d anonOK %d -> %-4d  %s\n",
					c.name, base.EdgeWrong, got.EdgeWrong, base.EdgeAnonOK, got.EdgeAnonOK, status)
			}
		}
	}
}
