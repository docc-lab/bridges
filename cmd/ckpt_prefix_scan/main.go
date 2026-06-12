// One-off corpus scan: minimal leading-byte prefix of checkpoint span IDs
// (big-endian, as packed by BigEndian8) needed to disambiguate all checkpoint
// spans within each trace, at a given CPD. Reports per-trace percentiles.
package main

import (
	"flag"
	"fmt"
	"math/bits"
	"os"
	"sort"

	"bridges/corpus"
)

type acc struct {
	ids     []uint64 // checkpoint span IDs
	depths  []uint16 // parallel to ids (used by --per-depth)
	parents map[uint64]struct{} // every span ID referenced as a parent (used by --exclude-leaves)
	seen    int
}

func main() {
	cpd := flag.Int("cpd", 2, "checkpoint distance")
	perDepth := flag.Bool("per-depth", false, "disambiguate only among checkpoint spans at the same depth (the payload's depth varint already separates levels); per-trace value is the max over depth groups")
	exclLeaves := flag.Bool("exclude-leaves", false, "exclude leaf checkpoints: a leaf's ID is never inherited as a bridge root, so it never needs disambiguating")
	flag.Parse()

	meta, err := corpus.ReadMeta("/mydata/uber/corpus_full/meta.bin")
	if err != nil {
		panic(err)
	}
	want := make(map[uint64]int, len(meta.TraceOrder))
	for i, tid := range meta.TraceOrder {
		want[tid] = int(meta.SpanCounts[i])
	}

	er, err := corpus.OpenEvents("/mydata/uber/corpus_full/events.bin")
	if err != nil {
		panic(err)
	}
	defer er.Close()

	var needed []int // per-trace minimal prefix bytes
	var forcing []int // size of the depth group that forced it (per-depth mode)
	nCkpt := make([]int, 0, len(want))
	// neededFor: minimal prefix bytes so all ids in the group are distinct.
	neededFor := func(ids []uint64) int {
		if len(ids) < 2 {
			return 0 // nothing to disambiguate
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		// Required bytes = longest common leading-byte run between any two
		// IDs + 1; for sorted IDs the max is attained by an adjacent pair.
		maxCommon := 0
		for i := 1; i < len(ids); i++ {
			c := bits.LeadingZeros64(ids[i-1]^ids[i]) / 8
			if c > maxCommon {
				maxCommon = c
			}
		}
		return maxCommon + 1
	}
	finalize := func(a *acc) {
		if *exclLeaves {
			ids := a.ids[:0]
			depths := a.depths[:0]
			for i, id := range a.ids {
				if _, ok := a.parents[id]; ok {
					ids = append(ids, id)
					depths = append(depths, a.depths[i])
				}
			}
			a.ids, a.depths = ids, depths
		}
		nCkpt = append(nCkpt, len(a.ids))
		if !*perDepth {
			needed = append(needed, neededFor(a.ids))
			return
		}
		// Per-depth: the depth varint in the payload already disambiguates
		// across levels; the ID prefix only breaks ties within one.
		byDepth := make(map[uint16][]uint64)
		for i, id := range a.ids {
			byDepth[a.depths[i]] = append(byDepth[a.depths[i]], id)
		}
		mx, fg := 0, 0
		for _, ids := range byDepth {
			if v := neededFor(ids); v > mx || (v == mx && len(ids) > fg) {
				mx, fg = v, len(ids)
			}
		}
		needed = append(needed, mx)
		forcing = append(forcing, fg)
	}

	state := make(map[uint64]*acc)
	for {
		ev, err := er.Next()
		if err != nil {
			break
		}
		if ev.Kind != corpus.KindStart {
			continue
		}
		a := state[ev.TraceID]
		if a == nil {
			a = &acc{}
			if *exclLeaves {
				a.parents = make(map[uint64]struct{})
			}
			state[ev.TraceID] = a
		}
		if int(ev.Depth)%*cpd == 0 {
			a.ids = append(a.ids, ev.SpanID)
			a.depths = append(a.depths, ev.Depth)
		}
		if *exclLeaves && ev.ParentID != 0 {
			a.parents[ev.ParentID] = struct{}{}
		}
		a.seen++
		if a.seen == want[ev.TraceID] {
			finalize(a)
			delete(state, ev.TraceID)
		}
	}
	for _, a := range state {
		finalize(a)
	}
	fmt.Fprintf(os.Stderr, "leftover unfinalized traces: %d\n", len(state))

	type pair struct{ need, force int }
	pairs := make([]pair, len(needed))
	for i := range needed {
		f := 0
		if i < len(forcing) {
			f = forcing[i]
		}
		pairs[i] = pair{needed[i], f}
	}
	sort.Ints(needed)
	N := len(needed)
	var sum, sumCk float64
	for _, v := range needed {
		sum += float64(v)
	}
	for _, v := range nCkpt {
		sumCk += float64(v)
	}
	q := func(p float64) int { return needed[int(p*float64(N-1))] }
	fmt.Printf("cpd=%d per-depth=%v exclude-leaves=%v  traces: %d  avg ckpt spans/trace: %.1f\n", *cpd, *perDepth, *exclLeaves, N, sumCk/float64(N))
	fmt.Printf("avg needed bytes: %.3f\n", sum/float64(N))
	for _, p := range []struct {
		name string
		v    float64
	}{{"p1", 0.01}, {"p25", 0.25}, {"p50", 0.50}, {"p75", 0.75}, {"p99", 0.99}} {
		fmt.Printf("%s: %d\n", p.name, q(p.v))
	}
	fmt.Printf("max: %d\n", needed[N-1])
	// Full histogram — cheap and more informative than quantiles alone.
	hist := map[int]int{}
	for _, v := range needed {
		hist[v]++
	}
	for b := 0; b <= 8; b++ {
		if hist[b] > 0 {
			fmt.Printf("  %d bytes: %d traces (%.2f%%)\n", b, hist[b], 100*float64(hist[b])/float64(N))
		}
	}
	// Per-bucket stats of the forcing group's size (per-depth mode only):
	// is the byte requirement driven by wide same-depth levels?
	if len(forcing) > 0 {
		byBucket := map[int][]int{}
		for _, p := range pairs {
			byBucket[p.need] = append(byBucket[p.need], p.force)
		}
		fmt.Println("forcing depth-group size by bucket (p50 / p90 / max):")
		for b := 0; b <= 8; b++ {
			g := byBucket[b]
			if len(g) == 0 {
				continue
			}
			sort.Ints(g)
			fmt.Printf("  %d bytes: n=%d  group p50=%d p90=%d max=%d\n",
				b, len(g), g[len(g)/2], g[len(g)*9/10], g[len(g)-1])
		}
	}
}
