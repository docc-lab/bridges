// One-off corpus scan: per-trace max fanout (children per parent) percentiles.
package main

import (
	"fmt"
	"os"
	"sort"

	"bridges/corpus"
)

type acc struct {
	counts map[uint64]int32 // parentID -> child count
	seen   int              // start events seen
}

func main() {
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

	state := make(map[uint64]*acc)
	var maxFanouts []int32
	finalize := func(a *acc) {
		var mx int32
		for _, c := range a.counts {
			if c > mx {
				mx = c
			}
		}
		maxFanouts = append(maxFanouts, mx)
	}

	n := 0
	for {
		ev, err := er.Next()
		if err != nil {
			break // io.EOF
		}
		n++
		if ev.Kind != corpus.KindStart {
			continue
		}
		a := state[ev.TraceID]
		if a == nil {
			a = &acc{counts: make(map[uint64]int32)}
			state[ev.TraceID] = a
		}
		if ev.ParentID != 0 {
			a.counts[ev.ParentID]++
		}
		a.seen++
		if a.seen == want[ev.TraceID] {
			finalize(a)
			delete(state, ev.TraceID)
		}
	}
	for _, a := range state { // safety: traces that never hit their count
		finalize(a)
	}
	fmt.Fprintf(os.Stderr, "events: %d, leftover unfinalized traces: %d\n", n, len(state))

	sort.Slice(maxFanouts, func(i, j int) bool { return maxFanouts[i] < maxFanouts[j] })
	N := len(maxFanouts)
	var sum float64
	for _, v := range maxFanouts {
		sum += float64(v)
	}
	q := func(p float64) int32 { return maxFanouts[int(p*float64(N-1))] }
	fmt.Printf("traces: %d\navg: %.2f\n", N, sum/float64(N))
	for _, p := range []struct {
		name string
		v    float64
	}{{"p1", 0.01}, {"p25", 0.25}, {"p50", 0.50}, {"p75", 0.75}, {"p99", 0.99}} {
		fmt.Printf("%s: %d\n", p.name, q(p.v))
	}
	fmt.Printf("max: %d\n", maxFanouts[N-1])
}
