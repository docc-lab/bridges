// Command archive_partition splits a trace ARCHIVE into N sub-archives whose
// loads are balanced, partitioning along TIME-DISJOINT COMPONENTS so each
// sub-archive is a self-contained slice with no cross-component temporal
// coupling. This lets cgprb / PCRS / S-Bridge reconstruction run in parallel,
// one process per sub-archive, with no interaction between them.
//
// Method:
//  1. Pass 1 — stream the archive, record each trace's interval
//     [min StartTS, max EndTS] and span count.
//  2. Merge overlapping intervals into time-disjoint components (sorted by
//     start; a gap = a clean cut point).
//  3. Greedily bin-pack whole components into N partitions by span count
//     (largest-first onto the least-loaded partition) — keeping every
//     component intact so intra-component cross-trace state is preserved.
//  4. Pass 2 — stream again, route each trace block to its partition's writer.
//
// Each sub-archive reuses the original meta.bin (service names); archive_to_corpus
// rebuilds per-partition trace order from the traces it actually contains.
//
//	archive_partition --archive corpus.arc --out-prefix DIR/part --partitions 32
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

	"bridges/corpus"
)

func main() {
	archivePath := flag.String("archive", "", "input archive (corpus.Archive)")
	outPrefix := flag.String("out-prefix", "", "output sub-archive path prefix (writes <prefix>_NN.arc)")
	nParts := flag.Int("partitions", 32, "number of balanced partitions")
	flag.Parse()
	if *archivePath == "" || *outPrefix == "" || *nParts < 1 {
		fmt.Fprintln(os.Stderr, "usage: archive_partition --archive A.arc --out-prefix DIR/part --partitions N")
		os.Exit(2)
	}

	type tinfo struct {
		tid        uint64
		start, end int64
		spans      int32
	}
	t0 := time.Now()

	// Pass 1: per-trace interval + span count.
	r, err := corpus.OpenArchive(*archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	var infos []tinfo
	for {
		tr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "read: %v\n", err)
			os.Exit(1)
		}
		if len(tr.Spans) == 0 {
			continue
		}
		mn, mx := int64(math.MaxInt64), int64(math.MinInt64)
		for _, s := range tr.Spans {
			if s.StartTS < mn {
				mn = s.StartTS
			}
			if s.EndTS > mx {
				mx = s.EndTS
			}
		}
		infos = append(infos, tinfo{tr.TraceID, mn, mx, int32(len(tr.Spans))})
	}
	r.Close()
	fmt.Fprintf(os.Stderr, "pass1: %d traces in %s\n", len(infos), time.Since(t0).Round(time.Millisecond))

	// Components via interval merge (sorted by start).
	order := make([]int, len(infos))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return infos[order[a]].start < infos[order[b]].start })
	comp := make([]int, len(infos)) // component id per trace (indexed by infos index)
	type cinfo struct{ spans, traces int64 }
	var comps []cinfo
	if len(order) > 0 {
		cur := 0
		curEnd := infos[order[0]].end
		comps = append(comps, cinfo{})
		for _, oi := range order {
			if infos[oi].start > curEnd {
				cur++
				comps = append(comps, cinfo{})
				curEnd = infos[oi].end
			}
			if infos[oi].end > curEnd {
				curEnd = infos[oi].end
			}
			comp[oi] = cur
			comps[cur].spans += int64(infos[oi].spans)
			comps[cur].traces++
		}
	}

	// Greedy bin-pack components (largest span-load first) onto least-loaded partition.
	cidx := make([]int, len(comps))
	for i := range cidx {
		cidx[i] = i
	}
	sort.Slice(cidx, func(a, b int) bool { return comps[cidx[a]].spans > comps[cidx[b]].spans })
	partOfComp := make([]int, len(comps))
	partLoad := make([]int64, *nParts)
	partComps := make([]int, *nParts)
	for _, ci := range cidx {
		best := 0
		for p := 1; p < *nParts; p++ {
			if partLoad[p] < partLoad[best] {
				best = p
			}
		}
		partOfComp[ci] = best
		partLoad[best] += comps[ci].spans
		partComps[best]++
	}

	// Pass 2: write each trace's block to its partition's sub-archive.
	writers := make([]*corpus.ArchiveWriter, *nParts)
	for p := 0; p < *nParts; p++ {
		path := fmt.Sprintf("%s_%02d.arc", *outPrefix, p)
		w, err := corpus.NewArchiveWriter(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create %s: %v\n", path, err)
			os.Exit(1)
		}
		writers[p] = w
	}
	partOfTrace := make(map[uint64]int, len(infos))
	for i := range infos {
		partOfTrace[infos[i].tid] = partOfComp[comp[i]]
	}
	r2, err := corpus.OpenArchive(*archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reopen: %v\n", err)
		os.Exit(1)
	}
	for {
		tr, err := r2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "read2: %v\n", err)
			os.Exit(1)
		}
		if len(tr.Spans) == 0 {
			continue
		}
		if p, ok := partOfTrace[tr.TraceID]; ok {
			if err := writers[p].WriteTrace(tr.TraceID, tr.Flags, tr.Spans); err != nil {
				fmt.Fprintf(os.Stderr, "write part %d: %v\n", p, err)
				os.Exit(1)
			}
		}
	}
	r2.Close()
	for _, w := range writers {
		if err := w.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close: %v\n", err)
			os.Exit(1)
		}
	}

	// Report balance.
	var totSpans int64
	for _, c := range comps {
		totSpans += c.spans
	}
	var minL, maxL int64 = math.MaxInt64, 0
	for p := 0; p < *nParts; p++ {
		if partLoad[p] < minL {
			minL = partLoad[p]
		}
		if partLoad[p] > maxL {
			maxL = partLoad[p]
		}
	}
	mean := float64(totSpans) / float64(*nParts)
	fmt.Printf("partitioned %d traces / %d spans into %d parts across %d components in %s\n",
		len(infos), totSpans, *nParts, len(comps), time.Since(t0).Round(time.Millisecond))
	fmt.Printf("partition load (spans): min=%d max=%d mean=%.0f  imbalance(max/mean)=%.2fx\n",
		minL, maxL, mean, float64(maxL)/mean)
	fmt.Printf("max partition = %.1f%% of total (parallel-speedup ceiling = %.1fx)\n",
		100*float64(maxL)/float64(totSpans), float64(totSpans)/float64(maxL))
}
