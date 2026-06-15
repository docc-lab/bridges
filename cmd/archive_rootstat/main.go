// Command archive_rootstat reports, per trace, how many ParentID==0 roots it
// has and how many spans are dangling (ParentID set but parent absent from the
// trace) — to check whether multi-root traces actually occur in the data.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"bridges/corpus"
)

func main() {
	archivePath := flag.String("archive", "", "archive (corpus.Archive)")
	examples := flag.Int("examples", 5, "print this many multi-root trace IDs")
	flag.Parse()
	r, err := corpus.OpenArchive(*archivePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer r.Close()
	var nt, multiRoot, anyDangling, zeroRoot int64
	dist := map[int]int64{} // #roots -> #traces
	var exShown int
	for {
		tr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if len(tr.Spans) == 0 {
			continue
		}
		nt++
		ids := make(map[uint64]struct{}, len(tr.Spans))
		for _, s := range tr.Spans {
			ids[s.SpanID] = struct{}{}
		}
		roots, dangling := 0, 0
		for _, s := range tr.Spans {
			if s.ParentID == 0 {
				roots++
			} else if _, ok := ids[s.ParentID]; !ok {
				dangling++
			}
		}
		b := roots
		if b > 4 {
			b = 5 // 5 = "5+"
		}
		dist[b]++
		if roots == 0 {
			zeroRoot++
		}
		if roots > 1 {
			multiRoot++
			if exShown < *examples {
				fmt.Printf("  multi-root example: tid=%016x roots=%d spans=%d dangling=%d\n", tr.TraceID, roots, len(tr.Spans), dangling)
				exShown++
			}
		}
		if dangling > 0 {
			anyDangling++
		}
	}
	fmt.Printf("traces=%d\n", nt)
	fmt.Printf("root-count distribution (#ParentID==0 per trace):\n")
	for k := 0; k <= 5; k++ {
		if dist[k] > 0 {
			label := fmt.Sprintf("%d", k)
			if k == 5 {
				label = "5+"
			}
			fmt.Printf("  %s roots: %d traces (%.3f%%)\n", label, dist[k], 100*float64(dist[k])/float64(nt))
		}
	}
	fmt.Printf("multi-root traces (>1 root): %d (%.3f%%)\n", multiRoot, 100*float64(multiRoot)/float64(nt))
	fmt.Printf("zero-root traces (all spans have a parent): %d (%.3f%%)\n", zeroRoot, 100*float64(zeroRoot)/float64(nt))
	fmt.Printf("traces with >=1 dangling span (parent absent): %d (%.3f%%)\n", anyDangling, 100*float64(anyDangling)/float64(nt))
}
