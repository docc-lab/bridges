// Command archive_to_store builds a per-trace trace-store (corpus.TraceStore)
// directly from a trace ARCHIVE, streaming one trace at a time — bounded memory,
// no global event list and no global sort (unlike archive_to_corpus, which
// accumulates every event in RAM to sort). Each trace's events are ordered with
// the same per-event key trace_prep uses, but only within that one trace, so the
// handler replay sees a valid causal order. Blocks are written in archive order.
//
// A sidecar meta.bin is written next to nothing — instead we write it into
// --meta-out so trace_recon --corpus can read service names + trace order.
// Service IDs in the archive are already global (trace_archive remapped them),
// so service names come from the input --meta verbatim.
//
//	archive_to_store --archive part.arc --meta full_meta.bin --store out.store --meta-out outdir
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"bridges/corpus"
)

// keepLargestTree applies the multi-root salvage: among the ParentID==0-rooted
// trees in a trace, keep only the largest (by reachable span count; ties broken
// by lowest root SpanID, matching loader's strict salvage), drop the rest.
// Single-root traces are returned unchanged. The archive has no dangling spans
// (the lenient loader pruned them at archive time), so reachable-from-root ==
// the whole tree.
func keepLargestTree(spans []corpus.ArchiveSpan) []corpus.ArchiveSpan {
	idIdx := make(map[uint64]int, len(spans))
	for i := range spans {
		idIdx[spans[i].SpanID] = i
	}
	children := make(map[uint64][]int)
	var roots []int
	for i := range spans {
		if spans[i].ParentID == 0 {
			roots = append(roots, i)
		} else if _, ok := idIdx[spans[i].ParentID]; ok {
			children[spans[i].ParentID] = append(children[spans[i].ParentID], i)
		}
	}
	if len(roots) <= 1 {
		return spans
	}
	best, bestSize := -1, -1
	var bestKeep []bool
	for _, r := range roots {
		keep := make([]bool, len(spans))
		stack := []int{r}
		sz := 0
		for len(stack) > 0 {
			c := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if keep[c] {
				continue
			}
			keep[c] = true
			sz++
			for _, ch := range children[spans[c].SpanID] {
				stack = append(stack, ch)
			}
		}
		if sz > bestSize || (sz == bestSize && spans[r].SpanID < spans[best].SpanID) {
			best, bestSize, bestKeep = r, sz, keep
		}
	}
	out := make([]corpus.ArchiveSpan, 0, bestSize)
	for i := range spans {
		if bestKeep[i] {
			out = append(out, spans[i])
		}
	}
	return out
}

// ev is a per-trace event with depth, used only to order events within a trace.
type ev struct {
	ts        int64
	kind      uint8
	depth     int
	spanID    uint64
	parentID  uint64
	serviceID uint16
}

func main() {
	archivePath := flag.String("archive", "", "input sub-archive (corpus.Archive)")
	metaIn := flag.String("meta", "", "input meta.bin (service names)")
	storeOut := flag.String("store", "", "output trace-store path")
	metaOutDir := flag.String("meta-out", "", "output dir for this partition's meta.bin (services + trace order)")
	progressN := flag.Int("progress", 0, "print progress every N traces (0 = silent)")
	salvage := flag.Bool("salvage-multiroot", true, "multi-root cleaning: keep only the largest ParentID==0 tree per trace, drop the rest (matches the strict-corpus salvage). Archive stays lossless; this cleans at store-build time.")
	flag.Parse()
	if *archivePath == "" || *metaIn == "" || *storeOut == "" || *metaOutDir == "" {
		fmt.Fprintln(os.Stderr, "usage: archive_to_store --archive A.arc --meta full_meta.bin --store out.store --meta-out DIR")
		os.Exit(2)
	}
	meta, err := corpus.ReadMeta(*metaIn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read meta: %v\n", err)
		os.Exit(1)
	}
	r, err := corpus.OpenArchive(*archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open archive: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()
	w, err := corpus.NewTraceStoreWriter(*storeOut)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create store: %v\n", err)
		os.Exit(1)
	}

	t0 := time.Now()
	var traceIDs []uint64
	var spanCounts []uint32
	var nt, ns, nSalvaged int64
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
		if *salvage {
			before := len(tr.Spans)
			tr.Spans = keepLargestTree(tr.Spans)
			if len(tr.Spans) != before {
				nSalvaged++
			}
		}
		// depth from topology (memoized over this trace's spans).
		idx := make(map[uint64]int, len(tr.Spans))
		for i := range tr.Spans {
			idx[tr.Spans[i].SpanID] = i
		}
		depth := make([]int, len(tr.Spans))
		for i := range depth {
			depth[i] = -1
		}
		var d func(i int) int
		d = func(i int) int {
			if depth[i] >= 0 {
				return depth[i]
			}
			dv := 0
			if pid := tr.Spans[i].ParentID; pid != 0 {
				if pj, ok := idx[pid]; ok {
					dv = d(pj) + 1
				}
			}
			depth[i] = dv
			return dv
		}
		evs := make([]ev, 0, 2*len(tr.Spans))
		for i := range tr.Spans {
			s := &tr.Spans[i]
			dp := d(i)
			evs = append(evs,
				ev{s.StartTS, uint8(corpus.KindStart), dp, s.SpanID, s.ParentID, s.ServiceID},
				ev{s.EndTS, uint8(corpus.KindEnd), dp, s.SpanID, s.ParentID, s.ServiceID},
			)
		}
		// Same ordering as trace_prep's global key, applied within this trace.
		sort.Slice(evs, func(a, b int) bool {
			x, y := evs[a], evs[b]
			if x.ts != y.ts {
				return x.ts < y.ts
			}
			if x.kind != y.kind {
				return x.kind < y.kind
			}
			var ar, br int16
			if x.kind == uint8(corpus.KindStart) {
				ar, br = int16(x.depth), int16(y.depth)
			} else {
				ar, br = -int16(x.depth), -int16(y.depth)
			}
			if ar != br {
				return ar < br
			}
			return x.spanID < y.spanID
		})
		stored := make([]corpus.StoredEvent, len(evs))
		for i, e := range evs {
			stored[i] = corpus.StoredEvent{Kind: e.kind, SpanID: e.spanID, ParentID: e.parentID, ServiceID: e.serviceID, TS: e.ts}
		}
		if err := w.WriteTrace(tr.TraceID, stored); err != nil {
			fmt.Fprintf(os.Stderr, "write trace %016x: %v\n", tr.TraceID, err)
			os.Exit(1)
		}
		traceIDs = append(traceIDs, tr.TraceID)
		spanCounts = append(spanCounts, uint32(len(tr.Spans)))
		nt++
		ns += int64(len(tr.Spans))
		if *progressN > 0 && nt%int64(*progressN) == 0 {
			fmt.Fprintf(os.Stderr, "  %d traces, %d spans in %s\n", nt, ns, time.Since(t0).Round(time.Second))
		}
	}
	if err := w.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close store: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*metaOutDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir meta-out: %v\n", err)
		os.Exit(1)
	}
	_, metaPath := corpus.Paths(*metaOutDir)
	outMeta := &corpus.Meta{Services: meta.Services, TraceOrder: traceIDs, SpanCounts: spanCounts}
	if err := corpus.WriteMeta(metaPath, outMeta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "multi-root salvage: %d traces reduced to largest tree\n", nSalvaged)
	fmt.Fprintf(os.Stderr, "store: %d traces, %d spans in %s -> %s (meta %s)\n",
		nt, ns, time.Since(t0).Round(time.Millisecond), *storeOut, metaPath)
}
