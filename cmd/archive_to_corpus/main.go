// Command archive_to_corpus regenerates the classic corpus (events.bin +
// meta.bin) from a minimal per-trace ARCHIVE (corpus.Archive*). It emits two
// events per span (start@StartTS, end@EndTS), recomputes Depth from the
// ParentID topology, global-sorts with the same key as trace_prep, and writes
// events.bin. The archive's sidecar meta (service names + trace order) is
// reused verbatim. This proves the archive is a lossless stand-in for the raw
// JSON: archive -> events.bin is byte-identical to the classic prep (modulo the
// service-id relabeling, which is order-of-arrival and carries no information).
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"bridges/corpus"
	"bridges/loader"
)

func main() {
	archivePath := flag.String("archive", "", "input archive (corpus.Archive)")
	metaIn := flag.String("meta", "", "input meta.bin (service names + trace order)")
	outDir := flag.String("output-dir", "", "output corpus dir (events.bin + meta.bin)")
	excludePruned := flag.Bool("exclude-pruned", false, "skip traces that had dangling-parent subtrees pruned (FlagPrunedDangling)")
	excludeDeduped := flag.Bool("exclude-deduped", false, "skip traces that had identical duplicate spans collapsed (FlagDedupedSpans)")
	flag.Parse()

	var excludeMask uint8
	if *excludePruned {
		excludeMask |= loader.FlagPrunedDangling
	}
	if *excludeDeduped {
		excludeMask |= loader.FlagDedupedSpans
	}
	if *archivePath == "" || *metaIn == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "usage: archive_to_corpus --archive A.arc --meta meta.bin --output-dir DIR")
		os.Exit(2)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
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

	t0 := time.Now()
	var events []corpus.Event
	var keptIDs []uint64
	var keptCounts []uint32
	nt := 0
	nExcluded := 0
	for {
		tr, err := r.Next()
		if err != nil {
			break
		}
		if excludeMask != 0 && tr.Flags&excludeMask != 0 {
			nExcluded++
			continue
		}
		nt++
		keptIDs = append(keptIDs, tr.TraceID)
		keptCounts = append(keptCounts, uint32(len(tr.Spans)))
		// Depth from topology: a span's parent must be present in the same trace,
		// else it's a root (depth 0). Memoized over the per-trace span set.
		byID := make(map[uint64]corpus.ArchiveSpan, len(tr.Spans))
		for _, s := range tr.Spans {
			byID[s.SpanID] = s
		}
		depth := make(map[uint64]int, len(tr.Spans))
		var d func(sid uint64) int
		d = func(sid uint64) int {
			if dv, ok := depth[sid]; ok {
				return dv
			}
			s := byID[sid]
			dv := 0
			if s.ParentID != 0 {
				if _, ok := byID[s.ParentID]; ok {
					dv = d(s.ParentID) + 1
				}
			}
			depth[sid] = dv
			return dv
		}
		for _, s := range tr.Spans {
			dp := uint16(d(s.SpanID))
			events = append(events,
				corpus.Event{TS: s.StartTS, Kind: corpus.KindStart, SpanID: s.SpanID, ParentID: s.ParentID, TraceID: tr.TraceID, Depth: dp, ServiceID: s.ServiceID},
				corpus.Event{TS: s.EndTS, Kind: corpus.KindEnd, SpanID: s.SpanID, ParentID: s.ParentID, TraceID: tr.TraceID, Depth: dp, ServiceID: s.ServiceID},
			)
		}
	}
	fmt.Fprintf(os.Stderr, "read %d traces, %d events in %s (%d excluded by flags)\n", nt, len(events), time.Since(t0).Round(time.Millisecond), nExcluded)

	// Same global sort key as trace_prep.
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.TS != b.TS {
			return a.TS < b.TS
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		var ar, br int16
		if a.Kind == corpus.KindStart {
			ar, br = int16(a.Depth), int16(b.Depth)
		} else {
			ar, br = -int16(a.Depth), -int16(b.Depth)
		}
		if ar != br {
			return ar < br
		}
		if a.TraceID != b.TraceID {
			return a.TraceID < b.TraceID
		}
		return a.SpanID < b.SpanID
	})

	eventsPath, metaPath := corpus.Paths(*outDir)
	ew, err := corpus.CreateEvents(eventsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create events: %v\n", err)
		os.Exit(1)
	}
	for _, e := range events {
		if err := ew.Write(e); err != nil {
			fmt.Fprintf(os.Stderr, "write event: %v\n", err)
			os.Exit(1)
		}
	}
	if err := ew.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close events: %v\n", err)
		os.Exit(1)
	}
	// Output meta lists exactly the traces we emitted (in read order). When
	// nothing is excluded this reproduces the input order; with exclusions it
	// stays consistent with events.bin. Service names are carried over verbatim.
	meta.TraceOrder = keptIDs
	meta.SpanCounts = keptCounts
	if err := corpus.WriteMeta(metaPath, meta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %s + %s\n", eventsPath, metaPath)
}
