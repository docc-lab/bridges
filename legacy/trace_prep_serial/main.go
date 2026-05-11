// LEGACY: this is the original single-threaded trace_prep, preserved as a
// reference / fallback. The active version lives at cmd/trace_prep/main.go
// and runs the parse phase across a worker pool.
//
// Build with: go build -o /tmp/trace_prep_serial ./legacy/trace_prep_serial/
//
// Command trace_prep memoizes a directory of Jaeger trace JSON files into a
// compact binary corpus (events.bin + meta.bin) for cheap repeated runs by
// trace_sim --corpus. Applies the cleanliness filter and CRISP timestamp
// normalization at prep time so the simulator never has to.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"bridges/corpus"
	"bridges/loader"
)

func main() {
	var (
		inputDir     = flag.String("input-dir", "", "Directory of Jaeger trace JSON files (required)")
		outputDir    = flag.String("output-dir", "", "Output corpus directory (required)")
		requireClean = flag.Bool("require-clean", true, "Drop traces failing _trace_is_clean (Python parity)")
	)
	flag.Parse()
	if *inputDir == "" || *outputDir == "" {
		fmt.Fprintln(os.Stderr, "usage: trace_prep --input-dir <json_dir> --output-dir <corpus_dir>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", *outputDir, err)
		os.Exit(1)
	}

	// Phase 1: parse all JSON files in alphabetical order; apply cleanliness
	// + CRISP. Collect events in memory.
	t0 := time.Now()
	entries, err := os.ReadDir(*inputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read dir: %v\n", err)
		os.Exit(1)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > 5 && name[len(name)-5:] == ".json" {
			files = append(files, name)
		}
	}
	sort.Strings(files)

	services := loader.NewServiceTable()
	type traceMeta struct {
		traceID   uint64
		spanCount uint32
	}
	var traceOrder []traceMeta
	var events []corpus.Event

	skipped := 0
	for _, name := range files {
		path := filepath.Join(*inputDir, name)
		traces, err := loader.LoadTraceFile(path, services, *requireClean)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", name, err)
			skipped++
			continue
		}
		for _, t := range traces {
			traceOrder = append(traceOrder, traceMeta{
				traceID:   t.TraceID,
				spanCount: uint32(len(t.Spans)),
			})
			for _, s := range t.Spans {
				events = append(events, corpus.Event{
					TS: s.StartNS, SpanID: s.SpanID, ParentID: s.ParentID,
					TraceID: t.TraceID, Depth: uint16(s.Depth),
					ServiceID: s.ServiceID, Kind: corpus.KindStart,
				})
				events = append(events, corpus.Event{
					TS: s.EndNS, SpanID: s.SpanID, ParentID: s.ParentID,
					TraceID: t.TraceID, Depth: uint16(s.Depth),
					ServiceID: s.ServiceID, Kind: corpus.KindEnd,
				})
			}
		}
	}
	fmt.Fprintf(os.Stderr, "Parsed %d files (%d skipped) in %s: %d traces, %d events\n",
		len(files), skipped, time.Since(t0).Round(time.Millisecond),
		len(traceOrder), len(events))

	// Phase 2: sort events globally with the same key Python uses.
	t1 := time.Now()
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.TS != b.TS {
			return a.TS < b.TS
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		// start: shallower first; end: deeper first
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
	fmt.Fprintf(os.Stderr, "Sorted %d events in %s\n", len(events), time.Since(t1).Round(time.Millisecond))

	// Phase 3: write events.bin and meta.bin.
	t2 := time.Now()
	eventsPath, metaPath := corpus.Paths(*outputDir)
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

	traceIDs := make([]uint64, len(traceOrder))
	spanCounts := make([]uint32, len(traceOrder))
	for i, tm := range traceOrder {
		traceIDs[i] = tm.traceID
		spanCounts[i] = tm.spanCount
	}
	meta := &corpus.Meta{
		Services:   services.IDToName,
		TraceOrder: traceIDs,
		SpanCounts: spanCounts,
	}
	if err := corpus.WriteMeta(metaPath, meta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Wrote corpus in %s: %s, %s\n",
		time.Since(t2).Round(time.Millisecond), eventsPath, metaPath)

	if eventsStat, err := os.Stat(eventsPath); err == nil {
		fmt.Fprintf(os.Stderr, "events.bin: %.1f MB\n", float64(eventsStat.Size())/(1<<20))
	}
}
