// Command trace_prep memoizes a directory of Jaeger trace JSON files into a
// compact binary corpus (events.bin + meta.bin) for cheap repeated runs by
// trace_sim --corpus. Applies the cleanliness filter and CRISP timestamp
// normalization at prep time so the simulator never has to.
//
// The parse phase runs across a worker pool (defaults to NumCPU). The sort
// and write phases stay serial. Bit-exact equivalent to the legacy single-
// threaded version, validated against the JSON-direct simulator path.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"bridges/corpus"
	"bridges/loader"
)

// parseTask is one unit of work handed to a parse worker.
type parseTask struct {
	fileIdx int
	path    string
	name    string
}

// traceMeta is one trace's load-order entry, used to assemble the meta file.
type traceMeta struct {
	traceID   uint64
	spanCount uint32
}

// prepResult is what each worker emits: the trace metas and events for a
// single file, plus the worker's local service intern table (so the
// aggregator can remap to global service IDs).
type prepResult struct {
	fileIdx       int
	name          string
	traceMetas    []traceMeta
	events        []corpus.Event
	localServices []string // index = local service id, value = service name
	err           error
}

func main() {
	var (
		inputDir     = flag.String("input-dir", "", "Directory of Jaeger trace JSON files (required)")
		outputDir    = flag.String("output-dir", "", "Output corpus directory (required)")
		requireClean = flag.Bool("require-clean", true, "Drop traces failing _trace_is_clean (Python parity)")
		workers      = flag.Int("workers", runtime.NumCPU(), "Parallel parse workers")
		progressN    = flag.Int("progress", 0, "Print progress every N files (0 = silent)")
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
	if *workers < 1 {
		*workers = 1
	}

	// Phase 1: enumerate files alphabetically (matching Python's
	// sorted(path.glob("*.json"))).
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
	fmt.Fprintf(os.Stderr, "Discovered %d JSON files in %s\n",
		len(files), time.Since(t0).Round(time.Millisecond))

	// Phase 2: parallel parse + normalize + extract events.
	t1 := time.Now()
	tasks := make(chan parseTask, *workers*2)
	results := make(chan prepResult, *workers*4)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parseWorker(tasks, results, *requireClean)
		}()
	}
	go func() {
		for i, name := range files {
			tasks <- parseTask{fileIdx: i, path: filepath.Join(*inputDir, name), name: name}
		}
		close(tasks)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	// Aggregator (this goroutine): remap local service IDs to global IDs as
	// results land, append events to the global slice, stash trace metas by
	// fileIdx so meta.TraceOrder stays alphabetical for Python parity.
	globalServices := loader.NewServiceTable()
	resultByIdx := make([]*prepResult, len(files))
	var allEvents []corpus.Event
	skipped := 0
	processed := 0
	for r := range results {
		processed++
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", r.name, r.err)
			skipped++
			continue
		}
		// Remap local -> global service IDs in this result's events.
		remap := make([]uint16, len(r.localServices))
		for localID, name := range r.localServices {
			remap[localID] = globalServices.Intern(name)
		}
		for i := range r.events {
			r.events[i].ServiceID = remap[r.events[i].ServiceID]
		}
		allEvents = append(allEvents, r.events...)
		// Free per-result storage we don't need beyond this point.
		r.events = nil
		r.localServices = nil
		resultByIdx[r.fileIdx] = &r

		if *progressN > 0 && processed%*progressN == 0 {
			fmt.Fprintf(os.Stderr, "  parsed %d/%d files, %d events so far\n",
				processed, len(files), len(allEvents))
		}
	}
	fmt.Fprintf(os.Stderr, "Parsed %d files (%d skipped) in %s with %d workers: %d events\n",
		len(files), skipped, time.Since(t1).Round(time.Millisecond), *workers, len(allEvents))

	// Phase 3: assemble TraceOrder in alphabetical file order.
	var traceOrder []traceMeta
	for _, r := range resultByIdx {
		if r == nil {
			continue
		}
		traceOrder = append(traceOrder, r.traceMetas...)
	}

	// Phase 4: global event sort. Same key Python uses.
	t2 := time.Now()
	sort.Slice(allEvents, func(i, j int) bool {
		a, b := allEvents[i], allEvents[j]
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
	fmt.Fprintf(os.Stderr, "Sorted %d events in %s\n",
		len(allEvents), time.Since(t2).Round(time.Millisecond))

	// Phase 5: write events.bin and meta.bin.
	t3 := time.Now()
	eventsPath, metaPath := corpus.Paths(*outputDir)
	ew, err := corpus.CreateEvents(eventsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create events: %v\n", err)
		os.Exit(1)
	}
	for _, e := range allEvents {
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
		Services:   globalServices.IDToName,
		TraceOrder: traceIDs,
		SpanCounts: spanCounts,
	}
	if err := corpus.WriteMeta(metaPath, meta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Wrote corpus in %s: %s, %s\n",
		time.Since(t3).Round(time.Millisecond), eventsPath, metaPath)

	if eventsStat, err := os.Stat(eventsPath); err == nil {
		fmt.Fprintf(os.Stderr, "events.bin: %.1f MB\n", float64(eventsStat.Size())/(1<<20))
	}
}

// parseWorker pulls one file at a time, parses + normalizes + extracts
// events using a per-worker ServiceTable. The aggregator remaps local
// service IDs to global IDs as each result arrives.
func parseWorker(tasks <-chan parseTask, out chan<- prepResult, requireClean bool) {
	for t := range tasks {
		local := loader.NewServiceTable()
		traces, err := loader.LoadTraceFile(t.path, local, requireClean)
		if err != nil {
			out <- prepResult{fileIdx: t.fileIdx, name: t.name, err: err}
			continue
		}
		var metas []traceMeta
		var events []corpus.Event
		for _, tr := range traces {
			metas = append(metas, traceMeta{traceID: tr.TraceID, spanCount: uint32(len(tr.Spans))})
			for _, s := range tr.Spans {
				events = append(events, corpus.Event{
					TS: s.StartNS, SpanID: s.SpanID, ParentID: s.ParentID,
					TraceID: tr.TraceID, Depth: uint16(s.Depth),
					ServiceID: s.ServiceID, Kind: corpus.KindStart,
				})
				events = append(events, corpus.Event{
					TS: s.EndNS, SpanID: s.SpanID, ParentID: s.ParentID,
					TraceID: tr.TraceID, Depth: uint16(s.Depth),
					ServiceID: s.ServiceID, Kind: corpus.KindEnd,
				})
			}
		}
		out <- prepResult{
			fileIdx:       t.fileIdx,
			name:          t.name,
			traceMetas:    metas,
			events:        events,
			localServices: local.IDToName,
		}
	}
}
