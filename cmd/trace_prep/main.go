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
		requireClean = flag.Bool("require-clean", true, "Cleanliness filter: drop dirty traces; multi-root traces keep only the biggest root tree (Python parity)")
		workers      = flag.Int("workers", runtime.NumCPU(), "Parallel parse workers")
		progressN    = flag.Int("progress", 0, "Print progress every N files (0 = silent)")
		stream       = flag.Bool("stream", false, "Stream events to events.bin per file (locally sorted), bounded memory — skips the global timestamp sort. events.bin is then per-trace-block ordered (correct for the trace-store path; only the streaming simulator needs global TS order). Use for large/unfiltered corpora that don't fit in RAM.")
		archiveOut = flag.String("archive-out", "", "LEGACY (does not record lenient repair flags — prefer cmd/trace_archive). Stream a minimal per-trace ARCHIVE (corpus.Archive*) to this path: each trace's spans (SpanID, ParentID, ServiceID, start/end TS) are written as a 34-byte/span block, then freed. meta.bin (service names + trace order) is written alongside.")
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
	// Per-event sort key (the order Python uses; applied globally in default
	// mode, per-file in --stream mode).
	less := func(a, b corpus.Event) bool {
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
	}

	eventsPath, metaPath := corpus.Paths(*outputDir)
	var ew *corpus.EventsWriter
	if *stream && *archiveOut == "" {
		var err error
		if ew, err = corpus.CreateEvents(eventsPath); err != nil {
			fmt.Fprintf(os.Stderr, "create events: %v\n", err)
			os.Exit(1)
		}
	}
	// Direct archive streaming: per-trace span blocks written as files are parsed.
	var aw *corpus.ArchiveWriter
	var tsOrder []traceMeta // (traceID, spanCount) in block-write order = meta.TraceOrder
	if *archiveOut != "" {
		var err error
		if aw, err = corpus.NewArchiveWriter(*archiveOut); err != nil {
			fmt.Fprintf(os.Stderr, "create archive: %v\n", err)
			os.Exit(1)
		}
	}

	skipped := 0
	processed := 0
	var written int64
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
		if *archiveOut != "" {
			// Direct minimal archive: group this file's events by trace, pair each
			// span's start/end events into one ArchiveSpan, write the per-trace block,
			// then free. Bounded memory; OOM-proof.
			byTrace := make(map[uint64]map[uint64]*corpus.ArchiveSpan, len(r.traceMetas))
			for i := range r.events {
				e := &r.events[i]
				sp := byTrace[e.TraceID]
				if sp == nil {
					sp = make(map[uint64]*corpus.ArchiveSpan)
					byTrace[e.TraceID] = sp
				}
				a := sp[e.SpanID]
				if a == nil {
					a = &corpus.ArchiveSpan{SpanID: e.SpanID, ParentID: e.ParentID, ServiceID: e.ServiceID}
					sp[e.SpanID] = a
				}
				if e.Kind == corpus.KindStart {
					a.StartTS = e.TS
				} else {
					a.EndTS = e.TS
				}
			}
			for _, tm := range r.traceMetas {
				sp := byTrace[tm.traceID]
				if len(sp) == 0 {
					continue
				}
				spans := make([]corpus.ArchiveSpan, 0, len(sp))
				for _, a := range sp {
					spans = append(spans, *a)
				}
				sort.Slice(spans, func(i, j int) bool { return spans[i].SpanID < spans[j].SpanID })
				// flags=0: this events-derived path can't see loader repair markers.
				// Use cmd/trace_archive for repair-flag-aware archives.
				if err := aw.WriteTrace(tm.traceID, 0, spans); err != nil {
					fmt.Fprintf(os.Stderr, "write trace %016x: %v\n", tm.traceID, err)
					os.Exit(1)
				}
				tsOrder = append(tsOrder, tm)
				written += int64(len(spans))
			}
		} else if *stream {
			// Bounded memory: locally sort this file's events and flush them now,
			// then free. events.bin ends up per-file-block ordered, not globally
			// TS-interleaved (correct for the trace-store path; corpus_split
			// regroups by trace). Timestamps are preserved (Event.TS).
			sort.Slice(r.events, func(i, j int) bool { return less(r.events[i], r.events[j]) })
			for _, e := range r.events {
				if err := ew.Write(e); err != nil {
					fmt.Fprintf(os.Stderr, "write event: %v\n", err)
					os.Exit(1)
				}
			}
			written += int64(len(r.events))
		} else {
			allEvents = append(allEvents, r.events...)
		}
		// Free per-result storage we don't need beyond this point.
		r.events = nil
		r.localServices = nil
		if *archiveOut == "" {
			resultByIdx[r.fileIdx] = &r // needed only for events.bin-mode TraceOrder
		}

		if *progressN > 0 && processed%*progressN == 0 {
			n := int64(len(allEvents))
			if *stream || *archiveOut != "" {
				n = written
			}
			fmt.Fprintf(os.Stderr, "  parsed %d/%d files, %d events so far\n",
				processed, len(files), n)
		}
	}
	nev := int64(len(allEvents))
	if *stream || *archiveOut != "" {
		nev = written
	}
	fmt.Fprintf(os.Stderr, "Parsed %d files (%d skipped) in %s with %d workers: %d events\n",
		len(files), skipped, time.Since(t1).Round(time.Millisecond), *workers, nev)

	// Phase 3: assemble TraceOrder. Trace-store mode uses block-write order
	// (tsOrder, which meta must match); events.bin modes use alphabetical order.
	var traceOrder []traceMeta
	if *archiveOut != "" {
		traceOrder = tsOrder
	} else {
		for _, r := range resultByIdx {
			if r == nil {
				continue
			}
			traceOrder = append(traceOrder, r.traceMetas...)
		}
	}

	// Phase 4+5: finalize the event output. Trace-store and --stream are already
	// written (just close); default mode global-sorts then writes events.bin.
	t3 := time.Now()
	if *archiveOut != "" {
		if err := aw.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close archive: %v\n", err)
			os.Exit(1)
		}
	} else if *stream {
		if err := ew.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close events: %v\n", err)
			os.Exit(1)
		}
	} else {
		t2 := time.Now()
		sort.Slice(allEvents, func(i, j int) bool { return less(allEvents[i], allEvents[j]) })
		fmt.Fprintf(os.Stderr, "Sorted %d events in %s\n",
			len(allEvents), time.Since(t2).Round(time.Millisecond))
		ewd, err := corpus.CreateEvents(eventsPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create events: %v\n", err)
			os.Exit(1)
		}
		for _, e := range allEvents {
			if err := ewd.Write(e); err != nil {
				fmt.Fprintf(os.Stderr, "write event: %v\n", err)
				os.Exit(1)
			}
		}
		if err := ewd.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close events: %v\n", err)
			os.Exit(1)
		}
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
	dataPath := eventsPath
	if *archiveOut != "" {
		dataPath = *archiveOut
	}
	fmt.Fprintf(os.Stderr, "Wrote corpus in %s: %s, %s (%d traces)\n",
		time.Since(t3).Round(time.Millisecond), dataPath, metaPath, len(traceOrder))

	if st, err := os.Stat(dataPath); err == nil {
		fmt.Fprintf(os.Stderr, "%s: %.1f MB\n", filepath.Base(dataPath), float64(st.Size())/(1<<20))
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
