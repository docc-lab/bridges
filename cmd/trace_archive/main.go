// Command trace_archive builds a minimal per-trace ARCHIVE (corpus.Archive*)
// straight from a directory of Jaeger trace JSON files, with bounded memory.
//
// Design (deliberately simple): a pool of parse workers each pull one file at a
// time, normalize its traces in LENIENT mode (loader requireClean=false: keep
// the trace but drop dangling-parent subtrees, collapse identical duplicate
// spans, reject cyclic/duplicate-conflict traces), convert each surviving trace
// to a 34-byte/span block, and hand it to a single writer goroutine. The writer
// remaps per-file service IDs to global IDs, appends the block, and records the
// trace order — so at most ~(workers + channel) files are ever in flight. There
// is no global event list and no global sort, so peak memory stays flat
// regardless of corpus size.
//
// Each trace carries the loader's repair markers (loader.FlagXxx) in its block,
// so a later archive_to_corpus pass can include or exclude repaired traces on
// demand. meta.bin (service names + trace order) is written alongside.
//
//	trace_archive --input-dir DIR --out corpus.arc --meta meta.bin [--workers N] [--progress 50000]
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

// arcTrace is one trace ready to write; ServiceID in spans is still LOCAL to
// the producing worker's service table until the writer remaps it.
type arcTrace struct {
	id    uint64
	flags uint8
	spans []corpus.ArchiveSpan
}

// fileResult is one file's worth of work emitted by a parse worker.
type fileResult struct {
	name   string
	traces []arcTrace
	names  []string // local serviceID -> name, for the writer's remap
	err    error
}

func main() {
	var (
		inputDir  = flag.String("input-dir", "", "Directory of Jaeger trace JSON files (required)")
		outPath   = flag.String("out", "", "Output archive path (required)")
		metaPath  = flag.String("meta", "", "Output meta.bin path (service names + trace order) (required)")
		workers   = flag.Int("workers", runtime.NumCPU(), "Parallel parse workers")
		progressN = flag.Int("progress", 0, "Print progress every N files (0 = silent)")
	)
	flag.Parse()
	if *inputDir == "" || *outPath == "" || *metaPath == "" {
		fmt.Fprintln(os.Stderr, "usage: trace_archive --input-dir DIR --out corpus.arc --meta meta.bin")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if *workers < 1 {
		*workers = 1
	}

	// Enumerate *.json alphabetically (deterministic trace order).
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
		if name := e.Name(); len(name) > 5 && name[len(name)-5:] == ".json" {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	fmt.Fprintf(os.Stderr, "Discovered %d JSON files in %s\n", len(files), time.Since(t0).Round(time.Millisecond))

	// Worker pool. Bounded in-flight set: tasks and results buffers are sized to
	// the worker count, so only ~2*workers files' worth of spans live at once.
	tasks := make(chan string, *workers)
	results := make(chan fileResult, *workers)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range tasks {
				results <- parseOne(filepath.Join(*inputDir, name), name)
			}
		}()
	}
	go func() {
		for _, name := range files {
			tasks <- name
		}
		close(tasks)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	// Writer (this goroutine).
	aw, err := corpus.NewArchiveWriter(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create archive: %v\n", err)
		os.Exit(1)
	}
	global := loader.NewServiceTable()
	var traceIDs []uint64
	var spanCounts []uint32
	var nTraces, nSpans, nPruned, nDeduped int64
	processed, skipped := 0, 0
	t1 := time.Now()
	for r := range results {
		processed++
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", r.name, r.err)
			skipped++
			continue
		}
		remap := make([]uint16, len(r.names))
		for id, name := range r.names {
			remap[id] = global.Intern(name)
		}
		for _, t := range r.traces {
			for i := range t.spans {
				t.spans[i].ServiceID = remap[t.spans[i].ServiceID]
			}
			if err := aw.WriteTrace(t.id, t.flags, t.spans); err != nil {
				fmt.Fprintf(os.Stderr, "write trace %016x: %v\n", t.id, err)
				os.Exit(1)
			}
			traceIDs = append(traceIDs, t.id)
			spanCounts = append(spanCounts, uint32(len(t.spans)))
			nTraces++
			nSpans += int64(len(t.spans))
			if t.flags&loader.FlagPrunedDangling != 0 {
				nPruned++
			}
			if t.flags&loader.FlagDedupedSpans != 0 {
				nDeduped++
			}
		}
		if *progressN > 0 && processed%*progressN == 0 {
			fmt.Fprintf(os.Stderr, "  %d/%d files | %d traces, %d spans | pruned=%d deduped=%d\n",
				processed, len(files), nTraces, nSpans, nPruned, nDeduped)
		}
	}
	if err := aw.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close archive: %v\n", err)
		os.Exit(1)
	}

	meta := &corpus.Meta{Services: global.IDToName, TraceOrder: traceIDs, SpanCounts: spanCounts}
	if err := corpus.WriteMeta(*metaPath, meta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr,
		"Done in %s: %d files (%d skipped), %d traces, %d spans | dangling-pruned=%d deduped=%d\n",
		time.Since(t1).Round(time.Millisecond), len(files), skipped, nTraces, nSpans, nPruned, nDeduped)
	if st, err := os.Stat(*outPath); err == nil {
		fmt.Fprintf(os.Stderr, "%s: %.1f MB\n", filepath.Base(*outPath), float64(st.Size())/(1<<20))
	}
}

// parseOne loads one file in lenient mode and converts its surviving traces to
// archive blocks. Service IDs stay local to the per-file table (the writer
// remaps them to global IDs).
func parseOne(path, name string) fileResult {
	local := loader.NewServiceTable()
	traces, err := loader.LoadTraceFile(path, local, false)
	if err != nil {
		return fileResult{name: name, err: err}
	}
	ats := make([]arcTrace, 0, len(traces))
	for _, tr := range traces {
		if len(tr.Spans) == 0 {
			continue
		}
		spans := make([]corpus.ArchiveSpan, len(tr.Spans))
		for i, s := range tr.Spans {
			spans[i] = corpus.ArchiveSpan{
				SpanID:    s.SpanID,
				ParentID:  s.ParentID,
				ServiceID: s.ServiceID,
				StartTS:   s.StartNS,
				EndTS:     s.EndNS,
			}
		}
		ats = append(ats, arcTrace{id: tr.TraceID, flags: tr.Flags, spans: spans})
	}
	return fileResult{name: name, traces: ats, names: local.IDToName}
}
