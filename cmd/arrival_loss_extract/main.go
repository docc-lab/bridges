// Command arrival_loss_extract walks every sanitized Uber trace JSON file
// and emits two CSVs of summary data used for studying whether trace
// "load-like" properties correlate with the rate of dangling-parent
// references (the C2 condition of the cleanliness filter):
//
//   traces.csv (one row per trace):
//     start_us,end_us,trace_duration_us,num_spans,num_dangling_spans,
//     max_concurrent_spans,max_depth,max_fanout
//
//   services.csv (one row per service, aggregated across all traces):
//     service_name,total_spans,total_dangling_spans,total_traces
//
// Definitions:
//   - dangling span: a span with at least one CHILD_OF reference whose
//     spanID is not present in this trace (after dedup of identical refs
//     within the span).
//   - max_concurrent_spans: maximum number of spans whose [start, end]
//     intervals overlap at any instant within the trace.
//   - max_depth, max_fanout: computed on the in-trace inferred forest,
//     using the first CHILD_OF reference per span as the parent edge.
//     Edges to spans not present in the trace are dropped (so dangling
//     spans become additional roots). Depth of a root = 0.
//   - For services, "dangling spans" counts each span where at least one
//     CHILD_OF ref is dangling, attributed to the span's own service
//     (i.e. child-side loss). Parent-side missing services cannot be
//     attributed because the missing parent itself is unknown.
//
// We deliberately do NOT apply the cleanliness filter; the dependent
// variable is precisely what the filter would remove.
//
// Usage:
//
//   go run ./cmd/arrival_loss_extract \
//       --src /mydata/uber/traces/traces-sanitized/ \
//       --out-dir /mydata/uber/results_full/arrival_loss/
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type rawRef struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
}

type rawSpan struct {
	SpanID     string   `json:"spanID"`
	References []rawRef `json:"references"`
	StartTime  int64    `json:"startTime"` // microseconds since epoch
	Duration   int64    `json:"duration"`  // microseconds
	ProcessID  string   `json:"processID"`
}

type rawProcess struct {
	ServiceName string `json:"serviceName"`
}

type rawTrace struct {
	TraceID   string                `json:"traceID"`
	Spans     []rawSpan             `json:"spans"`
	Processes map[string]rawProcess `json:"processes"`
}

type rawWrapper struct {
	Data []rawTrace `json:"data"`
}

type traceRow struct {
	startUs            int64
	endUs              int64
	traceDurationUs    int64
	numSpans           int
	numDanglingSpans   int
	maxConcurrentSpans int
	maxDepth           int
	maxFanout          int
}

type svcAgg struct {
	totalSpans         int64
	totalDanglingSpans int64
	totalTraces        int64
}

// computeMaxConcurrent does an event sweep over (start, +1) / (end, -1) markers
// and returns the peak running count of overlapping span intervals.
func computeMaxConcurrent(spans []rawSpan) int {
	if len(spans) == 0 {
		return 0
	}
	type ev struct {
		t int64
		d int8 // +1 for start, -1 for end (sentinel after start at same t)
	}
	evs := make([]ev, 0, 2*len(spans))
	for _, s := range spans {
		end := s.StartTime + s.Duration
		if end < s.StartTime {
			end = s.StartTime
		}
		evs = append(evs, ev{t: s.StartTime, d: +1})
		evs = append(evs, ev{t: end, d: -1})
	}
	// Sort: ties — process +1 before -1 so a zero-duration span still
	// contributes to the peak.
	sort.Slice(evs, func(i, j int) bool {
		if evs[i].t != evs[j].t {
			return evs[i].t < evs[j].t
		}
		return evs[i].d > evs[j].d
	})
	cur, peak := 0, 0
	for _, e := range evs {
		cur += int(e.d)
		if cur > peak {
			peak = cur
		}
	}
	return peak
}

// computeDepthFanout computes max depth and max fan-out over the in-trace
// inferred forest. Each span's parent edge is its first CHILD_OF that
// resolves to a span present in the trace; otherwise the span is treated
// as a root of its own tree.
func computeDepthFanout(spans []rawSpan, idSet map[string]struct{}) (int, int) {
	if len(spans) == 0 {
		return 0, 0
	}
	parent := make(map[string]string, len(spans))
	children := make(map[string][]string, len(spans))
	roots := make([]string, 0)
	for _, s := range spans {
		var p string
		seen := make(map[string]struct{})
		for _, r := range s.References {
			if r.RefType != "CHILD_OF" || r.SpanID == "" {
				continue
			}
			if _, dup := seen[r.SpanID]; dup {
				continue
			}
			seen[r.SpanID] = struct{}{}
			if p == "" {
				if _, ok := idSet[r.SpanID]; ok {
					p = r.SpanID
				}
			}
		}
		if p != "" {
			parent[s.SpanID] = p
			children[p] = append(children[p], s.SpanID)
		} else {
			roots = append(roots, s.SpanID)
		}
	}

	maxFanout := 0
	for _, c := range children {
		if len(c) > maxFanout {
			maxFanout = len(c)
		}
	}

	// Depth: BFS from each root, capping iterations to span count to
	// avoid pathological loops (shouldn't happen on real data).
	maxDepth := 0
	cap := len(spans)
	for _, r := range roots {
		// BFS
		type frame struct {
			id    string
			depth int
		}
		stack := []frame{{r, 0}}
		visited := make(map[string]struct{})
		steps := 0
		for len(stack) > 0 && steps < cap*2 {
			steps++
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, ok := visited[n.id]; ok {
				continue
			}
			visited[n.id] = struct{}{}
			if n.depth > maxDepth {
				maxDepth = n.depth
			}
			for _, ch := range children[n.id] {
				stack = append(stack, frame{ch, n.depth + 1})
			}
		}
	}
	return maxDepth, maxFanout
}

func analyzeTrace(t rawTrace) (traceRow, map[string]*svcAgg, bool) {
	if len(t.Spans) == 0 {
		return traceRow{}, nil, false
	}

	// Service of each span via processID -> processes table.
	svcOf := make(map[string]string, len(t.Spans))
	for _, s := range t.Spans {
		svc := "?"
		if p, ok := t.Processes[s.ProcessID]; ok && p.ServiceName != "" {
			svc = p.ServiceName
		}
		svcOf[s.SpanID] = svc
	}

	idSet := make(map[string]struct{}, len(t.Spans))
	for _, s := range t.Spans {
		idSet[s.SpanID] = struct{}{}
	}

	var minStart int64 = -1
	var maxEnd int64
	danglingTotal := 0

	// Per-trace per-service aggregator (returned to caller for merging).
	traceSvc := make(map[string]*svcAgg)
	bump := func(svc string) *svcAgg {
		a, ok := traceSvc[svc]
		if !ok {
			a = &svcAgg{}
			traceSvc[svc] = a
		}
		return a
	}

	for _, s := range t.Spans {
		if minStart < 0 || s.StartTime < minStart {
			minStart = s.StartTime
		}
		end := s.StartTime + s.Duration
		if end > maxEnd {
			maxEnd = end
		}

		svc := svcOf[s.SpanID]
		bump(svc).totalSpans++

		// dangling check (dedupe identical CHILD_OF refs first)
		seen := make(map[string]struct{})
		hasDangling := false
		for _, r := range s.References {
			if r.RefType != "CHILD_OF" || r.SpanID == "" {
				continue
			}
			if _, dup := seen[r.SpanID]; dup {
				continue
			}
			seen[r.SpanID] = struct{}{}
			if _, ok := idSet[r.SpanID]; !ok {
				hasDangling = true
			}
		}
		if hasDangling {
			danglingTotal++
			bump(svc).totalDanglingSpans++
		}
	}
	// One trace contribution per service that appeared.
	for _, a := range traceSvc {
		a.totalTraces = 1
	}

	maxC := computeMaxConcurrent(t.Spans)
	depth, fanout := computeDepthFanout(t.Spans, idSet)

	row := traceRow{
		startUs:            minStart,
		endUs:              maxEnd,
		traceDurationUs:    maxEnd - minStart,
		numSpans:           len(t.Spans),
		numDanglingSpans:   danglingTotal,
		maxConcurrentSpans: maxC,
		maxDepth:           depth,
		maxFanout:          fanout,
	}
	return row, traceSvc, true
}

type fileResult struct {
	rows   []traceRow
	svcAgg map[string]*svcAgg
	err    error
}

func parseFile(path string) fileResult {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileResult{err: err}
	}
	var w rawWrapper
	var traces []rawTrace
	if err := json.Unmarshal(data, &w); err == nil && len(w.Data) > 0 {
		traces = w.Data
	} else {
		var rt rawTrace
		if err := json.Unmarshal(data, &rt); err != nil {
			return fileResult{err: err}
		}
		traces = []rawTrace{rt}
	}
	rows := make([]traceRow, 0, len(traces))
	merged := make(map[string]*svcAgg)
	for _, t := range traces {
		row, ts, ok := analyzeTrace(t)
		if !ok {
			continue
		}
		rows = append(rows, row)
		for svc, a := range ts {
			ex, exists := merged[svc]
			if !exists {
				ex = &svcAgg{}
				merged[svc] = ex
			}
			ex.totalSpans += a.totalSpans
			ex.totalDanglingSpans += a.totalDanglingSpans
			ex.totalTraces += a.totalTraces
		}
	}
	return fileResult{rows: rows, svcAgg: merged}
}

func worker(tasks <-chan string, out chan<- fileResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for p := range tasks {
		out <- parseFile(p)
	}
}

func main() {
	var (
		src       = flag.String("src", "/mydata/uber/traces/traces-sanitized/", "Directory of trace JSON files")
		outDir    = flag.String("out-dir", "", "Output directory (required)")
		workers   = flag.Int("workers", runtime.NumCPU(), "Parallel parse workers")
		progressN = flag.Int("progress", 25000, "Print progress every N files (0 = silent)")
	)
	flag.Parse()
	if *outDir == "" {
		fmt.Fprintln(os.Stderr, "usage: arrival_loss_extract --src <dir> --out-dir <dir>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if *workers < 1 {
		*workers = 1
	}

	t0 := time.Now()
	entries, err := os.ReadDir(*src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read dir: %v\n", err)
		os.Exit(1)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	fmt.Fprintf(os.Stderr, "Found %d trace files in %s\n", len(files), *src)

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	tracesCsv := filepath.Join(*outDir, "traces.csv")
	servicesCsv := filepath.Join(*outDir, "services.csv")
	out, err := os.Create(tracesCsv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", tracesCsv, err)
		os.Exit(1)
	}
	defer out.Close()
	bw := bufio.NewWriterSize(out, 1<<20)
	defer bw.Flush()
	fmt.Fprintln(bw, "start_us,end_us,trace_duration_us,num_spans,num_dangling_spans,max_concurrent_spans,max_depth,max_fanout")

	tasks := make(chan string, *workers*2)
	results := make(chan fileResult, *workers*4)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(tasks, results, &wg)
	}
	go func() {
		for _, name := range files {
			tasks <- filepath.Join(*src, name)
		}
		close(tasks)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	globalSvc := make(map[string]*svcAgg)
	var filesDone, parseErrors int64
	var tracesWritten int64
	for r := range results {
		atomic.AddInt64(&filesDone, 1)
		if r.err != nil {
			atomic.AddInt64(&parseErrors, 1)
			continue
		}
		for _, row := range r.rows {
			fmt.Fprintf(bw, "%d,%d,%d,%d,%d,%d,%d,%d\n",
				row.startUs, row.endUs, row.traceDurationUs,
				row.numSpans, row.numDanglingSpans,
				row.maxConcurrentSpans, row.maxDepth, row.maxFanout)
			tracesWritten++
		}
		for svc, a := range r.svcAgg {
			ex, exists := globalSvc[svc]
			if !exists {
				ex = &svcAgg{}
				globalSvc[svc] = ex
			}
			ex.totalSpans += a.totalSpans
			ex.totalDanglingSpans += a.totalDanglingSpans
			ex.totalTraces += a.totalTraces
		}
		if *progressN > 0 && filesDone%int64(*progressN) == 0 {
			fmt.Fprintf(os.Stderr, "  %d/%d files (%.1f%%, %s elapsed, %d traces, %d services, %d parse errors)\n",
				filesDone, len(files),
				100*float64(filesDone)/float64(len(files)),
				time.Since(t0).Round(time.Second),
				tracesWritten, len(globalSvc), parseErrors)
		}
	}

	// Write services.csv (sorted by totalSpans desc).
	svcOut, err := os.Create(servicesCsv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", servicesCsv, err)
		os.Exit(1)
	}
	defer svcOut.Close()
	sbw := bufio.NewWriterSize(svcOut, 1<<16)
	defer sbw.Flush()
	fmt.Fprintln(sbw, "service_name,total_spans,total_dangling_spans,total_traces")
	type svcKV struct {
		name string
		a    *svcAgg
	}
	sorted := make([]svcKV, 0, len(globalSvc))
	for n, a := range globalSvc {
		sorted = append(sorted, svcKV{n, a})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].a.totalSpans > sorted[j].a.totalSpans })
	for _, s := range sorted {
		fmt.Fprintf(sbw, "%s,%d,%d,%d\n",
			s.name, s.a.totalSpans, s.a.totalDanglingSpans, s.a.totalTraces)
	}

	fmt.Fprintf(os.Stderr, "Done: %d files (%d parse errors) -> %d traces, %d services in %s\n",
		filesDone, parseErrors, tracesWritten, len(globalSvc),
		time.Since(t0).Round(time.Second))
	fmt.Fprintf(os.Stderr, "Output: %s, %s\n", tracesCsv, servicesCsv)
}
