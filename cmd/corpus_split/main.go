// Command corpus_split re-groups a timestamp-interleaved events.bin into a
// per-trace "trace store" (see corpus.TraceStore*): self-contained per-trace
// event blocks written in trace-COMPLETION order. This is a one-time pass that
// lets trace_recon replay traces through the handler + reconstruction in
// parallel (the global stream forces single-threaded streaming today), while a
// cheap serial pass over the store still reproduces the global drop RNG exactly
// (blocks are in the same order finishTrace fires).
//
//	corpus_split --corpus /mydata/uber/corpus_full -o /mydata/uber/corpus_full.store
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"bridges/corpus"
)

func main() {
	var corpusDir, out string
	flag.StringVar(&corpusDir, "corpus", "", "corpus dir (events.bin + meta.bin)")
	flag.StringVar(&out, "o", "", "output trace-store path")
	flag.Parse()
	if corpusDir == "" || out == "" {
		fmt.Fprintln(os.Stderr, "usage: corpus_split --corpus DIR -o STORE")
		os.Exit(2)
	}

	t0 := time.Now()
	eventsPath, metaPath := corpus.Paths(corpusDir)
	meta, err := corpus.ReadMeta(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read meta: %v\n", err)
		os.Exit(1)
	}
	// remaining[tid] = events left before the trace is complete (2 per span).
	remaining := make(map[uint64]int, len(meta.TraceOrder))
	for i, tid := range meta.TraceOrder {
		remaining[tid] = 2 * int(meta.SpanCounts[i])
	}

	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open events: %v\n", err)
		os.Exit(1)
	}
	defer er.Close()

	w, err := corpus.NewTraceStoreWriter(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create store: %v\n", err)
		os.Exit(1)
	}

	// Bucket events per open trace in arrival (stream) order; flush a trace's
	// block the instant its last event arrives — that emission order IS the
	// finishTrace/completion order the drop RNG depends on.
	buckets := make(map[uint64][]corpus.StoredEvent, 1<<16)
	var nEvents int64
	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "read event: %v\n", err)
			os.Exit(1)
		}
		nEvents++
		tid := ce.TraceID
		r, tracked := remaining[tid]
		if !tracked {
			continue // event of a trace not in meta (shouldn't happen)
		}
		buckets[tid] = append(buckets[tid], corpus.StoredEvent{
			Kind: ce.Kind, SpanID: ce.SpanID, ParentID: ce.ParentID, ServiceID: ce.ServiceID, TS: ce.TS,
		})
		r--
		remaining[tid] = r
		if r == 0 {
			if err := w.WriteTrace(tid, buckets[tid]); err != nil {
				fmt.Fprintf(os.Stderr, "write trace %016x: %v\n", tid, err)
				os.Exit(1)
			}
			delete(buckets, tid)
			delete(remaining, tid)
		}
	}
	if err := w.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close store: %v\n", err)
		os.Exit(1)
	}

	leftover := len(buckets)
	fmt.Fprintf(os.Stderr, "split %d traces (%d events) -> %s in %s\n",
		w.Count(), nEvents, out, time.Since(t0).Round(time.Millisecond))
	if leftover != 0 {
		fmt.Fprintf(os.Stderr, "WARNING: %d traces never completed (incomplete event counts)\n", leftover)
		os.Exit(1)
	}
	if w.Count() != len(meta.TraceOrder) {
		fmt.Fprintf(os.Stderr, "WARNING: wrote %d traces, meta has %d\n", w.Count(), len(meta.TraceOrder))
		os.Exit(1)
	}
}
