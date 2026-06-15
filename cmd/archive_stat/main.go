// Command archive_stat streams a trace ARCHIVE end-to-end (bounded memory) and
// reports independent counts — traces, spans, and repair-flag breakdown — then
// cross-checks them against the sidecar meta.bin. A clean read to EOF plus a
// trace-count and span-count match with meta is the integrity gate before the
// raw JSON the archive was built from can be deleted.
//
//	archive_stat --archive corpus.arc --meta meta.bin
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"bridges/corpus"
	"bridges/loader"
)

func main() {
	archivePath := flag.String("archive", "", "archive to inspect (corpus.Archive)")
	metaPath := flag.String("meta", "", "sidecar meta.bin to cross-check (optional)")
	flag.Parse()
	if *archivePath == "" {
		fmt.Fprintln(os.Stderr, "usage: archive_stat --archive corpus.arc [--meta meta.bin]")
		os.Exit(2)
	}

	r, err := corpus.OpenArchive(*archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open archive: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

	t0 := time.Now()
	var nTraces, nSpans, nPruned, nDeduped, nEmpty int64
	var minSpans, maxSpans int = 1 << 30, 0
	for {
		tr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: archive read error after %d traces: %v\n", nTraces, err)
			os.Exit(1)
		}
		nTraces++
		n := len(tr.Spans)
		nSpans += int64(n)
		if n == 0 {
			nEmpty++
		}
		if n < minSpans {
			minSpans = n
		}
		if n > maxSpans {
			maxSpans = n
		}
		if tr.Flags&loader.FlagPrunedDangling != 0 {
			nPruned++
		}
		if tr.Flags&loader.FlagDedupedSpans != 0 {
			nDeduped++
		}
	}
	fmt.Printf("archive: %d traces, %d spans in %s\n", nTraces, nSpans, time.Since(t0).Round(time.Millisecond))
	fmt.Printf("  spans/trace: min=%d max=%d avg=%.1f\n", minSpans, maxSpans, float64(nSpans)/float64(nTraces))
	fmt.Printf("  flags: dangling-pruned=%d deduped=%d | empty-traces=%d\n", nPruned, nDeduped, nEmpty)

	if *metaPath != "" {
		meta, err := corpus.ReadMeta(*metaPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read meta: %v\n", err)
			os.Exit(1)
		}
		var metaSpans int64
		for _, c := range meta.SpanCounts {
			metaSpans += int64(c)
		}
		fmt.Printf("meta: %d traces, %d spans, %d services\n", len(meta.TraceOrder), metaSpans, len(meta.Services))
		ok := int64(len(meta.TraceOrder)) == nTraces && metaSpans == nSpans
		if ok {
			fmt.Printf("CROSS-CHECK OK: archive matches meta (traces + spans)\n")
		} else {
			fmt.Printf("CROSS-CHECK FAIL: archive(%d traces, %d spans) != meta(%d traces, %d spans)\n",
				nTraces, nSpans, len(meta.TraceOrder), metaSpans)
			os.Exit(1)
		}
	}
}
