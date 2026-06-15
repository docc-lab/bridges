// Command archive_timechunks analyzes the temporal structure of a trace
// ARCHIVE to decide how the events could be chunked for a streaming
// (external) global sort instead of an all-in-RAM sort.
//
// Two views:
//  1. Time-disjoint components: treat each trace as the interval
//     [min span StartTS, max span EndTS]; merge overlapping intervals. Each
//     merged cluster is a chunk that shares NO timeline with any other, so it
//     can be sorted and emitted independently. Reports how many there are and
//     their sizes (events). If one cluster holds ~everything, there are no
//     natural cut points and you need fixed windows instead.
//  2. Fixed-window density: events per window of --window-ms. Reports the
//     count distribution so you can pick a window small enough to sort in RAM.
//
//	archive_timechunks --archive corpus.arc [--window-ms 1000]
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

	"bridges/corpus"
)

func main() {
	archivePath := flag.String("archive", "", "archive to analyze (corpus.Archive)")
	windowMs := flag.Int64("window-ms", 1000, "fixed-window size (ms) for the density histogram")
	flag.Parse()
	if *archivePath == "" {
		fmt.Fprintln(os.Stderr, "usage: archive_timechunks --archive corpus.arc [--window-ms N]")
		os.Exit(2)
	}
	r, err := corpus.OpenArchive(*archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open archive: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

	type iv struct {
		start, end int64
		events     int64 // 2 per span
	}
	var ivs []iv
	winNs := *windowMs * 1_000_000
	winCount := map[int64]int64{} // absolute window index -> event count
	var gmin, gmax int64 = math.MaxInt64, math.MinInt64
	var nTraces, totalSpans int64

	t0 := time.Now()
	for {
		tr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "read error: %v\n", err)
			os.Exit(1)
		}
		if len(tr.Spans) == 0 {
			continue
		}
		nTraces++
		totalSpans += int64(len(tr.Spans))
		mn, mx := int64(math.MaxInt64), int64(math.MinInt64)
		for _, s := range tr.Spans {
			if s.StartTS < mn {
				mn = s.StartTS
			}
			if s.EndTS > mx {
				mx = s.EndTS
			}
			// density: each span contributes a start event and an end event.
			winCount[s.StartTS/winNs]++
			winCount[s.EndTS/winNs]++
		}
		ivs = append(ivs, iv{mn, mx, int64(2 * len(tr.Spans))})
		if mn < gmin {
			gmin = mn
		}
		if mx > gmax {
			gmax = mx
		}
	}
	totalEvents := 2 * totalSpans
	span := time.Duration(gmax - gmin)
	fmt.Printf("read %d traces, %d spans (%d events) in %s\n", nTraces, totalSpans, totalEvents, time.Since(t0).Round(time.Millisecond))
	fmt.Printf("global time span: %s  (%.0f s)\n", span.Round(time.Second), span.Seconds())

	// (1) Time-disjoint components via interval merge.
	sort.Slice(ivs, func(i, j int) bool { return ivs[i].start < ivs[j].start })
	type comp struct {
		start, end, events, traces int64
	}
	var comps []comp
	var gaps []int64
	if len(ivs) > 0 {
		cur := comp{ivs[0].start, ivs[0].end, ivs[0].events, 1}
		for _, v := range ivs[1:] {
			if v.start > cur.end {
				comps = append(comps, cur)
				gaps = append(gaps, v.start-cur.end)
				cur = comp{v.start, v.end, v.events, 1}
			} else {
				if v.end > cur.end {
					cur.end = v.end
				}
				cur.events += v.events
				cur.traces++
			}
		}
		comps = append(comps, cur)
	}
	// Component event-size stats.
	sort.Slice(comps, func(i, j int) bool { return comps[i].events > comps[j].events })
	fmt.Printf("\n=== TIME-DISJOINT COMPONENTS: %d ===\n", len(comps))
	if len(comps) > 0 {
		big := comps[0]
		fmt.Printf("largest: %d events (%.2f%% of all), %d traces, span %s\n",
			big.events, 100*float64(big.events)/float64(totalEvents), big.traces,
			time.Duration(big.end-big.start).Round(time.Millisecond))
		show := len(comps)
		if show > 10 {
			show = 10
		}
		fmt.Printf("top %d components by events:\n", show)
		for i := 0; i < show; i++ {
			c := comps[i]
			fmt.Printf("  #%d: %d events, %d traces, span %s\n", i+1, c.events, c.traces,
				time.Duration(c.end-c.start).Round(time.Millisecond))
		}
		// How many components hold <1M events (trivially RAM-sortable)?
		var small int
		for _, c := range comps {
			if c.events < 1_000_000 {
				small++
			}
		}
		fmt.Printf("components < 1M events: %d / %d\n", small, len(comps))
	}
	if len(gaps) > 0 {
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		fmt.Printf("gaps between components: n=%d  min=%s  median=%s  max=%s\n",
			len(gaps),
			time.Duration(gaps[0]).Round(time.Microsecond),
			time.Duration(gaps[len(gaps)/2]).Round(time.Microsecond),
			time.Duration(gaps[len(gaps)-1]).Round(time.Millisecond))
	}

	// (2) Fixed-window density.
	counts := make([]int64, 0, len(winCount))
	for _, c := range winCount {
		counts = append(counts, c)
	}
	sort.Slice(counts, func(i, j int) bool { return counts[i] < counts[j] })
	var sum, mx int64
	for _, c := range counts {
		sum += c
		if c > mx {
			mx = c
		}
	}
	fmt.Printf("\n=== FIXED %dms WINDOWS: %d non-empty ===\n", *windowMs, len(counts))
	if len(counts) > 0 {
		fmt.Printf("events/window: min=%d  median=%d  p90=%d  p99=%d  max=%d  mean=%.0f\n",
			counts[0], counts[len(counts)/2], counts[len(counts)*90/100],
			counts[len(counts)*99/100], mx, float64(sum)/float64(len(counts)))
		fmt.Printf("(max window %d events = ~%.0f MB of corpus.Event in RAM to sort one chunk)\n",
			mx, float64(mx)*40/(1<<20))
	}
}
