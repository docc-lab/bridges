package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// sizeHistograms records exact, corpus-wide distributions at the event level.
// It is safe for sharded simulator modes; S-Bridge itself remains single-threaded
// because its DEE queues cross traces.
type sizeHistograms struct {
	mu      sync.Mutex
	baggage map[int]uint64
	payload map[int]uint64
}

func newSizeHistograms() *sizeHistograms {
	return &sizeHistograms{
		baggage: make(map[int]uint64),
		payload: make(map[int]uint64),
	}
}

func (h *sizeHistograms) recordBaggage(n int) {
	if h == nil || n <= 0 {
		return
	}
	h.mu.Lock()
	h.baggage[n]++
	h.mu.Unlock()
}

func (h *sizeHistograms) recordPayload(n int) {
	if h == nil || n <= 0 {
		return
	}
	h.mu.Lock()
	h.payload[n]++
	h.mu.Unlock()
}

type histogramBin struct {
	Bytes int    `json:"bytes"`
	Count uint64 `json:"count"`
}

type histogramOutput struct {
	Count    uint64         `json:"count"`
	SumBytes uint64         `json:"sum_bytes"`
	MinBytes int            `json:"min_bytes"`
	MaxBytes int            `json:"max_bytes"`
	Bins     []histogramBin `json:"bins"`
}

func snapshotHistogram(m map[int]uint64) histogramOutput {
	keys := make([]int, 0, len(m))
	for n := range m {
		keys = append(keys, n)
	}
	sort.Ints(keys)
	out := histogramOutput{Bins: make([]histogramBin, 0, len(keys))}
	for _, n := range keys {
		count := m[n]
		out.Count += count
		out.SumBytes += uint64(n) * count
		out.Bins = append(out.Bins, histogramBin{Bytes: n, Count: count})
	}
	if len(keys) > 0 {
		out.MinBytes = keys[0]
		out.MaxBytes = keys[len(keys)-1]
	}
	return out
}

type sizeHistogramFile struct {
	Schema             string          `json:"schema"`
	Mode               string          `json:"mode"`
	CheckpointDistance int             `json:"checkpoint_distance"`
	LehmerEE           bool            `json:"lehmer_ee"`
	BaggageCallBytes   histogramOutput `json:"baggage_call_bytes"`
	BridgePayloadBytes histogramOutput `json:"bridge_payload_bytes"`
}

func writeSizeHistograms(path string, c config, h *sizeHistograms) error {
	h.mu.Lock()
	out := sizeHistogramFile{
		Schema:             "bridges.size_histograms.v1",
		Mode:               c.mode,
		CheckpointDistance: c.checkpointDistance,
		LehmerEE:           c.lehmerEE,
		BaggageCallBytes:   snapshotHistogram(h.baggage),
		BridgePayloadBytes: snapshotHistogram(h.payload),
	}
	h.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// runHistogramMerge sums exact bins from compatible day/partition histogram
// files. Counts and byte sums remain exact; no quantiles are re-binned.
func runHistogramMerge(args []string) {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: trace_sim histmerge <out.json> <in1.json> <in2.json> [inN.json ...]")
		os.Exit(2)
	}
	outPath, inputs := args[0], args[1:]
	h := newSizeHistograms()
	var base sizeHistogramFile
	for i, path := range inputs {
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open %s: %v\n", path, err)
			os.Exit(1)
		}
		var in sizeHistogramFile
		err = json.NewDecoder(f).Decode(&in)
		f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "decode %s: %v\n", path, err)
			os.Exit(1)
		}
		if i == 0 {
			base = in
		} else if in.Schema != base.Schema || in.Mode != base.Mode ||
			in.CheckpointDistance != base.CheckpointDistance || in.LehmerEE != base.LehmerEE {
			fmt.Fprintf(os.Stderr, "incompatible histogram %s (schema/mode/cpd/lehmer mismatch)\n", path)
			os.Exit(1)
		}
		for _, b := range in.BaggageCallBytes.Bins {
			h.baggage[b.Bytes] += b.Count
		}
		for _, b := range in.BridgePayloadBytes.Bins {
			h.payload[b.Bytes] += b.Count
		}
	}
	c := config{mode: base.Mode, checkpointDistance: base.CheckpointDistance, lehmerEE: base.LehmerEE}
	if err := writeSizeHistograms(outPath, c, h); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", outPath, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Merged %d histogram files into %s\n", len(inputs), outPath)
}
