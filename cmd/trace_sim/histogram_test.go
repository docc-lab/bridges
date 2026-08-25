package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSizeHistogramSnapshotAndWrite(t *testing.T) {
	h := newSizeHistograms()
	for _, n := range []int{9, 3, 9, 5} {
		h.recordBaggage(n)
	}
	for _, n := range []int{12, 7, 12} {
		h.recordPayload(n)
	}
	b := snapshotHistogram(h.baggage)
	if b.Count != 4 || b.SumBytes != 26 || b.MinBytes != 3 || b.MaxBytes != 9 {
		t.Fatalf("baggage summary = %+v", b)
	}
	wantBins := []histogramBin{{Bytes: 3, Count: 1}, {Bytes: 5, Count: 1}, {Bytes: 9, Count: 2}}
	if !reflect.DeepEqual(b.Bins, wantBins) {
		t.Fatalf("bins = %+v, want %+v", b.Bins, wantBins)
	}

	path := filepath.Join(t.TempDir(), "hist.json")
	c := config{mode: "sbridge", checkpointDistance: 4, lehmerEE: true, deeQueueIDs: "instances.bin"}
	if err := writeSizeHistograms(path, c, h); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got sizeHistogramFile
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Schema != "bridges.size_histograms.v1" || got.Mode != "sbridge" ||
		got.CheckpointDistance != 4 || !got.LehmerEE || !got.DEEInstanceQueues || got.BridgePayloadBytes.Count != 3 {
		t.Fatalf("file = %+v", got)
	}
}
