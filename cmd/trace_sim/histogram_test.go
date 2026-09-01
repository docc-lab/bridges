package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"bridges/bridge"
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
	c := config{
		mode: "sbridge", checkpointDistance: 4, lehmerEE: true,
		deeQueueIDs: "instances.bin", deeDequeueOne: true,
		deeStats: bridge.NewDEEQueueStats(),
	}
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
		got.CheckpointDistance != 4 || !got.LehmerEE || !got.DEEInstanceQueues || !got.DEEDequeueOne ||
		got.DEEQueueStats == nil || got.BridgePayloadBytes.Count != 3 {
		t.Fatalf("file = %+v", got)
	}
}

func TestMergeDEEQueueStats(t *testing.T) {
	dst := bridge.DEEQueueStatsSnapshot{
		PickupAttempts: 2, EnqueuedRecords: 3, BacklogRecords: 1,
		MaxQueueRecords: 4, MaxQueueBytes: 10,
	}
	mergeDEEQueueStats(&dst, bridge.DEEQueueStatsSnapshot{
		PickupAttempts: 5, EmptyPickups: 2, PickupCalls: 3,
		EnqueuedRecords: 7, EnqueuedBytes: 70,
		DequeuedRecords: 4, DequeuedBytes: 40,
		BacklogQueues: 2, BacklogRecords: 3, BacklogBytes: 30,
		MaxQueueRecords: 6, MaxQueueBytes: 8,
	})
	if dst.PickupAttempts != 7 || dst.EmptyPickups != 2 || dst.PickupCalls != 3 ||
		dst.EnqueuedRecords != 10 || dst.EnqueuedBytes != 70 ||
		dst.DequeuedRecords != 4 || dst.DequeuedBytes != 40 ||
		dst.BacklogQueues != 2 || dst.BacklogRecords != 4 || dst.BacklogBytes != 30 ||
		dst.MaxQueueRecords != 6 || dst.MaxQueueBytes != 10 {
		t.Fatalf("merged stats = %+v", dst)
	}
}
