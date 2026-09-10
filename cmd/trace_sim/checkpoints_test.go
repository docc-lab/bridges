package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"bridges/bridge"
)

func TestRandomCheckpointMetadataRoundTrip(t *testing.T) {
	dir := t.TempDir()
	policy := &bridge.CheckpointRange{Min: 2, Max: 8, Seed: 91}
	metrics := []TraceMetrics{{NumSpans: 7, NumCheckpointSpans: 3, CheckpointSum: 45,
		CheckpointMax: 15, NumBaggageCalls: 6, BaggageSum: 96, BaggageMax: 16}}
	direct, csv, converted := filepath.Join(dir, "direct.json"), filepath.Join(dir, "stream.csv"), filepath.Join(dir, "converted.json")
	if err := writeBagsizeJSON(direct, 8, metrics, true, false, policy); err != nil {
		t.Fatal(err)
	}
	sw, err := newStreamWriter(csv, 8, true, false, policy)
	if err != nil {
		t.Fatal(err)
	}
	sw.writeRec(1, &metrics[0])
	if err := sw.close(); err != nil {
		t.Fatal(err)
	}
	runCSV2JSON([]string{csv, converted})
	a, err := os.ReadFile(direct)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(converted)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatal("CSV conversion lost checkpoint policy or changed metrics")
	}
	h := newSizeHistograms()
	h.recordBaggage(16)
	path := filepath.Join(dir, "hist.json")
	if err := writeSizeHistograms(path, config{mode: "pcrb", checkpointDistance: 8, checkpointPolicy: policy}, h); err != nil {
		t.Fatal(err)
	}
	b, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var hist sizeHistogramFile
	if err := json.Unmarshal(b, &hist); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hist.CheckpointRandomization, policy) || hist.CheckpointDistance != 8 {
		t.Fatalf("histogram policy = %+v", hist)
	}
}
