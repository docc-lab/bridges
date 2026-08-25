package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"bridges/corpus"
)

func writeTestCorpus(t *testing.T, dir string, events []corpus.Event, endpoints []uint32) string {
	t.Helper()
	if len(events) != len(endpoints) {
		t.Fatal("event/endpoint test fixture mismatch")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	eventsPath := filepath.Join(dir, "events.bin")
	w, err := corpus.CreateEvents(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := w.Write(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := corpus.WriteMeta(filepath.Join(dir, "meta.bin"), &corpus.Meta{
		Services:   []string{"root", "backend"},
		TraceOrder: []uint64{1}, SpanCounts: []uint32{4},
	}); err != nil {
		t.Fatal(err)
	}

	endpointPath := filepath.Join(dir, "endpoints.bin")
	f, err := os.Create(endpointPath)
	if err != nil {
		t.Fatal(err)
	}
	bw := bufio.NewWriter(f)
	for _, endpoint := range endpoints {
		if err := binary.Write(bw, binary.LittleEndian, endpoint); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return endpointPath
}

func TestAnalyzeAndAssignConcurrentFanout(t *testing.T) {
	dir := t.TempDir()
	events := []corpus.Event{
		{TS: 1, TraceID: 1, SpanID: 10, ServiceID: 0, Kind: corpus.KindStart},
		{TS: 2, TraceID: 1, SpanID: 11, ParentID: 10, ServiceID: 1, Kind: corpus.KindStart},
		{TS: 3, TraceID: 1, SpanID: 12, ParentID: 10, ServiceID: 1, Kind: corpus.KindStart},
		{TS: 4, TraceID: 1, SpanID: 11, ParentID: 10, ServiceID: 1, Kind: corpus.KindEnd},
		{TS: 5, TraceID: 1, SpanID: 13, ParentID: 10, ServiceID: 1, Kind: corpus.KindStart},
		{TS: 6, TraceID: 1, SpanID: 12, ParentID: 10, ServiceID: 1, Kind: corpus.KindEnd},
		{TS: 7, TraceID: 1, SpanID: 13, ParentID: 10, ServiceID: 1, Kind: corpus.KindEnd},
		{TS: 8, TraceID: 1, SpanID: 10, ServiceID: 0, Kind: corpus.KindEnd},
	}
	endpointPath := writeTestCorpus(t, dir, events, []uint32{3, 7, 7, 7, 7, 7, 7, 3})
	eventsPath := filepath.Join(dir, "events.bin")

	a, err := analyzeCorpus(eventsPath, endpointPath, 0, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.MaxConcurrent[pairKey{Service: 1, Endpoint: 7}]; got != 2 {
		t.Fatalf("backend pool = %d, want 2", got)
	}
	if a.EventCount != 8 || a.SpanCount != 4 {
		t.Fatalf("counts = events %d spans %d, want 8 and 4", a.EventCount, a.SpanCount)
	}

	_, byPair, total, err := buildPools(a.MaxConcurrent)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("total instances = %d, want 3", total)
	}
	outPath := filepath.Join(dir, "instances.bin")
	if err := assignInstances(eventsPath, endpointPath, outPath, a.EventCount, byPair, 0, io.Discard); err != nil {
		t.Fatal(err)
	}

	r, err := corpus.OpenDEEQueueIDs(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Pools sort by service+endpoint: root is queue 0, backend slots are 1/2.
	want := []uint32{0, 1, 2, 1, 1, 2, 1, 0}
	for i, expected := range want {
		got, err := r.Next()
		if err != nil || got != expected {
			t.Fatalf("queue ID %d = (%d, %v), want (%d, nil)", i, got, err, expected)
		}
	}
	if err := r.ValidateEOF(); err != nil {
		t.Fatal(err)
	}
}

func TestSeparateParentsDoNotInflatePool(t *testing.T) {
	dir := t.TempDir()
	events := []corpus.Event{
		{TS: 1, TraceID: 1, SpanID: 10, ServiceID: 0, Kind: corpus.KindStart},
		{TS: 2, TraceID: 1, SpanID: 20, ServiceID: 0, Kind: corpus.KindStart},
		{TS: 3, TraceID: 1, SpanID: 11, ParentID: 10, ServiceID: 1, Kind: corpus.KindStart},
		{TS: 4, TraceID: 1, SpanID: 21, ParentID: 20, ServiceID: 1, Kind: corpus.KindStart},
		{TS: 5, TraceID: 1, SpanID: 11, ParentID: 10, ServiceID: 1, Kind: corpus.KindEnd},
		{TS: 6, TraceID: 1, SpanID: 21, ParentID: 20, ServiceID: 1, Kind: corpus.KindEnd},
		{TS: 7, TraceID: 1, SpanID: 20, ServiceID: 0, Kind: corpus.KindEnd},
		{TS: 8, TraceID: 1, SpanID: 10, ServiceID: 0, Kind: corpus.KindEnd},
	}
	endpointPath := writeTestCorpus(t, dir, events, []uint32{3, 3, 7, 7, 7, 7, 3, 3})
	a, err := analyzeCorpus(filepath.Join(dir, "events.bin"), endpointPath, 0, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.MaxConcurrent[pairKey{Service: 1, Endpoint: 7}]; got != 1 {
		t.Fatalf("backend pool = %d, want 1; concurrent calls from separate parents must not add", got)
	}
}
