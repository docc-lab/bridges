package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"bridges/corpus"
)

func TestDecodeTracesAcceptsBothShapes(t *testing.T) {
	wrapped := []byte(`{"data":[{"traceID":"aa","spans":[{"spanID":"bb","operationName":"Op1","processID":"p1"}],"processes":{"p1":{"serviceName":"S"}}}]}`)
	got, err := decodeTraces(wrapped)
	if err != nil || len(got) != 1 || got[0].TraceID != "aa" || got[0].Spans[0].OperationName != "Op1" {
		t.Fatalf("wrapped: %v %+v", err, got)
	}

	bare := []byte(`{"traceID":"cc","spans":[{"spanID":"dd","operationName":"Op2","processID":"p1"}]}`)
	got, err = decodeTraces(bare)
	if err != nil || len(got) != 1 || got[0].TraceID != "cc" || got[0].Spans[0].OperationName != "Op2" {
		t.Fatalf("bare: %v %+v", err, got)
	}

	if _, err := decodeTraces([]byte(`{"data":[]}`)); err == nil {
		t.Fatal("empty data array should not decode as a trace")
	}
	if _, err := decodeTraces([]byte(`not json`)); err == nil {
		t.Fatal("garbage should fail")
	}
}

// writeRecords lays down the fixed 20-byte (traceID, spanID, endpointID) form
// deliberately out of order, so the join's sort is actually exercised.
func writeRecords(t *testing.T, path string, recs [][3]uint64) {
	t.Helper()
	buf := make([]byte, 0, len(recs)*recSize)
	for _, r := range recs {
		var b [recSize]byte
		binary.LittleEndian.PutUint64(b[0:8], r[0])
		binary.LittleEndian.PutUint64(b[8:16], r[1])
		binary.LittleEndian.PutUint32(b[16:20], uint32(r[2]))
		buf = append(buf, b[:]...)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeCorpus(t *testing.T, dir string, events []corpus.Event) {
	t.Helper()
	eventsPath, _ := corpus.Paths(dir)
	w, err := corpus.CreateEvents(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if err := w.Write(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJoinEmitsOneIDPerEventInCorpusOrder(t *testing.T) {
	dir := t.TempDir()
	// Two spans, each with a start and an end event, interleaved.
	writeCorpus(t, dir, []corpus.Event{
		{TS: 1, TraceID: 0x10, SpanID: 0xaa, Kind: 0},
		{TS: 2, TraceID: 0x10, SpanID: 0xbb, Kind: 0},
		{TS: 3, TraceID: 0x10, SpanID: 0xbb, Kind: 1},
		{TS: 4, TraceID: 0x10, SpanID: 0xaa, Kind: 1},
	})
	recs := filepath.Join(dir, "recs")
	// Reverse order on purpose, plus an extra span the corpus never references:
	// surplus records are normal (the raw archive holds spans cleaning removed).
	writeRecords(t, recs, [][3]uint64{
		{0x10, 0xbb, 7},
		{0x99, 0xff, 3},
		{0x10, 0xaa, 4},
	})

	out := filepath.Join(dir, "endpoints.bin")
	if err := runJoin(recs, dir, out, 0); err != nil {
		t.Fatalf("join: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4*4 {
		t.Fatalf("want 16 bytes (4 events, headerless), got %d", len(got))
	}
	want := []uint32{4, 7, 7, 4} // aa,bb,bb,aa in corpus event order
	for i, w := range want {
		if v := binary.LittleEndian.Uint32(got[i*4 : i*4+4]); v != w {
			t.Errorf("event %d: got %d want %d", i, v, w)
		}
	}
}

func TestJoinRefusesWhenACorpusEventHasNoRawSpan(t *testing.T) {
	dir := t.TempDir()
	writeCorpus(t, dir, []corpus.Event{
		{TS: 1, TraceID: 0x10, SpanID: 0xaa},
		{TS: 2, TraceID: 0x10, SpanID: 0xcc}, // no record for this one
	})
	recs := filepath.Join(dir, "recs")
	writeRecords(t, recs, [][3]uint64{{0x10, 0xaa, 1}})

	out := filepath.Join(dir, "endpoints.bin")
	if err := runJoin(recs, dir, out, 0); err == nil {
		t.Fatal("expected a refusal: an unmatched corpus event means invented ids")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("a refused join must leave no output file behind")
	}
}
