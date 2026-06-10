package loader

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// span builds a raw Jaeger span JSON object. parent == "" means no CHILD_OF.
func span(id, parent string, extraParents ...string) map[string]any {
	refs := []map[string]any{}
	if parent != "" {
		refs = append(refs, map[string]any{"refType": "CHILD_OF", "spanID": parent})
	}
	for _, p := range extraParents {
		refs = append(refs, map[string]any{"refType": "CHILD_OF", "spanID": p})
	}
	return map[string]any{
		"spanID":     id,
		"references": refs,
		"startTime":  int64(1700000000000000),
		"duration":   int64(1000),
		"processID":  "p1",
	}
}

func writeTrace(t *testing.T, spans []map[string]any) string {
	t.Helper()
	tr := map[string]any{
		"traceID":   "abc123",
		"spans":     spans,
		"processes": map[string]any{"p1": map[string]any{"serviceName": "svc"}},
	}
	data, err := json.Marshal(map[string]any{"data": []any{tr}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "trace.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadClean(t *testing.T, spans []map[string]any) []Trace {
	t.Helper()
	traces, err := LoadTraceFile(writeTrace(t, spans), NewServiceTable(), true)
	if err != nil {
		t.Fatal(err)
	}
	return traces
}

func spanIDs(tr Trace) map[uint64]bool {
	out := make(map[uint64]bool, len(tr.Spans))
	for _, s := range tr.Spans {
		out[s.SpanID] = true
	}
	return out
}

func TestCleanSingleRootKept(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		span("a1", ""),
		span("a2", "a1"),
		span("a3", "a1"),
	})
	if len(traces) != 1 || len(traces[0].Spans) != 3 {
		t.Fatalf("want 1 trace with 3 spans, got %+v", traces)
	}
}

func TestCleanSingleRootDanglingStillRejected(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		span("a1", ""),
		span("a2", "a1"),
		span("a3", "dead"), // dangling parent, single-root trace
	})
	if len(traces) != 0 {
		t.Fatalf("want trace rejected, got %+v", traces)
	}
}

func TestCleanMultiRootKeepsBiggestTree(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		// tree A: 2 spans
		span("a1", ""),
		span("a2", "a1"),
		// tree B: 3 spans — biggest, should be kept
		span("b1", ""),
		span("b2", "b1"),
		span("b3", "b2"),
	})
	if len(traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(traces))
	}
	ids := spanIDs(traces[0])
	if len(ids) != 3 || !ids[0xb1] || !ids[0xb2] || !ids[0xb3] {
		t.Fatalf("want spans {b1,b2,b3}, got %v", ids)
	}
}

func TestCleanMultiRootDanglingRejected(t *testing.T) {
	// C2 is trace-fatal and checked before multi-root salvage: a dangling
	// parent anywhere rejects the trace even if the biggest tree is clean.
	traces := loadClean(t, []map[string]any{
		span("a1", ""),
		span("a2", "a1"),
		span("b1", ""),
		span("c1", "dead"),
	})
	if len(traces) != 0 {
		t.Fatalf("want trace rejected (dangling parent before salvage), got %+v", traces)
	}
}

func TestCleanMultiRootTieLowestRootID(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		span("b1", ""),
		span("b2", "b1"),
		span("a1", ""),
		span("a2", "a1"),
	})
	if len(traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(traces))
	}
	ids := spanIDs(traces[0])
	if len(ids) != 2 || !ids[0xa1] || !ids[0xa2] {
		t.Fatalf("want tie broken to root a1, got %v", ids)
	}
}

func TestCleanMultiRootDirtyKeptTreeRejected(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		// biggest tree contains a span with two distinct CHILD_OF parents
		span("a1", ""),
		span("a2", "a1"),
		span("a3", "a1", "a2"),
		// smaller clean tree
		span("b1", ""),
	})
	if len(traces) != 0 {
		t.Fatalf("want trace rejected (multi-parent in kept tree), got %+v", traces)
	}
}

func TestCleanMultiRootDirtyDroppedTreeIgnored(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		// biggest tree is clean
		span("a1", ""),
		span("a2", "a1"),
		span("a3", "a2"),
		// smaller tree has a multi-parent span — dropped, must not reject
		span("b1", ""),
		span("b2", "b1", "a1"),
	})
	if len(traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(traces))
	}
	ids := spanIDs(traces[0])
	if len(ids) != 3 || !ids[0xa1] || !ids[0xa2] || !ids[0xa3] {
		t.Fatalf("want spans {a1,a2,a3}, got %v", ids)
	}
}

func TestCleanDuplicateSpanIDStillRejected(t *testing.T) {
	traces := loadClean(t, []map[string]any{
		span("a1", ""),
		span("a2", "a1"),
		span("a2", "a1"),
	})
	if len(traces) != 0 {
		t.Fatalf("want trace rejected (duplicate span ID), got %+v", traces)
	}
}

func TestNoCleanPassesMultiRootThrough(t *testing.T) {
	path := writeTrace(t, []map[string]any{
		span("a1", ""),
		span("b1", ""),
		span("b2", "b1"),
	})
	traces, err := LoadTraceFile(path, NewServiceTable(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != 1 || len(traces[0].Spans) != 3 {
		t.Fatalf("want all 3 spans without cleaning, got %+v", traces)
	}
}
