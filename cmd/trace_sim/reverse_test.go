package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/loader"
)

func reverseMetricsTrace() []loader.Trace {
	return []loader.Trace{{TraceID: 123, Spans: []loader.Span{
		{SpanID: 1, StartNS: 0, EndNS: 100, Depth: 0},
		{SpanID: 2, ParentID: 1, StartNS: 1, EndNS: 99, Depth: 1},
		{SpanID: 3, ParentID: 2, StartNS: 2, EndNS: 98, Depth: 2},
		{SpanID: 4, ParentID: 3, StartNS: 3, EndNS: 90, Depth: 3},
		{SpanID: 5, ParentID: 3, StartNS: 4, EndNS: 91, Depth: 3},
	}}}
}
func reverseTestConfig(mode string, q, p float64) config {
	return config{mode: mode, checkpointDistance: 8, prefixLen: 8, bloomFP: bridge.DefaultBloomFPRate,
		emitDepth: true, emitOC: true, fpBits: 64, lehmerEE: true, deeDequeueOne: true,
		reverse: &bridge.ReverseConfig{Policy: "probability", LeafRejectProbability: q, Probability: p, Seed: 42}}
}
func TestReverseZeroRejectionPreservesBaselineMetrics(t *testing.T) {
	for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
		t.Run(mode, func(t *testing.T) {
			c := reverseTestConfig(mode, 0, .25)
			h := newSizeHistograms()
			got := runInterleavedJSON(reverseMetricsTrace(), makeHandler(c, nil, nil), h, false)[0]
			c.reverse = nil
			base := runInterleavedJSON(reverseMetricsTrace(), makeHandler(c, nil, nil), nil, false)[0]
			r := got.Reverse
			if r == nil || r.OriginalCheckpointSpans != 1 || r.ForcedLeafCheckpointSpans != 2 || r.RejectedLeaves != 0 || r.ReverseEncodedSum != 0 {
				t.Fatalf("q=0 reverse metrics: %+v", r)
			}
			got.Reverse = nil
			if !reflect.DeepEqual(got, base) {
				t.Fatalf("q=0 changed base metrics: got %+v want %+v", got, base)
			}
			if h.reverse["reverse_encoded_baggage_bytes"][0] != 4 {
				t.Fatalf("empty returns not sampled: %+v", h.reverse)
			}
		})
	}
}
func TestReversePromotionCountsAndHistogramAccounting(t *testing.T) {
	for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
		for _, p := range []float64{0, 1} {
			t.Run(mode+formatPythonFloat(p), func(t *testing.T) {
				c := reverseTestConfig(mode, 1, p)
				h := newSizeHistograms()
				m := runInterleavedJSON(reverseMetricsTrace(), makeHandler(c, nil, nil), h, false)[0]
				r := m.Reverse
				wantCheckpoints, wantPromoted, wantDistance, wantNonempty := 1, 0, 6, 4
				if p == 1 {
					wantCheckpoints, wantPromoted, wantDistance, wantNonempty = 2, 1, 2, 2
				}
				if m.NumCheckpointSpans != wantCheckpoints || r.OriginalCheckpointSpans != 1 || r.PromotedCheckpointSpans != wantPromoted || r.ReceivingCheckpointSpans != 1 || r.ForcedLeafCheckpointSpans != 0 || r.RejectedLeaves != 2 || r.ReturnedTrusses != 2 || r.AcceptedTrusses != 2 || r.MaxAcceptedTrusses != 2 || r.NumReverseReturnEdges != 4 || r.NumNonemptyReverseReturnEdges != wantNonempty || r.ReverseDistanceSum != wantDistance {
					t.Fatalf("metrics: %+v %+v", m, r)
				}
				if r.CombinedCheckpointSum <= m.CheckpointSum || r.ReverseEncodedSum <= r.ReverseRawSum {
					t.Fatalf("envelope bytes missing: %+v", r)
				}
				d := snapshotReverseHistograms(h.reverse)
				for name, want := range map[string]int{"raw_truss_bytes": 2, "origin_metadata_bytes": 2, "combined_checkpoint_payload_bytes": wantCheckpoints, "returned_bundle_raw_bytes": wantNonempty, "reverse_raw_baggage_bytes": 4, "reverse_encoded_baggage_bytes": 4, "checkpoint_spans_per_trace": 1, "reverse_distance_span_hops": 2} {
					if d[name].Count != uint64(want) {
						t.Errorf("%s count=%d want%d", name, d[name].Count, want)
					}
				}
				if d["combined_checkpoint_payload_bytes"].Sum != uint64(r.CombinedCheckpointSum) || d["reverse_encoded_baggage_bytes"].Sum != uint64(r.ReverseEncodedSum) || d["reverse_raw_baggage_bytes"].Sum != uint64(r.ReverseRawSum) || d["checkpoint_spans_per_trace"].Sum != uint64(m.NumCheckpointSpans) {
					t.Fatalf("histogram totals differ from trace totals: %+v", d)
				}
			})
		}
	}
}
func TestReverseCSVJSONAndHistogramRoundTrip(t *testing.T) {
	c := reverseTestConfig("cgprb", 1, 0)
	c.checkpointPolicy = &bridge.CheckpointRange{Min: 2, Max: 8, Seed: 91}
	h := newSizeHistograms()
	m := runInterleavedJSON(reverseMetricsTrace(), makeHandler(c, nil, nil), h, false)
	dir := t.TempDir()
	direct, csv, converted := filepath.Join(dir, "direct.json"), filepath.Join(dir, "metrics.csv"), filepath.Join(dir, "converted.json")
	if err := writeBagsizeJSONWithConfig(direct, c, m); err != nil {
		t.Fatal(err)
	}
	sw, err := newStreamWriterWithConfig(csv, c)
	if err != nil {
		t.Fatal(err)
	}
	sw.writeRec(123, &m[0])
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
		t.Fatal("reverse metadata/metrics CSV round-trip changed output")
	}
	path := filepath.Join(dir, "hist.json")
	if err := writeSizeHistograms(path, c, h); err != nil {
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
	if hist.Schema != "bridges.size_histograms.v3" || !reflect.DeepEqual(hist.ReverseConfig, c.reverse) || hist.ReverseHistograms["reverse_encoded_baggage_bytes"].Sum != uint64(m[0].Reverse.ReverseEncodedSum) {
		t.Fatalf("bad histogram roundtrip: %+v", hist)
	}
	l, err := loadLabeledHistogram("cg="+path, "reverse_encoded_baggage_bytes")
	if err != nil {
		t.Fatal(err)
	}
	if l.hist.Count != uint64(m[0].Reverse.NumReverseReturnEdges) {
		t.Fatalf("histtable wrong reverse distribution")
	}
	if !compatibleHistogramFiles(hist, hist) {
		t.Fatal("identical configs incompatible")
	}
	changed := hist
	cfg := *hist.ReverseConfig
	cfg.Seed++
	changed.ReverseConfig = &cfg
	if compatibleHistogramFiles(hist, changed) {
		t.Fatal("histmerge accepts different reverse seed")
	}
	merged := filepath.Join(dir, "merged.json")
	runHistogramMerge([]string{merged, path, path})
	b, err = os.ReadFile(merged)
	if err != nil {
		t.Fatal(err)
	}
	var doubled sizeHistogramFile
	if err := json.Unmarshal(b, &doubled); err != nil {
		t.Fatal(err)
	}
	if doubled.ReverseHistograms["reverse_encoded_baggage_bytes"].Count != 2*hist.ReverseHistograms["reverse_encoded_baggage_bytes"].Count || len(doubled.ReverseRouteCounts) != len(hist.ReverseRouteCounts) {
		t.Fatal("histmerge lost reverse counts")
	}
	for i, r := range hist.ReverseRouteCounts {
		if doubled.ReverseRouteCounts[i].Count != 2*r.Count {
			t.Fatal("histmerge lost origin/depth strata")
		}
	}
}
func TestReverseFlagConfiguration(t *testing.T) {
	c := config{mode: "pcrb", checkpointDistance: 8, checkpointPolicy: &bridge.CheckpointRange{Min: 2, Max: 8}}
	ttl, err := parseReverseConfig(c, "ttl", 1, optionalProbability{}, "", 42)
	if err != nil || ttl.TTLMin != 2 || ttl.TTLMax != 8 {
		t.Fatalf("forward range not inherited: %+v %v", ttl, err)
	}
	if _, err := parseReverseConfig(c, "probability", 1, optionalProbability{}, "", 42); err == nil {
		t.Fatal("constant probability must be explicit")
	}
	if _, err := parseReverseConfig(c, "inverse_depth", 1, optionalProbability{set: true}, "", 42); err == nil {
		t.Fatal("nonconstant policy accepts probability argument")
	}
	if _, err := parseReverseConfig(c, "probability", 1, optionalProbability{set: true}, "", 42); err != nil {
		t.Fatal(err)
	}
	if _, err := parseReverseConfig(c, "ttl", 1, optionalProbability{}, "2:9", 42); err != nil {
		t.Fatal(err)
	}
	if r, err := parseReverseConfig(c, "ttl", 1, optionalProbability{}, "1:256", 42); err != nil || r.TTLMin != 1 || r.TTLMax != 256 {
		t.Fatalf("reverse TTL range unnecessarily constrained by forward packed format: %+v %v", r, err)
	}
	if _, err := parseReverseConfig(c, "ttl", 1, optionalProbability{}, "1:257", 42); err == nil {
		t.Fatal("reverse TTL range exceeds one-byte countdown")
	}
}

func TestReverseShardedHistogramOnly(t *testing.T) {
	traces := reverseMetricsTrace()
	second := traces[0]
	second.TraceID++
	traces = append(traces, second)
	path := filepath.Join(t.TempDir(), "events.bin")
	w, err := corpus.CreateEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range buildAndSortEvents(traces) {
		if err := w.Write(corpus.Event{TS: e.ts, TraceID: e.traceID, SpanID: e.spanID, ParentID: e.parentID, ServiceID: e.serviceID, Depth: uint16(e.depth), Kind: uint8(e.kind)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	er, err := corpus.OpenEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	defer er.Close()
	c := reverseTestConfig("pcrb", 1, 1)
	h := newSizeHistograms()
	meta := &corpus.Meta{TraceOrder: []uint64{123, 124}, SpanCounts: []uint32{5, 5}}
	m := runShardedFromCorpus(er, meta, func() bridge.Handler { return makeHandler(c, nil, nil) }, c, 2, nil, h)
	if m != nil {
		t.Fatal("histogram-only run retained per-trace metrics")
	}
	d := snapshotReverseHistograms(h.reverse)
	if d["checkpoint_spans_per_trace"].Count != 2 || d["checkpoint_spans_per_trace"].Sum != 4 || d["reverse_encoded_baggage_bytes"].Count != 8 {
		t.Fatalf("sharded histogram totals: %+v", d)
	}
}
