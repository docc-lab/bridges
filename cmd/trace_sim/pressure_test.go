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

func pressureTraces() []loader.Trace {
	t := reverseMetricsTrace()
	for i := range t[0].Spans {
		if i == 0 {
			t[0].Spans[i].ServiceID = 0
		} else if i < 3 {
			t[0].Spans[i].ServiceID = 1
		} else {
			t[0].Spans[i].ServiceID = 2
		}
	}
	return append(t, loader.Trace{TraceID: 124, Spans: []loader.Span{{SpanID: 1, ServiceID: 2, StartNS: 110, EndNS: 120}}})
}

func pressureInstanceFor(e streamEvent) uint32 {
	if e.serviceID == 2 {
		if e.spanID == 4 {
			return 20
		}
		return 21
	}
	return uint32(e.serviceID)
}

func runPressureTest(c config) (*checkpointPressure, []TraceMetrics) {
	traces := pressureTraces()
	s := newSimState([]uint64{123, 124}, []int{5, 1}, nil, false, nil)
	p := newCheckpointPressure([]string{"root", "middle", "terminal"}, true)
	s.pressure = p
	h := makeHandler(c, nil, nil)
	for _, e := range buildAndSortEvents(traces) {
		e.pressureInstanceID = pressureInstanceFor(e)
		s.onEvent(h, e)
	}
	return p, s.finalize()
}

func TestPressureOwnerRolesAndAccounting(t *testing.T) {
	for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
		for _, q := range []float64{0, 1} {
			t.Run(mode+formatPythonFloat(q), func(t *testing.T) {
				c := reverseTestConfig(mode, q, 1)
				p, m := runPressureTest(c)
				wantCP, wantLeafCP, wantPromoted := uint64(4), uint64(3), uint64(0)
				if q == 1 {
					wantCP, wantLeafCP, wantPromoted = 3, 1, 1
				}
				x := p.totals
				if p.numTraces != 2 || x.NumSpans != 6 || x.NumLeafSpans != 3 || x.NumNonleafSpans != 3 || x.NumRootSpans != 2 || x.NumRootCheckpointSpans != 2 || x.NumCheckpointSpans != wantCP || x.NumLeafCheckpointSpans != wantLeafCP || x.NumNonleafCheckpointSpans != wantCP-wantLeafCP || x.NumOriginalCheckpointSpans != 2 || x.NumPromotedCheckpointSpans != wantPromoted || x.NumReverseReturnEdges != 4 {
					t.Fatalf("wrong role counts: %+v", x)
				}
				if len(p.instances) != 4 || p.instances[pressureInstanceKey{2, 20}].NumSpans != 1 || p.instances[pressureInstanceKey{2, 21}].NumSpans != 2 {
					t.Fatal("different modeled instances of one service were merged")
				}
				if q == 1 && p.instances[pressureInstanceKey{2, 20}].NumCheckpointSpans != 0 {
					t.Fatal("zero-checkpoint instance missing or wrong")
				}
				if q == 1 && p.services[1].NumPromotedCheckpointSpans != 1 {
					t.Fatal("accepted trusses attributed to origin rather than receiver")
				}
				var own, combined, raw, bag, retRaw, retEncoded, numBag uint64
				for _, metric := range m {
					own += uint64(metric.CheckpointSum)
					bag += uint64(metric.BaggageSum)
					numBag += uint64(metric.NumBaggageCalls)
					combined += uint64(metric.Reverse.CombinedCheckpointSum)
					raw += uint64(metric.CheckpointSum + metric.Reverse.RawTrussSum + metric.Reverse.OriginMetadataSum)
					retRaw += uint64(metric.Reverse.ReverseRawSum)
					retEncoded += uint64(metric.Reverse.ReverseEncodedSum)
				}
				if x.OwnBRBytes != own || x.CombinedCheckpointPayloadBytes != combined || x.RawCheckpointContentBytes != raw || x.ForwardBaggageBytes != bag || x.NumBaggageCalls != numBag || x.ReverseRawBaggageBytes != retRaw || x.ReverseEncodedBaggageBytes != retEncoded {
					t.Fatal("pressure bytes/counts disagree with existing native trace metrics")
				}
				var services, instances, depths pressureCounters
				for _, row := range p.services {
					services.add(*row)
				}
				for _, row := range p.instances {
					instances.add(*row)
				}
				for _, row := range p.depths {
					depths.add(*row)
				}
				if services != x || instances != x || depths != x {
					t.Fatal("population rows do not sum to totals")
				}
				baseline := runInterleavedJSON(pressureTraces(), makeHandler(c, nil, nil), nil, false)
				if !reflect.DeepEqual(m, baseline) {
					t.Fatal("enabling pressure changed native trace metrics")
				}
			})
		}
	}
}

func TestPressureBaselineStartEmissionAndQZeroParity(t *testing.T) {
	for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
		c := reverseTestConfig(mode, 0, .25)
		zero, _ := runPressureTest(c)
		c.reverse = nil
		base, _ := runPressureTest(c)
		if !reflect.DeepEqual(base, zero) {
			t.Fatalf("%s q=0 changed pressure accounting", mode)
		}
		if base.totals.NumOriginalCheckpointSpans != 2 || base.totals.NumForcedLeafCheckpointSpans != 2 || base.totals.OwnBRBytes != base.totals.RawCheckpointContentBytes || base.totals.OwnBRBytes != base.totals.CombinedCheckpointPayloadBytes {
			t.Fatal("ordinary start/end emissions assigned incorrectly")
		}
	}
}

func TestPressureIdentityValidation(t *testing.T) {
	p := newCheckpointPressure([]string{"svc"}, true)
	e := streamEvent{traceID: 1, spanID: 1, pressureInstanceID: 7}
	p.onStart(e, bridge.StartResult{})
	e.pressureInstanceID = 8
	defer func() {
		if recover() == nil {
			t.Fatal("accepted mismatched start/end instance sidecar")
		}
	}()
	p.onEnd(e, bridge.EndResult{})
}

func TestPressureConfig(t *testing.T) {
	valid := config{mode: "pcrb", corpusDir: "corpus", checkpointPressure: "out.json", pressureInstanceIDs: "ids.bin", workers: 1}
	if err := validatePressureConfig(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*config){func(c *config) { c.workers = 2 }, func(c *config) { c.corpusDir = "" }, func(c *config) { c.checkpointPressure = "" }, func(c *config) { c.mode = "vanilla" }} {
		c := valid
		mutate(&c)
		if validatePressureConfig(c) == nil {
			t.Fatalf("invalid pressure configuration accepted: %+v", c)
		}
	}
}

func TestPressureCorpusSidecarAndStandaloneOutput(t *testing.T) {
	dir := t.TempDir()
	eventsPath, metaPath := corpus.Paths(dir)
	events := buildAndSortEvents(pressureTraces())
	w, err := corpus.CreateEvents(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	idsPath := filepath.Join(dir, "instances.bin")
	qw, err := corpus.CreateDEEQueueIDs(idsPath, uint64(len(events)))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if err := w.Write(corpus.Event{TS: e.ts, TraceID: e.traceID, SpanID: e.spanID, ParentID: e.parentID, ServiceID: e.serviceID, Depth: uint16(e.depth), Kind: uint8(e.kind)}); err != nil {
			t.Fatal(err)
		}
		if err := qw.Write(pressureInstanceFor(e)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := qw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := corpus.WriteMeta(metaPath, &corpus.Meta{Services: []string{"root", "middle", "terminal"}, TraceOrder: []uint64{123, 124}, SpanCounts: []uint32{5, 1}}); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
		c := reverseTestConfig(mode, 1, 1)
		c.corpusDir, c.workers = dir, 1
		c.checkpointPressure = filepath.Join(dir, mode+".json")
		if mode == "sb3" {
			c.deeQueueIDs = idsPath
		} else {
			c.pressureInstanceIDs = idsPath
		}
		if m := runFromCorpus(c, nil); m != nil {
			t.Fatal("standalone pressure retained per-trace metrics")
		}
		raw, err := os.ReadFile(c.checkpointPressure)
		if err != nil {
			t.Fatal(err)
		}
		var out checkpointPressureFile
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		if out.Schema != "bridges.checkpoint_pressure.v2" || out.NumTraces != 2 || out.Totals.NumSpans != 6 || len(out.Instances) != 4 || out.DEEInstanceQueues != (mode == "sb3") {
			t.Fatalf("bad standalone output: %+v", out)
		}
		for i, r := range out.Instances {
			if i > 0 && r.ServiceID < out.Instances[i-1].ServiceID {
				t.Fatal("unstable output order")
			}
		}
		if m := runFromCorpus(c, nil); m != nil {
			t.Fatal("standalone pressure retained metrics on repeat")
		}
		again, err := os.ReadFile(c.checkpointPressure)
		if err != nil || string(raw) != string(again) {
			t.Fatal("pressure JSON is not deterministic")
		}
	}
	badPath := filepath.Join(dir, "wrong-count.bin")
	bad, err := corpus.CreateDEEQueueIDs(badPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Close(); err != nil {
		t.Fatal(err)
	}
	if r, err := openPressureInstanceIDs(badPath, eventsPath); err == nil {
		r.Close()
		t.Fatal("sidecar with wrong event count accepted")
	}
}
