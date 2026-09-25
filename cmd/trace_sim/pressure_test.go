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
	p := newCheckpointPressure([]string{"root", "middle", "terminal"}, true, true)
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
	p := newCheckpointPressure([]string{"svc"}, true, true)
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

// The joint (service, depth) cells must reproduce both marginals exactly. If
// they did not, the service-level depth analysis would silently disagree with
// the service and depth tables printed from the same run.
func TestServiceDepthCellsReproduceBothMarginals(t *testing.T) {
	for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
		for _, q := range []float64{0, 1} {
			t.Run(mode+formatPythonFloat(q), func(t *testing.T) {
				p, _ := runPressureTest(reverseTestConfig(mode, q, 1))
				if len(p.serviceDepths) == 0 {
					t.Fatal("no joint cells accumulated")
				}
				bySvc := map[uint16]uint64{}
				byDepth := map[int]uint64{}
				var spans, bytes uint64
				for k, c := range p.serviceDepths {
					bySvc[k.serviceID] += c.NumCheckpointSpans
					byDepth[int(k.depth)] += c.NumCheckpointSpans
					spans += c.NumSpans
					bytes += c.CombinedCheckpointPayloadBytes
				}
				for id, want := range p.services {
					if got := bySvc[id]; got != want.NumCheckpointSpans {
						t.Errorf("service %d: joint cells give %d checkpoint spans, marginal says %d",
							id, got, want.NumCheckpointSpans)
					}
				}
				for d, want := range p.depths {
					if got := byDepth[d]; got != want.NumCheckpointSpans {
						t.Errorf("depth %d: joint cells give %d checkpoint spans, marginal says %d",
							d, got, want.NumCheckpointSpans)
					}
				}
				if spans != p.totals.NumSpans {
					t.Errorf("joint cells cover %d spans, totals say %d", spans, p.totals.NumSpans)
				}
				if bytes != p.totals.CombinedCheckpointPayloadBytes {
					t.Errorf("joint cells give %d payload bytes, totals say %d",
						bytes, p.totals.CombinedCheckpointPayloadBytes)
				}
			})
		}
	}
}

// Without the flag the joint map stays empty and the JSON omits the array, so
// existing pressure outputs are unchanged.
func TestServiceDepthCellsAreOptIn(t *testing.T) {
	s := newSimState([]uint64{123, 124}, []int{5, 1}, nil, false, nil)
	p := newCheckpointPressure([]string{"root", "middle", "terminal"}, true, false)
	s.pressure = p
	h := makeHandler(reverseTestConfig("pcrb", 1, 1), nil, nil)
	for _, e := range buildAndSortEvents(pressureTraces()) {
		e.pressureInstanceID = pressureInstanceFor(e)
		s.onEvent(h, e)
	}
	s.finalize()
	if len(p.serviceDepths) != 0 {
		t.Fatalf("joint cells accumulated without the flag: %d", len(p.serviceDepths))
	}
	if p.totals.NumSpans == 0 {
		t.Fatal("fixture produced no spans, so the check is vacuous")
	}
}

// The depth moments must be plain sums over the same spans every other counter
// covers, so a mean computed from any row matches the spans that row aggregates.
func TestSpanDepthMomentsMatchSpans(t *testing.T) {
	p, _ := runPressureTest(reverseTestConfig("pcrb", 1, 1))
	var sum, sq, n uint64
	for d, c := range p.depths {
		if c.SpanDepthSum != uint64(d)*c.NumSpans {
			t.Errorf("depth %d: sum %d, want %d", d, c.SpanDepthSum, uint64(d)*c.NumSpans)
		}
		if c.SpanDepthSqSum != uint64(d*d)*c.NumSpans {
			t.Errorf("depth %d: sq sum %d, want %d", d, c.SpanDepthSqSum, uint64(d*d)*c.NumSpans)
		}
		sum += c.SpanDepthSum
		sq += c.SpanDepthSqSum
		n += c.NumSpans
	}
	if sum != p.totals.SpanDepthSum || sq != p.totals.SpanDepthSqSum || n != p.totals.NumSpans {
		t.Fatalf("depth marginal gives (%d,%d,%d), totals say (%d,%d,%d)",
			sum, sq, n, p.totals.SpanDepthSum, p.totals.SpanDepthSqSum, p.totals.NumSpans)
	}
	var isum, in uint64
	for _, c := range p.instances {
		isum += c.SpanDepthSum
		in += c.NumSpans
	}
	if isum != sum || in != n {
		t.Fatalf("instance marginal gives (%d,%d), depth marginal says (%d,%d)", isum, in, sum, n)
	}
}

// Height must be the longest path down to a leaf, so relative path position is
// 0 at the root and exactly 1 at every leaf whatever the trace shape. Those two
// endpoints are what make the coordinate readable, so they are pinned.
func TestRelativePositionEndpoints(t *testing.T) {
	p, _ := runPressureTest(reverseTestConfig("pcrb", 1, 1))
	var leafSpans, rootSpans uint64
	for d, c := range p.depths {
		if d == 0 {
			rootSpans += c.NumSpans
			if c.RelPathSum != 0 || c.RelTraceSum != 0 {
				t.Errorf("root depth cell has nonzero relative position: path %d trace %d",
					c.RelPathSum, c.RelTraceSum)
			}
		}
		leafSpans += c.NumLeafSpans
	}
	if rootSpans == 0 || leafSpans == 0 {
		t.Fatal("fixture has no root or no leaf spans, so the check is vacuous")
	}
	// Every leaf sits at path position 1.0 except a single-span trace, whose one
	// span is both root and leaf and is reported at 0.
	for _, c := range p.instances {
		if c.RelPathSum > c.NumSpans*relScale {
			t.Fatalf("relative path position exceeds 1.0: sum %d over %d spans",
				c.RelPathSum, c.NumSpans)
		}
		if c.RelPathSqSum > c.NumSpans*relScale*relScale {
			t.Fatalf("squared position exceeds 1.0: sum %d over %d spans",
				c.RelPathSqSum, c.NumSpans)
		}
	}
	// The deepest spans in a trace can only be leaves, so their path position is
	// pinned at exactly 1.0. That is the endpoint the coordinate is read against.
	deepest := -1
	for d := range p.depths {
		if d > deepest {
			deepest = d
		}
	}
	if c := p.depths[deepest]; c.RelPathSum != c.NumSpans*relScale {
		t.Fatalf("deepest cell (depth %d): path sum %d over %d spans, want %d",
			deepest, c.RelPathSum, c.NumSpans, c.NumSpans*relScale)
	}
	// Marginals must agree, as for every other counter.
	var dsum, isum uint64
	for _, c := range p.depths {
		dsum += c.RelPathSum
	}
	for _, c := range p.instances {
		isum += c.RelPathSum
	}
	if dsum != isum || dsum != p.totals.RelPathSum {
		t.Fatalf("relative position marginals disagree: depths %d instances %d totals %d",
			dsum, isum, p.totals.RelPathSum)
	}
}

// The whole point of binning spans rather than instances is that the endpoints
// are exact: bin 0 holds the trace roots, which always checkpoint, and the last
// bin holds every leaf. An instance-level mean blends positions and loses this.
func TestRelPathBinsAreExactAtTheEndpoints(t *testing.T) {
	p, _ := runPressureTest(reverseTestConfig("pcrb", 1, 1))
	root := p.relPathBins[0]
	if root == nil {
		t.Fatal("no root bin accumulated")
	}
	if root.NumRootSpans != p.totals.NumRootSpans {
		t.Errorf("bin 0 holds %d root spans, trace has %d", root.NumRootSpans, p.totals.NumRootSpans)
	}
	if root.NumRootCheckpointSpans != root.NumRootSpans {
		t.Errorf("a trace root must always checkpoint: %d of %d in bin 0",
			root.NumRootCheckpointSpans, root.NumRootSpans)
	}
	last := p.relPathBins[relPathBinCount-1]
	if last == nil {
		t.Fatal("no leaf bin accumulated")
	}
	if last.NumLeafSpans != last.NumSpans {
		t.Errorf("the last bin must be all leaves: %d leaves of %d spans",
			last.NumLeafSpans, last.NumSpans)
	}
	// Every leaf in the trace lands there, except a single-span trace whose one
	// span is root and leaf at once and is binned at 0.
	var leaves uint64
	for _, c := range p.depths {
		leaves += c.NumLeafSpans
	}
	if last.NumLeafSpans+root.NumLeafSpans != leaves {
		t.Errorf("leaves split across bins: last %d + root %d != %d",
			last.NumLeafSpans, root.NumLeafSpans, leaves)
	}
	// And the marginal must cover the same population as every other one.
	var spans uint64
	for _, c := range p.relPathBins {
		spans += c.NumSpans
	}
	if spans != p.totals.NumSpans {
		t.Fatalf("rel-path bins cover %d spans, totals say %d", spans, p.totals.NumSpans)
	}
}
