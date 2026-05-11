package bridge

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
)

// Synthetic input — must match bridge/testdata/gen_golden.py make_traces().
type goldenSpan struct {
	traceHex, spanHex, parentHex string
	startNS, endNS               int64
	service                      string
}

var goldenInput = []goldenSpan{
	// Trace A
	{"aaaaaaaaaaaaaaa1", "aaaa000000000001", "", 100, 1000, "svc1"},
	{"aaaaaaaaaaaaaaa1", "aaaa000000000002", "aaaa000000000001", 120, 500, "svc1"},
	{"aaaaaaaaaaaaaaa1", "aaaa000000000003", "aaaa000000000001", 150, 900, "svc2"},
	{"aaaaaaaaaaaaaaa1", "aaaa000000000004", "aaaa000000000003", 200, 400, "svc1"},
	{"aaaaaaaaaaaaaaa1", "aaaa000000000005", "aaaa000000000003", 300, 800, "svc2"},
	// Trace B
	{"bbbbbbbbbbbbbbb2", "bbbb000000000001", "", 250, 950, "svc2"},
	{"bbbbbbbbbbbbbbb2", "bbbb000000000002", "bbbb000000000001", 280, 600, "svc1"},
	{"bbbbbbbbbbbbbbb2", "bbbb000000000003", "bbbb000000000001", 350, 900, "svc2"},
	// Trace C
	{"cccccccccccccccc", "cccc000000000001", "", 1100, 1500, "svc2"},
	{"cccccccccccccccc", "cccc000000000002", "cccc000000000001", 1150, 1300, "svc1"},
}

// goldenEntry mirrors the per-call JSON entries written by gen_golden.py.
type goldenEntry struct {
	Kind         string `json:"kind"`
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	Service      string `json:"service"`
	SeqNum       int    `json:"seq_num,omitempty"`
	BaggageFound bool   `json:"baggage_found,omitempty"`
	BaggageBytes int    `json:"baggage_bytes,omitempty"`
	EmitBytes    int    `json:"emit_bytes,omitempty"`
}

func parseHex(s string) uint64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		panic(err)
	}
	return v
}

// streamedEvent is one (start or end) event in the simulator's sorted order.
type streamedEvent struct {
	ts       int64
	kind     Kind
	depth    int // sort tie-break, not consumed by handlers
	span     *goldenSpan
	traceID  uint64
	spanID   uint64
	parentID uint64
}

// buildEvents reproduces trace_simulator.build_events + sort_events for the
// synthetic input. depth is the BFS tree depth used as a sort tie-break.
func buildEvents(spans []goldenSpan) []streamedEvent {
	// BFS depth per (traceID, spanID).
	type tk struct {
		t, s uint64
	}
	depths := map[tk]int{}
	traces := map[uint64][]int{} // index list per trace
	for i, sp := range spans {
		traces[parseHex(sp.traceHex)] = append(traces[parseHex(sp.traceHex)], i)
	}
	for _, idxs := range traces {
		// Build child map.
		children := map[uint64][]uint64{}
		spanByID := map[uint64]*goldenSpan{}
		for _, i := range idxs {
			sp := &spans[i]
			sid := parseHex(sp.spanHex)
			pid := parseHex(sp.parentHex)
			spanByID[sid] = sp
			children[pid] = append(children[pid], sid)
		}
		// Roots = spans with parentID == 0.
		var queue []uint64
		for _, i := range idxs {
			sp := &spans[i]
			if sp.parentHex == "" {
				sid := parseHex(sp.spanHex)
				depths[tk{parseHex(sp.traceHex), sid}] = 0
				queue = append(queue, sid)
			}
		}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			tid := parseHex(spanByID[cur].traceHex)
			d := depths[tk{tid, cur}]
			for _, ch := range children[cur] {
				if _, ok := depths[tk{tid, ch}]; !ok {
					depths[tk{tid, ch}] = d + 1
					queue = append(queue, ch)
				}
			}
		}
	}

	out := make([]streamedEvent, 0, len(spans)*2)
	for i := range spans {
		sp := &spans[i]
		tid := parseHex(sp.traceHex)
		sid := parseHex(sp.spanHex)
		pid := parseHex(sp.parentHex)
		d := depths[tk{tid, sid}]
		out = append(out, streamedEvent{ts: sp.startNS, kind: KindStart, depth: d, span: sp, traceID: tid, spanID: sid, parentID: pid})
		out = append(out, streamedEvent{ts: sp.endNS, kind: KindEnd, depth: d, span: sp, traceID: tid, spanID: sid, parentID: pid})
	}
	// Same sort key as Python sort_events: (ts, kind, depth-or-neg-depth, trace_id, span_id).
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.kind != b.kind {
			return a.kind < b.kind // start (0) before end (1)
		}
		// start: shallower first; end: deeper first.
		var ar, br int
		if a.kind == KindStart {
			ar, br = a.depth, b.depth
		} else {
			ar, br = -a.depth, -b.depth
		}
		if ar != br {
			return ar < br
		}
		if a.traceID != b.traceID {
			return a.traceID < b.traceID
		}
		return a.spanID < b.spanID
	})
	return out
}

// driveHandler runs the events through h, recording per-call golden entries
// in the same shape gen_golden.py produces.
func driveHandler(h Handler, events []streamedEvent, services map[string]uint16) []goldenEntry {
	out := make([]goldenEntry, 0, len(events))
	nextSeq := map[stateKey]int{}

	for _, e := range events {
		ev := &Event{
			TraceID:   e.traceID,
			SpanID:    e.spanID,
			ParentID:  e.parentID,
			ServiceID: services[e.span.service],
		}

		if e.kind == KindStart {
			seqNum := 0
			if e.parentID != 0 {
				k := stateKey{e.traceID, e.parentID}
				seqNum = nextSeq[k] + 1
				nextSeq[k] = seqNum
			}
			r := h.OnStart(ev, seqNum)
			out = append(out, goldenEntry{
				Kind:         "start",
				TraceID:      e.span.traceHex,
				SpanID:       e.span.spanHex,
				Service:      e.span.service,
				SeqNum:       seqNum,
				BaggageFound: r.BaggageFound,
				BaggageBytes: r.BaggageBytes,
				EmitBytes:    r.EmitBytes,
			})
		} else {
			r := h.OnEnd(ev)
			out = append(out, goldenEntry{
				Kind:      "end",
				TraceID:   e.span.traceHex,
				SpanID:    e.span.spanHex,
				Service:   e.span.service,
				EmitBytes: r.EmitBytes,
			})
		}
	}
	return out
}

func TestHandlersAgainstPythonGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatalf("read golden.json: %v", err)
	}
	var golden map[string][]goldenEntry
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden.json: %v", err)
	}

	services := map[string]uint16{"svc1": 1, "svc2": 2}
	events := buildEvents(append([]goldenSpan(nil), goldenInput...))

	cases := []struct {
		name string
		new  func() Handler
	}{
		{"vanilla", func() Handler { return NewVanillaHandler() }},
		{"pb_cpd1", func() Handler { return NewPathBridgeHandler(1, DefaultBloomFPRate) }},
		{"pb_cpd3", func() Handler { return NewPathBridgeHandler(3, DefaultBloomFPRate) }},
		{"cgpb_cpd1", func() Handler { return NewCGPBBridgeHandler(1, DefaultBloomFPRate) }},
		{"cgpb_cpd3", func() Handler { return NewCGPBBridgeHandler(3, DefaultBloomFPRate) }},
		{"sb_cpd1", func() Handler { return NewSBridgeHandler(1, nil) }},
		{"sb_cpd3", func() Handler { return NewSBridgeHandler(3, nil) }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := c.new()
			got := driveHandler(h, events, services)
			want := golden[c.name]
			if len(got) != len(want) {
				t.Fatalf("len mismatch: got=%d want=%d", len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("step %d:\n  got  = %s\n  want = %s",
						i, fmtEntry(got[i]), fmtEntry(want[i]))
				}
			}
		})
	}
}

func fmtEntry(e goldenEntry) string {
	return fmt.Sprintf("%-5s tid=%s sid=%s svc=%s seq=%d found=%t bag=%d emit=%d",
		e.Kind, e.TraceID[:8], e.SpanID[:8], e.Service, e.SeqNum, e.BaggageFound, e.BaggageBytes, e.EmitBytes)
}
