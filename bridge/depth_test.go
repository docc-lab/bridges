package bridge

import (
	"testing"
)

// driveDepth runs the golden event stream through h and returns the per-event
// golden entries (Baggage/Emit accounting) plus the _d bytes per span hex.
func driveDepth(h Handler, events []streamedEvent, services map[string]uint16) ([]goldenEntry, map[string]int) {
	entries := make([]goldenEntry, 0, len(events))
	depthBySpan := make(map[string]int)
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
			entries = append(entries, goldenEntry{
				Kind: "start", TraceID: e.span.traceHex, SpanID: e.span.spanHex,
				Service: e.span.service, SeqNum: seqNum,
				BaggageFound: r.BaggageFound, BaggageBytes: r.BaggageBytes, EmitBytes: r.EmitBytes,
			})
		} else {
			r := h.OnEnd(ev)
			entries = append(entries, goldenEntry{
				Kind: "end", TraceID: e.span.traceHex, SpanID: e.span.spanHex,
				Service: e.span.service, EmitBytes: r.EmitBytes,
			})
			if r.DepthBytes > 0 {
				depthBySpan[e.span.spanHex] = r.DepthBytes
			}
		}
	}
	return entries, depthBySpan
}

// TestEmitDepthAccounting verifies --emit-depth semantics on the golden input:
//
//  1. All depths in the golden corpus are < 128, so swapping varint(depthMod)
//     for varint(depth) must leave every EmitBytes/BaggageBytes value
//     byte-identical to the flag-off goldens.
//  2. The _d attribute (DepthKeyBytes + varint(depth) = 3 bytes here) must
//     appear on exactly the interior non-checkpoint spans.
//
// Golden topology (depths): A1 root(0) -> {A2 leaf(1), A3(1) -> {A4 leaf(2),
// A5 leaf(2)}}; B1 root(0) -> {B2 leaf(1), B3 leaf(1)}; C1 root(0) -> C2 leaf(1).
//
// All three bridge types share the checkpoint rule depth%cpd == 0 (roots are
// checkpoints), so at cpd 3 the only interior non-checkpoint span is A3.
func TestEmitDepthAccounting(t *testing.T) {
	services := map[string]uint16{"svc1": 1, "svc2": 2}
	events := buildEvents(append([]goldenSpan(nil), goldenInput...))

	cases := []struct {
		name      string
		new       func() Handler
		wantDepth map[string]int // spanHex -> _d bytes
	}{
		{"pb_cpd1", func() Handler {
			h := NewPathBridgeHandler(1, DefaultBloomFPRate)
			h.EmitDepth = true
			return h
		}, map[string]int{}},
		{"pb_cpd3", func() Handler {
			h := NewPathBridgeHandler(3, DefaultBloomFPRate)
			h.EmitDepth = true
			return h
		}, map[string]int{"aaaa000000000003": 3}},
		{"cgpb_cpd1", func() Handler {
			h := NewCGPBBridgeHandler(1, DefaultBloomFPRate)
			h.EmitDepth = true
			return h
		}, map[string]int{}},
		{"cgpb_cpd3", func() Handler {
			h := NewCGPBBridgeHandler(3, DefaultBloomFPRate)
			h.EmitDepth = true
			return h
		}, map[string]int{"aaaa000000000003": 3}},
		{"sb_cpd1", func() Handler {
			h := NewSBridgeHandler(1, nil)
			h.EmitDepth = true
			return h
		}, map[string]int{}},
		{"sb_cpd3", func() Handler {
			h := NewSBridgeHandler(3, nil)
			h.EmitDepth = true
			return h
		}, map[string]int{"aaaa000000000003": 3}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Flag-off reference run for the byte-parity check.
			refName := c.name
			hOff := mustHandlerByName(t, refName, false)
			want := driveHandler(hOff, events, services)

			got, depthBySpan := driveDepth(c.new(), events, services)
			if len(got) != len(want) {
				t.Fatalf("len mismatch: got=%d want=%d", len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("step %d accounting drifted under EmitDepth:\n  got  = %s\n  want = %s",
						i, fmtEntry(got[i]), fmtEntry(want[i]))
				}
			}

			if len(depthBySpan) != len(c.wantDepth) {
				t.Errorf("_d span count: got %d (%v), want %d (%v)",
					len(depthBySpan), depthBySpan, len(c.wantDepth), c.wantDepth)
			}
			for span, wantB := range c.wantDepth {
				if gotB, ok := depthBySpan[span]; !ok || gotB != wantB {
					t.Errorf("_d on %s: got %d bytes (present=%t), want %d", span, gotB, ok, wantB)
				}
			}
			for span := range depthBySpan {
				if _, ok := c.wantDepth[span]; !ok {
					t.Errorf("unexpected _d on %s (%d bytes)", span, depthBySpan[span])
				}
			}
		})
	}
}

func mustHandlerByName(t *testing.T, name string, emitDepth bool) Handler {
	t.Helper()
	switch name {
	case "pb_cpd1":
		h := NewPathBridgeHandler(1, DefaultBloomFPRate)
		h.EmitDepth = emitDepth
		return h
	case "pb_cpd3":
		h := NewPathBridgeHandler(3, DefaultBloomFPRate)
		h.EmitDepth = emitDepth
		return h
	case "cgpb_cpd1":
		h := NewCGPBBridgeHandler(1, DefaultBloomFPRate)
		h.EmitDepth = emitDepth
		return h
	case "cgpb_cpd3":
		h := NewCGPBBridgeHandler(3, DefaultBloomFPRate)
		h.EmitDepth = emitDepth
		return h
	case "sb_cpd1":
		h := NewSBridgeHandler(1, nil)
		h.EmitDepth = emitDepth
		return h
	case "sb_cpd3":
		h := NewSBridgeHandler(3, nil)
		h.EmitDepth = emitDepth
		return h
	}
	t.Fatalf("unknown handler %q", name)
	return nil
}

// TestEmitDepthVarintWidth pins the varint width crossover: a span at depth
// >= 128 pays a 2-byte varint in its _d attribute.
func TestEmitDepthVarintWidth(t *testing.T) {
	h := NewPathBridgeHandler(1000, DefaultBloomFPRate) // cpd large: nothing checkpoints except root
	h.EmitDepth = true

	// Chain of 130 spans: ids 1..130, span i+1 child of i, all in one trace.
	const tid = uint64(0x1)
	n := 130
	for i := 1; i <= n; i++ {
		var pid uint64
		seq := 0
		if i > 1 {
			pid = uint64(i - 1)
			seq = 1
		}
		h.OnStart(&Event{TraceID: tid, SpanID: uint64(i), ParentID: pid, ServiceID: 1}, seq)
	}
	// End in reverse: span n is the leaf (emits _br), the rest are interior.
	for i := n; i >= 1; i-- {
		r := h.OnEnd(&Event{TraceID: tid, SpanID: uint64(i), ParentID: 0, ServiceID: 1})
		depth := i - 1
		switch {
		case i == n: // leaf -> _br, no _d
			if r.DepthBytes != 0 {
				t.Errorf("leaf span got _d bytes %d", r.DepthBytes)
			}
		case depth == 0: // root checkpoint -> no _d
			if r.DepthBytes != 0 {
				t.Errorf("root span got _d bytes %d", r.DepthBytes)
			}
		case depth >= 128:
			if r.DepthBytes != DepthKeyBytes+2 {
				t.Errorf("depth %d: _d bytes = %d, want %d", depth, r.DepthBytes, DepthKeyBytes+2)
			}
		default:
			if r.DepthBytes != DepthKeyBytes+1 {
				t.Errorf("depth %d: _d bytes = %d, want %d", depth, r.DepthBytes, DepthKeyBytes+1)
			}
		}
	}
}
