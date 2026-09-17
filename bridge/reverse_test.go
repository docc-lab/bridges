package bridge

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func reverseTestBase(mode string, cpd int) Handler {
	switch mode {
	case "pb":
		return NewPCRBBridgeHandler(cpd, 8, 0.0001)
	case "cg":
		return NewCGPRBBridgeHandler(cpd, 8, 0.0001)
	default:
		h := NewSB3Handler(cpd, 8, 0.0001, nil)
		h.LehmerEE = true
		h.UseDEEQueueID = true
		h.DequeueOneDEE = true
		return h
	}
}
func reverseTestHandler(t *testing.T, mode string, cpd int, c ReverseConfig) *ReverseHandler {
	t.Helper()
	h, err := NewReverseHandler(reverseTestBase(mode, cpd), c)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func reverseTestChain(t *testing.T, mode string, c ReverseConfig) (map[uint64]EndResult, map[uint64][]byte) {
	t.Helper()
	h := reverseTestHandler(t, mode, 8, c)
	base := reverseTestBase(mode, 8)
	switch b := base.(type) {
	case *PCRBBridgeHandler:
		b.Capture = true
	case *CGPRBBridgeHandler:
		b.Capture = true
	case *SB3Handler:
		b.Capture = true
	}
	events := make([]Event, 7)
	baseline := make(map[uint64][]byte)
	for d := 0; d <= 6; d++ {
		ev := Event{TraceID: 42, SpanID: uint64(d + 1), ParentID: uint64(d), ServiceID: 1, DEEQueueID: 1}
		events[d] = ev
		normal := base.OnStart(&ev, 1)
		wrapped := h.OnStart(&ev, 1)
		if wrapped.BaggageBytes != normal.BaggageBytes || wrapped.CheckpointTTL != normal.CheckpointTTL || wrapped.BaggageFound != normal.BaggageFound {
			t.Fatal("reverse changed forward baggage or scheduling")
		}
		if wrapped.EmitBytes != 0 || len(wrapped.Payload) > 0 {
			t.Fatal("start emission was not deferred")
		}
		if len(normal.Payload) > 0 {
			baseline[ev.SpanID] = normal.Payload
		}
	}
	out := make(map[uint64]EndResult)
	for d := 6; d >= 0; d-- {
		ev := events[d]
		normal := base.OnEnd(&ev)
		if len(normal.Payload) > 0 {
			baseline[ev.SpanID] = normal.Payload
		}
		out[ev.SpanID] = h.OnEnd(&ev)
	}
	h.EvictTrace(42)
	if len(h.state) != 0 {
		t.Fatal("reverse state retained after eviction")
	}
	return out, baseline
}

func TestReverseProbabilityControlsAndOwnPayload(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		t.Run(mode, func(t *testing.T) {
			for _, p := range []float64{0, 1} {
				out, baseline := reverseTestChain(t, mode, ReverseConfig{Policy: "probability", Probability: p, LeafRejectProbability: 1, Seed: 9})
				rejected := out[7]
				if !rejected.Reverse.RejectedLeaf || rejected.EmitBytes != 0 || rejected.DepthBytes == 0 {
					t.Fatal("rejected leaf must retain ordinary depth and return its payload")
				}
				receiver := uint64(1)
				if p == 1 {
					receiver = 6
				}
				acceptedCount := 0
				for sid, result := range out {
					rr := result.Reverse
					acceptedCount += len(rr.Accepted)
					if len(rr.Accepted) > 0 {
						if sid != receiver || len(rr.Accepted) != 1 {
							t.Fatalf("p=%v accepted at %d, want %d", p, sid, receiver)
						}
						segment := rr.Accepted[0]
						if segment.OriginSpanID != 7 || segment.OriginDepth != 6 || !bytes.Equal(segment.Payload, baseline[7]) {
							t.Fatal("origin or payload changed in transit")
						}
						if result.EmitBytes == 0 || len(result.Payload) == 0 || result.DepthBytes != 0 {
							t.Fatal("receiver must emit its own checkpoint payload")
						}
						if rr.PromotedCheckpoint != (p == 1) || rr.OriginalCheckpoint != (p == 0) {
							t.Fatal("original checkpoint identity changed")
						}
						if rr.Routes[0].Distance != 6-int(receiver-1) {
							t.Fatal("wrong reverse distance")
						}
						if rr.CheckpointBytes <= result.EmitBytes {
							t.Fatal("exported payload did not include returned envelope")
						}
					}
					if sid != 1 && !rr.ReturnEdge {
						t.Fatal("missing zero-inclusive return edge")
					}
				}
				if acceptedCount != 1 {
					t.Fatalf("emitted rejected truss %d times", acceptedCount)
				}
			}
		})
	}
}

func TestReverseZeroRejectionPreservesCheckpointOutputs(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		out, baseline := reverseTestChain(t, mode, ReverseConfig{Policy: "inverse_depth", LeafRejectProbability: 0})
		for sid, r := range out {
			if !bytes.Equal(r.Payload, baseline[sid]) {
				t.Fatalf("%s span %d payload changed with q=0", mode, sid)
			}
			if len(r.Reverse.Accepted) > 0 || len(r.Reverse.Returned) > 0 || r.Reverse.CheckpointBytes != r.EmitBytes {
				t.Fatal("q=0 produced reverse traffic")
			}
		}
	}
}

func TestReverseOriginalCheckpointStopsTTLAndProbability(t *testing.T) {
	for _, c := range []ReverseConfig{
		{Policy: "probability", Probability: 0, LeafRejectProbability: 1},
		{Policy: "ttl", TTLMin: 8, TTLMax: 8, LeafRejectProbability: 1},
	} {
		h := reverseTestHandler(t, "pb", 2, c)
		events := []Event{{TraceID: 1, SpanID: 1}, {TraceID: 1, SpanID: 2, ParentID: 1}, {TraceID: 1, SpanID: 3, ParentID: 2}, {TraceID: 1, SpanID: 4, ParentID: 3}}
		for i := range events {
			h.OnStart(&events[i], 1)
		}
		leaf := h.OnEnd(&events[3])
		ckpt := h.OnEnd(&events[2])
		if !leaf.Reverse.RejectedLeaf || !ckpt.Reverse.OriginalCheckpoint || len(ckpt.Reverse.Accepted) != 1 || len(ckpt.Reverse.Returned) != 0 {
			t.Fatal("original checkpoint did not absorb return")
		}
		if ckpt.Reverse.Routes[0].OriginalCheckpointDepth != 2 || !ckpt.Reverse.Routes[0].Mandatory {
			t.Fatal("incorrect boundary route metadata")
		}
		for i := 1; i >= 0; i-- {
			if len(h.OnEnd(&events[i]).Reverse.Accepted) != 0 {
				t.Fatal("return passed original checkpoint")
			}
		}
	}
}

func TestReverseTTLDistanceMeansEveryOtherReceiverForTwo(t *testing.T) {
	out, _ := reverseTestChain(t, "pb", ReverseConfig{Policy: "ttl", TTLMin: 2, TTLMax: 2, LeafRejectProbability: 1})
	if *out[7].Reverse.Returned[0].TTL != 1 || *out[6].Reverse.Returned[0].TTL != 0 || len(out[5].Reverse.Accepted) != 1 {
		t.Fatal("TTL distance 2 should emit two span edges above the origin")
	}
	if out[5].Reverse.Routes[0].Distance != 2 {
		t.Fatal("TTL off by one")
	}
}

func TestReverseMixedFanInDoesNotPromoteToMandatoryAbsorption(t *testing.T) {
	h := reverseTestHandler(t, "pb", 8, ReverseConfig{Policy: "probability", Probability: 0, LeafRejectProbability: 1})
	root := Event{TraceID: 1, SpanID: 1}
	receiver := Event{TraceID: 1, SpanID: 2, ParentID: 1}
	h.OnStart(&root, 1)
	h.OnStart(&receiver, 1)
	zero := 0
	ready := ReverseSegment{Kind: "checkpoint.pb", OriginSpanID: 3, OriginDepth: 2, Payload: []byte{5, 2, 99}, TTL: &zero}
	waiting := ReverseSegment{Kind: "checkpoint.pb", OriginSpanID: 4, OriginDepth: 3, Payload: []byte{5, 3, 98}}
	state := h.state[stateKey{1, 2}]
	state.hasChildren = true
	context, err := EncodeReverseContext([]ReverseSegment{ready, waiting})
	if err != nil {
		t.Fatal(err)
	}
	state.pending = [][]byte{context}
	result := h.OnEnd(&receiver)
	if !result.Reverse.PromotedCheckpoint || result.Reverse.OriginalCheckpoint || len(result.Reverse.Accepted) != 1 || len(result.Reverse.Returned) != 1 {
		t.Fatal("accepting one sibling must not force acceptance of other siblings")
	}
	if result.Reverse.Accepted[0].OriginSpanID != 3 || result.Reverse.Returned[0].OriginSpanID != 4 {
		t.Fatal("explicit TTL did not take precedence")
	}
	atRoot := h.OnEnd(&root)
	if len(atRoot.Reverse.Accepted) != 1 || atRoot.Reverse.Accepted[0].OriginSpanID != 4 {
		t.Fatal("forwarded sibling not absorbed at root")
	}
}

func TestReverseRoutingSiblingOrderAndOriginDepth(t *testing.T) {
	c := ReverseConfig{Policy: "inverse_depth", Seed: 42}
	receiver := ReverseReceiver{TraceID: 11, SpanID: 18, Depth: 3}
	segments := []ReverseSegment{
		{Kind: "checkpoint.pb", OriginSpanID: 31, OriginDepth: 6, Payload: []byte{5, 6, 1, 0}},
		{Kind: "checkpoint.pb", OriginSpanID: 32, OriginDepth: 10, Payload: []byte{5, 10, 2, 0}},
	}
	if ReverseAcceptanceProbability(c.Policy, 0, 3, 6) != 1.0/6 || ReverseAcceptanceProbability(c.Policy, 0, 3, 10) != 0.1 {
		t.Fatal("probabilities must use each origin's own depth")
	}
	accepted, forwarded := RouteReverseSegments(c, receiver, segments)
	a2, f2 := RouteReverseSegments(c, receiver, []ReverseSegment{segments[1], segments[0]})
	decisions := func(a, f []ReverseSegment) map[uint64]bool {
		m := map[uint64]bool{}
		for _, s := range a {
			m[s.OriginSpanID] = true
		}
		for _, s := range f {
			m[s.OriginSpanID] = false
		}
		return m
	}
	if !reflect.DeepEqual(decisions(accepted, forwarded), decisions(a2, f2)) {
		t.Fatal("routing changed with sibling order")
	}
}

func TestReversePoliciesMatchAnalyticalEmissionDistributions(t *testing.T) {
	const samples = 40000
	for _, policy := range []string{"probability", "inverse_depth", "depth_linear", "upstream_pressure"} {
		c := ReverseConfig{Policy: policy, Probability: 0.25, Seed: 73}
		expected := [6]float64{}
		survival := 1.0
		for d := 5; d >= 1; d-- {
			p := ReverseAcceptanceProbability(policy, c.Probability, d, 6)
			expected[d] = survival * p
			survival *= 1 - p
		}
		expected[0] = survival
		counts := [6]int{}
		for tid := 1; tid <= samples; tid++ {
			pending := []ReverseSegment{{Kind: "checkpoint.pb", OriginSpanID: 99, OriginDepth: 6, Payload: []byte{5, 6, 0, 0}}}
			for d := 5; d >= 0; d-- {
				receiver := ReverseReceiver{TraceID: uint64(tid), SpanID: uint64(d + 1), Depth: d, OriginalCheckpoint: d == 0}
				accepted, forwarded := RouteReverseSegments(c, receiver, pending)
				if len(accepted) > 0 {
					counts[d]++
					break
				}
				pending = forwarded
			}
		}
		for d := 0; d <= 5; d++ {
			if math.Abs(float64(counts[d])/samples-expected[d]) > 0.008 {
				t.Fatalf("%s depth %d: measured %.4f, expected %.4f", policy, d, float64(counts[d])/samples, expected[d])
			}
		}
	}
}

func TestReverseEnvelopeRoundTripAndBinaryByteCounts(t *testing.T) {
	ttl := 0
	segments := []ReverseSegment{
		{Kind: "checkpoint.pb", OriginSpanID: 0x1020304050607080, OriginDepth: 128, Payload: []byte{0x85, 0x80, 1, 0, 255}, TTL: &ttl, OriginCheckpointDepth: 12},
		{Kind: "checkpoint.cgpb", OriginSpanID: 42, OriginDepth: 2, Payload: []byte{6, 2, 1, 2}},
	}
	encoded, err := EncodeReverseContext(segments)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReverseContext(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range segments {
		s.OriginCheckpointDepth = -1
		if !reflect.DeepEqual(s, decoded[i]) {
			t.Fatalf("segment changed: %#v versus %#v", s, decoded[i])
		}
	}
	if segments[0].RawBytes() != 8+2+5+1 || segments[0].OriginMetadataBytes() != 10 {
		t.Fatal("raw segment size incorrect")
	}
	if len(encoded) <= segments[0].RawBytes()+segments[1].RawBytes() {
		t.Fatal("encoded envelope omitted framing")
	}
	for _, malformed := range [][]byte{{2}, {1}, {1, 0}, {1, 128}, {1, 2, 0}} {
		if _, err := DecodeReverseEnvelope(malformed); err == nil {
			t.Fatal("malformed envelope accepted")
		}
	}
}

func TestReverseLifecycleNoRepeatEmissionAndLateReturnsFail(t *testing.T) {
	h := reverseTestHandler(t, "pb", 8, ReverseConfig{Policy: "probability", Probability: 0, LeafRejectProbability: 1})
	root := Event{TraceID: 1, SpanID: 1}
	child := Event{TraceID: 1, SpanID: 2, ParentID: 1}
	h.OnStart(&root, 1)
	h.OnStart(&child, 1)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("parent completion before child return should fail explicitly")
			}
		}()
		h.OnEnd(&root)
	}()
	h.OnEnd(&child)
	first := h.OnEnd(&root)
	second := h.OnEnd(&root)
	if len(first.Reverse.Accepted) != 1 || second.EmitBytes != 0 || second.Reverse != nil {
		t.Fatal("repeated completion duplicated return emission")
	}
}

func TestReverseRandomForwardScheduleAndFanoutStable(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		base, wrappedBase := reverseTestBase(mode, 8), reverseTestBase(mode, 8)
		checkpoint := &CheckpointRange{Min: 2, Max: 8, Seed: 42}
		if err := ConfigureCheckpoints(base, checkpoint); err != nil {
			t.Fatal(err)
		}
		if err := ConfigureCheckpoints(wrappedBase, checkpoint); err != nil {
			t.Fatal(err)
		}
		wrapper, err := NewReverseHandler(wrappedBase, ReverseConfig{Policy: "inverse_depth", LeafRejectProbability: 1, Seed: 99})
		if err != nil {
			t.Fatal(err)
		}
		// Overlapping sibling lifetimes and repeated ServiceID values model fanout
		// to distinct span instances of a shared service without merging their IDs.
		var walk func(id, parent uint64, depth, seq int)
		walk = func(id, parent uint64, depth, seq int) {
			ev := Event{TraceID: 25, SpanID: id, ParentID: parent, ServiceID: 3, DEEQueueID: 3}
			normal := base.OnStart(&ev, seq)
			got := wrapper.OnStart(&ev, seq)
			if got.CheckpointTTL != normal.CheckpointTTL || got.BaggageBytes != normal.BaggageBytes {
				t.Fatalf("%s span %d forward schedule changed", mode, id)
			}
			if depth < 4 {
				walk(2*id, id, depth+1, 1)
				walk(2*id+1, id, depth+1, 2)
			}
			base.OnEnd(&ev)
			wrapper.OnEnd(&ev)
		}
		walk(1, 0, 0, 1)
		wrapper.EvictTrace(25)
	}
}

func TestReverseOverlappingUnequalDepthFanoutBundlesOnce(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		h := reverseTestHandler(t, mode, 8, ReverseConfig{Policy: "probability", Probability: 0, LeafRejectProbability: 1, Seed: 10})
		// Both sibling requests are live together. Both call instances target the
		// same service, and one has an additional nested child.
		events := []Event{
			{TraceID: 7, SpanID: 1, ServiceID: 1},
			{TraceID: 7, SpanID: 2, ParentID: 1, ServiceID: 2},
			{TraceID: 7, SpanID: 3, ParentID: 2, ServiceID: 3},
			{TraceID: 7, SpanID: 4, ParentID: 2, ServiceID: 3},
			{TraceID: 7, SpanID: 5, ParentID: 4, ServiceID: 4},
		}
		for i := range events {
			seq := 1
			if i == 3 {
				seq = 2
			}
			h.OnStart(&events[i], seq)
		}
		shallow := h.OnEnd(&events[2])
		deep := h.OnEnd(&events[4])
		h.OnEnd(&events[3])
		fanin := h.OnEnd(&events[1])
		root := h.OnEnd(&events[0])
		if fanin.Reverse.PendingReceived != 2 || len(fanin.Reverse.Returned) != 2 || len(root.Reverse.Accepted) != 2 {
			t.Fatalf("%s fan-in lost or duplicated a truss", mode)
		}
		origins := map[uint64]ReverseSegment{}
		for _, s := range root.Reverse.Accepted {
			origins[s.OriginSpanID] = s
		}
		if origins[3].OriginDepth != 2 || origins[5].OriginDepth != 3 || !bytes.Equal(origins[3].Payload, shallow.Reverse.Returned[0].Payload) || !bytes.Equal(origins[5].Payload, deep.Reverse.Returned[0].Payload) {
			t.Fatal("fan-in combined or rewrote origin-specific payloads")
		}
		if !root.Reverse.ReceivingCheckpoint || root.Reverse.PromotedCheckpoint || root.EmitBytes == 0 {
			t.Fatal("fan-in must produce one original checkpoint with its own payload")
		}
		h.EvictTrace(7)
	}
}

func TestReverseMalformedProbabilitySegmentsPassThrough(t *testing.T) {
	receiver := ReverseReceiver{TraceID: 1, SpanID: 2, Depth: 1}
	segments := []ReverseSegment{
		{Kind: "checkpoint.pb", OriginSpanID: 3, OriginDepth: 2, Payload: []byte{5}},
		{Kind: "checkpoint.pb", OriginSpanID: 4, OriginDepth: 2, Payload: []byte{5, 0x80}},
		{Kind: "checkpoint.pb", OriginSpanID: 5, OriginDepth: 2, Payload: []byte{5, 3, 0, 0}},
		{Kind: "opaque", OriginSpanID: 6, OriginDepth: 2, Payload: []byte{0, 2, 0, 0}},
	}
	accepted, forwarded := RouteReverseSegments(ReverseConfig{Policy: "probability", Probability: 1}, receiver, segments)
	if len(accepted) != 0 || len(forwarded) != len(segments) {
		t.Fatal("malformed TTL-free context must pass ordinary receivers even at p=1")
	}
	receiver.OriginalCheckpoint = true
	accepted, forwarded = RouteReverseSegments(ReverseConfig{Policy: "probability", Probability: 1}, receiver, segments)
	if len(accepted) != len(segments) || len(forwarded) != 0 {
		t.Fatal("mandatory boundary must absorb opaque context")
	}
}

func TestReverseRejectsDanglingParentsAndMismatchedEnd(t *testing.T) {
	h := reverseTestHandler(t, "pb", 8, ReverseConfig{Policy: "probability", Probability: 0, LeafRejectProbability: 1})
	mustPanic := func(f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("expected invalid lifecycle to fail")
			}
		}()
		f()
	}
	mustPanic(func() { h.OnStart(&Event{TraceID: 1, SpanID: 2, ParentID: 99}, 1) })
	root := Event{TraceID: 1, SpanID: 1}
	h.OnStart(&root, 1)
	mustPanic(func() { h.OnEnd(&Event{TraceID: 1, SpanID: 1, ParentID: 99}) })
	h.OnEnd(&root)
	h.EvictTrace(1)
}

func TestReverseConservesRejectedTrussesAcrossEachTrace(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		h := reverseTestHandler(t, mode, 8, ReverseConfig{Policy: "inverse_depth", LeafRejectProbability: 1, Seed: 100})
		for tid := uint64(1); tid <= 20; tid++ {
			rejected, accepted := 0, 0
			acceptedOrigins := map[uint64]bool{}
			var walk func(uint64, uint64, int, int)
			walk = func(id, parent uint64, depth, seq int) {
				ev := Event{TraceID: tid, SpanID: id, ParentID: parent, ServiceID: 1}
				h.OnStart(&ev, seq)
				if depth < 4 {
					walk(2*id, id, depth+1, 1)
					walk(2*id+1, id, depth+1, 2)
				}
				result := h.OnEnd(&ev)
				if result.Reverse.RejectedLeaf {
					rejected++
				}
				for _, s := range result.Reverse.Accepted {
					if acceptedOrigins[s.OriginSpanID] {
						t.Fatal("origin emitted twice")
					}
					acceptedOrigins[s.OriginSpanID] = true
					accepted++
				}
			}
			walk(1, 0, 0, 1)
			if rejected != 16 || accepted != rejected {
				t.Fatalf("%s trace %d: rejected=%d accepted=%d", mode, tid, rejected, accepted)
			}
			h.EvictTrace(tid)
		}
	}
}
