package recon

import (
	"bytes"
	"testing"

	"bridges/bridge"
)

func reverseTestSegment(t *testing.T, kind string) (bridge.ReverseSegment, Config) {
	t.Helper()
	var h bridge.Handler
	switch kind {
	case "checkpoint.pb":
		p := bridge.NewPCRBBridgeHandler(4, 8, 0.0001)
		p.Capture = true
		h = p
	case "checkpoint.cgpb":
		p := bridge.NewCGPRBBridgeHandler(4, 8, 0.0001)
		p.Capture = true
		h = p
	case "checkpoint.sb":
		p := bridge.NewSB3Handler(4, 8, 0.0001, nil)
		p.Capture, p.LehmerEE = true, true
		p.FPBits = 64
		h = p
	default:
		t.Fatalf("bad test kind %s", kind)
	}
	policy, err := bridge.ParseCheckpointRange("4:4", 42)
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.ConfigureCheckpoints(h, policy); err != nil {
		t.Fatal(err)
	}
	root := &bridge.Event{TraceID: 7, SpanID: 1}
	parent := &bridge.Event{TraceID: 7, SpanID: 2, ParentID: 1}
	first := &bridge.Event{TraceID: 7, SpanID: 3, ParentID: 2}
	origin := &bridge.Event{TraceID: 7, SpanID: 4, ParentID: 2}
	h.OnStart(root, 0)
	h.OnStart(parent, 1)
	h.OnStart(first, 1)
	h.OnEnd(first)
	h.OnStart(origin, 2)
	r := h.OnEnd(origin)
	if len(r.Payload) == 0 {
		t.Fatal("test leaf did not emit its truss")
	}
	return bridge.ReverseSegment{
			Kind: kind, OriginSpanID: 4, OriginDepth: 2, Payload: r.Payload,
			OriginCheckpointDepth: 12345, // deliberately not valid evidence
		}, Config{
			RandomizedCheckpoints: true, CheckpointMin: 4, CPD: 4,
			BloomFP: 0.0001, PrefixLen: 8, FPBits: 64, SBridgeLehmer: true,
		}
}

func TestReverseEvidenceRetainsOriginAndPayload(t *testing.T) {
	for _, kind := range []string{"checkpoint.pb", "checkpoint.cgpb", "checkpoint.sb"} {
		t.Run(kind, func(t *testing.T) {
			segment, cfg := reverseTestSegment(t, kind)
			encoded, err := bridge.EncodeReverseContext([]bridge.ReverseSegment{segment})
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeReverseEvidence(1, encoded, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Fatalf("evidence count=%d, want 1", len(got))
			}
			e := got[0]
			if e.CarrierSpanID != 1 || e.OriginSpanID != 4 || e.OriginDepth != 2 || e.Kind != kind {
				t.Fatalf("origin/owner metadata changed: %+v", e)
			}
			rootBytes := bridge.BigEndian8(1)
			if !bytes.Equal(e.RawPayload, segment.Payload) || !bytes.Equal(e.CkptPrefix, rootBytes[:]) {
				t.Fatalf("payload or original checkpoint root changed: %+v", e)
			}
			if e.WindowCPD != 4 || e.BloomM == 0 || e.BloomK == 0 || !e.LeafCarrier {
				t.Fatalf("origin window metadata lost: %+v", e)
			}
			if kind != "checkpoint.pb" && (len(e.HA) != 1 || e.HA[0].ParentID != 2) {
				t.Fatalf("origin fanout witness lost: %+v", e.HA)
			}
			if kind == "checkpoint.sb" && (len(e.SparseOrdinals) != 1 || e.SparseOrdinals[0].Ord != 2) {
				t.Fatalf("origin ordinal evidence lost: %+v", e.SparseOrdinals)
			}
			// The direct-segment API must own its bytes as well; a reusable input
			// buffer must not silently change decoded evidence after collection.
			direct, err := DecodeReverseSegmentEvidence(1, segment, cfg)
			if err != nil {
				t.Fatal(err)
			}
			before := append([]byte(nil), direct.RawPayload...)
			segment.Payload[len(segment.Payload)-1] ^= 0xff
			if !bytes.Equal(before, direct.RawPayload) {
				t.Fatal("decoded evidence aliases input payload")
			}
		})
	}
}

func TestReverseEvidenceRejectsMismatchedOrigin(t *testing.T) {
	segment, cfg := reverseTestSegment(t, "checkpoint.pb")
	for _, tc := range []struct {
		name   string
		modify func(*bridge.ReverseSegment)
	}{
		{"depth", func(s *bridge.ReverseSegment) { s.OriginDepth++ }},
		{"kind", func(s *bridge.ReverseSegment) { s.Kind = "checkpoint.cgpb" }},
		{"unknown kind", func(s *bridge.ReverseSegment) { s.Kind = "opaque" }},
		{"zero origin", func(s *bridge.ReverseSegment) { s.OriginSpanID = 0 }},
		{"negative depth", func(s *bridge.ReverseSegment) { s.OriginDepth = -1 }},
		{"empty payload", func(s *bridge.ReverseSegment) { s.Payload = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := segment
			tc.modify(&bad)
			if _, err := DecodeReverseSegmentEvidence(1, bad, cfg); err == nil {
				t.Fatal("accepted invalid evidence")
			}
		})
	}
	if _, err := DecodeReverseSegmentEvidence(0, segment, cfg); err == nil {
		t.Fatal("accepted missing export owner")
	}
	bad := segment
	bad.OriginDepth++
	encoded, err := bridge.EncodeReverseContext([]bridge.ReverseSegment{segment, bad})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeReverseEvidence(1, encoded, cfg)
	if err == nil || got != nil {
		t.Fatalf("bundle should fail atomically: evidence=%v err=%v", got, err)
	}
}

// Consume the actual attribute returned by the simulator, rather than an
// independently fabricated bundle. This covers evidence extraction only;
// it does not claim that the reconstruction engines ingest reverse origins.
func TestReverseEvidenceFromEmittedCheckpointContext(t *testing.T) {
	for _, kind := range []string{"checkpoint.pb", "checkpoint.cgpb", "checkpoint.sb"} {
		t.Run(kind, func(t *testing.T) {
			var base bridge.Handler
			switch kind {
			case "checkpoint.pb":
				base = bridge.NewPCRBBridgeHandler(4, 8, .0001)
			case "checkpoint.cgpb":
				base = bridge.NewCGPRBBridgeHandler(4, 8, .0001)
			case "checkpoint.sb":
				h := bridge.NewSB3Handler(4, 8, .0001, nil)
				h.LehmerEE, h.UseDEEQueueID, h.DequeueOneDEE = true, true, true
				h.FPBits = 64
				base = h
			}
			if err := bridge.ConfigureCheckpoints(base, &bridge.CheckpointRange{Min: 4, Max: 4, Seed: 42}); err != nil {
				t.Fatal(err)
			}
			h, err := bridge.NewReverseHandler(base, bridge.ReverseConfig{Policy: "probability", Probability: 1, LeafRejectProbability: 1})
			if err != nil {
				t.Fatal(err)
			}
			events := []bridge.Event{
				{TraceID: 7, SpanID: 1, ServiceID: 1, DEEQueueID: 11},
				{TraceID: 7, SpanID: 2, ParentID: 1, ServiceID: 2, DEEQueueID: 22},
				{TraceID: 7, SpanID: 3, ParentID: 2, ServiceID: 3, DEEQueueID: 33},
				{TraceID: 7, SpanID: 4, ParentID: 2, ServiceID: 3, DEEQueueID: 34},
			}
			h.OnStart(&events[0], 0)
			h.OnStart(&events[1], 1)
			h.OnStart(&events[2], 1)
			first := h.OnEnd(&events[2])
			h.OnStart(&events[3], 2)
			second := h.OnEnd(&events[3])
			owner := h.OnEnd(&events[1])
			h.OnEnd(&events[0])
			h.EvictTrace(7)
			if !owner.Reverse.PromotedCheckpoint || owner.Reverse.OriginalCheckpoint || len(owner.Payload) == 0 {
				t.Fatal("expected an internal receiver with its own partial-window payload")
			}
			cfg := Config{RandomizedCheckpoints: true, CheckpointMin: 4, CPD: 4, BloomFP: .0001, PrefixLen: 8, FPBits: 64, SBridgeLehmer: true}
			evidence, err := DecodeReverseEvidence(2, owner.Reverse.CheckpointContext, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if len(evidence) != 2 {
				t.Fatal("collector did not extract both returned origins")
			}
			rootBytes := bridge.BigEndian8(1)
			for i, original := range []bridge.EndResult{first, second} {
				e := evidence[i]
				if e.CarrierSpanID != 2 || e.OriginSpanID != uint64(i+3) || e.OriginDepth != 2 || !bytes.Equal(e.CkptPrefix, rootBytes[:]) || !bytes.Equal(e.RawPayload, original.Reverse.Returned[0].Payload) {
					t.Fatal("export ownership replaced origin evidence or changed its forward window")
				}
			}
			if kind != "checkpoint.pb" && (len(evidence[1].HA) != 1 || evidence[1].HA[0].ParentID != 2) {
				t.Fatal("emitted bundle lost the exact fanout witness")
			}
			if kind == "checkpoint.sb" && (len(evidence[1].SparseOrdinals) != 1 || evidence[1].SparseOrdinals[0].Ord != 2) {
				t.Fatal("emitted bundle lost sparse ordinal evidence")
			}
		})
	}
}
