package bridge

import (
	"encoding/binary"
	"testing"
)

// validSegment builds a segment that passes validReverseCheckpointSegment, so
// the tests exercise the real acceptance path rather than an early reject.
func validSegment(originDepth int) ReverseSegment {
	payload := []byte{byte(PCRBBridgeTypeID)}
	payload = binary.AppendUvarint(payload, uint64(originDepth))
	payload = append(payload, 0x00, 0x00)
	return ReverseSegment{Kind: reverseKind(payload), OriginSpanID: 0xaa, OriginDepth: originDepth, Payload: payload}
}

// A scheduled checkpoint absorbs every truss that reaches it, whatever the
// acceptance policy says, unless PassCheckpoints is set. That default bounds a
// truss to the window its origin leaf started in: it can never climb past the
// first scheduled checkpoint above, so absorption stays near the leaves. With
// PassCheckpoints the policy alone decides how far a truss travels and only the
// trace root remains a forced absorber.
func TestPassCheckpointsControlsMandatoryAbsorption(t *testing.T) {
	seg := []ReverseSegment{validSegment(9)}
	if !validReverseCheckpointSegment(seg[0], 0) {
		t.Fatal("fixture segment must be valid, or the policy path is never reached")
	}
	// Probability 0 means the policy never accepts, so anything absorbed here
	// was absorbed because it was mandatory.
	never := ReverseConfig{Policy: "probability", Probability: 0, Seed: 1}
	pass := never
	pass.PassCheckpoints = true

	cases := []struct {
		name         string
		cfg          ReverseConfig
		recv         ReverseReceiver
		wantAccepted int
	}{
		{"scheduled checkpoint absorbs by default", never,
			ReverseReceiver{SpanID: 1, Depth: 4, OriginalCheckpoint: true}, 1},
		{"scheduled checkpoint forwards when passing", pass,
			ReverseReceiver{SpanID: 1, Depth: 4, OriginalCheckpoint: true}, 0},
		{"trace root absorbs by default", never,
			ReverseReceiver{SpanID: 2, Depth: 0, ReturnBoundary: true}, 1},
		{"trace root still absorbs when passing", pass,
			ReverseReceiver{SpanID: 2, Depth: 0, ReturnBoundary: true}, 1},
		{"ordinary span forwards either way", never,
			ReverseReceiver{SpanID: 3, Depth: 4}, 0},
		{"ordinary span forwards either way (passing)", pass,
			ReverseReceiver{SpanID: 3, Depth: 4}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			accepted, forwarded := RouteReverseSegments(c.cfg, c.recv, seg)
			if len(accepted) != c.wantAccepted {
				t.Fatalf("accepted %d, want %d", len(accepted), c.wantAccepted)
			}
			if len(accepted)+len(forwarded) != len(seg) {
				t.Fatalf("segment lost: accepted %d forwarded %d, want %d total",
					len(accepted), len(forwarded), len(seg))
			}
		})
	}
}

// A non-recording receiver cannot emit, so it forwards even where absorption
// would otherwise be mandatory. That check sits ahead of the mandatory one and
// must stay there under either setting.
func TestNonRecordingForwardsAheadOfMandatory(t *testing.T) {
	seg := []ReverseSegment{validSegment(9)}
	for _, pass := range []bool{false, true} {
		cfg := ReverseConfig{Policy: "probability", Probability: 1, Seed: 1, PassCheckpoints: pass}
		recv := ReverseReceiver{SpanID: 1, Depth: 4, OriginalCheckpoint: true, NonRecording: true}
		accepted, forwarded := RouteReverseSegments(cfg, recv, seg)
		if len(accepted) != 0 || len(forwarded) != 1 {
			t.Fatalf("pass=%v: non-recording receiver must forward, got accepted %d forwarded %d",
				pass, len(accepted), len(forwarded))
		}
	}
}
