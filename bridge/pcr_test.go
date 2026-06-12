package bridge

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// chain builds a simple parent chain t-1 -> 1..n and feeds it to the handler,
// returning the StartResults (index = depth).
func pcrChain(t *testing.T, h *PCRBridgeHandler, n int) []StartResult {
	t.Helper()
	res := make([]StartResult, n)
	for i := 0; i < n; i++ {
		ev := &Event{TraceID: 1, SpanID: uint64(100 + i), ParentID: 0}
		if i > 0 {
			ev.ParentID = uint64(100 + i - 1)
		}
		res[i] = h.OnStart(ev, 0)
	}
	return res
}

func TestPCRCheckpointCadenceAndPayload(t *testing.T) {
	h := NewPCRBridgeHandler(3, 4)
	h.Capture = true
	res := pcrChain(t, h, 8) // depths 0..7; checkpoints at 0, 3, 6

	for d, r := range res {
		isCkpt := d%3 == 0
		if (r.EmitBytes > 0) != isCkpt {
			t.Errorf("depth %d: emit=%d, want checkpoint emission iff depth%%3==0", d, r.EmitBytes)
		}
		if isCkpt {
			want := BRPropertyNameOverheadBytes + 1 + VarintLen(d) + 4
			if r.EmitBytes != want {
				t.Errorf("depth %d: emitBytes=%d want %d", d, r.EmitBytes, want)
			}
			// Payload: type || varint(depth) || 4-byte prefix.
			if r.Payload[0] != byte(PCRBridgeTypeID) {
				t.Fatalf("depth %d: type byte %d", d, r.Payload[0])
			}
			got, n := binary.Uvarint(r.Payload[1:])
			if int(got) != d {
				t.Errorf("depth %d: payload depth %d", d, got)
			}
			prefix := r.Payload[1+n:]
			if len(prefix) != 4 {
				t.Fatalf("depth %d: prefix len %d", d, len(prefix))
			}
			// Checkpoint payload names the PREVIOUS checkpoint: zeros for
			// the root, the checkpoint 3 above otherwise.
			var want8 [8]byte
			if d > 0 {
				want8 = BigEndian8(uint64(100 + d - 3))
			}
			if !bytes.Equal(prefix, want8[:4]) {
				t.Errorf("depth %d: prefix %x want %x", d, prefix, want8[:4])
			}
		}
	}

	// Baggage: child of a tracked parent carries 3 + varint(depth) + K.
	for d := 1; d < 8; d++ {
		want := BaggageKeyBytes + VarintLen(d) + 4
		if res[d].BaggageBytes != want {
			t.Errorf("depth %d: baggageBytes=%d want %d", d, res[d].BaggageBytes, want)
		}
	}
}

func TestPCRLeafAndInteriorEnd(t *testing.T) {
	h := NewPCRBridgeHandler(3, 3)
	h.Capture = true
	pcrChain(t, h, 6) // depths 0..5; checkpoints 0, 3; span at depth 5 is a leaf

	// Interior non-checkpoint (depth 4): _d only.
	r4 := h.OnEnd(&Event{TraceID: 1, SpanID: 104})
	if r4.EmitBytes != 0 || r4.DepthBytes != DepthKeyBytes+VarintLen(4) {
		t.Errorf("interior end: emit=%d depthBytes=%d", r4.EmitBytes, r4.DepthBytes)
	}
	// Leaf (depth 5): emits _br with inherited root = checkpoint at depth 3.
	r5 := h.OnEnd(&Event{TraceID: 1, SpanID: 105})
	if r5.EmitBytes != BRPropertyNameOverheadBytes+1+VarintLen(5)+3 {
		t.Errorf("leaf end: emitBytes=%d", r5.EmitBytes)
	}
	got, n := binary.Uvarint(r5.Payload[1:])
	if int(got) != 5 {
		t.Errorf("leaf payload depth %d", got)
	}
	want8 := BigEndian8(103)
	if !bytes.Equal(r5.Payload[1+n:], want8[:3]) {
		t.Errorf("leaf prefix %x want %x", r5.Payload[1+n:], want8[:3])
	}
}
