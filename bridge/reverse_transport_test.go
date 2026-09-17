package bridge

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// Read the returned values independently of the production decoder and its
// size helpers. This checks the actual binary values delivered by the simulator.
func checkReverseOutputBytes(t *testing.T, r EndResult) {
	t.Helper()
	rr := r.Reverse
	if rr == nil {
		t.Fatal("missing reverse result")
	}
	if rr.ReverseEncodedBytes != len(rr.ReturnContext) {
		t.Fatal("baggage count differs from delivered value length")
	}
	checkpointBytes := r.EmitBytes
	if len(rr.CheckpointContext) > 0 {
		checkpointBytes += len("bridges.checkpoint") + len(rr.CheckpointContext)
	}
	if rr.CheckpointBytes != checkpointBytes {
		t.Fatal("checkpoint count differs from emitted attributes")
	}
	if (len(rr.ReturnContext) > 0) != (len(rr.Returned) > 0) || (len(rr.CheckpointContext) > 0) != (len(rr.Accepted) > 0) {
		t.Fatal("nonempty truss list lacks its serialized output, or an empty list emitted an envelope")
	}
	raw := 0
	if len(rr.ReturnContext) > 0 {
		reader := bytes.NewReader(rr.ReturnContext)
		version, err := reader.ReadByte()
		if err != nil || version != 1 {
			t.Fatal("missing binary bundle version")
		}
		for reader.Len() > 0 {
			frame, err := binary.ReadUvarint(reader)
			if err != nil {
				t.Fatal(err)
			}
			before := reader.Len()
			var origin uint64
			if err := binary.Read(reader, binary.BigEndian, &origin); err != nil {
				t.Fatal(err)
			}
			if _, err := binary.ReadUvarint(reader); err != nil {
				t.Fatal(err)
			}
			raw += before - reader.Len()
			remaining := int(frame>>1) + int(frame&1)
			if remaining > reader.Len() {
				t.Fatal("short binary record")
			}
			reader.Seek(int64(remaining), 1)
			raw += remaining
		}
	}
	if rr.ReverseRawBytes != raw {
		t.Fatalf("raw baggage count %d differs from serialized segment content %d", rr.ReverseRawBytes, raw)
	}
}

func TestReverseTransportConsumesSerializedChildReturn(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		t.Run(mode, func(t *testing.T) {
			h := reverseTestHandler(t, mode, 8, ReverseConfig{Policy: "ttl", TTLMin: 2, TTLMax: 2, LeafRejectProbability: 1})
			events := []Event{
				{TraceID: 1, SpanID: 1},
				{TraceID: 1, SpanID: 2, ParentID: 1},
				{TraceID: 1, SpanID: 3, ParentID: 2},
			}
			for i := range events {
				h.OnStart(&events[i], 1)
			}
			leaf := h.OnEnd(&events[2])
			checkReverseOutputBytes(t, leaf)
			payload := append([]byte(nil), leaf.Reverse.Returned[0].Payload...)
			// The diagnostic segment list is not the transport. Mutating it
			// after the child returns must not rewrite the parent's input.
			leaf.Reverse.Returned[0].OriginSpanID = 999
			leaf.Reverse.Returned[0].Payload[0] ^= 0xff
			*leaf.Reverse.Returned[0].TTL = 0
			parent := h.OnEnd(&events[1])
			checkReverseOutputBytes(t, parent)
			if len(parent.Reverse.Accepted) != 0 || len(parent.Reverse.Returned) != 1 {
				t.Fatal("parent routed diagnostic objects instead of the delivered context")
			}
			root := h.OnEnd(&events[0])
			checkReverseOutputBytes(t, root)
			exported, err := DecodeReverseContext(root.Reverse.CheckpointContext)
			if err != nil {
				t.Fatal(err)
			}
			if len(exported) != 1 || exported[0].OriginSpanID != 3 || !bytes.Equal(exported[0].Payload, payload) || *exported[0].TTL != 0 {
				t.Fatal("serialized export lost or changed the originating evidence")
			}
			if exported[0].OriginCheckpointDepth != -1 || root.Reverse.Routes[0].OriginalCheckpointDepth != 0 {
				t.Fatal("analysis metadata leaked into transport or was lost from the audit")
			}
			h.EvictTrace(1)
		})
	}
}

func TestReverseTransportOutputConservation(t *testing.T) {
	for _, mode := range []string{"pb", "cg", "sb"} {
		for _, c := range []ReverseConfig{
			{Policy: "probability", Probability: 0, LeafRejectProbability: 1},
			{Policy: "probability", Probability: 1, LeafRejectProbability: 1},
			{Policy: "ttl", TTLMin: 2, TTLMax: 8, LeafRejectProbability: 1},
			{Policy: "inverse_depth", LeafRejectProbability: 1},
			{Policy: "depth_linear", LeafRejectProbability: 1},
			{Policy: "upstream_pressure", LeafRejectProbability: 1},
			{Policy: "inverse_depth", LeafRejectProbability: 0},
		} {
			h := reverseTestHandler(t, mode, 8, c)
			if err := ConfigureCheckpoints(h.base, &CheckpointRange{Min: 2, Max: 8, Seed: 42}); err != nil {
				t.Fatal(err)
			}
			for tid := uint64(1); tid <= 20; tid++ {
				rejected := map[uint64][]byte{}
				exported := map[uint64][]byte{}
				var walk func(uint64, uint64, int, int)
				walk = func(id, parent uint64, depth, seq int) {
					ev := Event{TraceID: tid, SpanID: id, ParentID: parent, ServiceID: uint16(id % 3), DEEQueueID: uint32(id % 5)}
					h.OnStart(&ev, seq)
					if depth < 6 {
						walk(2*id, id, depth+1, 1)
						if id%3 != 0 { // unequal-depth branches and shared services
							walk(2*id+1, id, depth+1, 2)
						}
					}
					r := h.OnEnd(&ev)
					checkReverseOutputBytes(t, r)
					if r.Reverse.RejectedLeaf {
						segments, err := DecodeReverseContext(r.Reverse.ReturnContext)
						if err != nil || len(segments) != 1 {
							t.Fatalf("rejected leaf did not return exactly one truss: %v", err)
						}
						rejected[id] = segments[0].Payload
					}
					if len(r.Reverse.CheckpointContext) > 0 {
						segments, err := DecodeReverseContext(r.Reverse.CheckpointContext)
						if err != nil {
							t.Fatal(err)
						}
						for _, segment := range segments {
							if _, duplicate := exported[segment.OriginSpanID]; duplicate {
								t.Fatal("origin emitted in multiple checkpoint bundles")
							}
							exported[segment.OriginSpanID] = segment.Payload
						}
					}
				}
				walk(1, 0, 0, 1)
				if len(rejected) != len(exported) {
					t.Fatalf("%s/%s trace %d: %d rejected, %d exported", mode, c.Policy, tid, len(rejected), len(exported))
				}
				for origin, payload := range rejected {
					if !bytes.Equal(payload, exported[origin]) {
						t.Fatalf("origin %d changed between return and final export", origin)
					}
				}
				h.EvictTrace(tid)
			}
		}
	}
}

func TestReverseTransportInvalidContextFailsBeforeSpanCompletion(t *testing.T) {
	h := reverseTestHandler(t, "pb", 8, ReverseConfig{Policy: "inverse_depth"})
	root := Event{TraceID: 1, SpanID: 1}
	h.OnStart(&root, 1)
	state := h.state[stateKey{1, 1}]
	state.pending = [][]byte{[]byte("not a serialized context")}
	defer func() {
		if recover() == nil {
			t.Error("invalid return context was silently ignored")
		}
		if state.ended {
			t.Error("span finalized before its returned evidence was decoded")
		}
	}()
	h.OnEnd(&root)
}
