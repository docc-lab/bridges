package bridge

import (
	"bytes"
	"fmt"
	"testing"

	"bridges/bloom"
)

func checkpointTestHandlers() map[string]func(int) Handler {
	return map[string]func(int) Handler{
		"pcrb":  func(cpd int) Handler { h := NewPCRBBridgeHandler(cpd, 8, 1e-4); h.Capture = true; return h },
		"cgprb": func(cpd int) Handler { h := NewCGPRBBridgeHandler(cpd, 8, 1e-4); h.Capture = true; return h },
		"sb3":   func(cpd int) Handler { h := NewSB3Handler(cpd, 8, 1e-4, nil); h.Capture = true; return h },
	}
}

func TestCheckpointTTLCountdownAndLeafEmission(t *testing.T) {
	for name, makeHandler := range checkpointTestHandlers() {
		t.Run(name, func(t *testing.T) {
			h := makeHandler(20)
			if err := ConfigureCheckpoints(h, &CheckpointRange{Min: 2, Max: 2}); err != nil {
				t.Fatal(err)
			}
			events := make([]Event, 8)
			for d := range events {
				events[d] = Event{TraceID: 99, SpanID: uint64(d + 1), ParentID: uint64(d)}
				r := h.OnStart(&events[d], 1)
				if got, want := r.EmitBytes > 0, d%2 == 0; got != want {
					t.Fatalf("depth %d checkpoint=%t, want %t", d, got, want)
				}
				if want := uint16(1 - d%2); r.CheckpointTTL != want {
					t.Fatalf("depth %d TTL=%d, want %d", d, r.CheckpointTTL, want)
				}
			}
			for d := len(events) - 1; d >= 0; d-- {
				r := h.OnEnd(&events[d])
				if d == len(events)-1 {
					if r.EmitBytes == 0 || !IsLeafPayload(r.Payload) {
						t.Fatal("early leaf did not emit a flagged checkpoint payload")
					}
				} else if r.EmitBytes != 0 {
					t.Fatalf("non-leaf depth %d emitted twice", d)
				}
			}
			h.EvictTrace(99)
		})
	}
}

func TestCheckpointTTLIsCopiedToSiblings(t *testing.T) {
	for name, makeHandler := range checkpointTestHandlers() {
		t.Run(name, func(t *testing.T) {
			h := makeHandler(9)
			if err := ConfigureCheckpoints(h, &CheckpointRange{Min: 2, Max: 2}); err != nil {
				t.Fatal(err)
			}
			h.OnStart(&Event{TraceID: 1, SpanID: 1}, 0)
			for i := 0; i < 12; i++ {
				r := h.OnStart(&Event{TraceID: 1, SpanID: uint64(i + 2), ParentID: 1}, i+1)
				if r.CheckpointTTL != 0 || r.EmitBytes != 0 {
					t.Fatalf("sibling %d consumed another sibling's TTL: %+v", i, r)
				}
			}
		})
	}
}

// stripDistanceByte removes the assigned-distance byte a randomized payload
// carries after its type byte, leaving the fixed-mode layout.
func stripDistanceByte(p []byte) []byte {
	if len(p) <= PayloadDistanceBytes {
		return p
	}
	out := make([]byte, 0, len(p)-PayloadDistanceBytes)
	out = append(out, p[0])
	return append(out, p[1+PayloadDistanceBytes:]...)
}

func TestCheckpointTTLAddsTheFixedContextWidth(t *testing.T) {
	for name, makeHandler := range checkpointTestHandlers() {
		t.Run(name, func(t *testing.T) {
			fixed, randomized := makeHandler(3), makeHandler(20)
			if err := ConfigureCheckpoints(randomized, &CheckpointRange{Min: 3, Max: 3}); err != nil {
				t.Fatal(err)
			}
			events := make([]Event, 8)
			for d := range events {
				events[d] = Event{TraceID: 17, SpanID: uint64(d + 1), ParentID: uint64(d)}
				a, b := fixed.OnStart(&events[d], 1), randomized.OnStart(&events[d], 1)
				extra := 0
				if d > 0 { // the root has no parent, so no baggage call
					extra = CheckpointContextBytes
				}
				// The randomized payload carries the assigned distance after the
				// type byte; strip it and the body must be identical. A span that
				// emits nothing carries no distance byte either.
				emitExtra := 0
				if b.EmitBytes > 0 {
					emitExtra = PayloadDistanceBytes
				}
				if b.BaggageBytes != a.BaggageBytes+extra ||
					a.EmitBytes+emitExtra != b.EmitBytes ||
					!bytes.Equal(a.Payload, stripDistanceByte(b.Payload)) {
					t.Fatalf("depth %d fixed=%+v randomized=%+v", d, a, b)
				}
			}
			for d := len(events) - 1; d >= 0; d-- {
				a, b := fixed.OnEnd(&events[d]), randomized.OnEnd(&events[d])
				extra := 0
				if len(b.Payload) > 0 {
					extra = PayloadDistanceBytes
				}
				if a.EmitBytes+extra != b.EmitBytes || a.DepthBytes != b.DepthBytes {
					t.Fatalf("emission accounting changed at depth %d", d)
				}
				body := stripDistanceByte(b.Payload)
				if len(body) > 0 {
					body[0] = PayloadType(body[0])
				}
				if !bytes.Equal(a.Payload, body) {
					t.Fatalf("leaf payload body changed at depth %d", d)
				}
			}
		})
	}
}

// The context is charged to baggage at one fixed width, whatever the range.
// A width that varied with the range would be a varint in all but name and
// would make byte totals incomparable between ranges.
//
// The comparison is against a fixed-distance handler, which only isolates the
// context when both sides use the same Bloom geometry. The root samples the
// window distance for the whole trace, so it is read back from the root's
// context and the comparator is built at that distance rather than guessed.
func TestCheckpointContextIsChargedAtOneFixedWidth(t *testing.T) {
	for _, rng := range []CheckpointRange{
		{Min: 3, Max: 3},
		{Min: 1, Max: 9, Seed: 42},
		{Min: 1, Max: 16, Seed: 42},
		{Min: 255, Max: 256, Seed: 42},
		{Min: 100, Max: 115, Seed: 42},
	} {
		name := fmt.Sprintf("%d:%d", rng.Min, rng.Max)
		t.Run(name, func(t *testing.T) {
			if err := rng.Validate(); err != nil {
				t.Fatal(err)
			}
			for hname, makeHandler := range checkpointTestHandlers() {
				policy := rng
				randomized := makeHandler(20)
				if err := ConfigureCheckpoints(randomized, &policy); err != nil {
					t.Fatal(err)
				}
				events := make([]Event, 8)
				// Depth 0 fixes the window distance for every span below it.
				events[0] = Event{TraceID: 17, SpanID: 1}
				root := randomized.OnStart(&events[0], 1)
				distance := policy.AssignedDistance(root.CheckpointTTL)
				if distance < policy.Min || distance > policy.Max {
					t.Fatalf("%s: sampled distance %d outside %s", hname, distance, name)
				}
				fixed := makeHandler(distance)
				if a := fixed.OnStart(&events[0], 1); a.BaggageBytes != root.BaggageBytes {
					t.Fatalf("%s: root has no parent so no baggage call; got %d vs %d",
						hname, root.BaggageBytes, a.BaggageBytes)
				}
				for d := 1; d < len(events); d++ {
					events[d] = Event{TraceID: 17, SpanID: uint64(d + 1), ParentID: uint64(d)}
					a := fixed.OnStart(&events[d], 1)
					b := randomized.OnStart(&events[d], 1)
					if b.BaggageBytes != a.BaggageBytes+CheckpointContextBytes {
						t.Fatalf("%s depth %d (distance %d): baggage %d, want %d (+%d context)",
							hname, d, distance, b.BaggageBytes, a.BaggageBytes+CheckpointContextBytes,
							CheckpointContextBytes)
					}
				}
			}
		})
	}
}

func TestCheckpointTTLByteLimitsAndSampling(t *testing.T) {
	// Malformed, out of bounds, or offering more distinct distances than the
	// payload type byte can advertise.
	for _, value := range []string{"0:2", "2:257", "8:2", "2", "x:4"} {
		if _, err := ParseCheckpointRange(value, 42); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
	// Every range the design allows: the two-byte context covers any distance up
	// to 256, and the payload's own distance byte advertises any of them.
	for _, value := range []string{"255:256", "241:256", "100:115", "16:31", "1:9", "1:16", "3:3", "1:17", "1:21", "1:256", "100:200"} {
		if _, err := ParseCheckpointRange(value, 42); err != nil {
			t.Fatalf("rejected %q: %v", value, err)
		}
	}
	for _, ttl := range []int{0, 255} {
		r := &CheckpointRange{Min: ttl + 1, Max: ttl + 1}
		incoming := uint16(0)
		for d := 0; d < 2*(ttl+1)+1; d++ {
			cp, next := r.next(&Event{TraceID: 1, SpanID: uint64(d + 1)}, d > 0, incoming, d, 1)
			if cp != (d%(ttl+1) == 0) {
				t.Fatalf("TTL %d checkpoint wrong at depth %d", ttl, d)
			}
			incoming = next
		}
	}
	r := CheckpointRange{Min: 2, Max: 8, Seed: 42}
	seen := make(map[uint16]bool)
	for i := uint64(1); i <= 1000; i++ {
		ev := &Event{TraceID: i, SpanID: i + 19}
		v := r.sample(ev)
		seen[v] = true
		if v < 1 || v > 7 || v != r.sample(ev) {
			t.Fatalf("invalid/nondeterministic sample %d", v)
		}
	}
	if len(seen) != 7 {
		t.Fatalf("range not exercised: %v", seen)
	}
}

func TestPackedCheckpointContext(t *testing.T) {
	for _, limits := range [][2]int{{2, 8}, {1, 16}, {9, 16}, {256, 256}} {
		p := CheckpointRange{Min: limits[0], Max: limits[1]}
		if err := p.Validate(); err != nil {
			t.Fatal(err)
		}
		seen := map[uint16]bool{}
		for distance := p.Min; distance <= p.Max; distance++ {
			initial := p.context(uint16(distance - 1))
			for remaining := distance - 1; remaining >= 0; remaining-- {
				context := initial - uint16(distance-1-remaining)
				if seen[context] || p.AssignedDistance(context) != distance || int(p.RemainingTTL(context)) != remaining {
					t.Fatalf("range %v distance %d remaining %d has ambiguous context %d", limits, distance, remaining, context)
				}
				seen[context] = true
				for _, typ := range []byte{PCRBBridgeTypeID, CGPRBBridgeTypeID, SB3BridgeTypeID} {
					payload := p.tagPayload([]byte{typ | LeafPayloadFlag, 0xAB}, context)
					got, err := PayloadDistance(payload[1], p.Min, p.Max)
					if err != nil || got != distance || PayloadType(payload[0]) != typ ||
						!IsLeafPayload(payload) || len(payload) != 2+PayloadDistanceBytes ||
						payload[1+PayloadDistanceBytes] != 0xAB {
						t.Fatalf("payload lost distance, type, leaf flag, or body: %x", payload)
					}
				}
			}
		}
	}
}

func TestRandomCheckpointsSizeEachAssignedWindow(t *testing.T) {
	for _, prime := range []bool{false, true} {
		for _, fp := range []float64{1e-4, 0.1} {
			for _, mode := range []string{"pcrb", "cgprb", "sb3"} {
				t.Run(fmt.Sprintf("%s/fp%g/prime%t", mode, fp, prime), func(t *testing.T) {
					oldPrime := bloom.PrimeM
					bloom.PrimeM = prime
					t.Cleanup(func() { bloom.PrimeM = oldPrime })
					var h Handler
					switch mode {
					case "pcrb":
						p := NewPCRBBridgeHandler(20, 8, fp)
						p.Capture = true
						h = p
					case "cgprb":
						p := NewCGPRBBridgeHandler(20, 8, fp)
						p.Capture = true
						h = p
					case "sb3":
						p := NewSB3Handler(20, 8, fp, nil)
						p.Capture = true
						h = p
					}
					policy := &CheckpointRange{Min: 2, Max: 8, Seed: 42}
					if err := ConfigureCheckpoints(h, policy); err != nil {
						t.Fatal(err)
					}
					var context uint16
					var ancestors []uint64
					seen := map[int]bool{}
					checkPayload := func(payload []byte, depth, distance, emitBytes int, ids []uint64) {
						t.Helper()
						got, err := PayloadDistance(payload[1], policy.Min, policy.Max)
						if err != nil || got != distance {
							t.Fatalf("depth %d geometry distance %d, want %d", depth, got, distance)
						}
						m, k := bloom.EstimateParameters(PCRBBloomCapacity(distance), fp)
						bf, _ := bloom.New(m, k)
						for _, id := range ids {
							key := HexOf(id)
							bf.Add(key[:])
						}
						start := 1 + PayloadDistanceBytes + VarintLen(depth) + 8
						end := start + int((m+7)/8)
						trailing := 0
						if mode == "sb3" {
							trailing = 2
						} // empty HA and sparse-ordinal counts
						if len(payload) != end+trailing || !bytes.Equal(payload[start:end], bf.ToBytes()) {
							t.Fatalf("depth %d CPD %d has incorrect geometry or inherited filter", depth, distance)
						}
						if emitBytes != BRPropertyNameOverheadBytes+len(payload) {
							t.Fatal("unaccounted payload byte")
						}
					}
					for depth := 0; depth < 600; depth++ {
						ev := Event{TraceID: 19, SpanID: uint64(depth + 1), ParentID: uint64(depth)}
						incoming := context
						r := h.OnStart(&ev, 1)
						context = r.CheckpointTTL
						if depth == 0 {
							incoming = context
						}
						incomingD, outgoingD := policy.AssignedDistance(incoming), policy.AssignedDistance(context)
						checkpoint := depth == 0 || policy.RemainingTTL(incoming) == 0
						if (r.EmitBytes > 0) != checkpoint {
							t.Fatalf("depth %d ignored packed TTL", depth)
						}
						if checkpoint {
							checkPayload(r.Payload, depth, incomingD, r.EmitBytes, ancestors)
							ancestors = nil
							seen[outgoingD] = true
						} else if outgoingD != incomingD || policy.RemainingTTL(context)+1 != policy.RemainingTTL(incoming) {
							t.Fatal("decrement changed the assigned distance")
						}
						if depth > 0 {
							m, _ := bloom.EstimateParameters(PCRBBloomCapacity(outgoingD), fp)
							want := BaggageKeyBytes + VarintLen(depth) + 8 + int((m+7)/8) +
								CheckpointContextBytes
							if mode == "sb3" {
								want += 2
							}
							if r.BaggageBytes != want {
								t.Fatalf("baggage size %d, want %d (fixed context width)", r.BaggageBytes, want)
							}
						}
						if depth == 599 {
							end := h.OnEnd(&ev)
							if !checkpoint {
								checkPayload(end.Payload, depth, outgoingD, end.EmitBytes, ancestors)
								if !IsLeafPayload(end.Payload) {
									t.Fatal("leaf lost its flag")
								}
							}
						}
						if !checkpoint {
							ancestors = append(ancestors, ev.SpanID)
						}
					}
					if len(seen) != 7 {
						t.Fatalf("did not exercise all assigned distances: %v", seen)
					}
				})
			}
		}
	}
}
