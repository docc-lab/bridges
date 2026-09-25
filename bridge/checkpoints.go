package bridge

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"

	"bridges/bloom"
)

// CheckpointRange samples an inclusive checkpoint distance in parent-child
// edges. Its context packs the assigned-distance index above a distance-1
// countdown. A span receiving a zero countdown checkpoints. Sampling depends
// only on seed and span identity, not event/worker ordering.
//
// The context always occupies CheckpointContextBytes baggage bytes. A width that
// varied with the configured range would be a varint in all but name, and would
// make byte totals incomparable between ranges; a fixed width keeps the
// accounting one constant. Two bytes covers every range the design allows, since
// Max is capped at 256.
type CheckpointRange struct {
	Min    int   `json:"distance_min"`
	Max    int   `json:"distance_max"`
	Seed   int64 `json:"seed"`
	blooms []checkpointBloom
}

type checkpointBloom struct{ m, k uint32 }

func ParseCheckpointRange(value string, seed int64) (*CheckpointRange, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("checkpoint range must be MIN:MAX (checkpoint distances)")
	}
	lo, e1 := strconv.Atoi(parts[0])
	hi, e2 := strconv.Atoi(parts[1])
	r := &CheckpointRange{Min: lo, Max: hi, Seed: seed}
	if e1 != nil || e2 != nil {
		return nil, fmt.Errorf("invalid checkpoint range %q", value)
	}
	return r, r.Validate()
}

func (r CheckpointRange) Validate() error {
	if r.Min < 1 || r.Max > 256 || r.Min > r.Max {
		return fmt.Errorf("checkpoint distance range must satisfy 1 <= MIN <= MAX <= 256")
	}
	if r.contextBits() > 8*CheckpointContextBytes {
		return fmt.Errorf("checkpoint range %d:%d needs %d bits for assigned distance and remaining TTL, more than the %d-byte context holds",
			r.Min, r.Max, r.contextBits(), CheckpointContextBytes)
	}
	if n := r.Max - r.Min + 1; n > payloadDistanceLimit {
		return fmt.Errorf("checkpoint range %d:%d offers %d distinct distances; the payload advertises at most %d",
			r.Min, r.Max, n, payloadDistanceLimit)
	}
	return nil
}

const (
	// CheckpointContextBytes is the fixed baggage width of the packed context.
	CheckpointContextBytes = 2
	// PayloadDistanceBytes is the payload width that advertises the assigned
	// distance to reconstruction, which needs it to size the window Bloom before
	// it can parse the filter. It sits immediately after the type byte, so the
	// type and early-leaf flags stay where they were.
	PayloadDistanceBytes = 1
	// payloadDistanceLimit is how many DISTINCT distances a range may offer,
	// bounded by that width, independently of the context width.
	payloadDistanceLimit = 1 << (8 * PayloadDistanceBytes)
)

func (r CheckpointRange) contextBits() int {
	return bits.Len(uint(r.Max-1)) + bits.Len(uint(r.Max-r.Min))
}

func (r CheckpointRange) MaxDistance() int { return r.Max }

// One context byte contains an assigned-distance index above the countdown.
// For 2:8 each field uses three bits. Singleton ranges need no index bits.
func (r CheckpointRange) ttlBits() uint { return uint(bits.Len(uint(r.Max - 1))) }
func (r CheckpointRange) RemainingTTL(context uint16) uint16 {
	return context & ((uint16(1) << r.ttlBits()) - 1)
}
func (r CheckpointRange) AssignedDistance(context uint16) int {
	return r.Min + int(context>>r.ttlBits())
}
func (r CheckpointRange) context(ttl uint16) uint16 {
	return uint16(int(ttl)+1-r.Min)<<r.ttlBits() | ttl
}

func (r *CheckpointRange) bloom(context uint16, m, k uint32) checkpointBloom {
	if r == nil {
		return checkpointBloom{m, k}
	}
	return r.blooms[int(context>>r.ttlBits())]
}

// tagPayload inserts the assigned-distance index after the type byte. In
// fixed-distance mode there is no distance to advertise and the payload is
// returned untouched, so those payloads are unchanged.
func (r *CheckpointRange) tagPayload(payload []byte, context uint16) []byte {
	if r == nil || len(payload) == 0 {
		return payload
	}
	out := make([]byte, 0, len(payload)+PayloadDistanceBytes)
	out = append(out, payload[0])
	out = append(out, byte(int(context>>r.ttlBits())))
	return append(out, payload[1:]...)
}

// PayloadDistanceWidth is how many bytes tagPayload adds. Nil is fixed-distance
// mode, which adds none.
func (r *CheckpointRange) PayloadDistanceWidth() int {
	if r == nil {
		return 0
	}
	return PayloadDistanceBytes
}

// PayloadDistance decodes the byte tagPayload wrote.
func PayloadDistance(b byte, min, max int) (int, error) {
	d := min + int(b)
	if min < 1 || d > max {
		return 0, fmt.Errorf("invalid checkpoint distance %d in payload, outside %d:%d", d, min, max)
	}
	return d, nil
}

func checkpointMix(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (r CheckpointRange) sample(ev *Event) uint16 {
	x := checkpointMix(uint64(r.Seed) ^ checkpointMix(ev.TraceID) ^ checkpointMix(ev.SpanID+0x9e3779b97f4a7c15))
	n := uint64(r.Max - r.Min + 1)
	// Rejection avoids modulo bias for ranges that do not divide 2^64.
	for x < -n%n {
		x = checkpointMix(x + 0x9e3779b97f4a7c15)
	}
	return uint16(r.Min + int(x%n) - 1)
}

func (r *CheckpointRange) next(ev *Event, hasParent bool, incoming uint16, depth, cpd int) (bool, uint16) {
	if r == nil {
		return depth%cpd == 0, 0
	}
	if !hasParent || r.RemainingTTL(incoming) == 0 {
		return true, r.context(r.sample(ev))
	}
	return false, incoming - 1
}

// ConfigureCheckpoints must be called before processing events. Each assigned
// distance has its own Bloom geometry. Nil preserves fixed-distance behavior.
func ConfigureCheckpoints(handler Handler, policy *CheckpointRange) error {
	if policy == nil {
		return nil
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	r := *policy
	set := func(fp float64) {
		r.blooms = make([]checkpointBloom, r.Max-r.Min+1)
		for i := range r.blooms {
			m, k := bloom.EstimateParameters(PCRBBloomCapacity(r.Min+i), fp)
			r.blooms[i] = checkpointBloom{m, k}
		}
	}
	switch h := handler.(type) {
	case *PCRBBridgeHandler:
		if len(h.state) != 0 {
			return fmt.Errorf("cannot change checkpoints after processing spans")
		}
		set(h.bloomFP)
		h.checkpoints = &r
	case *CGPRBBridgeHandler:
		if len(h.state) != 0 {
			return fmt.Errorf("cannot change checkpoints after processing spans")
		}
		set(h.bloomFP)
		h.checkpoints = &r
	case *SB3Handler:
		if len(h.state) != 0 {
			return fmt.Errorf("cannot change checkpoints after processing spans")
		}
		set(h.bloomFP)
		h.checkpoints = &r
	default:
		return fmt.Errorf("randomized checkpoints require pcrb, cgprb, or sb3 emission")
	}
	return nil
}

// LeafPayloadFlag marks an early leaf checkpoint in randomized mode, using a
// previously unused bit of the existing type byte. A zero-TTL checkpoint keeps
// the ordinary type, even when it later proves to be a leaf. Fixed-mode bytes
// are unchanged. Readers need this distinction because depth modulo CPD no
// longer identifies checkpoints that reset the propagated window.
const LeafPayloadFlag byte = 0x80

func PayloadType(tag byte) byte         { return tag & 0x07 }
func IsLeafPayload(payload []byte) bool { return len(payload) > 0 && payload[0]&LeafPayloadFlag != 0 }
