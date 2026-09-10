package bridge

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"

	"bridges/bloom"
)

// CheckpointRange samples an inclusive checkpoint distance in parent-child
// edges. Its context packs the assigned-distance index and a distance-1
// countdown into one byte. A span receiving a zero countdown checkpoints.
// Sampling depends only on seed and span identity, not event/worker ordering.
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
	if bits.Len(uint(r.Max-1))+bits.Len(uint(r.Max-r.Min)) > 8 {
		return fmt.Errorf("checkpoint range %d:%d needs more than one byte for assigned distance and remaining TTL", r.Min, r.Max)
	}
	return nil
}

func (r CheckpointRange) MaxDistance() int { return r.Max }

// One context byte contains an assigned-distance index above the countdown.
// For 2:8 each field uses three bits. Singleton ranges need no index bits.
func (r CheckpointRange) ttlBits() uint { return uint(bits.Len(uint(r.Max - 1))) }
func (r CheckpointRange) RemainingTTL(context byte) byte {
	return context & byte((uint16(1)<<r.ttlBits())-1)
}
func (r CheckpointRange) AssignedDistance(context byte) int {
	return r.Min + int(uint16(context)>>r.ttlBits())
}
func (r CheckpointRange) context(ttl byte) byte {
	return byte(uint16(int(ttl)+1-r.Min)<<r.ttlBits()) | ttl
}

func (r *CheckpointRange) bloom(context byte, m, k uint32) checkpointBloom {
	if r == nil {
		return checkpointBloom{m, k}
	}
	return r.blooms[int(uint16(context)>>r.ttlBits())]
}

// Bits 3..6 of the existing type byte identify the assigned distance; bit 7
// continues to identify an early leaf. The one-byte context constraint above
// limits the range to at most 16 choices, so no extra payload byte is needed.
func (r *CheckpointRange) tagPayload(payload []byte, context byte) {
	if r != nil && len(payload) > 0 {
		payload[0] |= byte(int(uint16(context)>>r.ttlBits()) << 3)
	}
}

func PayloadDistance(tag byte, min, max int) (int, error) {
	d := min + int((tag&0x78)>>3)
	if min < 1 || d > max {
		return 0, fmt.Errorf("invalid checkpoint distance index in payload tag")
	}
	return d, nil
}

func checkpointMix(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (r CheckpointRange) sample(ev *Event) byte {
	x := checkpointMix(uint64(r.Seed) ^ checkpointMix(ev.TraceID) ^ checkpointMix(ev.SpanID+0x9e3779b97f4a7c15))
	n := uint64(r.Max - r.Min + 1)
	// Rejection avoids modulo bias for ranges that do not divide 2^64.
	for x < -n%n {
		x = checkpointMix(x + 0x9e3779b97f4a7c15)
	}
	return byte(r.Min + int(x%n) - 1)
}

func (r *CheckpointRange) next(ev *Event, hasParent bool, incoming byte, depth, cpd int) (bool, byte) {
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
