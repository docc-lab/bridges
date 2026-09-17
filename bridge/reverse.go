package bridge

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ReverseConfig is independent of forward checkpoint scheduling. TTLMin and
// TTLMax specify inclusive reverse distances; callers normally default these
// to the forward CPD range. Depth and distance are corpus span edges, not RPCs.
type ReverseConfig struct {
	Policy                string  `json:"policy"`
	LeafRejectProbability float64 `json:"leaf_reject_probability"`
	Probability           float64 `json:"probability"`
	Exponent              float64 `json:"exponent,omitempty"`
	TTLMin                int     `json:"ttl_min"`
	TTLMax                int     `json:"ttl_max"`
	Seed                  uint64  `json:"seed"`
}

func (c ReverseConfig) Validate() error {
	if math.IsNaN(c.LeafRejectProbability) || c.LeafRejectProbability < 0 || c.LeafRejectProbability > 1 {
		return fmt.Errorf("reverse leaf rejection probability must be finite and in [0,1]")
	}
	switch c.Policy {
	case "ttl":
		if c.TTLMin < 1 || c.TTLMax > 256 || c.TTLMin > c.TTLMax {
			return fmt.Errorf("reverse TTL distance must satisfy 1 <= MIN <= MAX <= 256")
		}
	case "probability":
		if math.IsNaN(c.Probability) || c.Probability < 0 || c.Probability > 1 {
			return fmt.Errorf("reverse probability must be finite and in [0,1]")
		}
	case "depth_ratio":
		if math.IsNaN(c.Exponent) || math.IsInf(c.Exponent, 0) || c.Exponent <= 0 {
			return fmt.Errorf("reverse depth_ratio exponent must be finite and positive")
		}
	case "inverse_depth", "depth_linear", "depth_quadratic", "upstream_pressure":
	default:
		return fmt.Errorf("unknown reverse policy %q", c.Policy)
	}
	if c.Policy != "depth_ratio" && c.Exponent != 0 {
		return fmt.Errorf("reverse exponent is only valid with the depth_ratio policy")
	}
	if c.Policy != "probability" && c.Probability != 0 {
		return fmt.Errorf("reverse probability is only valid with probability policy")
	}
	return nil
}

// ReverseAcceptanceProbability returns the conditional trial probability at
// an ordinary valid ancestor. upstream_pressure is an experimental simulator
// policy, not an SDK configuration name. Boundary absorption is separate.
func ReverseAcceptanceProbability(policy string, p float64, receiverDepth, originDepth int) float64 {
	return reverseAcceptance(policy, p, 0, receiverDepth, originDepth)
}

// reverseAcceptance carries the depth_ratio exponent. Unlike the normalized
// depth_linear/depth_quadratic weights, whose first-hop probability falls as
// (k+1)/n, the depth_ratio family's first-hop probability is (n/(n+1))^m, which
// RISES with origin depth. It therefore absorbs deep origins early and shallow
// origins late, the opposite bias, and never reaches 1.
func reverseAcceptance(policy string, p, exponent float64, receiverDepth, originDepth int) float64 {
	if originDepth <= 0 || receiverDepth < 0 || receiverDepth >= originDepth {
		return 0
	}
	switch policy {
	case "probability":
		return p
	case "inverse_depth":
		return 1 / float64(originDepth)
	case "depth_linear":
		return 2 * (float64(receiverDepth) + 1) / (float64(originDepth) * (float64(originDepth) + 1))
	case "depth_ratio":
		return math.Pow((float64(receiverDepth)+1)/(float64(originDepth)+1), exponent)
	case "depth_quadratic":
		// Weights proportional to (d+1)^2 over the origin's ancestors, normalized
		// by sum_{j=1..n} j^2 = n(n+1)(2n+1)/6. Steeper than depth_linear, so more
		// of the mass sits on the deep receivers the truss actually reaches before
		// its window root absorbs it.
		d, n := float64(receiverDepth)+1, float64(originDepth)
		return 6 * d * d / (n * (n + 1) * (2*n + 1))
	case "upstream_pressure":
		return 1 / (float64(receiverDepth) + 1)
	default:
		return 0
	}
}

// ReverseSegment is one immutable origin and payload with an independently
// mutable reverse countdown. OriginCheckpointDepth is analysis-only metadata;
// it is deliberately excluded from serialization.
type ReverseSegment struct {
	Kind                  string
	OriginSpanID          uint64
	OriginDepth           int
	Payload               []byte
	TTL                   *int
	OriginCheckpointDepth int
}

func (s ReverseSegment) RawTrussBytes() int       { return len(s.Payload) }
func (s ReverseSegment) OriginMetadataBytes() int { return 8 + VarintLen(s.OriginDepth) }

// RawBytes includes origin metadata and a one-byte explicit TTL, when present.
// It excludes the binary bundle version and record length framing.
func (s ReverseSegment) RawBytes() int {
	n := s.RawTrussBytes() + s.OriginMetadataBytes()
	if s.TTL != nil {
		n++
	}
	return n
}

// ReverseReceiver describes one span-completion decision. NonRecording and
// SyntheticForcedLP bypass all decisions; the current Uber corpus uses neither.
type ReverseReceiver struct {
	TraceID            uint64
	SpanID             uint64
	Depth              int
	OriginalCheckpoint bool
	ReturnBoundary     bool
	NonRecording       bool
	SyntheticForcedLP  bool
}

// RouteReverseSegments never mutates an input segment. Trials are keyed by
// trace, origin, receiver, seed, and a separate reverse stream discriminator,
// so sibling order and forward random draws cannot change decisions.
func RouteReverseSegments(c ReverseConfig, receiver ReverseReceiver, pending []ReverseSegment) (accepted, forwarded []ReverseSegment) {
	for _, segment := range pending {
		if receiver.NonRecording || receiver.SyntheticForcedLP {
			forwarded = append(forwarded, segment)
			continue
		}
		if receiver.OriginalCheckpoint || receiver.ReturnBoundary {
			accepted = append(accepted, segment)
			continue
		}
		if segment.TTL != nil {
			if *segment.TTL == 0 {
				accepted = append(accepted, segment)
			} else {
				ttl := *segment.TTL - 1
				segment.TTL = &ttl
				forwarded = append(forwarded, segment)
			}
			continue
		}
		valid := validReverseCheckpointSegment(segment)
		p := 0.0
		if valid {
			p = reverseAcceptance(c.Policy, c.Probability, c.Exponent, receiver.Depth, segment.OriginDepth)
		}
		if p >= 1 || (p > 0 && reverseUniform(c.Seed, receiver.TraceID, segment.OriginSpanID, receiver.SpanID, reverseAcceptanceStream) < p) {
			accepted = append(accepted, segment)
		} else {
			forwarded = append(forwarded, segment)
		}
	}
	return
}

const (
	reverseLeafStream       uint64 = 0x7b9a20f08b146863
	reverseTTLStream        uint64 = 0x6083245a9f37d251
	reverseAcceptanceStream uint64 = 0xc17d7ef234086ba5
)

func reverseRandom(seed, tid, origin, receiver, stream uint64) uint64 {
	return checkpointMix(seed ^ stream ^ checkpointMix(tid) ^ checkpointMix(origin+0x9e3779b97f4a7c15) ^ checkpointMix(receiver+0xd1b54a32d192ed03))
}
func reverseUniform(seed, tid, origin, receiver, stream uint64) float64 {
	return float64(reverseRandom(seed, tid, origin, receiver, stream)>>11) * (1.0 / (1 << 53))
}
func reverseKind(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	switch PayloadType(payload[0]) {
	case PCRBBridgeTypeID:
		return "checkpoint.pb"
	case CGPRBBridgeTypeID:
		return "checkpoint.cgpb"
	case SB3BridgeTypeID:
		return "checkpoint.sb"
	default:
		return ""
	}
}

// All three checkpoint formats begin with type and absolute depth and have
// at least a one-byte checkpoint prefix and one-byte Bloom body. Full geometry
// validation belongs to their decoder; the router at least rejects truncated
// headers and inconsistent origin metadata instead of accepting them at p=1.
func validReverseCheckpointSegment(s ReverseSegment) bool {
	if len(s.Payload) < 4 || reverseKind(s.Payload) != s.Kind || s.Kind == "" {
		return false
	}
	depth, n := binary.Uvarint(s.Payload[1:])
	return n > 0 && len(s.Payload) >= 1+n+2 && s.OriginDepth >= 0 && depth == uint64(s.OriginDepth)
}

// ReverseRoute records final acceptance once, before modeled collection loss.
type ReverseRoute struct {
	OriginSpanID            uint64
	OriginDepth             int
	ReceiverSpanID          uint64
	ReceiverDepth           int
	OriginalCheckpointDepth int
	Distance                int
	Mandatory               bool
}

// ReverseEndResult keeps new metrics distinct from the existing _br metric.
// Encoded byte counts are the actual binary bundle values delivered on corpus
// span-return edges. They exclude external RPC/OTLP framing, like the forward
// bridge counters; logical span-return edges are not claimed to be RPCs.
type ReverseEndResult struct {
	OriginalCheckpoint   bool
	ForcedLeafCheckpoint bool
	RejectedLeaf         bool
	ReceivingCheckpoint  bool
	PromotedCheckpoint   bool
	ReturnEdge           bool
	PendingReceived      int
	Accepted             []ReverseSegment
	Returned             []ReverseSegment
	// ReturnContext is the serialized value delivered to the parent. The
	// parent decodes this value before routing; it does not receive Returned.
	// CheckpointContext is the emitted returned-truss attribute value. The
	// receiver's own _br remains in EndResult.Payload.
	// Both use the simulator's native binary format in reverse_wire.go.
	ReturnContext       []byte
	CheckpointContext   []byte
	CheckpointBytes     int
	ReverseRawBytes     int
	ReverseEncodedBytes int
	Routes              []ReverseRoute
}

type checkpointSnapshotter interface{ CheckpointPayload(*Event) []byte }

type reverseSpanState struct {
	event         Event
	parent        *reverseSpanState
	depth         int
	original      bool
	originalDepth int
	hasChildren   bool
	openChildren  int
	ended         bool
	own           []byte
	pending       [][]byte
}

// ReverseHandler wraps a forward handler without changing its scheduling or
// baggage. Original OnStart emissions are held until OnEnd, allowing all
// accepted trusses to share that span's single checkpoint export. Child returns
// must precede parent completion; malformed lifecycle order fails explicitly.
type ReverseHandler struct {
	base     Handler
	snapshot checkpointSnapshotter
	config   ReverseConfig
	state    map[stateKey]*reverseSpanState
	counts   map[uint64]*reverseTraceCounts
}

type reverseTraceCounts struct{ rejected, accepted int }

func NewReverseHandler(base Handler, c ReverseConfig) (*ReverseHandler, error) {
	if c.Policy == "" {
		c.Policy = "ttl"
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	snapshot, ok := base.(checkpointSnapshotter)
	if !ok {
		return nil, fmt.Errorf("reverse trusses require a checkpoint payload snapshot handler")
	}
	switch h := base.(type) {
	case *PCRBBridgeHandler:
		h.Capture = true
	case *CGPRBBridgeHandler:
		h.Capture = true
	case *SB3Handler:
		if h.EmitSink != nil {
			return nil, fmt.Errorf("reverse trusses require collecting final outputs through ReverseHandler, not SB3 EmitSink")
		}
		h.Capture = true
	}
	return &ReverseHandler{base: base, snapshot: snapshot, config: c, state: make(map[stateKey]*reverseSpanState), counts: make(map[uint64]*reverseTraceCounts)}, nil
}

func (h *ReverseHandler) OnStart(ev *Event, parentSeqNum int) StartResult {
	key := stateKey{ev.TraceID, ev.SpanID}
	if h.state[key] != nil {
		panic(fmt.Sprintf("reverse trusses: repeated start of trace %x span %x", ev.TraceID, ev.SpanID))
	}
	p := h.state[stateKey{ev.TraceID, ev.ParentID}]
	if ev.ParentID == 0 {
		p = nil
	} else if p == nil {
		panic(fmt.Sprintf("reverse trusses: span %x has unknown parent %x; parent start must precede child start", ev.SpanID, ev.ParentID))
	}
	if p != nil && p.ended {
		panic(fmt.Sprintf("reverse trusses: span %x starts after parent %x completed", ev.SpanID, ev.ParentID))
	}
	r := h.base.OnStart(ev, parentSeqNum)
	s := &reverseSpanState{event: *ev, parent: p, original: r.EmitBytes > 0, own: append([]byte(nil), r.Payload...)}
	if p != nil {
		s.depth = p.depth + 1
		s.originalDepth = p.originalDepth
		p.hasChildren = true
		p.openChildren++
	}
	if s.original {
		s.originalDepth = s.depth
	}
	if s.original && len(s.own) == 0 {
		panic("reverse trusses: original checkpoint handler did not capture its payload")
	}
	h.state[key] = s
	if h.counts[ev.TraceID] == nil {
		h.counts[ev.TraceID] = &reverseTraceCounts{}
	}
	r.EmitBytes = 0
	r.Payload = nil
	return r
}

func (h *ReverseHandler) OnEnd(ev *Event) EndResult {
	s := h.state[stateKey{ev.TraceID, ev.SpanID}]
	if s == nil {
		panic(fmt.Sprintf("reverse trusses: end without start for span %x", ev.SpanID))
	}
	if ev.ParentID != s.event.ParentID {
		panic(fmt.Sprintf("reverse trusses: span %x end parent %x differs from start parent %x", ev.SpanID, ev.ParentID, s.event.ParentID))
	}
	if s.ended {
		return EndResult{}
	}
	if s.openChildren > 0 {
		panic(fmt.Sprintf("reverse trusses: span %x completes with %d unfinished children; child returns must precede parent completion", ev.SpanID, s.openChildren))
	}
	var pending []ReverseSegment
	for _, context := range s.pending {
		segments, err := DecodeReverseContext(context)
		if err != nil {
			panic(fmt.Sprintf("reverse trusses: span %x received invalid return context: %v", ev.SpanID, err))
		}
		pending = append(pending, segments...)
	}
	rr := &ReverseEndResult{OriginalCheckpoint: s.original, ReturnEdge: s.parent != nil, PendingReceived: len(pending)}
	receiver := ReverseReceiver{TraceID: ev.TraceID, SpanID: ev.SpanID, Depth: s.depth, OriginalCheckpoint: s.original, ReturnBoundary: s.parent == nil}
	rr.Accepted, rr.Returned = RouteReverseSegments(h.config, receiver, pending)
	h.counts[ev.TraceID].accepted += len(rr.Accepted)
	s.pending = nil
	rr.ReceivingCheckpoint = len(rr.Accepted) > 0
	rr.PromotedCheckpoint = rr.ReceivingCheckpoint && !s.original
	var promotedPayload []byte
	if rr.PromotedCheckpoint {
		promotedPayload = h.snapshot.CheckpointPayload(ev)
	}
	r := h.base.OnEnd(ev)
	isUnscheduledLeaf := !s.original && !s.hasChildren
	if isUnscheduledLeaf && s.parent != nil && reverseUniform(h.config.Seed, ev.TraceID, ev.SpanID, 0, reverseLeafStream) < h.config.LeafRejectProbability {
		if len(r.Payload) == 0 {
			panic("reverse trusses: rejected leaf handler did not capture its payload")
		}
		segment := ReverseSegment{Kind: reverseKind(r.Payload), OriginSpanID: ev.SpanID, OriginDepth: s.depth, Payload: append([]byte(nil), r.Payload...), OriginCheckpointDepth: s.originalDepth}
		if h.config.Policy == "ttl" {
			x := reverseRandom(h.config.Seed, ev.TraceID, ev.SpanID, 0, reverseTTLStream)
			n := uint64(h.config.TTLMax - h.config.TTLMin + 1)
			for x < -n%n {
				x = checkpointMix(x + 0x9e3779b97f4a7c15)
			}
			ttl := h.config.TTLMin + int(x%n) - 1
			segment.TTL = &ttl
		}
		rr.Returned = append(rr.Returned, segment)
		rr.RejectedLeaf = true
		h.counts[ev.TraceID].rejected++
		r.EmitBytes = 0
		r.Payload = nil
		r.Depth = s.depth
		r.DepthBytes = DepthKeyBytes + VarintLen(s.depth)
	} else if isUnscheduledLeaf {
		rr.ForcedLeafCheckpoint = true
	}
	if s.original {
		r.Payload = s.own
		r.EmitBytes = BRPropertyNameOverheadBytes + len(s.own)
	} else if rr.PromotedCheckpoint {
		r.Payload = promotedPayload
		if len(r.Payload) == 0 {
			panic("reverse trusses: promoted receiver has no own checkpoint snapshot")
		}
		r.EmitBytes = BRPropertyNameOverheadBytes + len(r.Payload)
	}
	if r.EmitBytes > 0 {
		r.DepthBytes = 0
		r.OcBytes = 0
	}
	rr.CheckpointBytes = r.EmitBytes
	if len(rr.Accepted) > 0 {
		encoded, err := EncodeReverseContext(rr.Accepted)
		if err != nil {
			panic(err)
		}
		rr.CheckpointContext = encoded
		rr.CheckpointBytes += len("bridges.checkpoint") + len(rr.CheckpointContext)
		for _, segment := range rr.Accepted {
			// Forward checkpoint depth is audit metadata, not return baggage.
			// Recover it only for reporting after the routing decision is final.
			originalDepth := -1
			if origin := h.state[stateKey{ev.TraceID, segment.OriginSpanID}]; origin != nil {
				originalDepth = origin.originalDepth
			}
			rr.Routes = append(rr.Routes, ReverseRoute{OriginSpanID: segment.OriginSpanID, OriginDepth: segment.OriginDepth, ReceiverSpanID: ev.SpanID, ReceiverDepth: s.depth, OriginalCheckpointDepth: originalDepth, Distance: segment.OriginDepth - s.depth, Mandatory: s.original || s.parent == nil})
		}
	}
	if len(rr.Returned) > 0 {
		if s.parent == nil {
			panic("reverse trusses: terminal span forwarded a truss")
		}
		if s.parent.ended {
			panic(fmt.Sprintf("reverse trusses: late return from span %x to completed parent %x; child returns must precede parent completion", ev.SpanID, ev.ParentID))
		}
		for _, segment := range rr.Returned {
			rr.ReverseRawBytes += segment.RawBytes()
		}
		encoded, err := EncodeReverseContext(rr.Returned)
		if err != nil {
			panic(err)
		}
		rr.ReturnContext = encoded
		rr.ReverseEncodedBytes = len(rr.ReturnContext)
		// Delivery owns its bytes independently of the child's output buffers.
		s.parent.pending = append(s.parent.pending, append([]byte(nil), rr.ReturnContext...))
	}
	s.ended = true
	if s.parent != nil {
		s.parent.openChildren--
	}
	r.Reverse = rr
	return r
}

func (h *ReverseHandler) EvictTrace(tid uint64) {
	if c := h.counts[tid]; c != nil && c.rejected != c.accepted {
		panic(fmt.Sprintf("reverse trusses: trace %x rejected %d trusses but emitted %d", tid, c.rejected, c.accepted))
	}
	for key, s := range h.state {
		if key.traceID == tid {
			if !s.ended || len(s.pending) > 0 {
				panic(fmt.Sprintf("reverse trusses: trace %x evicted with unfinished span %x", tid, s.event.SpanID))
			}
			delete(h.state, key)
		}
	}
	delete(h.counts, tid)
	h.base.EvictTrace(tid)
}
