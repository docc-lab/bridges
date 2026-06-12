package bridge

import (
	"encoding/binary"

	"bridges/bloom"
)

// PCRBBridgeHandler is PCR plus a window bloom: the payload carries both the
// truncated checkpoint-root span ID (exact anchoring, as PCR) and a bloom
// filter of the ancestor window (as PB). Reconstruction anchors fragments by
// identity — the bloom is never consulted for anchor selection — and then
// uses the bloom only to thread the fragment through surviving same-window
// ancestors under that one checkpoint's subtree. The bloom's candidate
// population is therefore the in-window subtree of a single named
// checkpoint, not the trace width, so it can run at a much coarser FP rate
// than PB for the same (or lower) wrong-edge rate.
//
// Wire format of the _br value:
//
//	type(1) || varint(absolute depth) || ckptK || bloomBits
//
// Emission sites and the _d attribute on interior non-checkpoint spans
// mirror PCR/PB-EmitDepth. Payloads always carry the INHERITED (pre-self)
// bloom, and checkpoints never add themselves — the loaded population is
// exactly the spans strictly between two checkpoints, <= cpd-1 entries,
// which is what the bloom is sized for (see PCRBBloomCapacity).
type PCRBBridgeHandler struct {
	cpd       uint32
	prefixLen int
	bloomM    uint32
	bloomK    uint32
	bloomLen  int

	// Capture materializes serialized payloads for the reconstruction
	// harness. Off for bagsize runs.
	Capture bool

	state map[stateKey]*pcrbState
}

type pcrbState struct {
	ckpt        [8]byte
	bloomBytes  []byte // propagated bloom (inherited + self; checkpoints: empty)
	inherited   []byte // pre-self bloom — what this span emits in its own payload
	depth       uint32
	emitted     bool
	hasChildren bool
}

// PCRBBridgeTypeID identifies a PCRB _br payload (first byte of the value).
const PCRBBridgeTypeID = 5

// PCRBBloomCapacity is the bloom sizing population for PCRB: cpd-1, not cpd.
// Threading only ever tests spans STRICTLY between two checkpoints (the
// anchor is named by prefix, never membership-tested, and a payload's own
// span is never tested against itself), and the handler keeps the loaded
// population to exactly that set: checkpoints do not re-add themselves
// after reset, and payloads carry the INHERITED bloom (pre-self-add). The
// deepest possible payload therefore holds exactly cpd-1 entries. NOTE:
// this intentionally diverges from PB's geometry for the same --bloom-fp;
// the recon side must derive (m, k) from the same capacity.
func PCRBBloomCapacity(cpd int) int {
	if cpd <= 2 {
		return 1
	}
	return cpd - 1
}

// pcrbTypeTagBytes: honest 1-byte type tag in the emit accounting (see the
// pcrTypeTagBytes comment for why this differs from the legacy *TypeID-as-
// byte-count convention).
const pcrbTypeTagBytes = 1

func NewPCRBBridgeHandler(checkpointDistance, prefixLen int, bloomFPRate float64) *PCRBBridgeHandler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	if prefixLen < 1 {
		prefixLen = 1
	} else if prefixLen > 8 {
		prefixLen = 8
	}
	m, k := bloom.EstimateParameters(PCRBBloomCapacity(checkpointDistance), bloomFPRate)
	return &PCRBBridgeHandler{
		cpd:       uint32(checkpointDistance),
		prefixLen: prefixLen,
		bloomM:    m,
		bloomK:    k,
		bloomLen:  int((m + 7) / 8),
		state:     make(map[stateKey]*pcrbState),
	}
}

func (h *PCRBBridgeHandler) OnStart(ev *Event, _ int) StartResult {
	var parentState *pcrbState
	var baggageFound bool
	if ev.ParentID != 0 {
		parentState = h.state[stateKey{ev.TraceID, ev.ParentID}]
		if parentState != nil {
			parentState.hasChildren = true
			baggageFound = true
		}
	}

	var depth uint32
	var ckpt [8]byte
	var inherited []byte // parent's propagated bloom: ancestors strictly above this span, within the window
	if parentState != nil {
		depth = parentState.depth + 1
		ckpt = parentState.ckpt
		inherited = parentState.bloomBytes
	} else {
		bf, _ := bloom.New(h.bloomM, h.bloomK)
		inherited = bf.ToBytes()
	}

	isCheckpoint := depth%h.cpd == 0

	var emitBytes int
	var payload []byte
	if isCheckpoint {
		// Checkpoint payload: previous checkpoint's prefix + the INHERITED
		// window bloom — ancestors strictly between the two checkpoints,
		// exactly <= cpd-1 entries (see PCRBBloomCapacity).
		emitBytes = BRPropertyNameOverheadBytes + pcrbTypeTagBytes +
			VarintLen(int(depth)) + h.prefixLen + len(inherited)
		if h.Capture {
			payload = packPCRBPayload(int(depth), ckpt, h.prefixLen, inherited)
		}
		// Re-root and reset. The checkpoint does NOT add itself: threading
		// never tests the anchor (the prefix names it), so its entry would
		// only inflate the load.
		ckpt = BigEndian8(ev.SpanID)
		bf, _ := bloom.New(h.bloomM, h.bloomK)
		inherited = bf.ToBytes() // empty: nothing nameable below a checkpoint yet
	}

	// Propagated state: inherited + self (descendants must be able to test
	// this span). Checkpoints skip the self-add per the above.
	propagated := inherited
	if !isCheckpoint {
		bf := bloom.Deserialize(inherited, h.bloomM, h.bloomK)
		spanHex := HexOf(ev.SpanID)
		bf.Add(spanHex[:])
		propagated = bf.ToBytes()
	}

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + VarintLen(int(depth)) + h.prefixLen + len(propagated)
	}

	h.state[stateKey{ev.TraceID, ev.SpanID}] = &pcrbState{
		ckpt:       ckpt,
		bloomBytes: propagated,
		inherited:  inherited,
		depth:      depth,
		emitted:    isCheckpoint,
	}

	return StartResult{
		BaggageFound: baggageFound,
		BaggageBytes: baggageBytes,
		EmitBytes:    emitBytes,
		Payload:      payload,
	}
}

func (h *PCRBBridgeHandler) OnEnd(ev *Event) EndResult {
	key := stateKey{ev.TraceID, ev.SpanID}
	ps, ok := h.state[key]
	if !ok {
		return EndResult{}
	}
	isLeaf := !ps.hasChildren

	var emitBytes, depthBytes int
	var payload []byte
	if isLeaf && !ps.emitted {
		// Inherited (pre-self) bloom: a payload's own span is never tested.
		emitBytes = BRPropertyNameOverheadBytes + pcrbTypeTagBytes +
			VarintLen(int(ps.depth)) + h.prefixLen + len(ps.inherited)
		if h.Capture {
			payload = packPCRBPayload(int(ps.depth), ps.ckpt, h.prefixLen, ps.inherited)
		}
		ps.emitted = true
	} else if !ps.emitted {
		depthBytes = DepthKeyBytes + VarintLen(int(ps.depth))
	}
	return EndResult{EmitBytes: emitBytes, DepthBytes: depthBytes, Payload: payload, Depth: int(ps.depth)}
}

func packPCRBPayload(depth int, ckpt [8]byte, prefixLen int, bloomBytes []byte) []byte {
	out := make([]byte, 0, 1+VarintLen(depth)+prefixLen+len(bloomBytes))
	out = append(out, byte(PCRBBridgeTypeID))
	out = binary.AppendUvarint(out, uint64(depth))
	out = append(out, ckpt[:prefixLen]...)
	out = append(out, bloomBytes...)
	return out
}

func (h *PCRBBridgeHandler) EvictTrace(traceID uint64) {
	for k := range h.state {
		if k.traceID == traceID {
			delete(h.state, k)
		}
	}
}
