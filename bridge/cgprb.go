package bridge

import (
	"encoding/binary"

	"bridges/bloom"
)

// CGPRBBridgeHandler is the call-graph-preserving version of PCRB: it carries
// PCRB's payload (truncated checkpoint-root id for identity anchoring + the
// in-window ancestor bloom for threading) augmented with a window-local hash
// array (HA) that records the in-window BRANCH POINTS by exact parent id.
//
// This is a deliberate re-base of the legacy CGPB (bridge/cgpb.go), which
// augmented the original PB and is kept only for its bag-size/Python-parity
// numbers. CGPRB differs from legacy CGPB in three ways, all so the HA stays
// cheap and window-scoped to match the PCR reconstruction model:
//
//  1. EVERYTHING resets at a checkpoint — bloom AND HA — and the root re-roots.
//     Cross-window structure is recovered from the checkpoint-root chain, not
//     the HA, so the HA never needs to span windows.
//  2. The inherited HA flows ONLY down the first-child edge. Sibling subtrees
//     do not each re-carry the same ancestor entries, so an entry is never
//     duplicated across the fan-out.
//  3. Only the 2nd-started sibling of a parent appends one entry,
//     (parent_id || varint(absolute depth)). One record per branching parent
//     is a sufficient truth value that the branch exists; 3rd+ siblings carry
//     no HA at all.
//
// Wire format of the _br value:
//
//	type(1) || varint(absolute depth) || ckptK || bloomBits || haBytes
//
// bloomBits has the fixed PCRB geometry length, so the decoder splits the HA
// off as the trailing remainder. Depth is always absolute (PCR convention).
type CGPRBBridgeHandler struct {
	cpd       uint32
	prefixLen int
	bloomM    uint32
	bloomK    uint32
	bloomLen  int

	// Capture materializes serialized payloads for the reconstruction harness.
	Capture bool

	state map[stateKey]*cgprbState
}

type cgprbState struct {
	ckpt        [8]byte
	bloomBytes  []byte // propagated bloom (inherited + self; checkpoints: empty)
	inherited   []byte // pre-self bloom — what this span emits in its own payload
	ha          []byte // window-local HA this span emits AND propagates to its first child (empty at a checkpoint)
	depth       uint32
	emitted     bool
	hasChildren bool
}

// CGPRBBridgeTypeID identifies a CGPRB _br payload (first byte of the value).
const CGPRBBridgeTypeID = 6

func NewCGPRBBridgeHandler(checkpointDistance, prefixLen int, bloomFPRate float64) *CGPRBBridgeHandler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	if prefixLen < 1 {
		prefixLen = 1
	} else if prefixLen > 8 {
		prefixLen = 8
	}
	// Same bloom geometry as PCRB: the threadable population is the in-window
	// subtree (<= cpd-1 entries), not the trace width.
	m, k := bloom.EstimateParameters(PCRBBloomCapacity(checkpointDistance), bloomFPRate)
	return &CGPRBBridgeHandler{
		cpd:       uint32(checkpointDistance),
		prefixLen: prefixLen,
		bloomM:    m,
		bloomK:    k,
		bloomLen:  int((m + 7) / 8),
		state:     make(map[stateKey]*cgprbState),
	}
}

func (h *CGPRBBridgeHandler) OnStart(ev *Event, parentSeqNum int) StartResult {
	var parentState *cgprbState
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

	// HA inheritance: only the FIRST child carries the parent's accumulated HA
	// forward; 2nd+ siblings start fresh. The 2nd sibling then appends exactly
	// one entry recording that this parent branched. (3rd+ siblings: nothing.)
	var ha []byte
	if parentState != nil && parentSeqNum == 1 {
		ha = parentState.ha
	}
	if ev.ParentID != 0 && parentSeqNum == 2 {
		pid := BigEndian8(ev.ParentID)
		next := make([]byte, 0, len(ha)+8+VarintLen(int(depth)))
		next = append(next, ha...)
		next = append(next, pid[:]...)
		next = binary.AppendUvarint(next, uint64(depth))
		ha = next
	}

	isCheckpoint := depth%h.cpd == 0

	var emitBytes int
	var payload []byte
	if isCheckpoint {
		// Checkpoint payload closes the window above: previous checkpoint
		// prefix + the inherited window bloom + the window's HA. Then reset.
		emitBytes = BRPropertyNameOverheadBytes + cgprbTypeTagBytes +
			VarintLen(int(depth)) + h.prefixLen + len(inherited) + len(ha)
		if h.Capture {
			payload = packCGPRBPayload(int(depth), ckpt, h.prefixLen, inherited, ha)
		}
		// Re-root and reset bloom AND ha for the new window.
		ckpt = BigEndian8(ev.SpanID)
		bf, _ := bloom.New(h.bloomM, h.bloomK)
		inherited = bf.ToBytes()
		ha = nil
	}

	// Propagated bloom: inherited + self (checkpoints skip the self-add, same
	// as PCRB). The HA carries no self-entry — a branch is recorded by the
	// branching parent's 2nd child, never by the parent itself.
	propagated := inherited
	if !isCheckpoint {
		bf := bloom.Deserialize(inherited, h.bloomM, h.bloomK)
		spanHex := HexOf(ev.SpanID)
		bf.Add(spanHex[:])
		propagated = bf.ToBytes()
	}

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + VarintLen(int(depth)) + h.prefixLen + len(propagated) + len(ha)
	}

	h.state[stateKey{ev.TraceID, ev.SpanID}] = &cgprbState{
		ckpt:       ckpt,
		bloomBytes: propagated,
		inherited:  inherited,
		ha:         ha,
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

func (h *CGPRBBridgeHandler) OnEnd(ev *Event) EndResult {
	key := stateKey{ev.TraceID, ev.SpanID}
	ps, ok := h.state[key]
	if !ok {
		return EndResult{}
	}
	isLeaf := !ps.hasChildren

	var emitBytes, depthBytes int
	var payload []byte
	if isLeaf && !ps.emitted {
		// Leaf payload: inherited (pre-self) bloom + this span's window HA.
		emitBytes = BRPropertyNameOverheadBytes + cgprbTypeTagBytes +
			VarintLen(int(ps.depth)) + h.prefixLen + len(ps.inherited) + len(ps.ha)
		if h.Capture {
			payload = packCGPRBPayload(int(ps.depth), ps.ckpt, h.prefixLen, ps.inherited, ps.ha)
		}
		ps.emitted = true
	} else if !ps.emitted {
		depthBytes = DepthKeyBytes + VarintLen(int(ps.depth))
	}
	return EndResult{EmitBytes: emitBytes, DepthBytes: depthBytes, Payload: payload, Depth: int(ps.depth)}
}

// cgprbTypeTagBytes: honest 1-byte type tag in the emit accounting (mirrors
// pcrbTypeTagBytes).
const cgprbTypeTagBytes = 1

// packCGPRBPayload serializes the on-wire _br value:
// type(1) || varint(depth) || ckptK || bloomBits || haBytes. The bloom has the
// fixed PCRB geometry length, so the HA is recovered as the trailing remainder.
func packCGPRBPayload(depth int, ckpt [8]byte, prefixLen int, bloomBytes, haBytes []byte) []byte {
	out := make([]byte, 0, 1+VarintLen(depth)+prefixLen+len(bloomBytes)+len(haBytes))
	out = append(out, byte(CGPRBBridgeTypeID))
	out = binary.AppendUvarint(out, uint64(depth))
	out = append(out, ckpt[:prefixLen]...)
	out = append(out, bloomBytes...)
	out = append(out, haBytes...)
	return out
}

func (h *CGPRBBridgeHandler) EvictTrace(traceID uint64) {
	for k := range h.state {
		if k.traceID == traceID {
			delete(h.state, k)
		}
	}
}
