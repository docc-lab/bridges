package bridge

import (
	"encoding/binary"

	"bridges/bloom"
)

// CGPBBridgeHandler is the Go port of trace_simulator.py CGPBBridgeHandler.
// CGPB extends PB with a "hash array" of (parent_span_id, depth_mod) entries
// appended on the 2nd-started sibling of each parent. The hash array is NOT
// reset at checkpoints; only the bloom is.
type CGPBBridgeHandler struct {
	cpd      uint32
	bloomM   uint32
	bloomK   uint32
	bloomLen int

	state map[stateKey]*cgpbState
}

type cgpbState struct {
	bloomBytes  []byte
	haBytes     []byte
	depthMod    uint32
	emitted     bool
	hasChildren bool
}

func NewCGPBBridgeHandler(checkpointDistance int, bloomFPRate float64) *CGPBBridgeHandler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	n := checkpointDistance
	if n < 1 {
		n = 1
	}
	m, k := bloom.EstimateParameters(n, bloomFPRate)
	return &CGPBBridgeHandler{
		cpd:      uint32(checkpointDistance),
		bloomM:   m,
		bloomK:   k,
		bloomLen: int((m + 7) / 8),
		state:    make(map[stateKey]*cgpbState),
	}
}

func (h *CGPBBridgeHandler) OnStart(ev *Event, parentSeqNum int) StartResult {
	var parentState *cgpbState
	var baggageFound bool
	if ev.ParentID != 0 {
		parentState = h.state[stateKey{ev.TraceID, ev.ParentID}]
		if parentState != nil {
			parentState.hasChildren = true
			baggageFound = true
		}
	}

	var parentBloomBytes, parentHaBytes []byte
	var parentDepthMod uint32
	if parentState != nil {
		parentBloomBytes = parentState.bloomBytes
		parentHaBytes = parentState.haBytes
		parentDepthMod = parentState.depthMod
	}

	depthMod := (parentDepthMod + 1) % h.cpd

	var bf *bloom.Filter
	if parentState != nil && len(parentBloomBytes) > 0 {
		bf = bloom.Deserialize(parentBloomBytes, h.bloomM, h.bloomK)
	} else {
		bf, _ = bloom.New(h.bloomM, h.bloomK)
	}
	spanHex := HexOf(ev.SpanID)
	bf.Add(spanHex[:])

	// HA append: only the 2nd-started sibling of each parent appends.
	// Format: parent_span_id_8_bytes_big_endian || varint(depth_mod).
	haBytes := parentHaBytes
	if ev.ParentID != 0 && parentSeqNum == 2 {
		pid := BigEndian8(ev.ParentID)
		next := make([]byte, 0, len(haBytes)+8+VarintLen(int(depthMod)))
		next = append(next, haBytes...)
		next = append(next, pid[:]...)
		next = binary.AppendUvarint(next, uint64(depthMod))
		haBytes = next
	}

	isCheckpoint := depthMod == 0
	var emitBytes int
	if isCheckpoint {
		bfBytesPreReset := bf.ToBytes()
		emitBytes = BRPropertyNameOverheadBytes + CGPBridgeTypeID +
			VarintLen(int(depthMod)) + len(bfBytesPreReset) + len(haBytes)
		// Reset bloom (ha is NOT reset).
		bf, _ = bloom.New(h.bloomM, h.bloomK)
		bf.Add(spanHex[:])
	}
	bfBytes := bf.ToBytes()

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + VarintLen(int(depthMod)) + len(bfBytes) + len(haBytes)
	}

	h.state[stateKey{ev.TraceID, ev.SpanID}] = &cgpbState{
		bloomBytes: bfBytes,
		haBytes:    haBytes,
		depthMod:   depthMod,
		emitted:    isCheckpoint,
	}

	return StartResult{
		BaggageFound: baggageFound,
		BaggageBytes: baggageBytes,
		EmitBytes:    emitBytes,
	}
}

func (h *CGPBBridgeHandler) OnEnd(ev *Event) EndResult {
	key := stateKey{ev.TraceID, ev.SpanID}
	ps, ok := h.state[key]
	if !ok {
		return EndResult{}
	}
	isLeaf := !ps.hasChildren

	var emitBytes int
	if isLeaf && !ps.emitted {
		emitBytes = BRPropertyNameOverheadBytes + CGPBridgeTypeID +
			VarintLen(int(ps.depthMod)) + len(ps.bloomBytes) + len(ps.haBytes)
		ps.emitted = true
	}
	// See pb.go: state survives until EvictTrace to handle clock-skew children
	// that start after their parent's end.
	return EndResult{EmitBytes: emitBytes}
}

func (h *CGPBBridgeHandler) EvictTrace(traceID uint64) {
	for k := range h.state {
		if k.traceID == traceID {
			delete(h.state, k)
		}
	}
}
