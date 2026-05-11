package bridge

import "bridges/bloom"

// PathBridgeHandler is the Go port of trace_simulator.py PathBridgeHandler.
//
// Per-span state is kept in a flat map keyed by (trace_id, span_id). Entries
// are freed at OnEnd, so memory stays proportional to currently-open spans
// across all active traces (not the total span count seen so far). This
// matches what the Python sim *would* do if it cleaned up — which it
// doesn't, hence its OOM at scale.
type PathBridgeHandler struct {
	cpd      uint32
	bloomM   uint32
	bloomK   uint32
	bloomLen int

	state map[stateKey]*pbState
}

type stateKey struct {
	traceID uint64
	spanID  uint64
}

type pbState struct {
	bloomBytes  []byte // post-checkpoint-reset bloom bits (or pre-reset on non-checkpoint)
	depthMod    uint32
	emitted     bool // OnStart already emitted EMIT_PAYLOAD_BYTES (depth-checkpoint)
	hasChildren bool
}

// NewPathBridgeHandler matches Python PathBridgeHandler.__init__.
func NewPathBridgeHandler(checkpointDistance int, bloomFPRate float64) *PathBridgeHandler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	n := checkpointDistance
	if n < 1 {
		n = 1
	}
	m, k := bloom.EstimateParameters(n, bloomFPRate)
	return &PathBridgeHandler{
		cpd:      uint32(checkpointDistance),
		bloomM:   m,
		bloomK:   k,
		bloomLen: int((m + 7) / 8),
		state:    make(map[stateKey]*pbState),
	}
}

func (h *PathBridgeHandler) OnStart(ev *Event, _ int) StartResult {
	var parentState *pbState
	var baggageFound bool
	if ev.ParentID != 0 {
		parentState = h.state[stateKey{ev.TraceID, ev.ParentID}]
		if parentState != nil {
			parentState.hasChildren = true
			baggageFound = true
		}
	}

	var depthMod uint32
	var bf *bloom.Filter
	if parentState != nil {
		depthMod = (parentState.depthMod + 1) % h.cpd
		bf = bloom.Deserialize(parentState.bloomBytes, h.bloomM, h.bloomK)
	} else {
		depthMod = 0
		bf, _ = bloom.New(h.bloomM, h.bloomK)
	}

	spanHex := HexOf(ev.SpanID)
	bf.Add(spanHex[:])
	bfBytes := bf.ToBytes()
	isCheckpoint := depthMod == 0

	var emitBytes int
	if isCheckpoint {
		// Pre-reset bloom bytes are what the checkpoint payload measures.
		emitBytes = BRPropertyNameOverheadBytes + PBBridgeTypeID +
			VarintLen(int(depthMod)) + len(bfBytes)
		// Reset bloom and re-add this span.
		bf, _ = bloom.New(h.bloomM, h.bloomK)
		bf.Add(spanHex[:])
		bfBytes = bf.ToBytes()
	}

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + VarintLen(int(depthMod)) + len(bfBytes)
	}

	h.state[stateKey{ev.TraceID, ev.SpanID}] = &pbState{
		bloomBytes: bfBytes,
		depthMod:   depthMod,
		emitted:    isCheckpoint,
	}

	return StartResult{
		BaggageFound: baggageFound,
		BaggageBytes: baggageBytes,
		EmitBytes:    emitBytes,
	}
}

func (h *PathBridgeHandler) OnEnd(ev *Event) EndResult {
	key := stateKey{ev.TraceID, ev.SpanID}
	ps, ok := h.state[key]
	if !ok {
		return EndResult{}
	}
	isLeaf := !ps.hasChildren

	var emitBytes int
	if isLeaf && !ps.emitted {
		emitBytes = BRPropertyNameOverheadBytes + PBBridgeTypeID +
			VarintLen(int(ps.depthMod)) + len(ps.bloomBytes)
		ps.emitted = true
	}
	// State is intentionally NOT freed here: clock-skew children may start
	// after their parent's end, and they need to find parent state alive.
	// The simulator calls EvictTrace once all events for the trace are
	// processed, which is the safe point to free.
	return EndResult{EmitBytes: emitBytes}
}

func (h *PathBridgeHandler) EvictTrace(traceID uint64) {
	// Defensive cleanup: well-formed traces have no remaining state at trace
	// close (everything is freed at OnEnd). Iterates only state we still hold,
	// so this is bounded by leftover (i.e. malformed) entries.
	for k := range h.state {
		if k.traceID == traceID {
			delete(h.state, k)
		}
	}
}
