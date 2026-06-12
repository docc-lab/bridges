package bridge

import "encoding/binary"

// PCRBridgeHandler is the P-Checkpoint-Root bridge: structurally identical
// to PathBridgeHandler (same checkpoint cadence, same emission sites, same
// _d depth attribute on interior spans), but the propagated/emitted bridge
// metadata is a truncated span ID of the nearest checkpoint ancestor instead
// of a bloom filter of the ancestor window.
//
// Rationale (see the ckpt_prefix_scan analysis): within one trace, checkpoint
// span IDs scoped by depth — and the payload already carries the depth — are
// disambiguated corpus-wide by 3-4 leading bytes. Naming the attachment
// point by identity replaces P-Bridge's probabilistic membership machinery
// (and its entire misattachment phenomenology) with an exact, collision-
// detectable lookup.
//
// Wire format of the _br value: type(1) || varint(absolute depth) || ckptK,
// where ckptK is the first PrefixLen bytes of the big-endian 8-byte span ID
// of the nearest checkpoint ancestor STRICTLY ABOVE the emitting span
// (zeros for the root, which has none). Checkpoint spans emit the previous
// checkpoint's prefix (pre-reset, like PB's pre-reset bloom), then re-root
// to their own ID for descendants — mirroring S-Bridge's ckpt8 rooting,
// truncated.
//
// PCR always operates in EmitDepth semantics: payloads carry absolute depth
// and interior non-checkpoint spans carry a "_d" attribute. There is no
// depthMod-only legacy mode.
type PCRBridgeHandler struct {
	cpd       uint32
	prefixLen int

	// Capture materializes the serialized _br payloads (and _d depth values)
	// in Start/EndResult for the reconstruction harness. Off for bagsize runs.
	Capture bool

	state map[stateKey]*pcrState
}

type pcrState struct {
	ckpt        [8]byte // big-endian span ID of nearest checkpoint ancestor at/above this span (post-reset: self if checkpoint)
	depth       uint32  // absolute call depth (root = 0)
	emitted     bool    // OnStart already emitted (checkpoint)
	hasChildren bool
}

// DefaultPCRPrefixLen is the default truncation of the checkpoint root span
// ID. 4 bytes fully disambiguates same-depth non-leaf checkpoints in the
// entire Uber corpus (modulo one birthday fluke at cpd=3); 3 bytes covers
// 99.94-99.98% of traces.
const DefaultPCRPrefixLen = 4

// pcrTypeTagBytes is the on-wire size of the payload type tag. Note: unlike
// the legacy PB/CGPB/SBridge accounting, where the *TypeID constants double
// as both identifier values and byte counts (a coincidence we mirror for
// bit-exactness with the Python sim), PCR charges the honest 1 byte for its
// tag. PB's tag also happens to cost 1, so PB-vs-PCR emit accounting is
// apples-to-apples.
const pcrTypeTagBytes = 1

// PCRBridgeTypeID identifies a PCR _br payload (first byte of the value).
const PCRBridgeTypeID = 4

func NewPCRBridgeHandler(checkpointDistance, prefixLen int) *PCRBridgeHandler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	if prefixLen < 1 {
		prefixLen = 1
	} else if prefixLen > 8 {
		prefixLen = 8
	}
	return &PCRBridgeHandler{
		cpd:       uint32(checkpointDistance),
		prefixLen: prefixLen,
		state:     make(map[stateKey]*pcrState),
	}
}

func (h *PCRBridgeHandler) OnStart(ev *Event, _ int) StartResult {
	var parentState *pcrState
	var baggageFound bool
	if ev.ParentID != 0 {
		parentState = h.state[stateKey{ev.TraceID, ev.ParentID}]
		if parentState != nil {
			parentState.hasChildren = true
			baggageFound = true
		}
	}

	var depth uint32
	var ckpt [8]byte // inherited root: nearest checkpoint ancestor strictly above (zeros for root)
	if parentState != nil {
		depth = parentState.depth + 1
		ckpt = parentState.ckpt
	}

	isCheckpoint := depth%h.cpd == 0

	var emitBytes int
	var payload []byte
	if isCheckpoint {
		// The checkpoint payload names the PREVIOUS checkpoint (pre-reset),
		// exactly as PB's checkpoint payload carries the pre-reset bloom.
		emitBytes = BRPropertyNameOverheadBytes + pcrTypeTagBytes +
			VarintLen(int(depth)) + h.prefixLen
		if h.Capture {
			payload = packPCRPayload(int(depth), ckpt, h.prefixLen)
		}
		// Re-root: descendants inherit this checkpoint's own ID.
		ckpt = BigEndian8(ev.SpanID)
	}

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + VarintLen(int(depth)) + h.prefixLen
	}

	h.state[stateKey{ev.TraceID, ev.SpanID}] = &pcrState{
		ckpt:    ckpt,
		depth:   depth,
		emitted: isCheckpoint,
	}

	return StartResult{
		BaggageFound: baggageFound,
		BaggageBytes: baggageBytes,
		EmitBytes:    emitBytes,
		Payload:      payload,
	}
}

func (h *PCRBridgeHandler) OnEnd(ev *Event) EndResult {
	key := stateKey{ev.TraceID, ev.SpanID}
	ps, ok := h.state[key]
	if !ok {
		return EndResult{}
	}
	isLeaf := !ps.hasChildren

	var emitBytes, depthBytes int
	var payload []byte
	if isLeaf && !ps.emitted {
		// Non-checkpoint leaf: emits its inherited root (the checkpoint
		// strictly above it — post-reset ckpt equals inherited here since
		// the span is not a checkpoint).
		emitBytes = BRPropertyNameOverheadBytes + pcrTypeTagBytes +
			VarintLen(int(ps.depth)) + h.prefixLen
		if h.Capture {
			payload = packPCRPayload(int(ps.depth), ps.ckpt, h.prefixLen)
		}
		ps.emitted = true
	} else if !ps.emitted {
		// Interior non-checkpoint span: carries only "_d".
		depthBytes = DepthKeyBytes + VarintLen(int(ps.depth))
	}
	// State retention mirrors PB: freed by EvictTrace, not here (clock-skew
	// children may start after their parent's end).
	return EndResult{EmitBytes: emitBytes, DepthBytes: depthBytes, Payload: payload, Depth: int(ps.depth)}
}

// packPCRPayload serializes the on-wire _br value:
// type(1) || varint(absolute depth) || first prefixLen bytes of ckpt.
func packPCRPayload(depth int, ckpt [8]byte, prefixLen int) []byte {
	out := make([]byte, 0, 1+VarintLen(depth)+prefixLen)
	out = append(out, byte(PCRBridgeTypeID))
	out = binary.AppendUvarint(out, uint64(depth))
	out = append(out, ckpt[:prefixLen]...)
	return out
}

func (h *PCRBridgeHandler) EvictTrace(traceID uint64) {
	for k := range h.state {
		if k.traceID == traceID {
			delete(h.state, k)
		}
	}
}
