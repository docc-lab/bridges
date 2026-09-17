package bridge

// CheckpointPayload captures the ancestry evidence a span would export if it
// were promoted to a checkpoint at completion. Call before OnEnd, and only for
// a span that was not an original forward checkpoint. Original checkpoints
// close and reset their incoming window during OnStart; their original payload
// must instead be retained from StartResult.Payload.
//
// A snapshot never changes the forward window, Bloom geometry, ordinal state,
// or checkpoint countdown. In randomized mode the existing partial-window flag
// is set: a promoted receiver is a carrier, not a new forward window root.
func (h *PCRBBridgeHandler) CheckpointPayload(ev *Event) []byte {
	ps := h.state[stateKey{ev.TraceID, ev.SpanID}]
	if ps == nil || ps.emitted {
		return nil
	}
	payload := packPCRBPayload(int(ps.depth), ps.ckpt, h.prefixLen, ps.inherited)
	h.checkpoints.tagPayload(payload, ps.ttl)
	if h.checkpoints != nil {
		payload[0] |= LeafPayloadFlag
	}
	return payload
}

// CheckpointPayload implements the non-mutating completion snapshot for CGP0.
func (h *CGPRBBridgeHandler) CheckpointPayload(ev *Event) []byte {
	ps := h.state[stateKey{ev.TraceID, ev.SpanID}]
	if ps == nil || ps.emitted {
		return nil
	}
	payload := packCGPRBPayload(int(ps.depth), ps.ckpt, h.prefixLen, ps.inherited, ps.ha)
	h.checkpoints.tagPayload(payload, ps.ttl)
	if h.checkpoints != nil {
		payload[0] |= LeafPayloadFlag
	}
	return payload
}

// CheckpointPayload implements the non-mutating completion snapshot for SB3.
// DEE generation and pickup continue to follow the underlying handler's rules.
func (h *SB3Handler) CheckpointPayload(ev *Event) []byte {
	ps := h.state[stateKey{ev.TraceID, ev.SpanID}]
	if ps == nil || ps.emitted {
		return nil
	}
	payload := packSB3Payload(ps.depth, ps.ckpt, h.prefixLen, ps.inherited, ps.ha,
		ps.branches, ps.deeBytes, h.LehmerEE)
	h.checkpoints.tagPayload(payload, ps.ttl)
	if h.checkpoints != nil {
		payload[0] |= LeafPayloadFlag
	}
	return payload
}
