package bridge

import (
	"fmt"
	"os"
)

// DeeSizeLogger mirrors trace_simulator.py DeeSizeLogger. Pickup logs fire on
// drain when incoming exceeds threshold; queue-over-threshold logs fire on
// enqueue when the per-service queue total exceeds threshold.
//
// The logger reads service names and source-file paths via the lookup
// callbacks, since the corpus stores integer service_ids and uint64 trace_ids
// rather than the strings Python emits.
type DeeSizeLogger struct {
	ThresholdBytes int
	ServiceName    func(serviceID uint16) string
	SourceFile     func(traceID uint64) string
	Out            *os.File // defaults to os.Stderr
}

func (d *DeeSizeLogger) out() *os.File {
	if d.Out != nil {
		return d.Out
	}
	return os.Stderr
}

func (d *DeeSizeLogger) logPickup(serviceID uint16, incomingBytes int, traceID uint64) {
	if incomingBytes <= d.ThresholdBytes {
		return
	}
	src := ""
	if d.SourceFile != nil {
		src = d.SourceFile(traceID)
	}
	if src == "" {
		src = "?"
	}
	svc := ""
	if d.ServiceName != nil {
		svc = d.ServiceName(serviceID)
	}
	fmt.Fprintf(d.out(),
		"dee_log: kind=pickup service=%q incoming_bytes=%d trace_id=%016x source_file=%s\n",
		svc, incomingBytes, traceID, src)
}

func (d *DeeSizeLogger) logEnqueueOverThreshold(serviceID uint16, newQueueBytes, addedBytes int, traceID uint64) {
	if newQueueBytes <= d.ThresholdBytes {
		return
	}
	src := ""
	if d.SourceFile != nil {
		src = d.SourceFile(traceID)
	}
	if src == "" {
		src = "?"
	}
	svc := ""
	if d.ServiceName != nil {
		svc = d.ServiceName(serviceID)
	}
	fmt.Fprintf(d.out(),
		"dee_log: kind=queue_over_threshold service=%q queue_total_bytes=%d added_bytes=%d trace_id=%016x source_file=%s\n",
		svc, newQueueBytes, addedBytes, traceID, src)
}

// SBridgeHandler is the Go port of trace_simulator.py SBridgeBridgeHandler.
//
// Per-(trace,span) state is freed at OnEnd. The DEE queue is per-service and
// crosses traces, so EvictTrace does NOT touch it.
type SBridgeHandler struct {
	cpd       uint32
	deeLogger *DeeSizeLogger

	// EmitOC charges the "_o" attribute that every interior span (not a
	// checkpoint, not a leaf) exports: _o = varint(ordinal) || varint(depth) —
	// its own start-ordinal under its parent and its absolute depth. That is all
	// a severed survivor needs to be placed by the (depth, ordinal, own-fp,
	// parent-fp) 4-tuple; the fingerprints come from its real span/parent ids,
	// and checkpoints/leaves fold position into their _br instead.
	EmitOC bool

	// OmitOrdinal drops the ordinal from the interior _o attribute (emit depth
	// only). A severed survivor then self-places by (depth, own-fp, parent-fp) —
	// own-fp (from its real id) already distinguishes it from siblings, so the
	// ordinal is redundant there. Saves the ordinal varint per interior span.
	OmitOrdinal bool

	// FPBits is the bit width of each non-checkpoint parent fingerprint in the
	// chain (the top FPBits of the parent span id), bit-packed in the _br fp
	// section. Default 16 (the legacy 2-byte fp). Narrower = smaller payload but
	// more recovered-fp collisions (higher reject rate); wider = the reverse. The
	// checkpoint-root anchor (CkptBytes) is independent.
	FPBits int

	// CkptBytes is the truncated checkpoint-root span ID width (1-8 bytes) used
	// as each window's anchor — the S-Bridge analogue of PCR/CGPRB's prefix-len.
	// Default 4 (the legacy 4-byte anchor). 8 = the full span ID (collision-free
	// window stitching); narrower trades payload for ckpt-anchor collision risk.
	CkptBytes int

	// TopoOnly drops the Phase-2 (event-ordering) machinery from emission: no
	// per-level EE sublists in the chain and no DEE batches. Topology (ckpt4,
	// chain ordinals + fingerprints, _o) is untouched, so the call-graph shape
	// still reconstructs exactly — you just give up event-ordering recovery in
	// exchange for a smaller _br payload and zero DEE baggage.
	TopoOnly bool

	// EmitSink, when non-nil, receives the actual serialized _br payload at each
	// emit point (checkpoint OnStart, leaf OnEnd) — the faithful bytes a span
	// would persist. Opt-in: the bag-size study leaves it nil and pays no
	// serialization cost. Used by reconstruction tests / trace_recon --mode
	// sbridge to feed the decoder real payloads.
	EmitSink func(traceID, spanID uint64, payload []byte)

	// DEESink, when non-nil, receives every delayed-end-event quad the moment
	// it's generated, tagged with its origin trace id. In reality a DEE rides a
	// future request and is hunted down cross-trace; since that's functionally
	// equivalent, this accumulates them inline into a per-trace-id side store so
	// reconstruction can look them up directly. Opt-in (nil on the bag-size path,
	// which still uses the cross-trace deeQueue for cost accounting).
	DEESink func(traceID uint64, quad []byte)

	state            map[stateKey]*sbState
	parentEventCount map[stateKey]int
	childSeqStart    map[stateKey]int
	parentEEAcc      map[stateKey][]int

	// service_id -> queue of pending DEE quadruples (each quad is one []byte).
	deeQueue map[uint16][][]byte
}

type sbState struct {
	depth    int
	ckpt     [8]byte   // window anchor: full checkpoint-root span ID (truncated to CkptBytes on the wire)
	chain    []bcEntry // breadcrumb: per-level (ordinal, parent fingerprint, EE sub-list) since the last checkpoint
	deeBytes []byte

	packedLen   int // cached len of pack_sbridge_br applied to the fields above
	emitted     bool
	hasChildren bool
}

func NewSBridgeHandler(checkpointDistance int, deeLogger *DeeSizeLogger) *SBridgeHandler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	return &SBridgeHandler{
		cpd:              uint32(checkpointDistance),
		FPBits:           16, // legacy 2-byte non-checkpoint fp; override to sweep
		CkptBytes:        4,  // legacy 4-byte checkpoint-root anchor; override to sweep
		deeLogger:        deeLogger,
		state:            make(map[stateKey]*sbState),
		parentEventCount: make(map[stateKey]int),
		childSeqStart:    make(map[stateKey]int),
		parentEEAcc:      make(map[stateKey][]int),
		deeQueue:         make(map[uint16][][]byte),
	}
}

func (h *SBridgeHandler) bumpEvent(tid, parentSpanID uint64) int {
	k := stateKey{tid, parentSpanID}
	n := h.parentEventCount[k] + 1
	h.parentEventCount[k] = n
	return n
}

func (h *SBridgeHandler) drainDEE(serviceID uint16, traceID uint64) []byte {
	q, ok := h.deeQueue[serviceID]
	if !ok || len(q) == 0 {
		return nil
	}
	total := 0
	for _, b := range q {
		total += len(b)
	}
	out := make([]byte, 0, total)
	for _, b := range q {
		out = append(out, b...)
	}
	delete(h.deeQueue, serviceID)
	if h.deeLogger != nil {
		h.deeLogger.logPickup(serviceID, len(out), traceID)
	}
	return out
}

func (h *SBridgeHandler) enqueueDEE(serviceID uint16, triple []byte, traceID uint64) {
	prev := 0
	for _, b := range h.deeQueue[serviceID] {
		prev += len(b)
	}
	h.deeQueue[serviceID] = append(h.deeQueue[serviceID], triple)
	if h.deeLogger != nil {
		h.deeLogger.logEnqueueOverThreshold(serviceID, prev+len(triple), len(triple), traceID)
	}
}

func (h *SBridgeHandler) OnStart(ev *Event, parentSeqNum int) StartResult {
	tid, sid, pid := ev.TraceID, ev.SpanID, ev.ParentID

	var parentState *sbState
	var baggageFound bool
	if pid != 0 {
		parentState = h.state[stateKey{tid, pid}]
		if parentState != nil {
			parentState.hasChildren = true
			baggageFound = true
		}
	}

	deeIncoming := h.drainDEE(ev.ServiceID, tid)

	// Initial values pulled from parent's state (or zeroed for root).
	var (
		depth               int
		ckpt                [8]byte
		chain               []bcEntry
		parentDEEFromParent []byte
	)

	if pid != 0 {
		var parentDepth int
		var parentChain []bcEntry
		if parentState != nil {
			parentDepth = parentState.depth
			ckpt = parentState.ckpt
			parentChain = parentState.chain
			parentDEEFromParent = parentState.deeBytes
		}
		depth = parentDepth + 1

		// Copy the parent's breadcrumb chain and append this level: our start
		// ordinal; when our parent is NOT a checkpoint, its 2-byte fingerprint
		// (a checkpoint parent's identity is the 4-byte ckpt4 anchor, so that
		// edge carries no 2-byte entry); and THIS level's EE sub-list — the
		// start ordinals of our earlier siblings that ended in the gap before we
		// started, consumed from the parent's accumulator. EE rides on the chain
		// entry, so it propagates to every descendant positionally aligned with
		// the chain (set once per level, reset only at a checkpoint when the
		// chain restarts) — no flat inherit/reset, no owner inference. A flat
		// slice copy+append — no per-span map allocation.
		seqStart := h.bumpEvent(tid, pid)
		h.childSeqStart[stateKey{tid, sid}] = seqStart
		chain = make([]bcEntry, len(parentChain), len(parentChain)+1)
		copy(chain, parentChain)
		e := bcEntry{ord: seqStart}
		if parentDepth%int(h.cpd) != 0 { // parent is not a checkpoint
			e.fp = pid >> uint(64-h.FPBits) // top FPBits of the parent id, right-aligned
			e.hasFp = true
		}
		if !h.TopoOnly { // EE is Phase-2 only
			if accEnds := h.parentEEAcc[stateKey{tid, pid}]; len(accEnds) > 0 {
				e.ee = append([]int(nil), accEnds...)
			}
		}
		chain = append(chain, e)
		// Clear the parent's accumulator now that we've consumed it.
		h.parentEEAcc[stateKey{tid, pid}] = h.parentEEAcc[stateKey{tid, pid}][:0]
	}

	// DEE bytes: always concat dee_from_parent + dee_incoming; only zero out
	// for non-first siblings (root and first child both keep it).
	deeBytes := make([]byte, 0, len(parentDEEFromParent)+len(deeIncoming))
	deeBytes = append(deeBytes, parentDEEFromParent...)
	deeBytes = append(deeBytes, deeIncoming...)
	if pid != 0 && parentSeqNum != 1 {
		deeBytes = nil
	}

	isCheckpoint := depth%int(h.cpd) == 0
	var emitBytes int
	if isCheckpoint {
		// Only the payload SIZE matters for the bag-size study, so compute it
		// without serializing (no per-span allocation).
		emitBytes = BRPropertyNameOverheadBytes + SBridgeTypeID + sbridgeBRSize(depth, chain, deeBytes, h.FPBits, h.CkptBytes)
		if h.EmitSink != nil {
			// Pack BEFORE the reset: a checkpoint's own payload describes its
			// position within its PARENT window (pre-reset anchor + chain).
			h.EmitSink(tid, sid, PackSBridgeBR(depth, ckpt, h.CkptBytes, chain, deeBytes, h.FPBits))
		}

		// Reset state for the post-checkpoint snapshot: this span becomes the
		// new window anchor (its full span ID, truncated to CkptBytes on the
		// wire); the breadcrumb chain restarts empty (which also drops the
		// per-level EE, since EE now rides on chain entries).
		ckpt = BigEndian8(sid)
		chain = nil
		deeBytes = nil

		// Re-initialize this span's parent-perspective accumulators.
		h.parentEventCount[stateKey{tid, sid}] = 0
		h.parentEEAcc[stateKey{tid, sid}] = nil
	}

	packedLen := sbridgeBRSize(depth, chain, deeBytes, h.FPBits, h.CkptBytes)

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + packedLen
	}

	h.state[stateKey{tid, sid}] = &sbState{
		depth:     depth,
		ckpt:      ckpt,
		chain:     chain,
		deeBytes:  deeBytes,
		packedLen: packedLen,
		emitted:   isCheckpoint,
	}

	return StartResult{
		BaggageFound: baggageFound,
		BaggageBytes: baggageBytes,
		EmitBytes:    emitBytes,
	}
}

func (h *SBridgeHandler) OnEnd(ev *Event) EndResult {
	tid, sid := ev.TraceID, ev.SpanID
	key := stateKey{tid, sid}
	ps, ok := h.state[key]
	if !ok {
		return EndResult{}
	}

	// Append our start_seq to PARENT's accumulator (Phase-2 only).
	if startSeq, exists := h.childSeqStart[key]; exists {
		delete(h.childSeqStart, key)
		if !h.TopoOnly && ev.ParentID != 0 {
			pk := stateKey{tid, ev.ParentID}
			h.parentEEAcc[pk] = append(h.parentEEAcc[pk], startSeq)
		}
	}
	// Drain remaining ENDS that we never handed off to a later child:
	// they become a DEE quadruple, dropping the last (implicit at reconstruction).
	// The quad carries this span's own fingerprint as the owner ID, since the
	// drained batch has no chain context to recover it from. Skipped in TopoOnly
	// (no EE accumulation -> nothing to drain, and DEE is Phase-2 only).
	if !h.TopoOnly {
		rem := h.parentEEAcc[key]
		if len(rem) > 0 {
			kept := rem[:len(rem)-1]
			if len(kept) > 0 {
				quad := EncodeDEEQuad(TraceID16(tid), ps.depth, sid>>uint(64-h.FPBits), h.FPBits, kept)
				h.enqueueDEE(ev.ServiceID, quad, tid)
				if h.DEESink != nil {
					h.DEESink(tid, quad)
				}
			}
			h.parentEEAcc[key] = rem[:0] // clear in place (Python: .clear())
		}
	}
	// State survives until EvictTrace: clock-skew children may start after
	// their parent's end and need to find parent state alive.

	isLeaf := !ps.hasChildren
	var emitBytes, ocBytes int
	if isLeaf && !ps.emitted {
		emitBytes = BRPropertyNameOverheadBytes + SBridgeTypeID + ps.packedLen
		ps.emitted = true
		if h.EmitSink != nil {
			h.EmitSink(tid, sid, PackSBridgeBR(ps.depth, ps.ckpt, h.CkptBytes, ps.chain, ps.deeBytes, h.FPBits))
		}
	} else if !ps.emitted && h.EmitOC {
		// Interior span (not a checkpoint, not a leaf): exports
		// _o = varint(ordinal) || varint(depth) — its own start-ordinal under its
		// parent (the last breadcrumb entry's ordinal) and its absolute depth.
		ocBytes = OcKeyBytes + VarintLen(ps.depth)
		if !h.OmitOrdinal { // include the start-ordinal unless relaxed away
			ord := 0
			if n := len(ps.chain); n > 0 {
				ord = ps.chain[n-1].ord
			}
			ocBytes += VarintLen(ord)
		}
	}
	return EndResult{EmitBytes: emitBytes, OcBytes: ocBytes}
}

func (h *SBridgeHandler) EvictTrace(traceID uint64) {
	for k := range h.state {
		if k.traceID == traceID {
			delete(h.state, k)
		}
	}
	for k := range h.parentEventCount {
		if k.traceID == traceID {
			delete(h.parentEventCount, k)
		}
	}
	for k := range h.childSeqStart {
		if k.traceID == traceID {
			delete(h.childSeqStart, k)
		}
	}
	for k := range h.parentEEAcc {
		if k.traceID == traceID {
			delete(h.parentEEAcc, k)
		}
	}
	// DEE queue is intentionally not touched (cross-trace per-service state).
}
