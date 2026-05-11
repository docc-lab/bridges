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

	state            map[stateKey]*sbState
	parentEventCount map[stateKey]int
	childSeqStart    map[stateKey]int
	parentEEAcc      map[stateKey][]int

	// service_id -> queue of pending DEE triples (each triple is one []byte).
	deeQueue map[uint16][][]byte
}

type sbState struct {
	depth         int
	ckpt8         [8]byte
	ordinalGroups map[int][]int
	endEvents     []int
	deeBytes      []byte

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
		depth                int
		ckpt8                [8]byte
		ordinalGroups        map[int][]int
		endEvents            []int
		parentDEEFromParent  []byte
	)

	if pid != 0 {
		var parentDepth int
		var parentOrdinalGroups map[int][]int
		var parentEEFromParent []int
		if parentState != nil {
			parentDepth = parentState.depth
			ckpt8 = parentState.ckpt8
			parentOrdinalGroups = parentState.ordinalGroups
			parentEEFromParent = parentState.endEvents
			parentDEEFromParent = parentState.deeBytes
		}
		depth = parentDepth + 1

		// Deep-copy parent's ordinal groups and append our seq_start.
		ordinalGroups = make(map[int][]int, len(parentOrdinalGroups)+1)
		for d, seqs := range parentOrdinalGroups {
			cp := make([]int, len(seqs))
			copy(cp, seqs)
			ordinalGroups[d] = cp
		}
		seqStart := h.bumpEvent(tid, pid)
		h.childSeqStart[stateKey{tid, sid}] = seqStart
		ordinalGroups[depth] = append(ordinalGroups[depth], seqStart)

		accEnds := h.parentEEAcc[stateKey{tid, pid}]
		if parentSeqNum == 1 {
			endEvents = append([]int(nil), parentEEFromParent...)
			endEvents = append(endEvents, accEnds...)
		} else {
			endEvents = append([]int(nil), accEnds...)
		}
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
		prePayload := PackSBridgeBR(depth, ckpt8, ordinalGroups, endEvents, deeBytes)
		emitBytes = BRPropertyNameOverheadBytes + SBridgeTypeID + len(prePayload)

		// Reset state for the post-checkpoint snapshot.
		ckpt8 = BigEndian8(sid)
		ordinalGroups = nil
		endEvents = nil
		deeBytes = nil

		// Re-initialize this span's parent-perspective accumulators.
		h.parentEventCount[stateKey{tid, sid}] = 0
		h.parentEEAcc[stateKey{tid, sid}] = nil
	}

	packed := PackSBridgeBR(depth, ckpt8, ordinalGroups, endEvents, deeBytes)
	packedLen := len(packed)

	var baggageBytes int
	if baggageFound {
		baggageBytes = BaggageKeyBytes + packedLen
	}

	h.state[stateKey{tid, sid}] = &sbState{
		depth:         depth,
		ckpt8:         ckpt8,
		ordinalGroups: ordinalGroups,
		endEvents:     endEvents,
		deeBytes:      deeBytes,
		packedLen:     packedLen,
		emitted:       isCheckpoint,
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

	// Append our start_seq to PARENT's accumulator.
	if startSeq, exists := h.childSeqStart[key]; exists {
		delete(h.childSeqStart, key)
		if ev.ParentID != 0 {
			pk := stateKey{tid, ev.ParentID}
			h.parentEEAcc[pk] = append(h.parentEEAcc[pk], startSeq)
		}
	}

	// Drain remaining ENDS that we never handed off to a later child:
	// they become a DEE triple, dropping the last (implicit at reconstruction).
	rem := h.parentEEAcc[key]
	if len(rem) > 0 {
		kept := rem[:len(rem)-1]
		if len(kept) > 0 {
			triple := EncodeDEETriple(TraceID16(tid), ps.depth, kept)
			h.enqueueDEE(ev.ServiceID, triple, tid)
		}
		h.parentEEAcc[key] = rem[:0] // clear in place (Python: .clear())
	}
	// State survives until EvictTrace: clock-skew children may start after
	// their parent's end and need to find parent state alive.

	isLeaf := !ps.hasChildren
	var emitBytes int
	if isLeaf && !ps.emitted {
		emitBytes = BRPropertyNameOverheadBytes + SBridgeTypeID + ps.packedLen
		ps.emitted = true
	}
	return EndResult{EmitBytes: emitBytes}
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
