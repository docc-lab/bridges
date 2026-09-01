package bridge

import (
	"encoding/binary"

	"bridges/bloom"
)

// SB3BridgeTypeID identifies the sparse-ordinal S-Bridge payload.
const SB3BridgeTypeID = 7

// SB3Branch is one non-first edge in a window-local sparse ordinal chain.
// Ord retains the ordinary 1-based start ordinal (and is therefore >=2).
// EE has exactly the same meaning and ordinal namespace as S-Bridge's bcEntry.
type SB3Branch struct {
	Ord int
	EE  []int
}

// SB3Handler layers sparse branch ordinals and the existing EE/DEE machinery
// over CGPRB's checkpoint-prefix + window-Bloom + hash-array topology substrate.
// Unary and first-child edges append no ordinal; child 2 and later append one
// SB3Branch to the propagated chain.
type SB3Handler struct {
	cpd       uint32
	prefixLen int
	bloomM    uint32
	bloomK    uint32
	bloomLen  int
	deeLogger *DeeSizeLogger

	Capture bool

	TopoOnly      bool
	LehmerEE      bool
	FPBits        int
	UseDEEQueueID bool
	DequeueOneDEE bool
	DEEStats      *DEEQueueStats

	EmitSink func(traceID, spanID uint64, payload []byte)
	DEESink  func(traceID uint64, quad []byte)

	state            map[stateKey]*sb3State
	parentEventCount map[stateKey]int
	childSeqStart    map[stateKey]int
	parentEEAcc      map[stateKey][]int
	deeQueue         map[uint32][][]byte
}

type sb3State struct {
	depth       int
	ckpt        [8]byte
	bloomBytes  []byte // propagated ancestor bloom (inherited + self)
	inherited   []byte // pre-self bloom persisted by this span
	ha          []byte // CGPRB window-local fanout evidence
	branches    []SB3Branch
	deeBytes    []byte
	payloadBody int
	emitted     bool
	hasChildren bool
}

func NewSB3Handler(checkpointDistance, prefixLen int, bloomFPRate float64, deeLogger *DeeSizeLogger) *SB3Handler {
	if checkpointDistance < 1 {
		checkpointDistance = 1
	}
	if prefixLen < 1 {
		prefixLen = 1
	} else if prefixLen > 8 {
		prefixLen = 8
	}
	m, k := bloom.EstimateParameters(PCRBBloomCapacity(checkpointDistance), bloomFPRate)
	return &SB3Handler{
		cpd:              uint32(checkpointDistance),
		prefixLen:        prefixLen,
		bloomM:           m,
		bloomK:           k,
		bloomLen:         int((m + 7) / 8),
		deeLogger:        deeLogger,
		FPBits:           16,
		state:            make(map[stateKey]*sb3State),
		parentEventCount: make(map[stateKey]int),
		childSeqStart:    make(map[stateKey]int),
		parentEEAcc:      make(map[stateKey][]int),
		deeQueue:         make(map[uint32][][]byte),
	}
}

func (h *SB3Handler) deeQueueKey(ev *Event) uint32 {
	if h.UseDEEQueueID {
		return ev.DEEQueueID
	}
	return uint32(ev.ServiceID)
}

func (h *SB3Handler) bumpEvent(tid, parentSpanID uint64) int {
	k := stateKey{tid, parentSpanID}
	n := h.parentEventCount[k] + 1
	h.parentEventCount[k] = n
	return n
}

func (h *SB3Handler) drainDEE(queueID uint32, serviceID uint16, traceID uint64) []byte {
	q := h.deeQueue[queueID]
	if len(q) == 0 {
		h.DEEStats.recordPickupAttempt(false)
		return nil
	}
	h.DEEStats.recordPickupAttempt(true)
	if h.DequeueOneDEE {
		out := q[0]
		q[0] = nil
		if len(q) == 1 {
			delete(h.deeQueue, queueID)
		} else {
			h.deeQueue[queueID] = q[1:]
		}
		if h.deeLogger != nil {
			h.deeLogger.logPickup(serviceID, len(out), traceID)
		}
		h.DEEStats.recordDequeue(queueID, 1, len(out))
		return out
	}
	total := 0
	for _, b := range q {
		total += len(b)
	}
	out := make([]byte, 0, total)
	for _, b := range q {
		out = append(out, b...)
	}
	delete(h.deeQueue, queueID)
	h.DEEStats.recordDequeue(queueID, len(q), len(out))
	if h.deeLogger != nil {
		h.deeLogger.logPickup(serviceID, len(out), traceID)
	}
	return out
}

func (h *SB3Handler) enqueueDEE(queueID uint32, serviceID uint16, quad []byte, traceID uint64) {
	prev := 0
	for _, b := range h.deeQueue[queueID] {
		prev += len(b)
	}
	h.deeQueue[queueID] = append(h.deeQueue[queueID], quad)
	h.DEEStats.recordEnqueue(queueID, len(quad))
	if h.deeLogger != nil {
		h.deeLogger.logEnqueueOverThreshold(serviceID, prev+len(quad), len(quad), traceID)
	}
}

func cloneSB3Branches(in []SB3Branch, extra int) []SB3Branch {
	out := make([]SB3Branch, len(in), len(in)+extra)
	copy(out, in)
	return out
}

func (h *SB3Handler) OnStart(ev *Event, parentSeqNum int) StartResult {
	tid, sid, pid := ev.TraceID, ev.SpanID, ev.ParentID
	var parentState *sb3State
	if pid != 0 {
		parentState = h.state[stateKey{tid, pid}]
		if parentState != nil {
			parentState.hasChildren = true
		}
	}
	baggageFound := parentState != nil

	deeIncoming := h.drainDEE(h.deeQueueKey(ev), ev.ServiceID, tid)
	var depth int
	var ckpt [8]byte
	var inherited, ha, parentDEE []byte
	var branches []SB3Branch
	if parentState != nil {
		depth = parentState.depth + 1
		ckpt = parentState.ckpt
		inherited = parentState.bloomBytes
		parentDEE = parentState.deeBytes
		branches = cloneSB3Branches(parentState.branches, 1)
		// Preserve CGPRB's one-record-per-fanout HA propagation rule.
		if parentSeqNum == 1 {
			ha = append([]byte(nil), parentState.ha...)
		}
	} else {
		bf, _ := bloom.New(h.bloomM, h.bloomK)
		inherited = bf.ToBytes()
	}

	if pid != 0 {
		seqStart := h.bumpEvent(tid, pid)
		h.childSeqStart[stateKey{tid, sid}] = seqStart
		var ee []int
		if !h.TopoOnly {
			ee = append([]int(nil), h.parentEEAcc[stateKey{tid, pid}]...)
		}
		h.parentEEAcc[stateKey{tid, pid}] = h.parentEEAcc[stateKey{tid, pid}][:0]
		if seqStart >= 2 {
			branches = append(branches, SB3Branch{Ord: seqStart, EE: ee})
		}
		if seqStart == 2 {
			p := BigEndian8(pid)
			ha = append(ha, p[:]...)
			ha = binary.AppendUvarint(ha, uint64(depth))
		}
	}

	deeBytes := make([]byte, 0, len(parentDEE)+len(deeIncoming))
	deeBytes = append(deeBytes, parentDEE...)
	deeBytes = append(deeBytes, deeIncoming...)
	if pid != 0 && parentSeqNum != 1 {
		deeBytes = nil
	}

	isCheckpoint := depth%int(h.cpd) == 0
	var emitBytes int
	var payload []byte
	if isCheckpoint {
		body := sb3BodySize(depth, h.prefixLen, len(inherited), ha, branches, deeBytes, h.LehmerEE)
		emitBytes = BRPropertyNameOverheadBytes + 1 + body
		if h.Capture || h.EmitSink != nil {
			payload = packSB3Payload(depth, ckpt, h.prefixLen, inherited, ha, branches, deeBytes, h.LehmerEE)
			if h.EmitSink != nil {
				h.EmitSink(tid, sid, payload)
			}
		}
		ckpt = BigEndian8(sid)
		bf, _ := bloom.New(h.bloomM, h.bloomK)
		inherited = bf.ToBytes()
		ha = nil
		branches = nil
		deeBytes = nil
		h.parentEventCount[stateKey{tid, sid}] = 0
		h.parentEEAcc[stateKey{tid, sid}] = nil
	}

	propagated := inherited
	if !isCheckpoint {
		bf := bloom.Deserialize(inherited, h.bloomM, h.bloomK)
		spanHex := HexOf(sid)
		bf.Add(spanHex[:])
		propagated = bf.ToBytes()
	}

	payloadBody := sb3BodySize(depth, h.prefixLen, len(inherited), ha, branches, deeBytes, h.LehmerEE)
	var baggageBytes int
	if baggageFound {
		baggageBody := sb3BodySize(depth, h.prefixLen, len(propagated), ha, branches, deeBytes, h.LehmerEE)
		baggageBytes = BaggageKeyBytes + baggageBody
	}
	h.state[stateKey{tid, sid}] = &sb3State{
		depth: depth, ckpt: ckpt, bloomBytes: propagated, inherited: inherited,
		ha: ha, branches: branches, deeBytes: deeBytes, payloadBody: payloadBody,
		emitted: isCheckpoint,
	}
	return StartResult{BaggageFound: baggageFound, BaggageBytes: baggageBytes, EmitBytes: emitBytes, Payload: payload}
}

func (h *SB3Handler) OnEnd(ev *Event) EndResult {
	tid, sid := ev.TraceID, ev.SpanID
	key := stateKey{tid, sid}
	ps := h.state[key]
	if ps == nil {
		return EndResult{}
	}
	if startSeq, ok := h.childSeqStart[key]; ok {
		delete(h.childSeqStart, key)
		if !h.TopoOnly && ev.ParentID != 0 {
			pk := stateKey{tid, ev.ParentID}
			h.parentEEAcc[pk] = append(h.parentEEAcc[pk], startSeq)
		}
	}
	if !h.TopoOnly {
		rem := h.parentEEAcc[key]
		if len(rem) > 0 {
			kept := rem[:len(rem)-1]
			if len(kept) > 0 {
				quad := encodeDEEQuad(TraceID16(tid), ps.depth, sid>>uint(64-h.FPBits), h.FPBits,
					h.parentEventCount[key], kept, h.LehmerEE)
				h.enqueueDEE(h.deeQueueKey(ev), ev.ServiceID, quad, tid)
				if h.DEESink != nil {
					h.DEESink(tid, quad)
				}
			}
			h.parentEEAcc[key] = rem[:0]
		}
	}

	isLeaf := !ps.hasChildren
	var emitBytes, depthBytes int
	var payload []byte
	if isLeaf && !ps.emitted {
		emitBytes = BRPropertyNameOverheadBytes + 1 + ps.payloadBody
		ps.emitted = true
		if h.Capture || h.EmitSink != nil {
			payload = packSB3Payload(ps.depth, ps.ckpt, h.prefixLen, ps.inherited, ps.ha, ps.branches, ps.deeBytes, h.LehmerEE)
			if h.EmitSink != nil {
				h.EmitSink(tid, sid, payload)
			}
		}
	} else if !ps.emitted {
		depthBytes = DepthKeyBytes + VarintLen(ps.depth)
	}
	return EndResult{EmitBytes: emitBytes, DepthBytes: depthBytes, Payload: payload, Depth: ps.depth}
}

func (h *SB3Handler) EvictTrace(traceID uint64) {
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
}

// sb3BodySize excludes the one-byte type tag. HA is length-framed; the Bloom
// length is fixed by the deployment config; trailing DEE quads consume EOF.
func sb3BodySize(depth, prefixLen, bloomLen int, ha []byte, branches []SB3Branch, dee []byte, lehmer bool) int {
	n := VarintLen(depth) + prefixLen + bloomLen + VarintLen(len(ha)) + len(ha) + VarintLen(len(branches))
	for _, b := range branches {
		n += VarintLen(b.Ord) + VarintLen(len(b.EE))
		if lehmer {
			n += partialPermutationBytes(maxInt(b.Ord-1, 0), len(b.EE))
		} else {
			for _, e := range b.EE {
				n += VarintLen(e)
			}
		}
	}
	return n + len(dee)
}

func packSB3Payload(depth int, ckpt [8]byte, prefixLen int, bloomBits, ha []byte, branches []SB3Branch, dee []byte, lehmer bool) []byte {
	out := make([]byte, 0, 1+sb3BodySize(depth, prefixLen, len(bloomBits), ha, branches, dee, lehmer))
	out = append(out, byte(SB3BridgeTypeID))
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt[:prefixLen]...)
	out = append(out, bloomBits...)
	out = binary.AppendUvarint(out, uint64(len(ha)))
	out = append(out, ha...)
	out = binary.AppendUvarint(out, uint64(len(branches)))
	for _, b := range branches {
		out = binary.AppendUvarint(out, uint64(maxInt(b.Ord, 0)))
		out = binary.AppendUvarint(out, uint64(len(b.EE)))
		if lehmer {
			rank, err := encodePartialPermutation(maxInt(b.Ord-1, 0), b.EE)
			if err != nil {
				panic("invalid SB3 EE group: " + err.Error())
			}
			out = append(out, rank...)
		} else {
			for _, e := range b.EE {
				out = binary.AppendUvarint(out, uint64(maxInt(e, 0)))
			}
		}
	}
	out = append(out, dee...)
	return out
}
