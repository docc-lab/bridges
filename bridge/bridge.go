package bridge

import (
	"encoding/binary"
	"encoding/hex"
)

// Event is one start or end event for a span. The corpus stores these
// pre-sorted in the same order Python's sort_events would emit them, so the
// simulator drives the handler by feeding events sequentially.
//
// IDs are uint64 (8 bytes). For Uber-style traces (16-char lowercase hex)
// the numeric uint64 ordering matches Python's lex string ordering, so the
// sort tie-break is preserved. The string forms used by Python (e.g. as
// bloom-hash input or DEE log fields) are derived on demand via the helpers
// below.
type Event struct {
	TraceID   uint64
	SpanID    uint64
	ParentID  uint64 // 0 = root (no parent)
	ServiceID uint16
}

// Kind discriminates a Start vs End event in the simulator's stream.
type Kind uint8

const (
	KindStart Kind = 0
	KindEnd   Kind = 1
)

// StartResult is the data the simulator needs from OnStart to drive its
// per-trace metric accumulators (matching trace_simulator.py run_traces).
type StartResult struct {
	BaggageFound bool // parent had handler state -> count this as a baggage call
	BaggageBytes int  // 3 + len(packed_br) when BaggageFound, else 0
	EmitBytes    int  // EMIT_PAYLOAD_BYTES set in this OnStart, 0 if none
}

// EndResult is the data the simulator needs from OnEnd.
type EndResult struct {
	EmitBytes int
}

// Handler is the Go analogue of trace_simulator.py BridgeHandler.
//
// EvictTrace is called by the simulator when a trace closes (every span has
// ended); the handler should free any per-(trace, span) state for that trace.
// Cross-trace state (e.g. S-Bridge per-service DEE queues) must NOT be
// touched by EvictTrace.
type Handler interface {
	OnStart(ev *Event, parentSeqNum int) StartResult
	OnEnd(ev *Event) EndResult
	EvictTrace(traceID uint64)
}

// HexBuf is a 16-byte lowercase-hex rendering of a uint64. Pre-rendered to
// avoid allocation in the bloom hot path.
type HexBuf [16]byte

// HexOf returns the 16-char lowercase hex of id, suitable as a Python
// `span_id.encode("utf-8")` equivalent (the bloom-hash input).
func HexOf(id uint64) HexBuf {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], id)
	var out HexBuf
	hex.Encode(out[:], raw[:])
	return out
}

// BigEndian8 returns the 8-byte big-endian encoding of id, equivalent to
// Python's _span_id_hex_to_8bytes for normalized 16-char lowercase hex IDs.
func BigEndian8(id uint64) [8]byte {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], id)
	return out
}

// TraceID16 returns 16 bytes for a 64-bit trace_id, left-padded with zeros to
// match Python's _trace_id_hex_to_16bytes for short hex IDs (Uber's trace_id
// is 16-char hex = 8 bytes, so the upper 8 bytes are always zero).
func TraceID16(id uint64) [16]byte {
	var out [16]byte
	binary.BigEndian.PutUint64(out[8:], id)
	return out
}

// VanillaHandler is the no-op handler — matches Python VanillaHandler.
type VanillaHandler struct{}

func NewVanillaHandler() *VanillaHandler                                { return &VanillaHandler{} }
func (h *VanillaHandler) OnStart(_ *Event, _ int) StartResult           { return StartResult{} }
func (h *VanillaHandler) OnEnd(_ *Event) EndResult                      { return EndResult{} }
func (h *VanillaHandler) EvictTrace(_ uint64)                           {}
