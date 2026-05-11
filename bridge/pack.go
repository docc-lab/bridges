// Package bridge implements the four bridge handlers from
// bridges/trace_simulator.py (vanilla, pb, cgpb, sbridge), bit-exact.
//
// This file holds the wire-format helpers: varint, br pack/unpack, span/trace
// id hex conversion, and the per-handler payload packers. The handlers
// themselves live in vanilla.go / pb.go / cgpb.go / sbridge.go.
package bridge

import (
	"encoding/binary"
	"encoding/hex"
	"sort"
)

// Constants matching trace_simulator.py.
const (
	// "_br" property name overhead (Python BR_PROPERTY_NAME_OVERHEAD_BYTES).
	BRPropertyNameOverheadBytes = 3

	// Bridge type ID bytes — used directly as integer addends in the emit
	// payload accounting in Python (PB_BRIDGE_TYPE_ID = 1, CGP_BRIDGE_TYPE_ID
	// = 2, SB_BRIDGE_TYPE_ID = 3). They double as numeric identifiers and
	// as fixed byte counts; we mirror that arithmetic exactly.
	PBBridgeTypeID    = 1
	CGPBridgeTypeID   = 2
	SBridgeTypeID     = 3

	// Default bloom false-positive rate used by PB and CGPB.
	DefaultBloomFPRate = 0.0001

	// Baggage-key byte size used in baggage_byte_size accounting. The only
	// __bag.* tag set by any handler is __bag._br, whose stripped key is
	// "_br" = 3 bytes.
	BaggageKeyBytes = 3
)

// VarintEncode encodes a non-negative integer as a protobuf-style varint.
// Matches bridges/bloom.py-adjacent _varint_encode in trace_simulator.py.
func VarintEncode(n int) []byte {
	if n < 0 {
		n = 0
	}
	return binary.AppendUvarint(nil, uint64(n))
}

// VarintLen returns the byte length of VarintEncode(n) without allocating.
func VarintLen(n int) int {
	if n < 0 {
		n = 0
	}
	if n < 1<<7 {
		return 1
	}
	if n < 1<<14 {
		return 2
	}
	if n < 1<<21 {
		return 3
	}
	if n < 1<<28 {
		return 4
	}
	if n < 1<<35 {
		return 5
	}
	if n < 1<<42 {
		return 6
	}
	if n < 1<<49 {
		return 7
	}
	if n < 1<<56 {
		return 8
	}
	// int is 64-bit signed; max value < 1<<63 so the longest case is 9 bytes.
	return 9
}

// PackBR packs the path-bridge baggage payload: varint(depthMod) || bloomBytes.
func PackBR(depthMod int, bloomBytes []byte) []byte {
	out := make([]byte, 0, VarintLen(depthMod)+len(bloomBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depthMod, 0)))
	out = append(out, bloomBytes...)
	return out
}

// PackCGPBBR packs the CGPB baggage payload: varint(depthMod) || bloom || ha.
func PackCGPBBR(depthMod int, bloomBytes, haBytes []byte) []byte {
	out := make([]byte, 0, VarintLen(depthMod)+len(bloomBytes)+len(haBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depthMod, 0)))
	out = append(out, bloomBytes...)
	out = append(out, haBytes...)
	return out
}

// HAAppendEntry appends one CGPB hash-array entry to ha:
//
//	entry := parent_span_id_bytes(8) || varint(depthMod)
//
// parentSpanID is the parent span's 16-char hex string. Returns the original
// ha unchanged if parentSpanID is invalid hex (mirroring Python's None check).
func HAAppendEntry(ha []byte, parentSpanID string, depthMod int) []byte {
	pid, ok := SpanIDHexTo8Bytes(parentSpanID)
	if !ok {
		return ha
	}
	out := make([]byte, 0, len(ha)+8+VarintLen(depthMod))
	out = append(out, ha...)
	out = append(out, pid[:]...)
	out = binary.AppendUvarint(out, uint64(maxInt(depthMod, 0)))
	return out
}

// SpanIDHexTo8Bytes converts a Jaeger spanID hex string to a fixed 8-byte
// representation. Empty / invalid hex returns ok=false. Shorter inputs are
// left-padded with zeros; longer inputs keep the last 8 bytes (matching
// Python _span_id_hex_to_8bytes).
func SpanIDHexTo8Bytes(s string) ([8]byte, bool) {
	var out [8]byte
	if s == "" {
		return out, false
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return out, false
	}
	switch {
	case len(raw) == 8:
		copy(out[:], raw)
	case len(raw) > 8:
		copy(out[:], raw[len(raw)-8:])
	default:
		copy(out[8-len(raw):], raw)
	}
	return out, true
}

// TraceIDHexTo16Bytes converts a W3C trace_id hex string to a fixed 16-byte
// representation. Empty / invalid hex returns 16 zero bytes (matching Python
// _trace_id_hex_to_16bytes).
func TraceIDHexTo16Bytes(s string) [16]byte {
	var out [16]byte
	if s == "" {
		return out
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return out
	}
	switch {
	case len(raw) == 16:
		copy(out[:], raw)
	case len(raw) > 16:
		copy(out[:], raw[len(raw)-16:])
	default:
		copy(out[16-len(raw):], raw)
	}
	return out
}

// PackSBridgeBR packs the S-Bridge baggage payload, matching Python
// pack_sbridge_br exactly (sorted depth groups, varint counts, varint sequences,
// and a trailing dee_bytes blob).
//
// depth is clamped to >= 0; ckpt8 must be exactly 8 bytes (caller ensures).
func PackSBridgeBR(
	depth int,
	ckpt8 [8]byte,
	ordinalGroups map[int][]int,
	endEvents []int,
	deeBytes []byte,
) []byte {
	depths := make([]int, 0, len(ordinalGroups))
	for d := range ordinalGroups {
		depths = append(depths, d)
	}
	sort.Ints(depths)

	// Pre-compute size to avoid reallocation.
	size := VarintLen(depth) + 8 + VarintLen(len(depths))
	for _, d := range depths {
		seqs := ordinalGroups[d]
		size += VarintLen(d) + VarintLen(len(seqs))
		for _, s := range seqs {
			size += VarintLen(s)
		}
	}
	size += VarintLen(len(endEvents))
	for _, s := range endEvents {
		size += VarintLen(s)
	}
	size += len(deeBytes)

	out := make([]byte, 0, size)
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt8[:]...)
	out = binary.AppendUvarint(out, uint64(len(depths)))
	for _, d := range depths {
		seqs := ordinalGroups[d]
		out = binary.AppendUvarint(out, uint64(d))
		out = binary.AppendUvarint(out, uint64(len(seqs)))
		for _, s := range seqs {
			out = binary.AppendUvarint(out, uint64(maxInt(s, 0)))
		}
	}
	out = binary.AppendUvarint(out, uint64(len(endEvents)))
	for _, s := range endEvents {
		out = binary.AppendUvarint(out, uint64(maxInt(s, 0)))
	}
	out = append(out, deeBytes...)
	return out
}

// EncodeDEETriple encodes one delayed-end-event triple:
//
//	16-byte trace_id || varint(depth) || varint(n) || n * varint(start_seq)
func EncodeDEETriple(traceID16 [16]byte, depth int, seqs []int) []byte {
	size := 16 + VarintLen(depth) + VarintLen(len(seqs))
	for _, s := range seqs {
		size += VarintLen(s)
	}
	out := make([]byte, 0, size)
	out = append(out, traceID16[:]...)
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = binary.AppendUvarint(out, uint64(len(seqs)))
	for _, s := range seqs {
		out = binary.AppendUvarint(out, uint64(maxInt(s, 0)))
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
