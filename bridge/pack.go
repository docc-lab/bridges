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

	// Key byte size for the per-span "_d" attribute carrying varint(absolute
	// depth) on spans that never emit a _br payload (--emit-depth mode; see
	// docs/depth_emission.md).
	DepthKeyBytes = 2

	// Key byte size for the per-span "_oc" attribute carrying the window-
	// relative ordinal chain on interior non-checkpoint spans (S-Bridge
	// --emit-oc mode). Stripped key "_oc" = 3 bytes.
	OcKeyBytes = 3
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

// bcEntry is one level of an S-Bridge vertical breadcrumb chain: a start
// ordinal plus, for a non-checkpoint parent, that parent's 2-byte fingerprint
// (the top 2 bytes of its span ID). The chain is a flat POD slice, so building
// a child's breadcrumb is a cheap copy+append rather than a per-span map
// allocation — matching the live implementation and avoiding heavy GC over the
// hundreds of millions of spans in a corpus.
type bcEntry struct {
	ord   int
	fp    uint16 // big-endian first 2 bytes of the propagating parent's span ID
	hasFp bool
	// ee is this level's delayed-end-event sub-list: the start ordinals of the
	// spans that ENDED in the gap just before this level's span started (i.e. its
	// earlier siblings, children of this level's parent). Carried per-level so the
	// EE is positionally aligned with the chain — each end is attributable to its
	// owner (this entry's fp / the window anchor) with no inference, and a level
	// with no ends just serializes varint(0). Replaces the old single flat trailing
	// endEvents block.
	ee []int
}

// chainStartDepth is the absolute depth of chain[0] for a span at `depth`:
// the chain holds one entry per level from the last checkpoint's child down to
// (and including) the span itself.
func chainStartDepth(depth, chainLen int) int { return depth - chainLen + 1 }

// sbridgeBRSize returns the serialized size of an S-Bridge _br payload WITHOUT
// allocating it — used in the per-span hot path (only the byte count matters
// for the bag-size study). It matches PackSBridgeBR byte-for-byte in length.
func sbridgeBRSize(depth int, chain []bcEntry, deeBytes []byte) int {
	start := chainStartDepth(depth, len(chain))
	size := VarintLen(depth) + 4 + VarintLen(len(chain))
	for i := range chain {
		size += VarintLen(start+i) + VarintLen(1) + VarintLen(maxInt(chain[i].ord, 0))
		if chain[i].hasFp {
			size += 2
		}
		// per-level EE sub-list: varint(n) then n ordinals (varint(0) when empty).
		size += VarintLen(len(chain[i].ee))
		for _, s := range chain[i].ee {
			size += VarintLen(s)
		}
	}
	size += len(deeBytes)
	return size
}

// PackSBridgeBR packs the S-Bridge baggage payload from a flat breadcrumb
// chain: varint(depth), the 4-byte checkpoint-root anchor, varint(N), then per
// level varint(depth_i) varint(1) varint(ordinal) [2-byte parent fingerprint]
// varint(ee_n) ee_n*varint(ordinal), then the trailing dee_bytes blob. The
// end-event list is carried PER LEVEL (positionally aligned with the chain), so
// a level with no ends serializes a single varint(0); this replaces the old
// single flat trailing endEvents block.
func PackSBridgeBR(
	depth int,
	ckpt4 [4]byte,
	chain []bcEntry,
	deeBytes []byte,
) []byte {
	start := chainStartDepth(depth, len(chain))
	out := make([]byte, 0, sbridgeBRSize(depth, chain, deeBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt4[:]...)
	out = binary.AppendUvarint(out, uint64(len(chain)))
	for i := range chain {
		out = binary.AppendUvarint(out, uint64(maxInt(start+i, 0)))
		out = binary.AppendUvarint(out, 1)
		out = binary.AppendUvarint(out, uint64(maxInt(chain[i].ord, 0)))
		if chain[i].hasFp {
			out = append(out, byte(chain[i].fp>>8), byte(chain[i].fp))
		}
		out = binary.AppendUvarint(out, uint64(len(chain[i].ee)))
		for _, s := range chain[i].ee {
			out = binary.AppendUvarint(out, uint64(maxInt(s, 0)))
		}
	}
	out = append(out, deeBytes...)
	return out
}

// EncodeDEEQuad encodes one delayed-end-event quadruple:
//
//	16-byte trace_id || varint(depth) || 4-byte owner_fp || varint(n) || n * varint(start_seq)
//
// owner_fp is the top 4 bytes of the owning span's ID (same width as the ckpt4
// window anchor). Unlike the in-baggage EE list — whose owner is recoverable for
// free from the breadcrumb chain — a drained DEE batch carries no chain context,
// so (trace_id, depth) alone can't pick the owner among same-depth spans in the
// trace, and there's no surrounding path to narrow candidates the way a 2-byte
// chain fp can. The full 4-byte fingerprint pins it on its own.
func EncodeDEEQuad(traceID16 [16]byte, depth int, ownerFP uint32, seqs []int) []byte {
	size := 16 + VarintLen(depth) + 4 + VarintLen(len(seqs))
	for _, s := range seqs {
		size += VarintLen(s)
	}
	out := make([]byte, 0, size)
	out = append(out, traceID16[:]...)
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, byte(ownerFP>>24), byte(ownerFP>>16), byte(ownerFP>>8), byte(ownerFP))
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
