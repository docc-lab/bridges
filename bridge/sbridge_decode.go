package bridge

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// This file is the inverse of the S-Bridge packers in pack.go: it parses a
// serialized _br payload and the DEE quadruples back into structured form, for
// the reconstruction side (recon/sbridge.go). Keeping decode next to encode
// keeps the wire format in one place and lets pack->decode round-trip tests
// guard it.

// SBChainLevel is one decoded level of an S-Bridge breadcrumb chain.
type SBChainLevel struct {
	Depth int    // absolute depth of this level's span
	Ord   int    // start ordinal of this span under its parent
	FP    uint64 // parent fingerprint (top fpBits of the parent span ID, right-aligned)
	HasFP bool   // false when the parent is the checkpoint root (its identity is Ckpt)
	EE    []int  // start ordinals of earlier siblings that ended before this span started
}

// SBridgeBR is a fully decoded S-Bridge _br payload. Ckpt holds the
// checkpoint-root window anchor, left-aligned in an [8]byte (only the leading
// CkptBytes are meaningful; the rest are zero).
type SBridgeBR struct {
	Depth int
	Ckpt  [8]byte
	Chain []SBChainLevel
	DEE   []DEEQuad // the trailing dee_bytes, parsed into quads
}

// DEEQuad is one decoded delayed-end-event quadruple. OwnerFP is the owner's
// top-fpBits fingerprint, right-aligned (same encoding as a chain fp).
type DEEQuad struct {
	TraceID16 [16]byte
	Depth     int
	OwnerFP   uint64
	Seqs      []int
}

// cursor is a tiny sticky-error byte reader over a payload.
type cursor struct {
	b   []byte
	i   int
	err error
}

func (c *cursor) uvarint() int {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.i:])
	if n <= 0 {
		c.err = fmt.Errorf("truncated varint at offset %d", c.i)
		return 0
	}
	c.i += n
	return int(v)
}

func (c *cursor) take(n int) []byte {
	if c.err != nil {
		return nil
	}
	if c.i+n > len(c.b) {
		c.err = fmt.Errorf("need %d bytes at offset %d, have %d", n, c.i, len(c.b)-c.i)
		return nil
	}
	out := c.b[c.i : c.i+n]
	c.i += n
	return out
}

func (c *cursor) done() bool { return c.err == nil && c.i >= len(c.b) }

// DecodeSBridgeBR parses an _br payload produced by PackSBridgeBR (structure of
// arrays). cpd is the checkpoint distance, fpBits the non-checkpoint fingerprint
// width, and ckptBytes the checkpoint-root anchor width the payload was emitted
// with — all needed because per-level depth and fp presence are derived (not
// stored), the fps are bit-packed at fpBits, and the anchor occupies ckptBytes.
func DecodeSBridgeBR(b []byte, cpd, fpBits, ckptBytes int) (SBridgeBR, error) {
	if cpd < 1 {
		return SBridgeBR{}, errors.New("cpd must be >= 1")
	}
	if ckptBytes < 1 || ckptBytes > 8 {
		return SBridgeBR{}, errors.New("ckptBytes must be in 1..8")
	}
	c := &cursor{b: b}
	var br SBridgeBR
	br.Depth = c.uvarint()
	L := c.uvarint()
	if c.err != nil {
		return br, c.err
	}
	br.Chain = make([]SBChainLevel, L)
	start := chainStartDepth(br.Depth, L)
	// ORDINALS — and derive each level's depth + fp presence.
	nFP := 0
	for i := 0; i < L; i++ {
		br.Chain[i].Depth = start + i
		br.Chain[i].Ord = c.uvarint()
		br.Chain[i].HasFP = (br.Chain[i].Depth-1)%cpd != 0 // parent non-checkpoint
		if br.Chain[i].HasFP {
			nFP++
		}
	}
	// FPS — the ckpt anchor leads, then nFP bit-packed fpBits-wide fingerprints.
	copy(br.Ckpt[:], c.take(ckptBytes))
	fpBytes := c.take((nFP*fpBits + 7) / 8)
	if c.err != nil {
		return br, c.err
	}
	fps := unpackBits(fpBytes, nFP, fpBits)
	fi := 0
	for i := 0; i < L; i++ {
		if br.Chain[i].HasFP {
			br.Chain[i].FP = fps[fi]
			fi++
		}
	}
	// END-EVENTS — per level: varint(n) then n ordinals.
	for i := 0; i < L; i++ {
		m := c.uvarint()
		if m > 0 {
			br.Chain[i].EE = make([]int, 0, m)
			for j := 0; j < m; j++ {
				br.Chain[i].EE = append(br.Chain[i].EE, c.uvarint())
			}
		}
		if c.err != nil {
			return br, c.err
		}
	}
	// Whatever remains is the dee_bytes blob: a sequence of self-delimiting quads.
	// Each carries the owner fp in ceil(fpBits/8) bytes (matching the emit width).
	ownerBytes := (fpBits + 7) / 8
	for !c.done() {
		q, err := decodeDEEQuadAt(c, ownerBytes)
		if err != nil {
			return br, err
		}
		br.DEE = append(br.DEE, q)
	}
	return br, c.err
}

// DecodeDEEQuads parses a concatenation of DEE quadruples (e.g. a drained queue).
// fpBits is the fingerprint width the quads were emitted with (sets owner width).
func DecodeDEEQuads(b []byte, fpBits int) ([]DEEQuad, error) {
	ownerBytes := (fpBits + 7) / 8
	if ownerBytes < 1 {
		ownerBytes = 1
	}
	c := &cursor{b: b}
	var out []DEEQuad
	for !c.done() {
		q, err := decodeDEEQuadAt(c, ownerBytes)
		if err != nil {
			return out, err
		}
		out = append(out, q)
	}
	return out, c.err
}

func decodeDEEQuadAt(c *cursor, ownerBytes int) (DEEQuad, error) {
	var q DEEQuad
	copy(q.TraceID16[:], c.take(16))
	q.Depth = c.uvarint()
	fp := c.take(ownerBytes)
	if c.err == nil {
		var v uint64
		for _, b := range fp { // big-endian, right-aligned
			v = v<<8 | uint64(b)
		}
		q.OwnerFP = v
	}
	n := c.uvarint()
	if n > 0 {
		q.Seqs = make([]int, 0, n)
		for j := 0; j < n; j++ {
			q.Seqs = append(q.Seqs, c.uvarint())
		}
	}
	return q, c.err
}
