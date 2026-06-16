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
	FP    uint16 // parent fingerprint (top 2 bytes of the parent span ID)
	HasFP bool   // false when the parent is the checkpoint root (its identity is Ckpt4)
	EE    []int  // start ordinals of earlier siblings that ended before this span started
}

// SBridgeBR is a fully decoded S-Bridge _br payload.
type SBridgeBR struct {
	Depth int
	Ckpt4 [4]byte
	Chain []SBChainLevel
	DEE   []DEEQuad // the trailing dee_bytes, parsed into quads
}

// DEEQuad is one decoded delayed-end-event quadruple.
type DEEQuad struct {
	TraceID16 [16]byte
	Depth     int
	OwnerFP   uint32
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

// DecodeSBridgeBR parses an _br payload produced by PackSBridgeBR. cpd is the
// checkpoint distance the payload was emitted with; it's required because the
// presence of a level's 2-byte parent fingerprint is implicit (written iff the
// parent is non-checkpoint), so we recompute HasFP from the level depth.
func DecodeSBridgeBR(b []byte, cpd int) (SBridgeBR, error) {
	if cpd < 1 {
		return SBridgeBR{}, errors.New("cpd must be >= 1")
	}
	c := &cursor{b: b}
	var br SBridgeBR
	br.Depth = c.uvarint()
	copy(br.Ckpt4[:], c.take(4))
	n := c.uvarint()
	if c.err != nil {
		return br, c.err
	}
	br.Chain = make([]SBChainLevel, 0, n)
	for k := 0; k < n; k++ {
		var lv SBChainLevel
		lv.Depth = c.uvarint()
		if one := c.uvarint(); one != 1 {
			// The legacy per-depth "count" field; S-Bridge always emits 1.
			if c.err == nil {
				c.err = fmt.Errorf("level %d: expected count 1, got %d", k, one)
			}
		}
		lv.Ord = c.uvarint()
		// fp present iff the parent (at lv.Depth-1) is not a checkpoint.
		lv.HasFP = (lv.Depth-1)%cpd != 0
		if lv.HasFP {
			fp := c.take(2)
			if c.err == nil {
				lv.FP = uint16(fp[0])<<8 | uint16(fp[1])
			}
		}
		m := c.uvarint()
		if m > 0 {
			lv.EE = make([]int, 0, m)
			for j := 0; j < m; j++ {
				lv.EE = append(lv.EE, c.uvarint())
			}
		}
		br.Chain = append(br.Chain, lv)
		if c.err != nil {
			return br, c.err
		}
	}
	// Whatever remains is the dee_bytes blob: a sequence of self-delimiting quads.
	for !c.done() {
		q, err := decodeDEEQuadAt(c)
		if err != nil {
			return br, err
		}
		br.DEE = append(br.DEE, q)
	}
	return br, c.err
}

// DecodeDEEQuads parses a concatenation of DEE quadruples (e.g. a drained queue).
func DecodeDEEQuads(b []byte) ([]DEEQuad, error) {
	c := &cursor{b: b}
	var out []DEEQuad
	for !c.done() {
		q, err := decodeDEEQuadAt(c)
		if err != nil {
			return out, err
		}
		out = append(out, q)
	}
	return out, c.err
}

func decodeDEEQuadAt(c *cursor) (DEEQuad, error) {
	var q DEEQuad
	copy(q.TraceID16[:], c.take(16))
	q.Depth = c.uvarint()
	fp := c.take(4)
	if c.err == nil {
		q.OwnerFP = binary.BigEndian.Uint32(fp)
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
