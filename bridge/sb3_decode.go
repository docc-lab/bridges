package bridge

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type SB3HAEntry struct {
	ParentID uint64
	Depth    int // child depth, matching CGPRB HA semantics
}

type SB3Payload struct {
	Depth      int
	CkptPrefix []byte
	BloomBits  []byte
	HA         []SB3HAEntry
	Branches   []SB3Branch
	DEE        []DEEQuad
}

// DecodeSB3Payload parses the combined CGPRB + sparse-ordinal payload. Bloom
// and checkpoint-prefix widths are deployment configuration; HA is explicitly
// length-framed and trailing bytes are ordinary S-Bridge DEE quads.
func DecodeSB3Payload(b []byte, prefixLen, bloomLen, fpBits int, lehmer bool) (SB3Payload, error) {
	var out SB3Payload
	if prefixLen < 1 || prefixLen > 8 {
		return out, errors.New("SB3 prefixLen must be in 1..8")
	}
	if bloomLen < 0 {
		return out, errors.New("SB3 bloomLen must be non-negative")
	}
	c := &cursor{b: b}
	tag := c.take(1)
	if c.err != nil || len(tag) != 1 || tag[0] != byte(SB3BridgeTypeID) {
		return out, errors.New("not an SB3 payload")
	}
	out.Depth = c.uvarint()
	out.CkptPrefix = append([]byte(nil), c.take(prefixLen)...)
	out.BloomBits = append([]byte(nil), c.take(bloomLen)...)
	haLen := c.uvarint()
	haRaw := c.take(haLen)
	if c.err != nil {
		return out, c.err
	}
	for len(haRaw) > 0 {
		if len(haRaw) < 8 {
			return out, errors.New("truncated SB3 HA parent id")
		}
		pid := binary.BigEndian.Uint64(haRaw[:8])
		haRaw = haRaw[8:]
		d, n := binary.Uvarint(haRaw)
		if n <= 0 {
			return out, errors.New("truncated SB3 HA depth")
		}
		haRaw = haRaw[n:]
		out.HA = append(out.HA, SB3HAEntry{ParentID: pid, Depth: int(d)})
	}

	nBranches := c.uvarint()
	for i := 0; i < nBranches; i++ {
		ord := c.uvarint()
		if ord < 2 {
			return out, fmt.Errorf("SB3 branch %d has non-sparse ordinal %d", i, ord)
		}
		nEE := c.uvarint()
		br := SB3Branch{Ord: ord}
		if nEE > 0 {
			if lehmer {
				raw := c.take(partialPermutationBytes(ord-1, nEE))
				var err error
				br.EE, err = decodePartialPermutation(ord-1, nEE, raw)
				if err != nil {
					return out, fmt.Errorf("SB3 branch %d EE: %w", i, err)
				}
			} else {
				br.EE = make([]int, 0, nEE)
				for j := 0; j < nEE; j++ {
					br.EE = append(br.EE, c.uvarint())
				}
			}
		}
		if c.err != nil {
			return out, c.err
		}
		out.Branches = append(out.Branches, br)
	}

	ownerBytes := (fpBits + 7) / 8
	if ownerBytes < 1 {
		ownerBytes = 1
	}
	for !c.done() {
		q, err := decodeDEEQuadAt(c, ownerBytes, lehmer)
		if err != nil {
			return out, err
		}
		out.DEE = append(out.DEE, q)
	}
	return out, c.err
}
