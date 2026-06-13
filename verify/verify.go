// Package verify is an independent checker for reconstruction outputs
// (the certifying-algorithms pattern): a deliberately small program that
// re-derives structural validity and per-bridge payload evidence from
// raw inputs, sharing no logic with the reconstruction engine. If the
// engine were arbitrarily buggy, a fabricated or malformed output would
// still fail here.
//
// The checker consumes plain structs (see Trace) so it can run in two
// modes: in-process after each reconstruction (cheap, always-on) and as
// a standalone binary auditing serialized artifacts cold (the headline
// mode: the verifier never even sees the engine).
package verify

import (
	"encoding/binary"
	"fmt"
)

// Span is one surviving span plus its raw bridge payload (nil for
// non-carriers). Payload bytes are the OBSERVATIONS — the evidence the
// checker replays.
type Span struct {
	ID, Parent uint64
	Depth      int
	BR         []byte
}

// Bridge is one claimed reattachment.
type Bridge struct {
	Orphan, Anchor uint64
	Synthetic      int
	ViaCarrier     uint64 // carrier whose payload justified the anchor (0 = orphan's own)
	Forced         bool
}

// Trace is one trace's reconstruction artifact.
type Trace struct {
	TID        uint64
	Survivors  []Span
	Bridges    []Bridge
	Unanchored []uint64
}

// Config mirrors the deployment constants needed to replay evidence.
type Config struct {
	PrefixLen    int
	BloomM       uint32
	BloomK       uint32
	PCRBTypeByte byte
	CPD          int
}

// Violation is one failed check.
type Violation struct {
	TID  uint64
	Kind string
	Msg  string
}

func (v Violation) String() string {
	return fmt.Sprintf("trace %016x [%s] %s", v.TID, v.Kind, v.Msg)
}

// Check runs every check on one trace and returns all violations.
func Check(t Trace, cfg Config) []Violation {
	var out []Violation
	bad := func(kind, format string, args ...any) {
		out = append(out, Violation{t.TID, kind, fmt.Sprintf(format, args...)})
	}

	// --- structural validity ---
	byID := make(map[uint64]*Span, len(t.Survivors))
	byDepth := make(map[int][]*Span)
	for i := range t.Survivors {
		s := &t.Survivors[i]
		if _, dup := byID[s.ID]; dup {
			bad("dup-span", "%016x appears twice", s.ID)
		}
		byID[s.ID] = s
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}
	// real-edge consistency: a surviving parent must sit exactly one
	// level up
	for i := range t.Survivors {
		s := &t.Survivors[i]
		if p, ok := byID[s.Parent]; ok && p.Depth != s.Depth-1 {
			bad("edge-depth", "%016x depth %d under parent %016x depth %d", s.ID, s.Depth, p.ID, p.Depth)
		}
	}

	// fragment partition: root = survivor whose parent is absent
	rootOf := func(id uint64) uint64 {
		r := id
		for {
			s := byID[r]
			if s == nil {
				return 0
			}
			p, ok := byID[s.Parent]
			if !ok {
				return r
			}
			r = p.ID
		}
	}
	bridged := make(map[uint64]*Bridge, len(t.Bridges))
	for i := range t.Bridges {
		b := &t.Bridges[i]
		if _, dup := bridged[b.Orphan]; dup {
			bad("dup-bridge", "fragment %016x bridged twice", b.Orphan)
		}
		bridged[b.Orphan] = b
	}
	lost := make(map[uint64]bool, len(t.Unanchored))
	for _, id := range t.Unanchored {
		lost[id] = true
	}
	// every fragment root: exactly one disposition
	seenRoot := map[uint64]bool{}
	for i := range t.Survivors {
		r := rootOf(t.Survivors[i].ID)
		if r == 0 || seenRoot[r] {
			continue
		}
		seenRoot[r] = true
		rs := byID[r]
		isTrueRoot := rs.Parent == 0
		_, hasBridge := bridged[r]
		n := 0
		if isTrueRoot {
			n++
		}
		if hasBridge {
			n++
		}
		if lost[r] {
			n++
		}
		if n != 1 {
			bad("disposition", "fragment root %016x: trueRoot=%v bridged=%v lost=%v (want exactly one)", r, isTrueRoot, hasBridge, lost[r])
		}
	}

	// --- bridge checks ---
	for _, b := range t.Bridges {
		o, oOK := byID[b.Orphan]
		a, aOK := byID[b.Anchor]
		if !oOK || !aOK {
			bad("dangling", "bridge %016x->%016x references missing span", b.Orphan, b.Anchor)
			continue
		}
		// depth arithmetic
		if o.Depth != a.Depth+1+b.Synthetic {
			bad("depth-arith", "bridge %016x(d%d)->%016x(d%d) syn=%d", b.Orphan, o.Depth, b.Anchor, a.Depth, b.Synthetic)
		}
		// spacing: a fragment root's parent dropped, so the level above
		// it can never be a real placed span
		if b.Synthetic < 1 {
			bad("spacing", "bridge %016x->%016x has %d synthetics (claims a real edge that cannot exist)", b.Orphan, b.Anchor, b.Synthetic)
		}
		// acyclicity: following anchor -> its fragment root -> its
		// bridge ... must reach a parentless root
		hops := 0
		cur := b.Anchor
		seen := map[uint64]bool{b.Orphan: true}
		for {
			if seen[cur] {
				bad("cycle", "bridge chain from %016x revisits %016x", b.Orphan, cur)
				break
			}
			seen[cur] = true
			r := rootOf(cur)
			rs := byID[r]
			if rs == nil || rs.Parent == 0 {
				break // reached the trace root: grounded
			}
			nb, ok := bridged[r]
			if !ok {
				if !lost[r] {
					bad("ungrounded", "bridge %016x: ancestor fragment %016x has no disposition", b.Orphan, r)
				}
				break
			}
			cur = nb.Anchor
			if hops++; hops > len(t.Survivors) {
				bad("cycle", "bridge chain from %016x exceeds survivor count", b.Orphan)
				break
			}
		}
		// --- evidence replay ---
		// The evidence payload: the bridge's recorded carrier, else the
		// fragment's own nearest payload-bearing descendant.
		carrier := b.ViaCarrier
		if carrier == 0 {
			carrier = findOwnCarrier(o, byID)
		}
		c := byID[carrier]
		if c == nil || len(c.BR) == 0 {
			bad("no-evidence", "bridge %016x->%016x: carrier %016x has no payload", b.Orphan, b.Anchor, carrier)
			continue
		}
		prefix, bloom, perr := decodePCRB(c.BR, cfg)
		if perr != nil {
			bad("payload", "carrier %016x: %v", carrier, perr)
			continue
		}
		// The payload names the carrier's window: its anchor checkpoint
		// sits at the spec depth below.
		ckptDepth := (c.Depth / cfg.CPD) * cfg.CPD
		if c.Depth%cfg.CPD == 0 {
			ckptDepth = c.Depth - cfg.CPD
		}
		if ckptDepth < 0 {
			bad("window", "bridge %016x: carrier %016x window floor below root", b.Orphan, carrier)
			continue
		}
		// (a) prefix replay: a surviving checkpoint at the window depth
		// must match the payload's prefix bytes — the named anchor exists.
		named := false
		for _, s := range byDepth[ckptDepth] {
			var id8 [8]byte
			binary.BigEndian.PutUint64(id8[:], s.ID)
			ok := true
			for i := 0; i < cfg.PrefixLen; i++ {
				if id8[i] != prefix[i] {
					ok = false
					break
				}
			}
			if ok {
				named = true
				break
			}
		}
		if !named {
			bad("prefix", "bridge %016x: no surviving checkpoint at depth %d matches payload prefix", b.Orphan, ckptDepth)
		}
		// (b) the effective anchor must lie within the window, above the
		// orphan: [checkpoint depth, orphan depth).
		if a.Depth < ckptDepth || a.Depth >= o.Depth {
			bad("window", "bridge %016x: effective anchor depth %d outside [%d,%d)", b.Orphan, a.Depth, ckptDepth, o.Depth)
		}
		// (c) bloom replay. The orphan's dropped parent is a true window
		// member if the attachment is genuine — no-false-negative blooms
		// make a negative an outright refutation. For placement-mode
		// bridges (carrier outside the orphan's own fragment), the
		// orphan root itself must also test positive.
		if bloom != nil && o.Parent != 0 {
			if !bloomTest(bloom, cfg, hexOf(o.Parent)) {
				bad("bloom", "bridge %016x: dropped parent %016x negative in carrier %016x window", b.Orphan, o.Parent, carrier)
			}
			if rootOf(carrier) != b.Orphan && !bloomTest(bloom, cfg, hexOf(b.Orphan)) {
				bad("bloom", "placed orphan %016x negative in placement window of %016x", b.Orphan, carrier)
			}
		}
	}
	return out
}

// findOwnCarrier walks real edges below the fragment root for the
// nearest payload-bearing descendant (the carrier whose window covers
// the hole above the root).
func findOwnCarrier(root *Span, byID map[uint64]*Span) uint64 {
	// breadth-first via repeated scans (n is small per trace); the
	// checker favors obvious code over clever code.
	kids := map[uint64][]*Span{}
	for _, s := range byID {
		if _, ok := byID[s.Parent]; ok {
			kids[s.Parent] = append(kids[s.Parent], s)
		}
	}
	queue := []*Span{root}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		if len(s.BR) > 0 {
			return s.ID
		}
		queue = append(queue, kids[s.ID]...)
	}
	return 0
}
