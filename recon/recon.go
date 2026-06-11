// Package recon reconstructs trace topology from surviving spans and their
// bridge payloads. v1 implements the Path Bridge (P-Bridge) only; it requires
// payloads produced in EmitDepth mode (absolute depth in _br, _d on interior
// non-checkpoint spans). See docs/depth_emission.md for the wire formats.
package recon

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"

	"bridges/bloom"
	"bridges/bridge"
)

var debugScore = os.Getenv("TRACE_RECON_DEBUG") == "1"

// Span is one surviving span as the reconstructor sees it: the standard
// record fields plus the decoded bridge attributes. Depth comes from either
// the _br payload or the _d attribute — in EmitDepth mode every span has it.
type Span struct {
	SpanID   uint64
	ParentID uint64 // 0 = root
	Depth    int

	// BloomBits is the raw bloom bit array from the _br payload, nil for
	// spans that carried only _d.
	BloomBits []byte
}

// Config is the global knowledge a real reconstruction module would have:
// the bridge deployment configuration. Bloom geometry derives from CPD and
// the false-positive rate exactly as the SDK computes it.
type Config struct {
	CPD    int
	BloomM uint32
	BloomK uint32

	// BottomUp processes orphans deepest-first and lets shallower orphans
	// discover descendant carriers by walking across already-reconstructed
	// bridges (verified by bloom containment) before falling back to the
	// band-bounded membership scan. Default (false) anchors every orphan
	// independently.
	BottomUp bool

	// ChainCheck requires anchor candidates to pass ancestry-chain
	// consistency: walking up from the candidate, every ancestor ID we can
	// name must also test positive in the orphan's bloom. Through surviving
	// ancestors the walk continues (each survivor's record names its
	// parent); at a disconnection point the dropped parent's ID (recorded
	// on its child) is tested and the walk must stop; at the window floor
	// the walk stops untested — that span is the checkpoint whose reset
	// created the bloom, so its parent legitimately is not in the filter.
	//
	// A true anchor's chain consists of the orphan's own ancestors, so it
	// always passes (blooms have no false negatives). An FP candidate must
	// win an independent ~FP coincidence per step.
	ChainCheck bool
}

func NewConfig(cpd int, bloomFPRate float64) Config {
	if cpd < 1 {
		cpd = 1
	}
	m, k := bloom.EstimateParameters(cpd, bloomFPRate)
	return Config{CPD: cpd, BloomM: m, BloomK: k}
}

// DecodePBPayload parses a P-Bridge _br value: type(1) || varint(depth) ||
// bloom bits. The bloom length is implied by the configured geometry.
func DecodePBPayload(p []byte, cfg Config) (depth int, bloomBits []byte, err error) {
	if len(p) < 2 || p[0] != byte(bridge.PBBridgeTypeID) {
		return 0, nil, errors.New("recon: not a PB payload")
	}
	d, n := binary.Uvarint(p[1:])
	if n <= 0 {
		return 0, nil, errors.New("recon: bad depth varint")
	}
	bits := p[1+n:]
	if len(bits) != int((cfg.BloomM+7)/8) {
		return 0, nil, errors.New("recon: bloom length mismatch")
	}
	return int(d), bits, nil
}

// Bridge is one reconstructed orphan attachment.
type Bridge struct {
	OrphanID  uint64
	AnchorID  uint64 // surviving ancestor the orphan was reattached under
	Synthetic int    // synthetic spans inserted between anchor and orphan
	Ambiguous bool   // >1 candidate tested positive at the anchor depth

	// ViaCarrier is the span whose bloom was borrowed through the
	// membership-based fallback (0 when the orphan's own bloom or an
	// in-fragment descendant's bloom was used).
	ViaCarrier uint64
}

// Result is the outcome of reconstructing one trace.
type Result struct {
	Orphans     int
	Reconnected int
	Bridges     []Bridge
	Unanchored  []uint64 // orphan IDs with no positive bloom candidate
}

// ReconstructPB reattaches orphaned spans using P-Bridge payloads.
//
// For each orphan (surviving span whose parent is not in the surviving set):
//
//  1. Locate the covering bloom: the orphan's own _br if present, else the
//     nearest _br-carrying descendant within the orphan's surviving fragment.
//     Blooms reset at checkpoints, so the nearest carrier's window is
//     guaranteed to span the hole above the orphan (any intermediate
//     checkpoint would itself be a nearer carrier).
//  2. Test surviving spans at depths [lastCkptDepth, orphan.depth-1] against
//     the bloom; the deepest positive is the anchor. The window lower bound
//     is config-derivable: the last protected checkpoint strictly above the
//     orphan, at depth cpd*floor((d-1)/cpd).
//  3. The depth difference fixes the synthetic chain length exactly:
//     orphan.depth - anchor.depth - 1.
//
// Per the paper (§3.4), P-Bridge does NOT merge synthetic parents: siblings
// of a lost parent each get their own synthetic chain.
func ReconstructPB(survivors []Span, cfg Config) Result {
	byID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		byID[survivors[i].SpanID] = &survivors[i]
	}
	children := make(map[uint64][]*Span, len(survivors))
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 {
			if _, ok := byID[s.ParentID]; ok {
				children[s.ParentID] = append(children[s.ParentID], s)
			}
		}
	}
	// Candidate index: surviving spans grouped by depth.
	byDepth := make(map[int][]*Span)
	for i := range survivors {
		s := &survivors[i]
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}
	// All _br carriers, shallowest first (for the membership fallback).
	var carriers []*Span
	for i := range survivors {
		if survivors[i].BloomBits != nil {
			carriers = append(carriers, &survivors[i])
		}
	}
	sort.Slice(carriers, func(i, j int) bool {
		if carriers[i].Depth != carriers[j].Depth {
			return carriers[i].Depth < carriers[j].Depth
		}
		return carriers[i].SpanID < carriers[j].SpanID
	})

	var orphans []*Span
	for i := range survivors {
		o := &survivors[i]
		if o.ParentID == 0 {
			continue
		}
		if _, ok := byID[o.ParentID]; ok {
			continue
		}
		orphans = append(orphans, o)
	}
	if cfg.BottomUp {
		sort.Slice(orphans, func(i, j int) bool {
			if orphans[i].Depth != orphans[j].Depth {
				return orphans[i].Depth > orphans[j].Depth
			}
			return orphans[i].SpanID < orphans[j].SpanID
		})
	}

	// reconChildren accumulates anchor->orphan edges as bridges land
	// (bottom-up mode only); coveringBloom walks them, with verification.
	reconChildren := make(map[uint64][]*Span)

	var res Result
	res.Orphans = len(orphans)
	for _, o := range orphans {
		bits, viaCarrier := coveringBloom(o, children, reconChildren, carriers, cfg)
		if bits == nil {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		bf := bloom.Deserialize(bits, cfg.BloomM, cfg.BloomK)

		// Window: [last checkpoint strictly above orphan, orphan.depth-1].
		lo := ((o.Depth - 1) / cfg.CPD) * cfg.CPD
		anchor, ambiguous := findAnchor(bf, byDepth, byID, lo, o.Depth-1, cfg)
		if anchor == nil {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		res.Reconnected++
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:   o.SpanID,
			AnchorID:   anchor.SpanID,
			Synthetic:  o.Depth - anchor.Depth - 1,
			Ambiguous:  ambiguous,
			ViaCarrier: viaCarrier,
		})
		if cfg.BottomUp {
			reconChildren[anchor.SpanID] = append(reconChildren[anchor.SpanID], o)
		}
	}
	return res
}

// coveringBloom returns the bloom bits whose window covers the hole above o,
// trying the sources in order:
//
//  1. o's own _br;
//  2. the nearest _br-carrying descendant reachable below o — through real
//     surviving edges, and (bottom-up mode) across already-reconstructed
//     bridges. A carrier reached across a reconstructed edge is verified by
//     testing o's ID against its bloom before borrowing, since the structure
//     it was reached through is itself a reconstruction guess;
//  3. membership scan: the shallowest carrier whose bloom contains o's span
//     ID. Containment means (modulo bloom FP) the carrier descends from o
//     with no checkpoint in between — a checkpoint between them would have
//     reset the carrier's bloom and evicted o from it.
//
// All sources are bounded to the orphan's checkpoint band: a true claimant
// must sit at depth in (o.depth, nextCkpt], where nextCkpt is the first
// checkpoint level below o. Anything deeper has a checkpoint (which
// survives) between itself and o, so its bloom cannot contain o — scanning
// it would be pure false-positive surface.
//
// The second return value is the carrier span ID when the bloom was borrowed
// (source 3, or source 2 across a reconstructed edge), else 0.
func coveringBloom(o *Span, children, reconChildren map[uint64][]*Span, carriers []*Span, cfg Config) ([]byte, uint64) {
	if o.BloomBits != nil {
		return o.BloomBits, 0
	}
	band := (o.Depth/cfg.CPD + 1) * cfg.CPD
	hexO := bridge.HexOf(o.SpanID)

	// Level-order walk below o so "nearest" is by depth. borrowed marks
	// paths that crossed a reconstructed edge.
	type item struct {
		s        *Span
		borrowed bool
	}
	var queue []item
	for _, c := range children[o.SpanID] {
		queue = append(queue, item{c, false})
	}
	for _, c := range reconChildren[o.SpanID] {
		queue = append(queue, item{c, true})
	}
	for len(queue) > 0 {
		var next []item
		for _, it := range queue {
			if it.s.Depth > band {
				continue // a checkpoint separates this subtree from o
			}
			if it.s.BloomBits != nil {
				if !it.borrowed {
					return it.s.BloomBits, 0
				}
				bf := bloom.Deserialize(it.s.BloomBits, cfg.BloomM, cfg.BloomK)
				if bf.Test(hexO[:]) {
					return it.s.BloomBits, it.s.SpanID
				}
				// Verification failed: within the band a descendant's bloom
				// is a superset of its parent's, so this whole subtree is a
				// misattachment — skip it.
				continue
			}
			for _, c := range children[it.s.SpanID] {
				next = append(next, item{c, it.borrowed})
			}
			for _, c := range reconChildren[it.s.SpanID] {
				next = append(next, item{c, true})
			}
		}
		queue = next
	}

	// Membership scan, band-bounded. carriers is sorted shallowest-first, so
	// the first claimant is the nearest-by-depth descendant — the safest
	// borrow — and the sort lets us stop at the band edge.
	for _, c := range carriers {
		if c.Depth <= o.Depth {
			continue
		}
		if c.Depth > band {
			break
		}
		bf := bloom.Deserialize(c.BloomBits, cfg.BloomM, cfg.BloomK)
		if bf.Test(hexO[:]) {
			return c.BloomBits, c.SpanID
		}
	}
	return nil, 0
}

// findAnchor tests candidates at depths [hi..lo] (deepest first) against the
// bloom and returns the deepest positive (passing chain consistency when
// enabled). Ties at the same depth are broken by smallest span ID and
// reported as ambiguous.
func findAnchor(bf *bloom.Filter, byDepth map[int][]*Span, byID map[uint64]*Span, lo, hi int, cfg Config) (*Span, bool) {
	for d := hi; d >= lo; d-- {
		var hits []*Span
		for _, c := range byDepth[d] {
			hex := bridge.HexOf(c.SpanID)
			if !bf.Test(hex[:]) {
				continue
			}
			if cfg.ChainCheck && !chainConsistent(bf, c, byID, lo) {
				continue
			}
			hits = append(hits, c)
		}
		if len(hits) == 0 {
			continue
		}
		if debugScore && len(hits) > 1 {
			ones := 0
			for _, b := range bf.ToBytes() {
				for ; b != 0; b &= b - 1 {
					ones++
				}
			}
			fmt.Fprintf(os.Stderr, "ambig: depth=%d hits=%d bloom_setbits=%d/%d\n",
				d, len(hits), ones, bf.M())
		}
		sort.Slice(hits, func(i, j int) bool { return hits[i].SpanID < hits[j].SpanID })
		return hits[0], len(hits) > 1
	}
	return nil, false
}

// chainConsistent walks up from candidate c, testing every nameable ancestor
// ID against the orphan's bloom:
//
//   - while the current span survives and sits above the window floor, its
//     recorded parent ID is tested (the ID is known even when the parent
//     itself was dropped);
//   - if the parent survives, the walk continues from it;
//   - if the parent was dropped (a disconnection point), there is nothing
//     further to name — stop, consistent so far;
//   - at the window floor the walk stops untested: that span is the
//     checkpoint whose reset created the bloom, so its parent is
//     legitimately absent from the filter.
//
// True anchors always pass (their chain is the orphan's own ancestry, and
// blooms have no false negatives); an FP candidate must pass ~FP per step.
func chainConsistent(bf *bloom.Filter, c *Span, byID map[uint64]*Span, lo int) bool {
	cur := c
	for cur.Depth > lo {
		pid := cur.ParentID
		if pid == 0 {
			return true // root: nothing above to test
		}
		ph := bridge.HexOf(pid)
		if !bf.Test(ph[:]) {
			return false
		}
		p, ok := byID[pid]
		if !ok {
			return true // disconnection point: no further IDs nameable
		}
		cur = p
	}
	return true
}

// ---- Scoring against ground truth ----

// TruthSpan is one span of the pre-drop trace.
type TruthSpan struct {
	SpanID   uint64
	ParentID uint64
	Depth    int
}

// Score compares a reconstruction Result against the pre-drop trace.
type Score struct {
	Spans         int
	Dropped       int
	Orphans       int
	Reconnected   int
	AnchorCorrect int // anchor == true nearest surviving ancestor
	GapCorrect    int // synthetic count == true dropped spans in between
	Misattached   int // reconnected but anchor wrong (bloom FP / ambiguity)
	Unanchored    int
	Synthetic     int // total synthetic spans created
	Borrowed      int // bridges built via the membership-based bloom fallback
	Ambiguous     int // bridges where >1 candidate tested positive at the anchor depth
	AmbiguousBad  int // ... of which the anchor was wrong
}

// ScorePB computes per-trace reconstruction accuracy. truth holds every span
// of the original trace; dropped is the set of span IDs removed by the drop
// policy.
func ScorePB(res Result, truth []TruthSpan, dropped map[uint64]struct{}) Score {
	parent := make(map[uint64]uint64, len(truth))
	depth := make(map[uint64]int, len(truth))
	for _, t := range truth {
		parent[t.SpanID] = t.ParentID
		depth[t.SpanID] = t.Depth
	}

	sc := Score{
		Spans:       len(truth),
		Dropped:     len(dropped),
		Orphans:     res.Orphans,
		Reconnected: res.Reconnected,
		Unanchored:  len(res.Unanchored),
	}
	for _, b := range res.Bridges {
		sc.Synthetic += b.Synthetic
		if b.ViaCarrier != 0 {
			sc.Borrowed++
		}
		if b.Ambiguous {
			sc.Ambiguous++
		}

		// True nearest surviving ancestor: walk the real parent chain upward
		// past dropped spans.
		trueAnchor := uint64(0)
		gap := 0
		for p := parent[b.OrphanID]; p != 0; p = parent[p] {
			if _, isDropped := dropped[p]; !isDropped {
				trueAnchor = p
				break
			}
			gap++
		}
		if b.AnchorID == trueAnchor {
			sc.AnchorCorrect++
			if b.Synthetic == gap {
				sc.GapCorrect++
			}
		} else {
			sc.Misattached++
			if b.Ambiguous {
				sc.AmbiguousBad++
			}
			if debugScore {
				fmt.Fprintf(os.Stderr,
					"misattach: orphan=%016x d=%d got_anchor=%016x (d=%d, ambig=%t, syn=%d) want_anchor=%016x (d=%d, gap=%d)\n",
					b.OrphanID, depth[b.OrphanID],
					b.AnchorID, depth[b.AnchorID], b.Ambiguous, b.Synthetic,
					trueAnchor, depth[trueAnchor], gap)
			}
		}
	}
	return sc
}
