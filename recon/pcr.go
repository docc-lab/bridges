package recon

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"

	"bridges/bridge"
)

// PCR reconstruction: orphans are reattached to a NAMED checkpoint ancestor
// rather than a discovered one. The payload carries (absolute depth, K-byte
// prefix of the nearest checkpoint ancestor strictly above the emitting
// span), so reconstruction is a deterministic lookup:
//
//  1. Obtain a covering payload for the orphan: its own _br (checkpoint and
//     leaf orphans always carry one), else the nearest _br carrier reachable
//     downward through REAL surviving edges within the orphan's checkpoint
//     band. Every in-band carrier names the same checkpoint — the one above
//     the orphan — so no membership verification is needed or possible.
//     Unlike PB, reconstructed bridges are never walked: PCR anchors are
//     always checkpoints, and checkpoint orphans always self-carry, so no
//     bridge ever lands on a span another orphan would walk through.
//  2. The named checkpoint's depth is implied by the payload's depth d_p:
//     d_p - cpd if the carrier is itself a checkpoint (it names its
//     predecessor), else cpd*floor(d_p/cpd).
//  3. The anchor is the surviving span at that depth whose big-endian span
//     ID starts with the K-byte prefix. Zero matches -> unanchored; more
//     than one -> prefix collision: flagged Ambiguous, smallest ID wins.
//
// Error surface: prefix collisions (birthday-bounded, detectable) and
// nothing else — there is no false-positive membership channel. Against
// ScorePB's nearest-surviving-ancestor metric, PCR "misattaches" whenever an
// intermediate ancestor survived between checkpoint and orphan; the
// AnchorAncestor metric separates that structural coarseness from genuinely
// wrong (non-ancestor) attachments.

// DecodePCRPayload parses a PCR _br value: type(1) || varint(depth) || ckptK.
func DecodePCRPayload(p []byte, cfg Config) (depth int, prefix []byte, err error) {
	if len(p) < 2 || p[0] != byte(bridge.PCRBridgeTypeID) {
		return 0, nil, errors.New("recon: not a PCR payload")
	}
	d, n := binary.Uvarint(p[1:])
	if n <= 0 {
		return 0, nil, errors.New("recon: bad depth varint")
	}
	pre := p[1+n:]
	if len(pre) != cfg.PrefixLen {
		return 0, nil, errors.New("recon: prefix length mismatch")
	}
	return int(d), pre, nil
}

// ReconstructPCR reattaches orphaned spans using PCR payloads. Survivor
// spans carry CkptPrefix (nil for _d-only spans); cfg.PrefixLen and cfg.CPD
// must match the emitting handler.
func ReconstructPCR(survivors []Span, cfg Config) Result {
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
	byDepth := make(map[int][]*Span)
	for i := range survivors {
		s := &survivors[i]
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}

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

	var res Result
	res.Orphans = len(orphans)
	for _, o := range orphans {
		carrierDepth, prefix, viaCarrier := coveringPrefix(o, children, cfg)
		if prefix == nil {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		// Named checkpoint depth from the carrier's own depth: a checkpoint
		// carrier names its predecessor; an interior/leaf carrier names the
		// checkpoint at the top of its window.
		var ckptDepth int
		if carrierDepth%cfg.CPD == 0 {
			ckptDepth = carrierDepth - cfg.CPD
		} else {
			ckptDepth = (carrierDepth / cfg.CPD) * cfg.CPD
		}
		if ckptDepth < 0 {
			// Root payload (depth 0, zero prefix): nothing above to attach to.
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}

		// Prefix match among survivors at the named depth (all checkpoints).
		var hits []*Span
		for _, c := range byDepth[ckptDepth] {
			id8 := bridge.BigEndian8(c.SpanID)
			match := true
			for i := 0; i < cfg.PrefixLen; i++ {
				if id8[i] != prefix[i] {
					match = false
					break
				}
			}
			if match {
				hits = append(hits, c)
			}
		}
		if len(hits) == 0 {
			res.Unanchored = append(res.Unanchored, o.SpanID)
			continue
		}
		sort.Slice(hits, func(i, j int) bool { return hits[i].SpanID < hits[j].SpanID })
		anchor := hits[0]

		res.Reconnected++
		res.Bridges = append(res.Bridges, Bridge{
			OrphanID:   o.SpanID,
			AnchorID:   anchor.SpanID,
			Synthetic:  o.Depth - anchor.Depth - 1,
			Ambiguous:  len(hits) > 1, // prefix collision
			ViaCarrier: viaCarrier,
		})
	}
	return res
}

// coveringPrefix returns the payload (carrier depth, ckpt prefix) covering
// the hole above o: o's own _br if present, else the nearest _br carrier
// reachable downward through real surviving edges, bounded to o's checkpoint
// band (a carrier below the band names a checkpoint below o). The third
// return value is the carrier's span ID when borrowed, else 0.
func coveringPrefix(o *Span, children map[uint64][]*Span, cfg Config) (int, []byte, uint64) {
	if o.CkptPrefix != nil {
		return o.Depth, o.CkptPrefix, 0
	}
	band := (o.Depth/cfg.CPD + 1) * cfg.CPD
	queue := children[o.SpanID]
	for len(queue) > 0 {
		var next []*Span
		for _, s := range queue {
			if s.Depth > band {
				continue
			}
			if s.CkptPrefix != nil {
				return s.Depth, s.CkptPrefix, s.SpanID
			}
			next = append(next, children[s.SpanID]...)
		}
		queue = next
	}
	return 0, nil, 0
}

// ScorePCR scores a PCR reconstruction with fragment-level semantics:
// everything ScorePB measures, plus thrown-away accounting and span-weighted
// window-skip impact (AncestorsSkipped, SpansInSkipped). Under the
// fragment model an unanchored orphan is not left dangling — its entire
// fragment (the orphan and every survivor connected below it) is discarded
// and recorded: FragmentsLost / SpansLost. The operative accuracy metric is
// AnchorAncestor (fragment attached to its true window checkpoint);
// Misattached under the nearest-survivor metric is retained for PB
// comparability but conflates within-window nesting coarseness with real
// error (see docs/pb_reconstruction.md).
func ScorePCR(res Result, truth []TruthSpan, dropped map[uint64]struct{}) Score {
	parent := make(map[uint64]uint64, len(truth))
	for _, t := range truth {
		parent[t.SpanID] = t.ParentID
	}

	// Lost set first: fragment root of each survivor via memoized walk up
	// SURVIVING parents; fragments rooted at unanchored roots are discarded
	// wholesale. The lost set then feeds the reduced-set scoring below.
	unanch := make(map[uint64]struct{}, len(res.Unanchored))
	for _, id := range res.Unanchored {
		unanch[id] = struct{}{}
	}
	lost := make(map[uint64]bool, len(res.Unanchored))
	rootMemo := make(map[uint64]uint64, len(truth))
	var rootOf func(id uint64) uint64
	rootOf = func(id uint64) uint64 {
		if r, ok := rootMemo[id]; ok {
			return r
		}
		r := id
		if p := parent[id]; p != 0 {
			if _, gone := dropped[p]; !gone {
				r = rootOf(p)
			}
		}
		rootMemo[id] = r
		return r
	}
	if len(unanch) > 0 {
		for _, t := range truth {
			if _, gone := dropped[t.SpanID]; gone {
				continue
			}
			if _, ok := unanch[rootOf(t.SpanID)]; ok {
				lost[t.SpanID] = true
			}
		}
	}

	// Reduced-set scoring: nearest-surviving-ancestor treats lost spans as
	// absent — attaching past a discarded fragment is exact, not a skip.
	sc := scorePB(res, truth, dropped, lost)
	sc.FragmentsLost = len(res.Unanchored)
	sc.OpenEnds = res.OpenEnds
	sc.OpenEndsMatched = res.OpenEndsMatched
	sc.ForcedMatches = res.ForcedMatches
	sc.OrphansPlaced = res.OrphansPlaced
	sc.OrphanOpenEnds = res.OrphanOpenEnds
	sc.SpansLost = len(lost)

	// Per-bridge skip count: REDUCED-set surviving ancestors strictly
	// between fragment root and anchor. Only meaningful when the anchor is
	// a true ancestor (wrong-checkpoint bridges excluded).
	var survKids, truthKids map[uint64]int
	var depthOf map[uint64]int
	if debugScore {
		survKids = make(map[uint64]int, len(truth))
		truthKids = make(map[uint64]int, len(truth))
		depthOf = make(map[uint64]int, len(truth))
		for _, t := range truth {
			depthOf[t.SpanID] = t.Depth
			if t.ParentID != 0 {
				truthKids[t.ParentID]++
			}
			if _, gone := dropped[t.SpanID]; gone || lost[t.SpanID] {
				continue
			}
			if _, pg := dropped[t.ParentID]; t.ParentID != 0 && !pg && !lost[t.ParentID] {
				survKids[t.ParentID]++
			}
		}
	}
	skippedRoots := make(map[uint64]struct{})
	for _, b := range res.Bridges {
		n := 0
		reached := false
		var bypassed []uint64
		for p := parent[b.OrphanID]; p != 0; p = parent[p] {
			if p == b.AnchorID {
				reached = true
				break
			}
			if _, gone := dropped[p]; !gone && !lost[p] {
				n++
				bypassed = append(bypassed, p)
			}
		}
		if reached && n > 0 {
			sc.AncestorsSkipped += n
			skippedRoots[b.OrphanID] = struct{}{}
			if debugScore {
				for _, p := range bypassed {
					// open end = lost every surviving child but had
					// children in truth (a true leaf has nothing to lose)
					open := survKids[p] == 0 && truthKids[p] > 0
					fmt.Fprintf(os.Stderr, "BENIGNSKIP C=%x depth=%d open=%t anchor=%x adepth=%d\n",
						p, depthOf[p], open, b.AnchorID, depthOf[b.AnchorID])
				}
			}
		}
	}
	if len(skippedRoots) > 0 {
		for _, t := range truth {
			if _, gone := dropped[t.SpanID]; gone {
				continue
			}
			if _, ok := skippedRoots[rootOf(t.SpanID)]; ok {
				sc.SpansInSkipped++
			}
		}
	}
	return sc
}
