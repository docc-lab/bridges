// Package recon reconstructs trace topology from surviving spans and their
// bridge payloads. v1 implements the Path Bridge (P-Bridge) only; it requires
// payloads produced in EmitDepth mode (absolute depth in _br, _d on interior
// non-checkpoint spans). See docs/depth_emission.md for the wire formats.
//
// The algorithm and its rationale are specified in
// docs/pb_reconstruction.md. Comments below cite the numbered algorithm box
// there as [alg. N]; the function decomposition matches it one-to-one:
//
//	ReconstructPB   = reconnect_orphans  [alg. 1-11]
//	coveringBloom   = covering_bloom     [alg. 12-22]
//	findAnchor      = find_anchor        [alg. 23-29]
//	chainConsistent = chain_consistent   [alg. 30-38]
//
// Conceptual flow: every dropped span leaves behind orphans — surviving
// spans whose parent is gone. Each orphan roots a detached fragment. To
// reattach a fragment we need (a) a bloom filter whose window covers the
// hole above the orphan (coveringBloom), (b) the surviving ancestor to
// attach to, found by testing candidate IDs against that bloom
// (findAnchor), and (c) the exact number of synthetic spans to chain in
// between, which absolute depths give us for free. Reconstruction errors
// are misattachments — picking a wrong anchor via bloom false positives or
// same-depth ambiguity — never failures to reconnect.
package recon

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"bridges/bloom"
	"bridges/bridge"
)

var debugScore = os.Getenv("TRACE_RECON_DEBUG") == "1"

// Cluster export for the independent C++ (CP-SAT) solver validator. When
// TRACE_RECON_CLUSTERDUMP names a file, each solved per-cluster
// optimization instance is appended in a compact text format (see
// cmd_validate). The validator re-solves each cluster to optimality and
// checks our branch-and-bound's bestScore against the true optimum.
var (
	clusterDumpMu   sync.Mutex
	clusterDumpFile *os.File
	clusterDumpOnce sync.Once
)

func clusterDumpWriter() *os.File {
	clusterDumpOnce.Do(func() {
		if p := os.Getenv("TRACE_RECON_CLUSTERDUMP"); p != "" {
			if f, err := os.Create(p); err == nil {
				clusterDumpFile = f
			}
		}
	})
	return clusterDumpFile
}

// debugEnds: comma-separated hex span IDs (TRACE_RECON_DEBUG_ENDS) whose
// ledger placements are logged at census time (ENDMATCH lines).
var debugEnds = func() map[uint64]bool {
	m := make(map[uint64]bool)
	for _, h := range strings.Split(os.Getenv("TRACE_RECON_DEBUG_ENDS"), ",") {
		if h == "" {
			continue
		}
		var id uint64
		fmt.Sscanf(h, "%x", &id)
		m[id] = true
	}
	return m
}()

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

	// CkptPrefix is the truncated checkpoint-root span ID from a PCR _br
	// payload (see recon/pcr.go), nil for spans that carried only _d.
	// Mutually exclusive with BloomBits — a corpus is run in one mode.
	CkptPrefix []byte

	// LeafCarrier marks a span provably a leaf: it carries a _br payload at
	// a non-checkpoint depth, and only leaves emit there. A leaf has no
	// descendants, so it can never be a threading candidate — any bloom
	// positive on it is a structurally impossible attachment.
	LeafCarrier bool

	// HA is the decoded CGPRB hash array: the in-window branch points this
	// span witnesses, each entry (branch-parent id, child depth). nil outside
	// cgprb mode. Used to materialize dropped branch points and attach their
	// direct children losslessly before bloom-threading.
	HA []HAEntry
}

// HAEntry is one decoded CGPRB hash-array record: a branching parent's exact
// span id and the depth of its children (= parent depth + 1). The parent may
// or may not have survived; the branch point exists either way.
type HAEntry struct {
	ParentID uint64
	Depth    int
}

// Config is the global knowledge a real reconstruction module would have:
// the bridge deployment configuration. Bloom geometry derives from CPD and
// the false-positive rate exactly as the SDK computes it.
type Config struct {
	CPD    int
	BloomM uint32
	BloomK uint32

	// PrefixLen is the truncated checkpoint-root ID length for PCR mode
	// (unused by PB).
	PrefixLen int

	// StopOnGap (PCRB threading): stop the level walk at the first level
	// with zero candidates instead of continuing past it. Maximally
	// FP-averse: refuses to stitch across unnameable dropped levels, at the
	// cost of benign skips wherever the surviving chain is interrupted.
	StopOnGap bool

	// TiePolicy (PCRS thread items) selects how an exact posterior tie
	// between threading candidates is resolved:
	//   "stop"  — abstain (synthetic level; benign-shallow attachment)
	//   "id"    — force deterministically by span ID
	//   "aware" — force, preferring the candidate whose bit is not already
	//             explained by another chain (global bit-accounting), span
	//             ID as final fallback. Default.
	TiePolicy string

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

	// ViaCarrier is the span whose payload was used in place of the orphan's
	// own. The semantics differ by mode:
	//   PB:  a bloom reached through reconstructed structure or the
	//        membership scan — a verified GUESS (own bloom and real-edge
	//        in-fragment carriers leave this 0).
	//   PCR: any in-fragment descendant carrier, reached through real
	//        surviving edges only, band-bounded — exact inheritance, never
	//        a guess (PCR cannot cross bridges or scan: it has no
	//        membership test). Tracked for visibility, not as an error
	//        channel.
	ViaCarrier uint64

	// Forced marks an open-end attachment made by the forced-resolution
	// tier: the sound fixpoint left the open end unsatisfied, and total
	// coverage is an invariant, so the most-plausible claimant (fewest
	// deeper candidates) was committed deterministically. Flagged because
	// the evidence did not single it out.
	Forced bool

	// ReconFanout is the CGPRB-recovered identity of this fragment's IMMEDIATE
	// reconstructed parent (the node at orphan.Depth-1): the true span id of the
	// dropped fan-out it was coalesced under, or 0 if its direct parent is a
	// surviving span or an un-recovered anonymous synthetic. Two fragments with
	// the same non-zero ReconFanout were reconstructed as siblings under that
	// recovered fan-out. Set only in cgprb mode; 0 everywhere else (P-Bridge
	// gives each a private synthetic parent — never reconstructed siblings).
	ReconFanout uint64
}

// Result is the outcome of reconstructing one trace.
type Result struct {
	Orphans     int
	Reconnected int
	Bridges     []Bridge
	Unanchored  []uint64 // orphan IDs with no positive bloom candidate

	// Open-end census (PCRB): surviving non-carrier spans with zero
	// surviving children (provably lost ALL children), and how many of
	// them received >=1 bridge. See Score.OpenEnds.
	OpenEnds        int
	OpenEndsMatched int
	ForcedMatches   int // open-end attachments committed by the forced tier

	// Orphan placement (v7): orphan fragments fitted into synthetic slots
	// using descendant-fragment bloom testimony, exact depths, and spacing
	// constraints. OrphanOpenEnds counts open ends INSIDE placed orphans —
	// coverage for those requires orphans as threading material (v7.1) and
	// is reported separately from the main census.
	OrphansPlaced  int
	OrphanOpenEnds int
}

// ReconstructPB reattaches orphaned spans using P-Bridge payloads. This is
// reconnect_orphans [alg. 1-11], the top-level driver.
//
// For each orphan (surviving span whose parent is not in the surviving set):
//
//  1. Locate the covering bloom [alg. 5, coveringBloom]: the orphan's own
//     _br if present, else the nearest _br-carrying descendant within the
//     orphan's surviving fragment. Blooms reset at checkpoints, so the
//     nearest carrier's window is guaranteed to span the hole above the
//     orphan (any intermediate checkpoint would itself be a nearer carrier).
//  2. Test surviving spans at depths [lastCkptDepth, orphan.depth-2] against
//     the bloom [alg. 6, findAnchor]; the deepest positive is the anchor.
//     The window lower bound is config-derivable: the last protected
//     checkpoint strictly above the orphan, at depth cpd*floor((d-1)/cpd).
//     The upper bound excludes depth d-1 — the orphan's parent at d-1 is
//     dropped by definition, so that level holds only its siblings, which
//     pass the chain check structurally and would steal the anchor on a
//     single membership FP.
//  3. The depth difference fixes the synthetic chain length exactly
//     [alg. 7-8]: orphan.depth - anchor.depth - 1. This is what EmitDepth
//     mode's per-span absolute depth pays for — the hole size is computed,
//     never inferred.
//
// Per the paper (§3.4), P-Bridge does NOT merge synthetic parents: siblings
// of a lost parent each get their own synthetic chain.
func ReconstructPB(survivors []Span, cfg Config) Result {
	// --- Index building (implementation detail; the algorithm box treats
	// these as set membership tests over S) ---

	// byID: is this span in the surviving set? Used for orphan detection
	// [alg. 2] and for naming surviving ancestors in the chain check.
	byID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		byID[survivors[i].SpanID] = &survivors[i]
	}
	// children: the real surviving edges — the fragments. coveringBloom
	// walks these downward [alg. 15].
	children := make(map[uint64][]*Span, len(survivors))
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 {
			if _, ok := byID[s.ParentID]; ok {
				children[s.ParentID] = append(children[s.ParentID], s)
			}
		}
	}
	// byDepth: anchor-candidate index. findAnchor scans one depth level at a
	// time, deepest first [alg. 25-26].
	byDepth := make(map[int][]*Span)
	for i := range survivors {
		s := &survivors[i]
		byDepth[s.Depth] = append(byDepth[s.Depth], s)
	}
	// carriers: every _br-carrying span, sorted shallowest-first for the
	// membership-scan fallback [alg. 19-21] (shallowest claimant = nearest
	// descendant by depth = safest borrow, and the sort allows early exit at
	// the band edge).
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

		// Window: [last checkpoint strictly above orphan, orphan.depth-2].
		// Depth o.Depth-1 is excluded: the orphan's depth-(k-1) ancestor is
		// by definition its dropped parent, so no span at that depth can be
		// the true anchor — while the dropped parent's surviving siblings
		// live there and pass the chain check for free (their chain above
		// k-2 is the orphan's own ancestry). Scanning k-1 is pure
		// misattachment surface. The floor never collides: under the v1
		// drop policy a checkpoint-depth parent cannot drop, so for any
		// orphan (k-1) % cpd != 0 and lo <= k-2.
		lo := ((o.Depth - 1) / cfg.CPD) * cfg.CPD
		anchor, ambiguous := findAnchor(bf, byDepth, byID, lo, o.Depth-2, cfg)
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
	// AnchorAncestor counts bridges whose anchor is a TRUE ancestor of the
	// orphan, even if not the nearest surviving one. For PB the two
	// essentially coincide (the deepest-first scan cannot pass the nearest
	// survivor by). For PCR — which structurally anchors at the checkpoint —
	// this is the operative correctness metric: AnchorAncestor failures are
	// genuinely wrong attachments (prefix collisions), while
	// AnchorCorrect-but-not-nearest gaps measure structural coarseness.
	AnchorAncestor int
	GapCorrect     int // synthetic count == true dropped spans in between
	Misattached    int // reconnected but anchor wrong (bloom FP / ambiguity)
	Unanchored     int
	Synthetic      int // total synthetic spans created
	Borrowed       int // bridges built via the membership-based bloom fallback
	Ambiguous      int // bridges where >1 candidate tested positive at the anchor depth
	AmbiguousBad   int // ... of which the anchor was wrong

	// PCR fragment accounting (ScorePCR only; zero under ScorePB). A
	// carrier-less fragment — an unanchored orphan plus every survivor
	// connected below it — cannot be placed and is recorded as thrown away.
	FragmentsLost int // unanchored fragments discarded
	SpansLost     int // surviving spans inside those fragments (incl. the roots)

	// Open-end accounting (ScorePCR only). An open end is a surviving
	// non-carrier span with zero surviving children: non-carriers are
	// interior by construction (leaves always carry _br), so all its
	// children were dropped — and because true leaves and checkpoints
	// always survive, at least one fragment provably belongs below it.
	// Unmatched open ends are a completeness deficit, not an error.
	OpenEnds        int // provably-dangling chain ends
	OpenEndsMatched int // ... that received >=1 bridge attachment
	ForcedMatches   int // ... matched by forced resolution (flagged, not evidence-unique)
	OrphansPlaced   int // orphan fragments fitted into synthetic slots (v7)
	OrphanOpenEnds  int // open ends inside placed orphans, coverage pending v7.1

	// Window-skip impact, span-weighted (ScorePCR only). A bridge "skips"
	// when >=1 surviving same-window ancestor sits strictly between the
	// anchor and the orphan (Misattached counts those bridges; no spans are
	// lost — only the intra-window nesting between co-window fragments).
	AncestorsSkipped int // total surviving ancestors skipped, summed over bridges
	SpansInSkipped   int // surviving spans in fragments whose bridge skipped
}

// ScorePB computes per-trace reconstruction accuracy. truth holds every span
// of the original trace; dropped is the set of span IDs removed by the drop
// policy.
func ScorePB(res Result, truth []TruthSpan, dropped map[uint64]struct{}) Score {
	return scorePB(res, truth, dropped, nil)
}

// scorePB is ScorePB with an optional `absent` set: spans that physically
// survived but are NOT part of the reconstruction (discarded fragments under
// the PCR fragment model). For nearest-surviving-ancestor and gap purposes
// they count like dropped spans — attaching past one is correct in the
// reduced survivor set, and its window slot is legitimately synthetic.
func scorePB(res Result, truth []TruthSpan, dropped map[uint64]struct{}, absent map[uint64]bool) Score {
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
		// past dropped spans. The same walk (continued) answers whether the
		// chosen anchor is a true ancestor at all.
		trueAnchor := uint64(0)
		gap := 0
		isAncestor := false
		for p := parent[b.OrphanID]; p != 0; p = parent[p] {
			if p == b.AnchorID {
				isAncestor = true
			}
			if trueAnchor == 0 {
				_, isDropped := dropped[p]
				if !isDropped && !absent[p] {
					trueAnchor = p
				} else {
					gap++
				}
			} else if isAncestor {
				break // both questions answered
			}
		}
		if isAncestor {
			sc.AnchorAncestor++
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
