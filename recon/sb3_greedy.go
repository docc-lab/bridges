package recon

import (
	"fmt"
	mathbits "math/bits"
	"os"
	"sort"
	"time"

	"bridges/bloom"
	"bridges/bridge"
)

// sb3GreedyStats reports how much work the dedicated SB3 topology search did.
// An override is a final choice that differs from the ordinary deepest-first,
// HA-preferring greedy choice because sparse-ordinal evidence made another
// topology strictly more compatible.
type sb3GreedyStats struct {
	CandidateEvaluations int
	OrdinalOverrides     int
	HardOverrides        int
	HardConflicts        int
	ParentConflicts      int
	HAConflicts          int
	Chain                GreedyChainStats
	Fanout               GreedyFanoutStats
}

// sb3WinBloom is one surviving carrier's window-local ancestry evidence.
// carrier keeps the record identity so evidence pooled through multiple hard
// facts is deduplicated rather than accidentally counted twice.
type sb3WinBloom struct {
	carrier uint64
	depth   int
	bf      *bloom.Filter
}

// sb3HAWitness is a hard fact carried by a surviving record: carrier is a
// descendant of fanoutID, whose absolute depth is depth. Unlike a Bloom hit,
// this relationship is not probabilistic.
type sb3HAWitness struct {
	fanoutID uint64
	depth    int
	carrier  uint64
	bloom    sb3WinBloom
}

// sb3FanoutEvidenceGroup pools carrier Blooms whose records prove that they
// pass through the same fanout. A candidate at or above depth is a common
// ancestor and must therefore appear in every applicable Bloom in the group.
type sb3FanoutEvidenceGroup struct {
	id      uint64
	depth   int
	blooms  []sb3WinBloom
	seen    map[uint64]bool
	witness bool
}

type sb3BloomIndexEntry struct {
	id   uint64
	span *Span
	mask uint64
}

// sb3BloomIDIndex is an exact inverted accelerator for small Bloom geometries.
// CPD=4/FPR=1e-4 uses 58 bits. Each possible candidate ID is represented by the
// mask of probes its lookup tests and bucketed by the low 16 mask bits. A query
// visits only buckets whose key is a subset of the carrier bitmap, then checks
// the complete mask. Wider geometries retain the exact scan fallback.
type sb3BloomIDIndex struct {
	cfg     Config
	fast    bool
	all     []sb3BloomIndexEntry
	buckets [4]map[uint16][]sb3BloomIndexEntry
}

func newSB3BloomIDIndex(cfg Config) *sb3BloomIDIndex {
	idx := &sb3BloomIDIndex{cfg: cfg, fast: cfg.BloomM <= 64}
	for i := range idx.buckets {
		idx.buckets[i] = make(map[uint16][]sb3BloomIndexEntry)
	}
	return idx
}

func (idx *sb3BloomIDIndex) add(id uint64, span *Span) {
	e := sb3BloomIndexEntry{id: id, span: span}
	if idx.fast {
		key := bridge.HexOf(id)
		mask, ok := bloom.ProbeMask64(key[:], idx.cfg.BloomM, idx.cfg.BloomK, idx.cfg.Prehashed)
		if !ok {
			idx.fast = false
		} else {
			e.mask = mask
			for band := range idx.buckets {
				part := uint16(mask >> (16 * band))
				idx.buckets[band][part] = append(idx.buckets[band][part], e)
			}
		}
	}
	idx.all = append(idx.all, e)
}

func (idx *sb3BloomIDIndex) query(bf *bloom.Filter) []sb3BloomIndexEntry {
	if idx == nil || bf == nil {
		return nil
	}
	out := make([]sb3BloomIndexEntry, 0, 4)
	if idx.fast {
		filterMask, ok := bf.BitMask64()
		if ok {
			bestBand, bestPop := 0, 17
			for band := 0; band < 4; band++ {
				valid := int(idx.cfg.BloomM) - 16*band
				if valid <= 0 {
					continue
				}
				if valid > 16 {
					valid = 16
				}
				part := uint16(filterMask >> (16 * band))
				pop := mathbits.OnesCount16(part)
				// Prefer the band with the most zero positions (strongest
				// subset filter), then the fewest enumerated submasks.
				zeros, bestZeros := valid-pop, -1
				if bestPop <= 16 {
					bestValid := int(idx.cfg.BloomM) - 16*bestBand
					if bestValid > 16 {
						bestValid = 16
					}
					bestZeros = bestValid - bestPop
				}
				if zeros > bestZeros || (zeros == bestZeros && pop < bestPop) {
					bestBand, bestPop = band, pop
				}
			}
			bandBits := uint16(filterMask >> (16 * bestBand))
			for sub := bandBits; ; sub = (sub - 1) & bandBits {
				for _, e := range idx.buckets[bestBand][sub] {
					if e.mask&^filterMask == 0 {
						out = append(out, e)
					}
				}
				if sub == 0 {
					break
				}
			}
			sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
			return out
		}
	}
	for _, e := range idx.all {
		key := bridge.HexOf(e.id)
		if bf.Test(key[:]) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// sb3FragmentEvidence is the complete, non-solver candidate set for one
// surviving fragment. Unlike cgp0, it does not stop at the first Bloom match.
// Unlike cgp1/cgp2, it is consumed only by the dedicated SB3 greedy search.
type sb3FragmentEvidence struct {
	frag        *cgpFragment
	anchors     []*Span
	ckpt        *Span
	checkpoints []*Span
	fanouts     map[int][]uint64
	blooms      []sb3WinBloom
	haWitnesses []sb3HAWitness
	resolved    bool
}

// sb3RouteUnit is one exact dropped-parent group. Every member fragment names
// parentID literally in its surviving span record, so the member roots are
// certain siblings. Their Bloom evidence is intersected before any guess is
// made about the path above that parent.
type sb3RouteUnit struct {
	parentID    uint64
	depth       int
	members     []*sb3FragmentEvidence
	knownFanout bool

	anchors []*Span // deepest first; all are confirmed by every resolved sibling
	anchor  *Span

	// At each intermediate depth, choose either one confirmed named HA fanout
	// or zero (a private anonymous gap node). choices are deterministic by ID.
	fanoutsByDepth map[int][]uint64
	requiredFanout map[int]uint64
	nodeChoice     map[int]uint64
	anonAtDepth    map[int]uint64

	applicableFanoutGroups      int
	applicableMultiBloomGroups  int
	fanoutCandidateTests        int
	fanoutBloomTests            int
	fanoutCandidatesPruned      int
	hardRouteCandidatesRejected int
}

type sb3ChainMatch struct {
	ok                  bool
	matchedLevels       int
	positiveBloomChecks int
	carriers            map[uint64]bool
}

// sb3ConfirmAnchorChain is the measured form of the anchor-chain predicate.
// It walks from a surviving candidate toward the exact checkpoint and tests
// every nameable non-checkpoint ID against every applicable carrier Bloom.
// A missing parent ends the walk after its literal ID is tested because no
// surviving record exposes the rest of that parent chain.
func sb3ConfirmAnchorChain(sk *cgpSkeleton, e *sb3FragmentEvidence, start *Span) sb3ChainMatch {
	m := sb3ChainMatch{carriers: make(map[uint64]bool)}
	if e == nil || e.frag == nil || e.frag.anchorCkpt == nil || start == nil {
		return m
	}
	lo := e.frag.anchorCkpt.Depth
	checkpointIDs := make(map[uint64]bool, len(e.checkpoints))
	for _, ckpt := range e.checkpoints {
		checkpointIDs[ckpt.SpanID] = true
	}
	confirmLevel := func(id uint64, depth int) bool {
		key := bridge.HexOf(id)
		applicable := 0
		for _, wb := range e.blooms {
			if wb.depth <= depth {
				continue
			}
			applicable++
			if !wb.bf.Test(key[:]) {
				return false
			}
			m.positiveBloomChecks++
			m.carriers[wb.carrier] = true
		}
		if applicable > 0 {
			m.matchedLevels++
		}
		return true
	}

	seen := make(map[uint64]bool)
	for cur := start; cur != nil && !seen[cur.SpanID]; {
		seen[cur.SpanID] = true
		if cur.Depth <= lo {
			m.ok = cur.Depth == lo && checkpointIDs[cur.SpanID]
			return m
		}
		if !confirmLevel(cur.SpanID, cur.Depth) {
			return m
		}
		parentDepth := cur.Depth - 1
		if parentDepth == lo {
			m.ok = checkpointIDs[cur.ParentID]
			return m
		}
		parent := sk.byID[cur.ParentID]
		if parent == nil {
			if cur.ParentID != 0 && confirmLevel(cur.ParentID, parentDepth) {
				m.ok = true
			}
			return m
		}
		if parent.Depth != parentDepth {
			return m
		}
		cur = parent
	}
	return m
}

// sb3CollectFragmentEvidence copies the useful candidate-generation rules from
// the CGP family but deliberately owns the search space here. It enumerates all
// admissible surviving ancestors and HA fanouts in the fragment's checkpoint
// window. Sparse ordinals will decide among these candidates later.
func sb3CollectFragmentEvidence(sk *cgpSkeleton, cfg Config) map[uint64]*sb3FragmentEvidence {
	return sb3CollectFragmentEvidenceWithStats(sk, cfg, nil)
}

func sb3CollectFragmentEvidenceWithStats(sk *cgpSkeleton, cfg Config, chain *GreedyChainStats) map[uint64]*sb3FragmentEvidence {
	cpd := cfg.CPD
	if cpd < 1 {
		cpd = 1
	}
	haIndex := make(map[int]*sb3BloomIDIndex)
	for id, fo := range sk.fanouts {
		idx := haIndex[fo.depth]
		if idx == nil {
			idx = newSB3BloomIDIndex(cfg)
			haIndex[fo.depth] = idx
		}
		idx.add(id, nil)
	}
	survIndex := make(map[int]*sb3BloomIDIndex)
	ckptByDepthPrefix := make(map[int]map[string][]*Span)
	for _, s := range sk.byID {
		if !s.LeafCarrier {
			idx := survIndex[s.Depth]
			if idx == nil {
				idx = newSB3BloomIDIndex(cfg)
				survIndex[s.Depth] = idx
			}
			idx.add(s.SpanID, s)
		}
		if s.Depth%cpd == 0 {
			id := bridge.BigEndian8(s.SpanID)
			n := cfg.PrefixLen
			if n > len(id) {
				n = len(id)
			}
			byPrefix := ckptByDepthPrefix[s.Depth]
			if byPrefix == nil {
				byPrefix = make(map[string][]*Span)
				ckptByDepthPrefix[s.Depth] = byPrefix
			}
			byPrefix[string(id[:n])] = append(byPrefix[string(id[:n])], s)
		}
	}
	wtop := func(d int) int {
		if d%cpd == 0 {
			return d - cpd
		}
		return (d / cpd) * cpd
	}

	out := make(map[uint64]*sb3FragmentEvidence, len(sk.frags))
	for _, f := range sk.frags {
		e := &sb3FragmentEvidence{frag: f, ckpt: f.anchorCkpt, fanouts: make(map[int][]uint64)}
		out[f.root.SpanID] = e
		if f.bf == nil || f.anchorCkpt == nil || f.carrier == nil {
			continue
		}
		e.resolved = true
		lo, hi := f.anchorCkpt.Depth, f.root.Depth
		prefixLen := cfg.PrefixLen
		if prefixLen > len(f.prefix) {
			prefixLen = len(f.prefix)
		}
		if prefixLen > 0 {
			e.checkpoints = append(e.checkpoints, ckptByDepthPrefix[lo][string(f.prefix[:prefixLen])]...)
		}
		if len(e.checkpoints) == 0 {
			e.checkpoints = append(e.checkpoints, f.anchorCkpt)
		}
		for _, s := range f.spans {
			if s.BloomBits != nil && wtop(s.Depth) == lo {
				e.blooms = append(e.blooms, sb3WinBloom{
					carrier: s.SpanID, depth: s.Depth, bf: cgpBloom(s.BloomBits, cfg),
				})
			}
		}
		if len(e.blooms) == 0 {
			e.blooms = append(e.blooms, sb3WinBloom{
				carrier: f.carrier.SpanID, depth: f.carrier.Depth, bf: f.bf,
			})
		}
		bloomByCarrier := make(map[uint64]sb3WinBloom, len(e.blooms))
		for _, wb := range e.blooms {
			bloomByCarrier[wb.carrier] = wb
		}
		if !cfg.NoFanout {
			for _, s := range f.spans {
				wb, carriesBloom := bloomByCarrier[s.SpanID]
				if !carriesBloom {
					continue
				}
				for _, h := range s.HA {
					d := h.Depth - 1
					if d < lo || d >= s.Depth {
						continue // not an ancestor in this carrier's active window
					}
					e.haWitnesses = append(e.haWitnesses, sb3HAWitness{
						fanoutID: h.ParentID, depth: d, carrier: s.SpanID, bloom: wb,
					})
				}
			}
		}
		confirmedByAll := func(key []byte, depth int) bool {
			for _, wb := range e.blooms {
				if wb.depth > depth && !wb.bf.Test(key) {
					return false
				}
			}
			return true
		}
		queryBloom := e.blooms[0].bf
		for _, wb := range e.blooms[1:] {
			if wb.bf.PopCount() < queryBloom.PopCount() {
				queryBloom = wb.bf
			}
		}
		// root.Depth-1 is the exact named parent, not a routing choice.
		for d := hi - 2; d > lo; d-- {
			for _, candidate := range haIndex[d].query(queryBloom) {
				key := bridge.HexOf(candidate.id)
				if confirmedByAll(key[:], d) {
					e.fanouts[d] = append(e.fanouts[d], candidate.id)
				}
			}
		}
		for d := hi - 2; d > lo; d-- {
			for _, candidate := range survIndex[d].query(queryBloom) {
				if chain != nil {
					chain.CandidateInitialHits++
				}
				match := sb3ConfirmAnchorChain(sk, e, candidate.span)
				if chain != nil {
					chain.CandidatePositiveBloomChecks += match.positiveBloomChecks
					if match.ok {
						chain.CandidateAccepted++
						if chain.AcceptedByMatchedLevels == nil {
							chain.AcceptedByMatchedLevels = make(map[int]int)
						}
						chain.AcceptedByMatchedLevels[match.matchedLevels]++
					} else {
						chain.CandidateRejected++
						if chain.RejectedAfterMatchedLevels == nil {
							chain.RejectedAfterMatchedLevels = make(map[int]int)
						}
						chain.RejectedAfterMatchedLevels[match.matchedLevels]++
					}
				}
				if !match.ok {
					continue
				}
				e.anchors = append(e.anchors, candidate.span)
			}
		}
	}
	return out
}

func sb3IntersectRouteUnits(sk *cgpSkeleton, evidence map[uint64]*sb3FragmentEvidence) []*sb3RouteUnit {
	return sb3IntersectRouteUnitsWithConfig(sk, evidence, Config{})
}

func sb3IntersectRouteUnitsWithConfig(sk *cgpSkeleton, evidence map[uint64]*sb3FragmentEvidence, cfg Config) []*sb3RouteUnit {
	return sb3IntersectRouteUnitsWithStats(sk, evidence, cfg, nil)
}

func sb3IntersectRouteUnitsWithStats(sk *cgpSkeleton, evidence map[uint64]*sb3FragmentEvidence, cfg Config, fanout *GreedyFanoutStats) []*sb3RouteUnit {
	byParent := make(map[uint64][]*sb3FragmentEvidence)
	for _, f := range sk.frags {
		if f.root.ParentID == 0 {
			continue
		}
		if _, survived := sk.byID[f.root.ParentID]; survived {
			continue
		}
		byParent[f.root.ParentID] = append(byParent[f.root.ParentID], evidence[f.root.SpanID])
	}

	units := make([]*sb3RouteUnit, 0, len(byParent))
	for parentID, members := range byParent {
		sort.Slice(members, func(i, j int) bool {
			return members[i].frag.root.SpanID < members[j].frag.root.SpanID
		})
		u := &sb3RouteUnit{
			parentID:       parentID,
			depth:          members[0].frag.root.Depth - 1,
			members:        members,
			fanoutsByDepth: make(map[int][]uint64),
			requiredFanout: make(map[int]uint64),
			nodeChoice:     make(map[int]uint64),
			anonAtDepth:    make(map[int]uint64),
		}
		resolved := 0
		anchorCount := make(map[uint64]int)
		anchorByID := make(map[uint64]*Span)
		fanoutCount := make(map[uint64]int)
		fanoutDepth := make(map[uint64]int)
		representativeChosen := false
		for _, e := range members {
			if e == nil || !e.resolved {
				continue
			}
			// The grouped-evidence ablation deliberately lets one deterministic
			// member nominate candidates for the shared route. The full engine
			// intersects every exact sibling's evidence, as all of them must pass
			// through the same named parent and upstream ancestry.
			useForCandidates := !cfg.GreedyNoGroupedEvidence || !representativeChosen
			if useForCandidates {
				representativeChosen = true
				resolved++
				seenAnchor := make(map[uint64]bool)
				for _, a := range e.anchors {
					if !seenAnchor[a.SpanID] {
						seenAnchor[a.SpanID] = true
						anchorCount[a.SpanID]++
						anchorByID[a.SpanID] = a
					}
				}
				for _, ckpt := range e.checkpoints {
					if !seenAnchor[ckpt.SpanID] {
						seenAnchor[ckpt.SpanID] = true
						anchorCount[ckpt.SpanID]++
						anchorByID[ckpt.SpanID] = ckpt
					}
				}
				seenFanout := make(map[uint64]bool)
				for depth, ids := range e.fanouts {
					for _, id := range ids {
						if !seenFanout[id] {
							seenFanout[id] = true
							fanoutCount[id]++
							fanoutDepth[id] = depth
						}
					}
				}
			}
			if !cfg.GreedyNoHardHA {
				for _, w := range e.haWitnesses {
					if w.depth >= u.depth {
						continue // inside the surviving fragment, not on this unit's gap
					}
					if prev := u.requiredFanout[w.depth]; prev == 0 || w.fanoutID < prev {
						u.requiredFanout[w.depth] = w.fanoutID
					}
				}
			}
		}
		if resolved > 0 {
			for id, count := range anchorCount {
				if count == resolved && anchorByID[id].Depth < u.depth {
					u.anchors = append(u.anchors, anchorByID[id])
				}
			}
			for id, count := range fanoutCount {
				if count == resolved {
					d := fanoutDepth[id]
					if d < u.depth {
						u.fanoutsByDepth[d] = append(u.fanoutsByDepth[d], id)
					}
				}
			}
		}
		sort.Slice(u.anchors, func(i, j int) bool {
			if u.anchors[i].Depth != u.anchors[j].Depth {
				return u.anchors[i].Depth > u.anchors[j].Depth
			}
			return u.anchors[i].SpanID < u.anchors[j].SpanID
		})
		for depth := range u.fanoutsByDepth {
			sort.Slice(u.fanoutsByDepth[depth], func(i, j int) bool {
				return u.fanoutsByDepth[depth][i] < u.fanoutsByDepth[depth][j]
			})
		}
		units = append(units, u)
	}
	sb3ApplyFreeFanoutEvidence(sk, units, !cfg.GreedyNoGroupedEvidence, fanout)
	sort.Slice(units, func(i, j int) bool {
		hi, hj := len(units[i].requiredFanout) > 0, len(units[j].requiredFanout) > 0
		if hi != hj {
			return hi // establish explicit HA routes before optional Bloom joins
		}
		if units[i].depth != units[j].depth {
			return units[i].depth < units[j].depth
		}
		return units[i].parentID < units[j].parentID
	})
	return units
}

func sb3AddGroupBloom(g *sb3FanoutEvidenceGroup, wb sb3WinBloom) {
	if g.seen == nil {
		g.seen = make(map[uint64]bool)
	}
	if g.seen[wb.carrier] {
		return
	}
	g.seen[wb.carrier] = true
	g.blooms = append(g.blooms, wb)
}

func sb3AppendUniqueID(ids []uint64, id uint64) []uint64 {
	for _, have := range ids {
		if have == id {
			return ids
		}
	}
	return append(ids, id)
}

// sb3ApplyFreeFanoutEvidence makes fanout points the unit of Bloom
// corroboration. Groups arise from two non-probabilistic facts in surviving
// records: fragment roots that literally name the same ParentID, and carriers
// whose HA entry explicitly says they descend through a fanout. A candidate at
// or above such a point must survive every applicable branch Bloom.
func sb3ApplyFreeFanoutEvidence(sk *cgpSkeleton, units []*sb3RouteUnit, useGroupedEvidence bool, stats *GreedyFanoutStats) {
	groups := make(map[uint64]*sb3FanoutEvidenceGroup)
	group := func(id uint64, depth int) *sb3FanoutEvidenceGroup {
		g := groups[id]
		if g == nil {
			g = &sb3FanoutEvidenceGroup{id: id, depth: depth}
			groups[id] = g
		}
		return g
	}

	// Literal ParentID groups: every member Bloom is below the exact parent.
	for _, u := range units {
		g := group(u.parentID, u.depth)
		if len(u.members) >= 2 {
			g.witness = true // matching parent IDs prove a surviving fanout group
		}
		for _, e := range u.members {
			for _, wb := range e.blooms {
				sb3AddGroupBloom(g, wb)
			}
		}
	}
	// HA groups can span several exact-parent route units. Only the carrier that
	// actually carries the HA record is pooled; other Blooms in its connected
	// fragment need not lie below that particular fanout.
	for _, u := range units {
		for _, e := range u.members {
			for _, w := range e.haWitnesses {
				g := group(w.fanoutID, w.depth)
				g.witness = true
				sb3AddGroupBloom(g, w.bloom)
			}
		}
	}
	if stats != nil {
		stats.EvidenceGroups += len(groups)
		for _, g := range groups {
			if g.witness {
				stats.WitnessedFanoutGroups++
			}
			if len(g.blooms) >= 2 {
				stats.MultiBloomEvidenceGroups++
			}
		}
	}

	for _, u := range units {
		applicable := make(map[uint64]*sb3FanoutEvidenceGroup)
		if g := groups[u.parentID]; g != nil {
			if useGroupedEvidence {
				applicable[g.id] = g
			}
			u.knownFanout = g.witness
		}
		for d, id := range u.requiredFanout {
			g := groups[id]
			if useGroupedEvidence && g != nil {
				applicable[id] = g
			}
			if _, survived := sk.byID[id]; survived {
				u.anchors = append(u.anchors, sk.byID[id])
			} else {
				u.fanoutsByDepth[d] = sb3AppendUniqueID(u.fanoutsByDepth[d], id)
			}
		}
		for _, g := range applicable {
			if !g.witness {
				continue
			}
			u.applicableFanoutGroups++
			if len(g.blooms) >= 2 {
				u.applicableMultiBloomGroups++
			}
		}
		confirms := func(g *sb3FanoutEvidenceGroup, id uint64, depth int) bool {
			if g.witness {
				u.fanoutCandidateTests++
			}
			key := bridge.HexOf(id)
			for _, wb := range g.blooms {
				if wb.depth > depth {
					if g.witness {
						u.fanoutBloomTests++
					}
					if !wb.bf.Test(key[:]) {
						return false
					}
				}
			}
			return true
		}

		anchorOut := make([]*Span, 0, len(u.anchors))
		for _, a := range u.anchors {
			checkpointCeiling := false
			for _, e := range u.members {
				for _, ckpt := range e.checkpoints {
					if ckpt.SpanID == a.SpanID {
						checkpointCeiling = true
						break
					}
				}
				if checkpointCeiling {
					break
				}
			}
			ok := true
			prunedByFanout := false
			for _, g := range applicable {
				if !checkpointCeiling && a.Depth <= g.depth && !confirms(g, a.SpanID, a.Depth) {
					// An explicit surviving HA point is hard evidence and need not
					// pass its own probabilistic encoding.
					if a.SpanID != g.id {
						prunedByFanout = g.witness
						ok = false
						break
					}
				}
			}
			if ok {
				duplicate := false
				for _, have := range anchorOut {
					if have.SpanID == a.SpanID {
						duplicate = true
						break
					}
				}
				if !duplicate {
					anchorOut = append(anchorOut, a)
				}
			} else if stats != nil {
				stats.GroupedAnchorCandidatesPruned++
				if prunedByFanout {
					u.fanoutCandidatesPruned++
				}
			}
		}
		u.anchors = anchorOut

		for d, ids := range u.fanoutsByDepth {
			out := make([]uint64, 0, len(ids))
			for _, id := range ids {
				hard := u.requiredFanout[d] == id
				ok := hard
				prunedByFanout := false
				if !hard {
					ok = true
					for _, g := range applicable {
						if d <= g.depth && !confirms(g, id, d) {
							prunedByFanout = g.witness
							ok = false
							break
						}
					}
				}
				if ok {
					out = sb3AppendUniqueID(out, id)
				} else if stats != nil {
					stats.GroupedFanoutCandidatesPruned++
					if prunedByFanout {
						u.fanoutCandidatesPruned++
					}
				}
			}
			u.fanoutsByDepth[d] = out
		}

		sort.Slice(u.anchors, func(i, j int) bool {
			if u.anchors[i].Depth != u.anchors[j].Depth {
				return u.anchors[i].Depth > u.anchors[j].Depth
			}
			return u.anchors[i].SpanID < u.anchors[j].SpanID
		})
		for d, ids := range u.fanoutsByDepth {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			if required := u.requiredFanout[d]; required != 0 {
				if _, survived := sk.byID[required]; !survived {
					u.nodeChoice[d] = required
				}
			} else if len(ids) > 0 {
				u.nodeChoice[d] = ids[0]
			}
		}
		if len(u.anchors) > 0 {
			u.anchor = u.anchors[0]
		}
	}
}

// sb3AssignAnonymousIDs gives every route-unit/depth pair a stable private ID.
// Candidate trials therefore differ only in topology choices, not allocator
// order, which keeps ordinal conflict comparisons deterministic.
func sb3AssignAnonymousIDs(sk *cgpSkeleton, units []*sb3RouteUnit) {
	used := make(map[uint64]bool, len(sk.byID)+len(sk.fanouts)+len(units))
	for id := range sk.byID {
		used[id] = true
	}
	for id := range sk.fanouts {
		used[id] = true
	}
	for _, u := range units {
		used[u.parentID] = true
	}
	next := uint64(1)
	alloc := func() uint64 {
		for used[next] {
			next++
		}
		id := next
		used[id] = true
		next++
		return id
	}
	for _, u := range units {
		if len(u.anchors) == 0 {
			continue
		}
		minDepth := u.anchors[len(u.anchors)-1].Depth
		for d := minDepth + 1; d < u.depth; d++ {
			u.anonAtDepth[d] = alloc()
		}
	}
}

func sb3CollectSelectedRouteEvidence(sk *cgpSkeleton, units []*sb3RouteUnit) []GreedyRouteEvidence {
	out := make([]GreedyRouteEvidence, 0, len(units))
	for _, u := range units {
		r := GreedyRouteEvidence{
			ParentID: u.parentID, Routed: u.anchor != nil,
			RequiredHAFanouts:           len(u.requiredFanout),
			ApplicableFanoutGroups:      u.applicableFanoutGroups,
			ApplicableMultiBloomGroups:  u.applicableMultiBloomGroups,
			FanoutCandidateTests:        u.fanoutCandidateTests,
			FanoutBloomTests:            u.fanoutBloomTests,
			FanoutCandidatesPruned:      u.fanoutCandidatesPruned,
			HardRouteCandidatesRejected: u.hardRouteCandidatesRejected,
		}
		for _, e := range u.members {
			if e != nil && e.frag != nil && e.frag.root != nil {
				r.OrphanIDs = append(r.OrphanIDs, e.frag.root.SpanID)
			}
		}
		if u.anchor == nil {
			out = append(out, r)
			continue
		}
		r.AnchorID = u.anchor.SpanID
		r.AnchorDepth = u.anchor.Depth
		minLevels := -1
		haveCheckpointDepth := false
		carriers := make(map[uint64]bool)
		for _, e := range u.members {
			if e == nil || !e.resolved || e.frag == nil || e.frag.anchorCkpt == nil {
				continue
			}
			if !haveCheckpointDepth || e.frag.anchorCkpt.Depth < r.CheckpointDepth {
				r.CheckpointDepth = e.frag.anchorCkpt.Depth
				haveCheckpointDepth = true
			}
			match := sb3ConfirmAnchorChain(sk, e, u.anchor)
			if !match.ok {
				// This should be unreachable: u.anchors is the intersection of
				// member-admissible anchors.  Record the weakest possible value
				// rather than overstating support if an invariant is violated.
				minLevels = 0
				continue
			}
			if minLevels < 0 || match.matchedLevels < minLevels {
				minLevels = match.matchedLevels
			}
			r.PositiveBloomChecks += match.positiveBloomChecks
			for carrier := range match.carriers {
				carriers[carrier] = true
			}
		}
		if minLevels > 0 {
			r.MatchedLevels = minLevels
		}
		r.SupportingCarriers = len(carriers)
		out = append(out, r)
	}
	return out
}

func sb3EmitGreedyTopology(sk *cgpSkeleton, units []*sb3RouteUnit, accepted map[uint64]uint64) Result {
	// Candidate evaluation is incremental and transactional. Emit the exact map
	// it accepted; rebuilding routes from mutable unit choices can accidentally
	// serialize the last rejected trial rather than the committed topology.
	parent := make(map[uint64]uint64, len(accepted))
	for child, p := range accepted {
		parent[child] = p
	}
	anon := make(map[uint64]bool)
	usedHA := make(map[uint64]bool)
	var res Result

	for _, u := range units {
		res.Orphans += len(u.members)
		if u.knownFanout {
			usedHA[u.parentID] = true
		}
	}
	presentNodes := make(map[uint64]bool, len(parent)*2)
	for child, p := range parent {
		presentNodes[child] = true
		presentNodes[p] = true
		if sk.fanouts[child] != nil {
			usedHA[child] = true
		}
		if sk.fanouts[p] != nil {
			usedHA[p] = true
		}
	}
	for _, u := range units {
		for _, id := range u.anonAtDepth {
			if presentNodes[id] {
				anon[id] = true
			}
		}
	}

	nearestSurvivor := func(start uint64) (uint64, int) {
		seen := make(map[uint64]bool)
		cur := start
		for cur != 0 && !seen[cur] {
			seen[cur] = true
			if s := sk.byID[cur]; s != nil {
				return cur, s.Depth
			}
			cur = parent[cur]
		}
		return 0, 0
	}
	for _, u := range units {
		for _, e := range u.members {
			f := e.frag
			anchorID, anchorDepth := nearestSurvivor(u.parentID)
			if anchorID == 0 {
				res.Unanchored = append(res.Unanchored, f.root.SpanID)
				continue
			}
			res.Bridges = append(res.Bridges, Bridge{
				OrphanID: f.root.SpanID, AnchorID: anchorID,
				Synthetic:  f.root.Depth - anchorDepth - 1,
				Ambiguous:  len(u.anchors) > 1 || f.anchorAmbig,
				ViaCarrier: f.viaCarrier, ReconFanout: u.parentID,
			})
			res.Reconnected++
		}
	}
	res.ReconParent = parent
	res.ReconAnon = anon
	res.ReconHAFanouts = usedHA
	return res
}

func sb3HasAncestor(parent map[uint64]uint64, node, ancestor uint64) bool {
	seen := make(map[uint64]bool)
	for cur := node; cur != 0 && !seen[cur]; cur = parent[cur] {
		if cur == ancestor {
			return true
		}
		seen[cur] = true
	}
	return false
}

// sb3CheckHardEvidence verifies facts obtained without probabilistic lookup.
// Native ParentIDs fix every surviving edge or immediate synthetic parent; HA
// entries fix witness ancestry. These constraints outrank Bloom depth and
// sparse-ordinal preferences in the greedy score.
func sb3CheckHardEvidenceForMode(survivors []Span, topo Result, includeHA bool) (parentConflicts, haConflicts int, nodes map[uint64]bool) {
	diag := os.Getenv("TRACE_RECON_SB3DIAG") != ""
	nodes = make(map[uint64]bool)
	seenHA := make(map[[2]uint64]bool)
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 && topo.ReconParent[s.SpanID] != s.ParentID {
			parentConflicts++
			if diag && parentConflicts+haConflicts <= 32 {
				fmt.Fprintf(os.Stderr, "SB3HARD parent carrier=%016x depth=%d want=%016x got=%016x\n",
					s.SpanID, s.Depth, s.ParentID, topo.ReconParent[s.SpanID])
			}
			nodes[s.SpanID] = true
			nodes[s.ParentID] = true
		}
		if !includeHA {
			continue
		}
		for _, h := range s.HA {
			key := [2]uint64{h.ParentID, s.SpanID}
			if seenHA[key] {
				continue
			}
			seenHA[key] = true
			if !sb3HasAncestor(topo.ReconParent, s.SpanID, h.ParentID) {
				haConflicts++
				if diag && parentConflicts+haConflicts <= 32 {
					fmt.Fprintf(os.Stderr, "SB3HARD ha carrier=%016x carrier_depth=%d fanout=%016x fanout_depth=%d\n",
						s.SpanID, s.Depth, h.ParentID, h.Depth-1)
				}
				nodes[s.SpanID] = true
				nodes[h.ParentID] = true
			}
		}
	}
	return parentConflicts, haConflicts, nodes
}

func sb3CheckHardEvidence(survivors []Span, topo Result) (parentConflicts, haConflicts int, nodes map[uint64]bool) {
	return sb3CheckHardEvidenceForMode(survivors, topo, true)
}

func sb3DiagHardRoutes(units []*sb3RouteUnit, survivors []Span, topo Result) {
	if os.Getenv("TRACE_RECON_SB3DIAG") == "" {
		return
	}
	carrierUnit := make(map[uint64]*sb3RouteUnit)
	spanByID := make(map[uint64]*Span, len(survivors))
	for i := range survivors {
		spanByID[survivors[i].SpanID] = &survivors[i]
	}
	for _, u := range units {
		for _, e := range u.members {
			for _, s := range e.frag.spans {
				carrierUnit[s.SpanID] = u
			}
		}
	}
	printed := 0
	for i := range survivors {
		s := &survivors[i]
		for _, h := range s.HA {
			if sb3HasAncestor(topo.ReconParent, s.SpanID, h.ParentID) {
				continue
			}
			u := carrierUnit[s.SpanID]
			if u == nil {
				fmt.Fprintf(os.Stderr, "SB3HARDROUTE carrier=%016x no_route_unit fanout=%016x\n", s.SpanID, h.ParentID)
			} else {
				anchor := uint64(0)
				anchorDepth := -1
				if u.anchor != nil {
					anchor = u.anchor.SpanID
					anchorDepth = u.anchor.Depth
				}
				fmt.Fprintf(os.Stderr, "SB3HARDROUTE carrier=%016x root=%016x root_depth=%d parent=%016x unit_depth=%d fanout=%016x fanout_depth=%d required=%016x choice=%016x anchor=%016x anchor_depth=%d anchors=%d\n",
					s.SpanID, u.members[0].frag.root.SpanID, u.members[0].frag.root.Depth,
					u.parentID, u.depth, h.ParentID, h.Depth-1,
					u.requiredFanout[h.Depth-1], u.nodeChoice[h.Depth-1], anchor, anchorDepth, len(u.anchors))
				fmt.Fprint(os.Stderr, "SB3HARDPATH")
				seen := make(map[uint64]bool)
				for cur := s.SpanID; cur != 0 && !seen[cur]; cur = topo.ReconParent[cur] {
					seen[cur] = true
					depth := -1
					if span := spanByID[cur]; span != nil {
						depth = span.Depth
					}
					fmt.Fprintf(os.Stderr, " %016x(d=%d)", cur, depth)
				}
				fmt.Fprintln(os.Stderr)
			}
			printed++
			if printed >= 32 {
				return
			}
		}
	}
}

// sb3SeedGreedyParent installs only facts that are literal in surviving
// records. Candidate routes are added to this map one at a time; they are never
// materialized speculatively as a complete topology just to be scored later.
func sb3SeedGreedyParent(sk *cgpSkeleton, units []*sb3RouteUnit) map[uint64]uint64 {
	parent := make(map[uint64]uint64, len(sk.byID)+len(units)*2)
	for _, s := range sk.byID {
		if s.ParentID != 0 {
			if _, survived := sk.byID[s.ParentID]; survived {
				parent[s.SpanID] = s.ParentID
			}
		}
	}
	for _, u := range units {
		for _, e := range u.members {
			parent[e.frag.root.SpanID] = u.parentID
		}
	}
	return parent
}

// sb3ApplyUnitRoute adds the selected route to a partially reconstructed
// topology. The returned keys are exactly the assignments made by this call,
// allowing a rejected candidate to be rolled back without rebuilding the
// trace. Reaching an already-parented named node joins its established route.
func sb3ApplyUnitRoute(parent map[uint64]uint64, u *sb3RouteUnit) []uint64 {
	if u.anchor == nil {
		return nil
	}
	cur := u.parentID
	inserted := make([]uint64, 0, u.depth-u.anchor.Depth)
	for d := u.depth - 1; d > u.anchor.Depth; d-- {
		if _, joined := parent[cur]; joined {
			return inserted
		}
		node := u.nodeChoice[d]
		if node == 0 {
			node = u.anonAtDepth[d]
		}
		if node == 0 {
			return inserted
		}
		parent[cur] = node
		inserted = append(inserted, cur)
		cur = node
	}
	if _, joined := parent[cur]; !joined {
		parent[cur] = u.anchor.SpanID
		inserted = append(inserted, cur)
	}
	return inserted
}

func sb3RollbackRoute(parent map[uint64]uint64, inserted []uint64) {
	for i := len(inserted) - 1; i >= 0; i-- {
		delete(parent, inserted[i])
	}
}

// sb3UnitHardCompatible treats the partial topology as a three-valued path:
// satisfied, contradicted, or unresolved. Every reconstructed node has an
// absolute depth. Reaching the witness depth with a different node proves the
// candidate impossible; ending above that depth merely defers the fact to the
// unresolved upstream route. This is the distinction a plain ancestor lookup
// cannot make.
func sb3UnitHardCompatible(u *sb3RouteUnit, parent map[uint64]uint64, nodeDepth map[uint64]int) bool {
	for _, e := range u.members {
		for _, w := range e.haWitnesses {
			seen := make(map[uint64]bool)
			cur := w.carrier
			for cur != 0 && !seen[cur] {
				if cur == w.fanoutID {
					break
				}
				seen[cur] = true
				if depth, known := nodeDepth[cur]; known && depth <= w.depth {
					return false // crossed the exact fanout depth without its ID
				}
				next, connected := parent[cur]
				if !connected {
					break // unresolved above this node; not a contradiction yet
				}
				cur = next
			}
		}
	}
	return true
}

func sb3BuildNodeDepth(sk *cgpSkeleton, units []*sb3RouteUnit) map[uint64]int {
	depth := make(map[uint64]int, len(sk.byID)+len(sk.fanouts)+len(units)*2)
	for id, s := range sk.byID {
		depth[id] = s.Depth
	}
	for id, fo := range sk.fanouts {
		depth[id] = fo.depth
	}
	for _, u := range units {
		depth[u.parentID] = u.depth
		for d, id := range u.requiredFanout {
			if id != 0 {
				depth[id] = d
			}
		}
		for d, ids := range u.fanoutsByDepth {
			for _, id := range ids {
				depth[id] = d
			}
		}
		for d, id := range u.anonAtDepth {
			depth[id] = d
		}
	}
	return depth
}

type sb3HAConstraint struct {
	carrier uint64
	fanout  uint64
	depth   int

	terminal  uint64
	satisfied bool
	active    bool
}

type sb3HATracker struct {
	nodeDepth map[uint64]int
	waiting   map[uint64]map[*sb3HAConstraint]bool
	all       []*sb3HAConstraint
	derived   map[[2]uint64]*sb3HAConstraint
}

type sb3HAConstraintState struct {
	terminal  uint64
	satisfied bool
}

type sb3HATxn struct {
	owner     *sb3HATracker
	originals map[*sb3HAConstraint]sb3HAConstraintState
	added     []*sb3HAConstraint
}

func (h *sb3HATracker) addWaiting(c *sb3HAConstraint) {
	if c == nil || !c.active || c.satisfied || c.terminal == 0 {
		return
	}
	set := h.waiting[c.terminal]
	if set == nil {
		set = make(map[*sb3HAConstraint]bool)
		h.waiting[c.terminal] = set
	}
	set[c] = true
}

func (h *sb3HATracker) removeWaiting(c *sb3HAConstraint) {
	if c == nil || !c.active || c.satisfied || c.terminal == 0 {
		return
	}
	set := h.waiting[c.terminal]
	delete(set, c)
	if len(set) == 0 {
		delete(h.waiting, c.terminal)
	}
}

// terminalCompatible checks the constraints that would share the ancestry
// above one unresolved terminal. Two different fanout identities at the same
// absolute depth cannot both occur on that single path. This joint
// contradiction is stronger than either constraint in isolation: each may be
// unresolved while the pair is already impossible.
func (h *sb3HATracker) terminalCompatible(c *sb3HAConstraint, terminal uint64) bool {
	if c == nil || terminal == 0 {
		return false
	}
	for other := range h.waiting[terminal] {
		if other == c || other.satisfied {
			continue
		}
		if other.depth == c.depth && other.fanout != c.fanout {
			return false
		}
	}
	return true
}

// advance follows the currently materialized ancestry beginning at start.
// It returns false only after the path has crossed the exact witness depth
// through a different node. A missing parent above that depth remains pending.
func (h *sb3HATracker) advance(c *sb3HAConstraint, start uint64, parent map[uint64]uint64) bool {
	seen := make(map[uint64]bool)
	for cur := start; cur != 0 && !seen[cur]; {
		if cur == c.fanout {
			c.satisfied = true
			c.terminal = 0
			return true
		}
		seen[cur] = true
		if depth, known := h.nodeDepth[cur]; known && depth <= c.depth {
			return false
		}
		next, connected := parent[cur]
		if !connected {
			c.terminal = cur
			return h.terminalCompatible(c, cur)
		}
		cur = next
	}
	return false // zero/cycle before reaching the required depth is impossible
}

// ensureTerminalImplications closes the hard HA facts under ancestry. Once
// two witnessed fanouts of different depths share an accepted unresolved
// terminal, the deeper fanout must itself descend through the shallower one.
// Recording that implication lets the tracker prune the deeper fanout's
// upstream route when it is selected later. Implications created during a
// candidate trial belong to its transaction and disappear on rollback.
func (h *sb3HATracker) ensureTerminalImplications(terminal uint64, parent map[uint64]uint64, tx *sb3HATxn) bool {
	queue := []uint64{terminal}
	queued := map[uint64]bool{terminal: true}
	for len(queue) > 0 {
		terminal = queue[0]
		queue = queue[1:]
		constraints := make([]*sb3HAConstraint, 0, len(h.waiting[terminal]))
		seenFanout := make(map[uint64]bool)
		for c := range h.waiting[terminal] {
			if !c.active || c.satisfied || seenFanout[c.fanout] {
				continue
			}
			seenFanout[c.fanout] = true
			constraints = append(constraints, c)
		}
		sort.Slice(constraints, func(i, j int) bool {
			if constraints[i].depth != constraints[j].depth {
				return constraints[i].depth > constraints[j].depth
			}
			return constraints[i].fanout < constraints[j].fanout
		})
		for i := 0; i+1 < len(constraints); i++ {
			deeper, shallower := constraints[i], constraints[i+1]
			if deeper.depth == shallower.depth || deeper.fanout == shallower.fanout {
				continue
			}
			key := [2]uint64{deeper.fanout, shallower.fanout}
			if have := h.derived[key]; have != nil && have.active {
				continue
			}
			implied := &sb3HAConstraint{
				carrier: deeper.fanout, fanout: shallower.fanout, depth: shallower.depth,
				active: true,
			}
			h.derived[key] = implied
			h.all = append(h.all, implied)
			if tx != nil {
				tx.added = append(tx.added, implied)
			}
			if !h.advance(implied, implied.carrier, parent) {
				if tx == nil {
					implied.active = false
					delete(h.derived, key)
				}
				return false
			}
			h.addWaiting(implied)
			if implied.terminal != 0 && !queued[implied.terminal] {
				queued[implied.terminal] = true
				queue = append(queue, implied.terminal)
			}
		}
	}
	return true
}

func sb3BuildHATracker(survivors []Span, parent map[uint64]uint64, nodeDepth map[uint64]int) (*sb3HATracker, int) {
	h := &sb3HATracker{
		nodeDepth: nodeDepth,
		waiting:   make(map[uint64]map[*sb3HAConstraint]bool),
		derived:   make(map[[2]uint64]*sb3HAConstraint),
	}
	seen := make(map[[2]uint64]bool)
	conflicts := 0
	for i := range survivors {
		s := &survivors[i]
		for _, witness := range s.HA {
			key := [2]uint64{witness.ParentID, s.SpanID}
			if seen[key] {
				continue
			}
			seen[key] = true
			c := &sb3HAConstraint{carrier: s.SpanID, fanout: witness.ParentID, depth: witness.Depth - 1, active: true}
			if !h.advance(c, c.carrier, parent) {
				conflicts++
				continue
			}
			h.all = append(h.all, c)
			h.addWaiting(c)
		}
	}
	terminals := make([]uint64, 0, len(h.waiting))
	for terminal := range h.waiting {
		terminals = append(terminals, terminal)
	}
	for _, terminal := range terminals {
		if !h.ensureTerminalImplications(terminal, parent, nil) {
			conflicts++
		}
	}
	return h, conflicts
}

// tryEdges advances exactly the HA obligations whose previously unresolved
// terminal received a parent in this candidate. The update is transactional so
// a contradicted candidate restores every obligation before topology rollback.
func (h *sb3HATracker) tryEdges(inserted []uint64, parent map[uint64]uint64) (*sb3HATxn, bool) {
	if h == nil || len(inserted) == 0 {
		return nil, true
	}
	tx := &sb3HATxn{owner: h, originals: make(map[*sb3HAConstraint]sb3HAConstraintState)}
	for _, node := range inserted {
		set := h.waiting[node]
		if len(set) == 0 {
			continue
		}
		constraints := make([]*sb3HAConstraint, 0, len(set))
		for c := range set {
			constraints = append(constraints, c)
		}
		for _, c := range constraints {
			if _, touched := tx.originals[c]; touched {
				continue
			}
			tx.originals[c] = sb3HAConstraintState{terminal: c.terminal, satisfied: c.satisfied}
			h.removeWaiting(c)
			if !h.advance(c, node, parent) {
				tx.rollback()
				return nil, false
			}
			h.addWaiting(c)
			if c.terminal != 0 && !h.ensureTerminalImplications(c.terminal, parent, tx) {
				tx.rollback()
				return nil, false
			}
		}
	}
	return tx, true
}

func (tx *sb3HATxn) rollback() {
	if tx == nil {
		return
	}
	for c, old := range tx.originals {
		tx.owner.removeWaiting(c)
		c.terminal = old.terminal
		c.satisfied = old.satisfied
		tx.owner.addWaiting(c)
	}
	for i := len(tx.added) - 1; i >= 0; i-- {
		c := tx.added[i]
		tx.owner.removeWaiting(c)
		c.active = false
		delete(tx.owner.derived, [2]uint64{c.carrier, c.fanout})
	}
}

func (h *sb3HATracker) pending() int {
	if h == nil {
		return 0
	}
	n := 0
	for _, c := range h.all {
		if c.active && !c.satisfied {
			n++
		}
	}
	return n
}

func (h *sb3HATracker) diagPending(units []*sb3RouteUnit, parent map[uint64]uint64) {
	if h == nil || os.Getenv("TRACE_RECON_SB3DIAG") == "" {
		return
	}
	unitByParent := make(map[uint64]*sb3RouteUnit, len(units))
	for _, u := range units {
		unitByParent[u.parentID] = u
	}
	printed := 0
	for terminal, set := range h.waiting {
		for c := range set {
			u := unitByParent[terminal]
			anchors, unitDepth := -1, -1
			if u != nil {
				anchors, unitDepth = len(u.anchors), u.depth
			}
			fmt.Fprintf(os.Stderr, "SB3HAPENDING carrier=%016x fanout=%016x fanout_depth=%d terminal=%016x terminal_depth=%d owner_depth=%d owner_anchors=%d fanout_parent=%016x\n",
				c.carrier, c.fanout, c.depth, terminal, h.nodeDepth[terminal], unitDepth, anchors, parent[c.fanout])
			printed++
			if printed >= 32 {
				return
			}
		}
	}
}

func sb3NearestSurvivingAncestor(sk *cgpSkeleton, parent map[uint64]uint64, start uint64) *Span {
	seen := make(map[uint64]bool)
	for cur := start; cur != 0 && !seen[cur]; cur = parent[cur] {
		seen[cur] = true
		if s := sk.byID[cur]; s != nil {
			return s
		}
	}
	return nil
}

func sb3AppendAnchor(u *sb3RouteUnit, anchor *Span) {
	if u == nil || anchor == nil || anchor.Depth >= u.depth {
		return
	}
	for _, have := range u.anchors {
		if have.SpanID == anchor.SpanID {
			return
		}
	}
	u.anchors = append(u.anchors, anchor)
	sort.Slice(u.anchors, func(i, j int) bool {
		if u.anchors[i].Depth != u.anchors[j].Depth {
			return u.anchors[i].Depth > u.anchors[j].Depth
		}
		return u.anchors[i].SpanID < u.anchors[j].SpanID
	})
}

// sb3ResolvePendingHA revisits an upstream route unit when a committed
// descendant path hands it an exact HA obligation after its ordinary greedy
// turn. The witnessed fanout becomes a required node in that unit's candidate
// path. This is still pre-commit candidate pruning: no emitted topology is
// inspected or repaired.
func sb3ResolvePendingHA(cfg Config, sk *cgpSkeleton, units []*sb3RouteUnit, ordinals *sb3OrdinalAssignments, ha *sb3HATracker, parent map[uint64]uint64, stats *sb3GreedyStats) {
	if ha == nil {
		return
	}
	unitByParent := make(map[uint64]*sb3RouteUnit, len(units))
	for _, u := range units {
		unitByParent[u.parentID] = u
	}
	for {
		before := ha.pending()
		terminals := make([]uint64, 0, len(ha.waiting))
		for terminal := range ha.waiting {
			terminals = append(terminals, terminal)
		}
		sort.Slice(terminals, func(i, j int) bool {
			di, dj := ha.nodeDepth[terminals[i]], ha.nodeDepth[terminals[j]]
			if di != dj {
				return di > dj // propagate from descendants toward the root
			}
			return terminals[i] < terminals[j]
		})
		for _, terminal := range terminals {
			u := unitByParent[terminal]
			if u == nil || parent[terminal] != 0 {
				continue
			}
			consistent := true
			for c := range ha.waiting[terminal] {
				if have := u.requiredFanout[c.depth]; have != 0 && have != c.fanout {
					consistent = false
					break
				}
				u.requiredFanout[c.depth] = c.fanout
				sb3AppendAnchor(u, sb3NearestSurvivingAncestor(sk, parent, c.fanout))
			}
			if consistent && len(u.anchors) > 0 {
				sb3SelectGreedyRoute(cfg, sk, u, ordinals, ha, parent, stats)
			}
		}
		if ha.pending() >= before {
			return
		}
	}
}

// sb3OrdinalCursor is one carrier being inserted into the incremental
// fanout-label automaton. pos is the next topology edge and remPos is the next
// sparse ordinal to consume.
type sb3OrdinalCursor struct {
	carrier uint64
	path    []uint64
	pos     int
	rem     []bridge.SB3Branch
	remPos  int
}

type sb3OrdinalChildState struct {
	ord     int
	ee      []int
	pending []sb3OrdinalCursor
}

type sb3OrdinalNodeState struct {
	first    uint64
	children map[uint64]*sb3OrdinalChildState
	byOrd    map[int]uint64
}

func cloneSB3OrdinalNodeState(in *sb3OrdinalNodeState) *sb3OrdinalNodeState {
	if in == nil {
		return nil
	}
	out := &sb3OrdinalNodeState{
		first:    in.first,
		children: make(map[uint64]*sb3OrdinalChildState, len(in.children)),
		byOrd:    make(map[int]uint64, len(in.byOrd)),
	}
	for ord, child := range in.byOrd {
		out.byOrd[ord] = child
	}
	for child, g := range in.children {
		cg := &sb3OrdinalChildState{ord: g.ord, ee: append([]int(nil), g.ee...)}
		cg.pending = append([]sb3OrdinalCursor(nil), g.pending...)
		out.children[child] = cg
	}
	return out
}

type sb3OrdinalTxn struct {
	owner     *sb3OrdinalAssignments
	originals map[uint64]*sb3OrdinalNodeState
	connected []*Span
}

func (t *sb3OrdinalTxn) touch(node uint64) *sb3OrdinalNodeState {
	if _, seen := t.originals[node]; !seen {
		t.originals[node] = cloneSB3OrdinalNodeState(t.owner.nodes[node])
	}
	st := t.owner.nodes[node]
	if st == nil {
		st = &sb3OrdinalNodeState{
			children: make(map[uint64]*sb3OrdinalChildState),
			byOrd:    make(map[int]uint64),
		}
		t.owner.nodes[node] = st
	}
	return st
}

func (t *sb3OrdinalTxn) rollback() {
	if t == nil {
		return
	}
	for node, original := range t.originals {
		if original == nil {
			delete(t.owner.nodes, node)
		} else {
			t.owner.nodes[node] = original
		}
	}
}

// sb3OrdinalAssignments is an incremental deterministic automaton over the
// reconstructed topology. Each fanout has one epsilon/implicit-first edge and
// an injective map of ordinals 2..k to its other child edges. Carriers are added
// only when their route becomes connected to an exact checkpoint window, so a
// candidate touches the few fanouts on its own path instead of rescanning the
// entire window.
type sb3OrdinalAssignments struct {
	cfg            Config
	carrierWindow  map[uint64]uint64
	unitCarriers   map[*sb3RouteUnit][]*Span
	nodes          map[uint64]*sb3OrdinalNodeState
	added          map[uint64]bool
	ignored        map[uint64]bool
	committed      map[uint64]bool
	pending        []*Span
	invalidWindows map[uint64]bool
	initialInvalid int
	disabled       int
	initialAdded   int
	trialAdded     int
	maxPending     int
}

func sb3BuildOrdinalAssignments(sk *cgpSkeleton, cfg Config, units []*sb3RouteUnit, parent map[uint64]uint64) *sb3OrdinalAssignments {
	cpd := cfg.CPD
	if cpd < 1 {
		cpd = 1
	}
	prefixLen := cfg.PrefixLen
	if prefixLen < 1 {
		prefixLen = 1
	} else if prefixLen > 8 {
		prefixLen = 8
	}
	type windowKey struct {
		depth  int
		prefix string
	}
	checkpoints := make(map[windowKey][]uint64)
	for id, s := range sk.byID {
		if s.Depth%cpd != 0 {
			continue
		}
		raw := bridge.BigEndian8(id)
		key := windowKey{depth: s.Depth, prefix: string(raw[:prefixLen])}
		checkpoints[key] = append(checkpoints[key], id)
	}

	a := &sb3OrdinalAssignments{
		cfg:            cfg,
		carrierWindow:  make(map[uint64]uint64),
		unitCarriers:   make(map[*sb3RouteUnit][]*Span, len(units)),
		nodes:          make(map[uint64]*sb3OrdinalNodeState),
		added:          make(map[uint64]bool),
		ignored:        make(map[uint64]bool),
		committed:      make(map[uint64]bool),
		invalidWindows: make(map[uint64]bool),
	}
	for _, s := range sk.byID {
		if s.BloomBits == nil || s.Depth == 0 || len(s.CkptPrefix) < prefixLen {
			continue
		}
		floor := (s.Depth / cpd) * cpd
		if s.Depth%cpd == 0 {
			floor = s.Depth - cpd
		}
		key := windowKey{depth: floor, prefix: string(s.CkptPrefix[:prefixLen])}
		hits := checkpoints[key]
		if len(hits) != 1 {
			continue
		}
		window := hits[0]
		a.carrierWindow[s.SpanID] = window
	}

	for _, u := range units {
		seen := make(map[uint64]bool)
		for _, e := range u.members {
			if e == nil {
				continue
			}
			for _, s := range e.frag.spans {
				if a.carrierWindow[s.SpanID] != 0 && !seen[s.SpanID] {
					seen[s.SpanID] = true
					a.unitCarriers[u] = append(a.unitCarriers[u], s)
				}
			}
		}
		sort.Slice(a.unitCarriers[u], func(i, j int) bool {
			x, y := a.unitCarriers[u][i], a.unitCarriers[u][j]
			if x.Depth != y.Depth {
				return x.Depth < y.Depth
			}
			return x.SpanID < y.SpanID
		})
	}

	carriers := make([]*Span, 0, len(a.carrierWindow))
	for id := range a.carrierWindow {
		carriers = append(carriers, sk.byID[id])
	}
	sort.Slice(carriers, func(i, j int) bool {
		if carriers[i].Depth != carriers[j].Depth {
			return carriers[i].Depth < carriers[j].Depth
		}
		return carriers[i].SpanID < carriers[j].SpanID
	})
	byWindow := make(map[uint64][]*Span)
	for _, s := range carriers {
		if _, ok := a.connectedCode(s, parent); ok {
			byWindow[a.carrierWindow[s.SpanID]] = append(byWindow[a.carrierWindow[s.SpanID]], s)
		}
	}
	windows := make([]uint64, 0, len(byWindow))
	for window := range byWindow {
		windows = append(windows, window)
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })
	for _, window := range windows {
		windowCarriers := byWindow[window]
		tx := &sb3OrdinalTxn{owner: a, originals: make(map[uint64]*sb3OrdinalNodeState)}
		valid := true
		for _, s := range windowCarriers {
			code, _ := a.connectedCode(s, parent)
			if !a.insert(tx, window, code) {
				valid = false
				break
			}
		}
		if !valid {
			tx.rollback()
			a.invalidWindows[window] = true
			a.initialInvalid++
			continue
		}
		for _, s := range windowCarriers {
			a.added[s.SpanID] = true
			a.initialAdded++
		}
	}
	return a
}

func (a *sb3OrdinalAssignments) connectedCode(s *Span, parent map[uint64]uint64) (sb3OrdinalCursor, bool) {
	window := a.carrierWindow[s.SpanID]
	if window == 0 || a.invalidWindows[window] {
		return sb3OrdinalCursor{}, false
	}
	cpd := a.cfg.CPD
	if cpd < 1 {
		cpd = 1
	}
	floor := (s.Depth / cpd) * cpd
	if s.Depth%cpd == 0 {
		floor = s.Depth - cpd
	}
	cur := s.SpanID
	rev := make([]uint64, 0, s.Depth-floor)
	for d := s.Depth; d > floor; d-- {
		rev = append(rev, cur)
		p, ok := parent[cur]
		if !ok {
			return sb3OrdinalCursor{}, false
		}
		cur = p
	}
	if cur != window {
		return sb3OrdinalCursor{}, false
	}
	path := make([]uint64, len(rev))
	for i := range rev {
		path[i] = rev[len(rev)-1-i]
	}
	return sb3OrdinalCursor{
		carrier: s.SpanID,
		path:    path,
		rem:     s.SparseOrdinals,
	}, true
}

func sb3CursorBranch(c sb3OrdinalCursor) (bridge.SB3Branch, bool) {
	if c.remPos >= len(c.rem) {
		return bridge.SB3Branch{}, false
	}
	return c.rem[c.remPos], true
}

func (a *sb3OrdinalAssignments) insert(tx *sb3OrdinalTxn, node uint64, code sb3OrdinalCursor) bool {
	if code.pos == len(code.path) {
		return code.remPos == len(code.rem)
	}
	child := code.path[code.pos]
	st := a.nodes[node]
	if st != nil && st.first != 0 {
		group := st.children[child]
		if group == nil {
			st = tx.touch(node)
			b, ok := sb3CursorBranch(code)
			if !ok || b.Ord < 2 || st.byOrd[b.Ord] != 0 {
				return false
			}
			group = &sb3OrdinalChildState{ord: b.Ord, ee: append([]int(nil), b.EE...)}
			st.children[child] = group
			st.byOrd[b.Ord] = child
		}
		if group.ord != 1 {
			b, ok := sb3CursorBranch(code)
			if !ok || b.Ord != group.ord || !equalInts(b.EE, group.ee) {
				return false
			}
			code.remPos++
		}
		code.pos++
		return a.insert(tx, child, code)
	}

	st = tx.touch(node)
	group := st.children[child]
	if group == nil {
		group = &sb3OrdinalChildState{}
		st.children[child] = group
	}
	group.pending = append(group.pending, code)

	// An unresolved node has not seen an exhausted remainder yet; otherwise it
	// would already have fixed its first child. Therefore only the newly added
	// cursor can resolve it. Avoid rescanning every pending child group on each
	// insertion (quadratic at very wide fanouts).
	if code.remPos != len(code.rem) {
		return true
	}
	forcedFirst := child

	st.first = forcedFirst
	children := make([]uint64, 0, len(st.children))
	for childID := range st.children {
		children = append(children, childID)
	}
	sort.Slice(children, func(i, j int) bool { return children[i] < children[j] })
	for _, childID := range children {
		g := st.children[childID]
		if childID == forcedFirst {
			g.ord = 1
			st.byOrd[1] = childID
			continue
		}
		ord := 0
		var ee []int
		for _, pending := range g.pending {
			b, ok := sb3CursorBranch(pending)
			if !ok {
				return false
			}
			if ord == 0 {
				ord = b.Ord
				ee = b.EE
			} else if b.Ord != ord || !equalInts(b.EE, ee) {
				return false
			}
		}
		if ord < 2 || st.byOrd[ord] != 0 {
			return false
		}
		g.ord = ord
		g.ee = append([]int(nil), ee...)
		st.byOrd[ord] = childID
	}
	for _, childID := range children {
		g := st.children[childID]
		pending := g.pending
		g.pending = nil
		for _, next := range pending {
			if g.ord != 1 {
				next.remPos++
			}
			next.pos++
			if !a.insert(tx, childID, next) {
				return false
			}
		}
	}
	return true
}

func (a *sb3OrdinalAssignments) tryUnit(u *sb3RouteUnit, parent map[uint64]uint64) (*sb3OrdinalTxn, bool) {
	if a == nil {
		return nil, true
	}
	tx := &sb3OrdinalTxn{owner: a, originals: make(map[uint64]*sb3OrdinalNodeState)}
	seen := make(map[uint64]bool)
	candidates := make([]*Span, 0, len(a.pending)+len(a.unitCarriers[u]))
	for _, list := range [][]*Span{a.pending, a.unitCarriers[u]} {
		for _, s := range list {
			if s != nil && !a.added[s.SpanID] && !a.ignored[s.SpanID] && !seen[s.SpanID] {
				seen[s.SpanID] = true
				candidates = append(candidates, s)
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Depth != candidates[j].Depth {
			return candidates[i].Depth < candidates[j].Depth
		}
		return candidates[i].SpanID < candidates[j].SpanID
	})
	for _, s := range candidates {
		code, ok := a.connectedCode(s, parent)
		if !ok {
			continue
		}
		if !a.insert(tx, a.carrierWindow[s.SpanID], code) {
			tx.rollback()
			return nil, false
		}
		tx.connected = append(tx.connected, s)
	}
	return tx, true
}

func (a *sb3OrdinalAssignments) accept(tx *sb3OrdinalTxn, u *sb3RouteUnit) {
	if a == nil {
		return
	}
	for _, s := range tx.connected {
		a.added[s.SpanID] = true
		a.trialAdded++
	}
	for _, s := range a.unitCarriers[u] {
		a.committed[s.SpanID] = true
	}
	seen := make(map[uint64]bool)
	next := make([]*Span, 0, len(a.pending)+len(a.unitCarriers[u]))
	for _, list := range [][]*Span{a.pending, a.unitCarriers[u]} {
		for _, s := range list {
			if s != nil && a.committed[s.SpanID] && !a.added[s.SpanID] &&
				!a.ignored[s.SpanID] && !a.invalidWindows[a.carrierWindow[s.SpanID]] && !seen[s.SpanID] {
				seen[s.SpanID] = true
				next = append(next, s)
			}
		}
	}
	a.pending = next
	if len(a.pending) > a.maxPending {
		a.maxPending = len(a.pending)
	}
}

func (a *sb3OrdinalAssignments) disableUnit(u *sb3RouteUnit) {
	if a == nil {
		return
	}
	a.disabled++
	for _, s := range a.unitCarriers[u] {
		a.ignored[s.SpanID] = true
	}
	next := a.pending[:0]
	for _, s := range a.pending {
		if !a.ignored[s.SpanID] {
			next = append(next, s)
		}
	}
	a.pending = next
}

func sb3ChoiceDiffers(u *sb3RouteUnit, baseAnchor uint64, baseNodes map[int]uint64) bool {
	if u.anchor == nil || u.anchor.SpanID != baseAnchor {
		return true
	}
	for d, base := range baseNodes {
		if u.nodeChoice[d] != base {
			return true
		}
	}
	return false
}

// sb3RouteHasChoice reports whether Bloom/HA enumeration left competing
// surviving-anchor paths. Intermediate HA identities are forced when witnessed
// and otherwise use the ordinary named-first Bloom choice; treating every
// named-vs-private intermediate node as another global ordinal search variable
// manufactured thousands of artificial binary decisions on wide traces.
func sb3RouteHasChoice(sk *cgpSkeleton, u *sb3RouteUnit) bool {
	_ = sk
	return len(u.anchors) > 1
}

// sb3SelectGreedyRoute enumerates Bloom/HA-admissible choices in the ordinary
// deepest-anchor, named-fanout-first order. Hard facts and sparse chains are
// predicates over each candidate: a contradiction prunes that path; ordinals
// are never converted into a post-hoc global penalty.
func sb3SelectGreedyRoute(cfg Config, sk *cgpSkeleton, u *sb3RouteUnit, ordinals *sb3OrdinalAssignments, ha *sb3HATracker, parent map[uint64]uint64, stats *sb3GreedyStats) bool {
	if len(u.anchors) == 0 {
		return false
	}
	ordinalGuidance := !cfg.SB3IgnoreOrdinals && sb3RouteHasChoice(sk, u)
	baseAnchor := uint64(0)
	if u.anchor != nil {
		baseAnchor = u.anchor.SpanID
	}
	baseNodes := make(map[int]uint64, len(u.nodeChoice))
	for d, id := range u.nodeChoice {
		baseNodes[d] = id
	}
	rejectedHard, rejectedOrdinal := false, false
	var hardFallbackAnchor *Span
	var hardFallbackNodes map[int]uint64

	anchors := u.anchors
	if cfg.GreedyNoRouteFallback && len(anchors) > 1 {
		anchors = anchors[:1]
	}
	for _, anchor := range anchors {
		u.anchor = anchor
		depths := make([]int, 0, u.depth-anchor.Depth-1)
		for d := u.depth - 1; d > anchor.Depth; d-- {
			depths = append(depths, d)
		}
		var choose func(int) bool
		choose = func(i int) bool {
			if i < len(depths) {
				d := depths[i]
				var options []uint64
				if required := u.requiredFanout[d]; required != 0 && sk.byID[required] == nil {
					options = []uint64{required}
				} else {
					// Optional named joins are probabilistic. Try the preferred
					// deterministic hit, then the private path; do not enumerate
					// every same-depth Bloom hit after a hard contradiction.
					if preferred := baseNodes[d]; preferred != 0 {
						options = append(options, preferred)
					}
					if !cfg.GreedyNoRouteFallback || len(options) == 0 {
						options = append(options, 0)
					}
				}
				for _, choice := range options {
					u.nodeChoice[d] = choice
					if choose(i + 1) {
						return true
					}
				}
				return false
			}

			inserted := sb3ApplyUnitRoute(parent, u)
			stats.CandidateEvaluations++
			haTxn, hardOK := ha.tryEdges(inserted, parent)
			ordinalOK := true
			var ordinalTxn *sb3OrdinalTxn
			if hardOK && !cfg.SB3IgnoreOrdinals {
				ordinalTxn, ordinalOK = ordinals.tryUnit(u, parent)
			}
			if hardOK && ordinalOK {
				if ordinalTxn != nil {
					ordinals.accept(ordinalTxn, u)
				}
				if sb3ChoiceDiffers(u, baseAnchor, baseNodes) {
					if rejectedHard {
						stats.HardOverrides++
					} else if rejectedOrdinal && ordinalGuidance {
						stats.OrdinalOverrides++
					}
				}
				return true // keep the accepted route installed
			}
			if hardOK && hardFallbackAnchor == nil {
				hardFallbackAnchor = u.anchor
				hardFallbackNodes = make(map[int]uint64, len(u.nodeChoice))
				for d, id := range u.nodeChoice {
					hardFallbackNodes[d] = id
				}
			}
			if !hardOK {
				rejectedHard = true
				stats.Fanout.HardRouteCandidatesRejected++
				u.hardRouteCandidatesRejected++
			} else if !ordinalOK {
				rejectedOrdinal = true
			}
			if haTxn != nil {
				haTxn.rollback()
			}
			sb3RollbackRoute(parent, inserted)
			return false
		}
		if choose(0) {
			return true
		}
	}
	if hardFallbackAnchor != nil {
		u.anchor = hardFallbackAnchor
		for d, id := range hardFallbackNodes {
			u.nodeChoice[d] = id
		}
		inserted := sb3ApplyUnitRoute(parent, u)
		if _, ok := ha.tryEdges(inserted, parent); !ok {
			stats.Fanout.HardRouteCandidatesRejected++
			u.hardRouteCandidatesRejected++
			sb3RollbackRoute(parent, inserted)
			u.anchor = nil
			return false
		}
		ordinals.disableUnit(u)
		stats.HardOverrides++ // exact HA evidence outranks ordinal admissibility
		return true
	}
	u.anchor = nil
	return false
}

// reconstructFullEvidenceGreedyTopology is the shared non-SAT CGP0/SB3
// topology engine. Bloom evidence enumerates possible routes; surviving
// ParentID/HA facts and (for SB3) sparse ordinal chains prune impossible ones
// before the ordinary greedy preference selects a route. It never invokes an
// older reconstructor or applies evidence as a post-hoc repair.
func reconstructFullEvidenceGreedyTopology(survivors []Span, cfg Config) (Result, sb3GreedyStats) {
	if cfg.CPD < 1 {
		cfg.CPD = 1
	}
	var stats sb3GreedyStats
	stats.Fanout.HAEnabled = !cfg.NoFanout
	distinctHA := make(map[uint64]bool)
	for i := range survivors {
		if len(survivors[i].HA) > 0 {
			stats.Fanout.HACarriersAvailable++
		}
		stats.Fanout.HAEntriesAvailable += len(survivors[i].HA)
		for _, entry := range survivors[i].HA {
			distinctHA[entry.ParentID] = true
		}
	}
	stats.Fanout.DistinctHAFanoutsAvailable = len(distinctHA)
	diag := os.Getenv("TRACE_RECON_SB3DIAG") != ""
	stage := time.Now()
	sk := cgpParse(survivors, cfg)
	stats.Fanout.RecoveredHAFanoutsUsed = len(sk.fanouts)
	if diag {
		fmt.Fprintf(os.Stderr, "SB3STAGE parse survivors=%d frags=%d elapsed=%s\n", len(survivors), len(sk.frags), time.Since(stage))
	}
	stage = time.Now()
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	if diag {
		fmt.Fprintf(os.Stderr, "SB3STAGE base_evidence elapsed=%s\n", time.Since(stage))
	}
	stage = time.Now()
	evidence := sb3CollectFragmentEvidenceWithStats(sk, cfg, &stats.Chain)
	if diag {
		fmt.Fprintf(os.Stderr, "SB3STAGE candidates elapsed=%s\n", time.Since(stage))
	}
	stage = time.Now()
	units := sb3IntersectRouteUnitsWithStats(sk, evidence, cfg, &stats.Fanout)
	stats.Fanout.RouteUnits = len(units)
	for _, u := range units {
		if len(u.requiredFanout) > 0 {
			stats.Fanout.RouteUnitsWithRequiredHA++
			stats.Fanout.RequiredHAConstraints += len(u.requiredFanout)
		}
	}
	sb3AssignAnonymousIDs(sk, units)
	nodeDepth := sb3BuildNodeDepth(sk, units)
	if diag {
		fmt.Fprintf(os.Stderr, "SB3STAGE fanout_groups units=%d elapsed=%s\n", len(units), time.Since(stage))
	}

	parent := sb3SeedGreedyParent(sk, units)
	var haTracker *sb3HATracker
	initialHAConflicts := 0
	if !cfg.NoFanout && !cfg.GreedyNoHardHA {
		haTracker, initialHAConflicts = sb3BuildHATracker(survivors, parent, nodeDepth)
	}
	if initialHAConflicts != 0 {
		stats.HAConflicts += initialHAConflicts
	}
	var ordinalAssignments *sb3OrdinalAssignments
	if !cfg.SB3IgnoreOrdinals {
		ordinalStage := time.Now()
		ordinalAssignments = sb3BuildOrdinalAssignments(sk, cfg, units, parent)
		if diag {
			fmt.Fprintf(os.Stderr, "SB3STAGE ordinal_seed initial=%d invalid_windows=%d elapsed=%s\n",
				ordinalAssignments.initialAdded, ordinalAssignments.initialInvalid, time.Since(ordinalStage))
		}
	}
	stage = time.Now()
	if diag {
		ambiguous := 0
		for _, u := range units {
			if sb3RouteHasChoice(sk, u) {
				ambiguous++
			}
		}
		fmt.Fprintf(os.Stderr, "SB3STAGE route_domains ambiguous=%d deterministic=%d\n", ambiguous, len(units)-ambiguous)
	}
	for _, u := range units {
		sb3SelectGreedyRoute(cfg, sk, u, ordinalAssignments, haTracker, parent, &stats)
	}
	sb3ResolvePendingHA(cfg, sk, units, ordinalAssignments, haTracker, parent, &stats)
	stats.Chain.Routes = sb3CollectSelectedRouteEvidence(sk, units)
	if diag {
		fmt.Fprintf(os.Stderr, "SB3STAGE ordinal_prune candidates=%d elapsed=%s\n", stats.CandidateEvaluations, time.Since(stage))
		fmt.Fprintf(os.Stderr, "SB3HASTATE pending=%d initial_conflicts=%d\n", haTracker.pending(), initialHAConflicts)
		haTracker.diagPending(units, parent)
		if ordinalAssignments != nil {
			fmt.Fprintf(os.Stderr, "SB3ORDSTATE trial_added=%d quarantined_units=%d max_pending=%d\n",
				ordinalAssignments.trialAdded, ordinalAssignments.disabled, ordinalAssignments.maxPending)
		}
	}
	topo := sb3EmitGreedyTopology(sk, units, parent)
	stats.ParentConflicts, stats.HAConflicts, _ = sb3CheckHardEvidenceForMode(survivors, topo, !cfg.NoFanout)
	stats.HardConflicts = stats.ParentConflicts + stats.HAConflicts
	topo.GreedyMode = "full-evidence"
	topo.GreedyCandidateEvaluations = stats.CandidateEvaluations
	topo.GreedyHardOverrides = stats.HardOverrides
	topo.GreedyHardConflicts = stats.HardConflicts
	topo.GreedyParentConflicts = stats.ParentConflicts
	topo.GreedyHAConflicts = stats.HAConflicts
	topo.GreedyChain = stats.Chain
	topo.GreedyFanout = stats.Fanout
	if stats.HardConflicts > 0 {
		sb3DiagHardRoutes(units, survivors, topo)
	}
	if os.Getenv("TRACE_RECON_SB3DIAG") != "" {
		fmt.Fprintf(os.Stderr, "SB3GREEDY units=%d candidates=%d ordinal_prunes=%d hard_prunes=%d hard_conflicts=%d\n",
			len(units), stats.CandidateEvaluations, stats.OrdinalOverrides, stats.HardOverrides, stats.HardConflicts)
	}
	return topo, stats
}
