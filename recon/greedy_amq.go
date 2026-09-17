package recon

import (
	"sort"

	"bridges/bloom"
	"bridges/bridge"
)

// greedyAMQConstraint follows one carrier's ancestry evidence upward. A
// missing parent suspends the check; it does not discard the filter. The
// checkpoint root and the emitting span are outside the filter's population.
type greedyAMQConstraint struct {
	carrier      uint64
	carrierDepth int
	start        uint64
	startDepth   int
	floor        int
	bf           *bloom.Filter
	state        greedyAMQState
}

type greedyAMQState struct {
	terminal uint64
	depth    int
	done     bool
}

type greedyAMQTracker struct {
	named   map[uint64]bool
	all     []*greedyAMQConstraint
	waiting map[uint64]map[*greedyAMQConstraint]bool
}

type greedyAMQTxn struct {
	owner     *greedyAMQTracker
	originals map[*greedyAMQConstraint]greedyAMQState
}

func newGreedyAMQTracker(sk *cgpSkeleton, cfg Config, parent map[uint64]uint64) *greedyAMQTracker {
	t := &greedyAMQTracker{
		named:   make(map[uint64]bool),
		waiting: make(map[uint64]map[*greedyAMQConstraint]bool),
	}
	for _, s := range sk.byID {
		t.named[s.SpanID] = true
		if s.ParentID != 0 {
			t.named[s.ParentID] = true
		}
		if !cfg.NoFanout {
			for _, h := range s.HA {
				t.named[h.ParentID] = true
			}
		}
	}
	seen := make(map[[2]uint64]bool)
	add := func(carrier *Span, start uint64, depth, floor int, bf *bloom.Filter) {
		key := [2]uint64{carrier.SpanID, start}
		if depth <= floor || seen[key] {
			return
		}
		seen[key] = true
		c := &greedyAMQConstraint{
			carrier: carrier.SpanID, carrierDepth: carrier.Depth,
			start: start, startDepth: depth, floor: floor, bf: bf,
			state: greedyAMQState{terminal: start, depth: depth},
		}
		t.all = append(t.all, c)
		// Literal input edges cannot be discarded to accommodate an invalid
		// payload. The final audit reports any contradictions already present.
		if t.advance(c, parent) {
			t.addWaiting(c)
		}
	}
	ids := make([]uint64, 0, len(sk.byID))
	for id := range sk.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		s := sk.byID[id]
		if s.BloomBits == nil || s.Depth == 0 {
			continue
		}
		roots := sk.checkpoints.matches(s)
		if !commonCheckpointDepth(roots) {
			continue // no unambiguous depth boundary for this payload yet
		}
		floor, bf := roots[0].Depth, cgpSpanBloom(s, cfg)
		add(s, s.SpanID, s.Depth, floor, bf)
		if !cfg.NoFanout && !cfg.GreedyNoHardHA {
			for _, h := range s.HA {
				// Witnessed ancestry is already certain even when its carrier
				// is disconnected. Constrain the fanout's upstream route now;
				// waiting for the eventual join could strand that hard witness.
				add(s, h.ParentID, h.Depth-1, floor, bf)
			}
		}
	}
	for _, f := range sk.frags {
		if f.viaCarrier != 0 && f.anchorCkpt != nil && f.bf != nil {
			// Evidence borrowed to admit an orphan must remain applicable as
			// its own unresolved upstream path is subsequently reconstructed.
			add(f.carrier, f.root.SpanID, f.root.Depth, f.anchorCkpt.Depth, f.bf)
		}
	}
	return t
}

func (t *greedyAMQTracker) addWaiting(c *greedyAMQConstraint) {
	if c.state.done || c.state.terminal == 0 {
		return
	}
	set := t.waiting[c.state.terminal]
	if set == nil {
		set = make(map[*greedyAMQConstraint]bool)
		t.waiting[c.state.terminal] = set
	}
	set[c] = true
}

func (t *greedyAMQTracker) removeWaiting(c *greedyAMQConstraint) {
	set := t.waiting[c.state.terminal]
	delete(set, c)
	if len(set) == 0 {
		delete(t.waiting, c.state.terminal)
	}
}

func (t *greedyAMQTracker) advance(c *greedyAMQConstraint, parent map[uint64]uint64) bool {
	for c.state.depth > c.floor {
		id := c.state.terminal
		if id == 0 {
			return false
		}
		if c.state.depth < c.carrierDepth && t.named[id] {
			key := bridge.HexOf(id)
			if !c.bf.Test(key[:]) {
				return false
			}
		}
		next, connected := parent[id]
		if !connected {
			return true
		}
		c.state.terminal = next
		c.state.depth--
	}
	c.state.terminal, c.state.done = 0, true
	return true
}

// Only filters waiting on a changed edge need advancing. Each filter follows
// the actual accepted ancestry, including routes installed by earlier units.
// Rejected HA/ordinal trials can roll this state back with their topology.
func (t *greedyAMQTracker) tryEdges(inserted []uint64, parent map[uint64]uint64) (*greedyAMQTxn, bool) {
	if t == nil || len(inserted) == 0 {
		return nil, true
	}
	tx := &greedyAMQTxn{owner: t, originals: make(map[*greedyAMQConstraint]greedyAMQState)}
	for _, id := range inserted {
		set := t.waiting[id]
		constraints := make([]*greedyAMQConstraint, 0, len(set))
		for c := range set {
			constraints = append(constraints, c)
		}
		for _, c := range constraints {
			if _, touched := tx.originals[c]; touched {
				continue
			}
			tx.originals[c] = c.state
			t.removeWaiting(c)
			if !t.advance(c, parent) {
				tx.rollback()
				return nil, false
			}
			t.addWaiting(c)
		}
	}
	return tx, true
}

func (tx *greedyAMQTxn) rollback() {
	if tx == nil {
		return
	}
	for c, state := range tx.originals {
		tx.owner.removeWaiting(c)
		c.state = state
		tx.owner.addWaiting(c)
	}
}

// Audit complete named ancestry, independently of which candidate nominated
// an anchor or how far its original surviving chain could be checked. One
// carrier counts once even if both its path and a mandatory HA path conflict.
func (t *greedyAMQTracker) conflicts(parent map[uint64]uint64) int {
	if t == nil {
		return 0
	}
	bad := make(map[uint64]bool)
	for _, c := range t.all {
		for id, depth := c.start, c.startDepth; id != 0 && depth > c.floor; depth-- {
			if depth < c.carrierDepth && t.named[id] {
				key := bridge.HexOf(id)
				if !c.bf.Test(key[:]) {
					bad[c.carrier] = true
					break
				}
			}
			id = parent[id]
		}
	}
	return len(bad)
}
