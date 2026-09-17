package recon

import (
	"sort"

	"bridges/bridge"
)

// checkpointIndex uses only surviving records and their decoded bridge data.
// In randomized mode an unflagged carrier reset its window; an early leaf did
// not. No original topology or simulator checkpoint decisions are consulted.
type checkpointIndex struct {
	cfg      Config
	byPrefix map[string][]*Span
	// A span's outgoing window differs from its payload's incoming window
	// when that span resets baggage. Keep the two roles separate.
	bySpan    map[uint64]*checkpointWindow
	byCarrier map[uint64]*checkpointWindow
}

type checkpointWindow struct {
	roots []*Span
	known bool // an empty known domain is contradictory; unknown is unconstrained
}

func newCheckpointIndex(spans map[uint64]*Span, cfg Config) *checkpointIndex {
	idx := &checkpointIndex{cfg: cfg, byPrefix: make(map[string][]*Span)}
	n := cfg.PrefixLen
	if n < 1 || n > 8 {
		return idx
	}
	for _, s := range spans {
		if cfg.RandomizedCheckpoints {
			if s.LeafCarrier || s.ParentUnknown || (s.BloomBits == nil && s.ParentID != 0) {
				continue
			}
		} else if s.Depth%max(1, cfg.CPD) != 0 {
			continue
		}
		raw := bridge.BigEndian8(s.SpanID)
		key := string(raw[:n])
		idx.byPrefix[key] = append(idx.byPrefix[key], s)
	}
	for _, spans := range idx.byPrefix {
		sort.Slice(spans, func(i, j int) bool { return spans[i].SpanID < spans[j].SpanID })
	}
	if cfg.RandomizedCheckpoints {
		idx.groupWindows(spans)
	}
	return idx
}

func (idx *checkpointIndex) matches(carrier *Span) []*Span {
	if carrier != nil {
		if w := idx.byCarrier[carrier.SpanID]; w != nil && w.known {
			return w.roots
		}
	}
	return idx.prefixMatches(carrier)
}

func (idx *checkpointIndex) prefixMatches(carrier *Span) []*Span {
	if carrier == nil || carrier.Depth <= 0 || idx.cfg.PrefixLen < 1 || idx.cfg.PrefixLen > 8 || len(carrier.CkptPrefix) < idx.cfg.PrefixLen {
		return nil
	}
	cpd := max(1, idx.cfg.CPD)
	floor := ((carrier.Depth - 1) / cpd) * cpd
	var hits []*Span
	for _, s := range idx.byPrefix[string(carrier.CkptPrefix[:idx.cfg.PrefixLen])] {
		if idx.cfg.RandomizedCheckpoints {
			if s.Depth >= carrier.Depth || carrier.Depth-s.Depth > cpd {
				continue
			}
		} else if s.Depth != floor {
			continue
		}
		hits = append(hits, s)
	}
	return hits
}

// groupWindows coalesces context membership through certain span records:
// connected parent edges, shared named parents, and witnessed fanouts. It never
// joins across a baggage reset and never uses Bloom positives to merge windows.
func (idx *checkpointIndex) groupWindows(spans map[uint64]*Span) {
	parent := make(map[uint64]uint64)
	find := func(id uint64) uint64 {
		if _, ok := parent[id]; !ok {
			parent[id] = id
		}
		root := id
		for parent[root] != root {
			root = parent[root]
		}
		for id != root {
			next := parent[id]
			parent[id] = root
			id = next
		}
		return root
	}
	join := func(a, b uint64) {
		if a == 0 || b == 0 {
			return
		}
		x, y := find(a), find(b)
		if x > y {
			x, y = y, x
		}
		parent[y] = x
	}
	// A reverse-promoted checkpoint has no parent record, which does not make
	// it a root.
	resets := func(s *Span) bool {
		return (s.ParentID == 0 && !s.ParentUnknown) || (s.BloomBits != nil && !s.LeafCarrier)
	}
	incoming := func(s *Span) uint64 {
		if resets(s) {
			return s.ParentID
		}
		return s.SpanID
	}
	for _, s := range spans {
		find(s.SpanID)
		if !resets(s) {
			join(s.SpanID, s.ParentID)
		}
		if !idx.cfg.NoFanout && s.BloomBits != nil {
			for _, h := range s.HA {
				join(incoming(s), h.ParentID)
			}
		}
	}
	windows := make(map[uint64]*checkpointWindow)
	window := func(id uint64) *checkpointWindow {
		key := find(id)
		w := windows[key]
		if w == nil {
			w = &checkpointWindow{}
			windows[key] = w
		}
		return w
	}
	restrict := func(w *checkpointWindow, roots []*Span) {
		if !w.known {
			w.roots = append([]*Span(nil), roots...)
			w.known = true
			return
		}
		var common []*Span
		for _, a := range w.roots {
			for _, b := range roots {
				if a.SpanID == b.SpanID {
					common = append(common, a)
					break
				}
			}
		}
		w.roots = common
	}
	idx.bySpan = make(map[uint64]*checkpointWindow)
	idx.byCarrier = make(map[uint64]*checkpointWindow)
	for _, s := range spans {
		if resets(s) {
			restrict(window(s.SpanID), []*Span{s})
		}
		if s.BloomBits != nil && s.Depth > 0 {
			w := window(incoming(s))
			restrict(w, idx.prefixMatches(s))
			idx.byCarrier[s.SpanID] = w
		}
	}
	for id := range parent {
		idx.bySpan[id] = window(id)
	}
	// A named context root must be above every non-reset span in its group.
	// This can resolve short-prefix collisions using connected records alone.
	for _, s := range spans {
		if resets(s) {
			continue
		}
		w := idx.bySpan[s.SpanID]
		out := w.roots[:0]
		for _, root := range w.roots {
			if root.Depth < s.Depth {
				out = append(out, root)
			}
		}
		w.roots = out
	}
}

// allows rejects evidence from a different known checkpoint root. A fragment
// without connected checkpoint evidence may still be tested using a borrowed
// Bloom, so unknown membership is not itself a contradiction.
func (idx *checkpointIndex) allows(id uint64, roots []*Span) bool {
	if idx == nil {
		return true
	}
	w := idx.bySpan[id]
	if w == nil || !w.known {
		return true
	}
	for _, a := range w.roots {
		for _, b := range roots {
			if a.SpanID == b.SpanID {
				return true
			}
		}
	}
	return false
}

// An orphan's first admissible borrowed carrier assigns its window. Keep that
// assignment for all fragments joined by certain context relationships, so
// later candidate generation cannot treat the same fragment as two windows.
func (idx *checkpointIndex) bindBorrowed(id uint64, roots []*Span) {
	if w := idx.bySpan[id]; w != nil && !w.known {
		w.roots = append([]*Span(nil), roots...)
		w.known = true
	}
}

func (idx *checkpointIndex) contains(carrier, root *Span) bool {
	if root == nil {
		return false
	}
	for _, hit := range idx.matches(carrier) {
		if hit.SpanID == root.SpanID {
			return true
		}
	}
	return false
}

// A truncated-prefix collision at different depths does not establish a
// window floor. Keep it unresolved rather than silently mixing those windows.
func commonCheckpointDepth(hits []*Span) bool {
	if len(hits) == 0 {
		return false
	}
	for _, s := range hits[1:] {
		if s.Depth != hits[0].Depth {
			return false
		}
	}
	return true
}
