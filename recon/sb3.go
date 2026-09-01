package recon

import (
	"fmt"
	"os"
	"sort"
	"time"

	"bridges/bridge"
)

// DecodeSB3SpanPayload decodes an SB3 carrier into the fields consumed by the
// common PB/CGP topology reconstructor plus its sparse ordinal evidence.
func DecodeSB3SpanPayload(p []byte, cfg Config) (depth int, prefix, bits []byte, ha []HAEntry, ords []bridge.SB3Branch, err error) {
	depth, prefix, bits, ha, ords, _, err = DecodeSB3SpanPayloadFull(p, cfg)
	return
}

// DecodeSB3SpanPayloadFull also returns any trailing DEEs. A collector should
// route those records by DEEQuad.TraceID16 before passing them to
// ReconstructSB3WithDEE; the carrier span may belong to a later trace.
func DecodeSB3SpanPayloadFull(p []byte, cfg Config) (depth int, prefix, bits []byte, ha []HAEntry, ords []bridge.SB3Branch, dees []bridge.DEEQuad, err error) {
	bloomLen := int((cfg.BloomM + 7) / 8)
	fpBits := cfg.FPBits
	if fpBits <= 0 {
		fpBits = 16
	}
	d, err := bridge.DecodeSB3Payload(p, cfg.PrefixLen, bloomLen, fpBits, cfg.SBridgeLehmer)
	if err != nil {
		return 0, nil, nil, nil, nil, nil, err
	}
	ha = make([]HAEntry, len(d.HA))
	for i, e := range d.HA {
		ha[i] = HAEntry{ParentID: e.ParentID, Depth: e.Depth}
	}
	return d.Depth, d.CkptPrefix, d.BloomBits, ha, d.Branches, d.DEE, nil
}

// SB3Result contains the greedy topology and the same ordered-tree shape used
// by the existing S-Bridge structure pass. Compatible is false when the sparse
// chains cannot be aligned to the recovered topology without contradiction.
type SB3Result struct {
	Topology  Result
	Structure SBResult
	// StructureStatus is the completed lateral phase over Structure. It is
	// independent of topology scoring: sparse-unlabeled first children have
	// already received ordinal 1 before EE/DEE evidence is applied.
	StructureStatus SBStructureStatus

	Compatible           bool
	Reason               string
	OrdinalPlaced        int
	ImplicitOrdinals     int
	Fanouts              int
	Conflicts            int
	HardConflicts        int
	ParentConflicts      int
	HAConflicts          int
	GreedyMode           string // always "sb3-greedy"; never a PB/CGP or SAT engine
	OrdinalGuidance      bool
	CandidateEvaluations int
	OrdinalOverrides     int
	HardOverrides        int
	conflictNodes        map[uint64]bool
}

// ReconstructSB3 runs the shared full-evidence greedy topology engine with
// sparse-ordinal compatibility inside the candidate loop; no older
// reconstructor or SAT solver is called. The final alignment produces the
// ordered S-Bridge tree.
func ReconstructSB3(survivors []Span, cfg Config) SB3Result {
	return ReconstructSB3WithDEE(survivors, nil, cfg)
}

// ReconstructSB3WithDEE runs both SB3 layers: ordinal-guided topology followed
// by exact first-child labeling and the lateral EE/DEE event-order pass. DEEs
// must already be grouped by their embedded origin trace ID by the collector.
func ReconstructSB3WithDEE(survivors []Span, dees []bridge.DEEQuad, cfg Config) SB3Result {
	topo, stats := reconstructFullEvidenceGreedyTopology(survivors, cfg)
	out := alignSB3(survivors, cfg, topo)
	out.GreedyMode = "sb3-greedy"
	out.OrdinalGuidance = !cfg.SB3IgnoreOrdinals
	out.CandidateEvaluations = stats.CandidateEvaluations
	out.OrdinalOverrides = stats.OrdinalOverrides
	out.HardOverrides = stats.HardOverrides
	out.HardConflicts = stats.HardConflicts
	out.ParentConflicts = stats.ParentConflicts
	out.HAConflicts = stats.HAConflicts
	if stats.HardConflicts > 0 {
		out.Compatible = false
		if out.Reason == "" {
			out.Reason = fmt.Sprintf("%d surviving-record constraints remain unsatisfied", stats.HardConflicts)
		}
	}
	if out.Compatible && !cfg.SB3TopoOnly {
		out.StructureStatus = ApplyStructureEvidence(&out.Structure, dees)
	} else if out.Compatible {
		out.StructureStatus.Omitted = true
		out.StructureStatus.Reason = "EE/DEE structure was intentionally omitted"
	} else {
		out.StructureStatus.Reason = "topology/ordinal alignment is incompatible: " + out.Reason
	}
	return stampSB3(out, cfg.SB3IgnoreOrdinals)
}

func stampSB3(r SB3Result, ignoreOrdinals bool) SB3Result {
	if ignoreOrdinals {
		// The ablation is scored on identical topology criteria without charging
		// the solver for ordinal evidence it was explicitly forbidden to use.
		r.Topology.SB3OrdinalChecked = r.HardConflicts > 0
		r.Topology.SB3OrdinalCompatible = r.HardConflicts == 0
		r.Topology.SB3OrdinalConflicts = r.HardConflicts
	} else {
		r.Topology.SB3OrdinalChecked = true
		r.Topology.SB3OrdinalCompatible = r.Compatible
		r.Topology.SB3OrdinalConflicts = r.Conflicts + r.HardConflicts
	}
	return r
}

type sb3Code struct {
	carrier uint64
	path    []uint64
	rem     []bridge.SB3Branch
}

type sb3Aligner struct {
	surv          map[uint64]bool
	parent        map[uint64]uint64
	children      map[uint64][]uint64
	depth         map[uint64]int
	ord           map[uint64]int
	ee            map[uint64][]int
	placed        int
	implicit      int
	fanouts       int
	conflicts     int
	firstError    string
	conflictNodes map[uint64]bool
}

func (a *sb3Aligner) conflictAt(node uint64, format string, args ...any) {
	a.conflicts++
	a.conflictNodes[node] = true
	if os.Getenv("TRACE_RECON_SB3DIAG") != "" && a.conflicts <= 32 {
		fmt.Fprintf(os.Stderr, "SB3ORD "+format+"\n", args...)
	}
	if a.firstError == "" {
		a.firstError = fmt.Sprintf(format, args...)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cloneCode(c sb3Code) sb3Code {
	out := c
	out.path = append([]uint64(nil), c.path...)
	out.rem = append([]bridge.SB3Branch(nil), c.rem...)
	return out
}

// alignNode consumes sparse records recursively within one checkpoint window.
// At a fanout exactly one child group must contain an all-first carrier (an
// empty remainder); every other child group must share one leading ordinal.
func (a *sb3Aligner) alignNode(node uint64, codes []sb3Code, bottomDepth int) {
	for _, c := range codes {
		if len(c.path) == 0 && len(c.rem) != 0 {
			a.conflictAt(node, "carrier %016x has %d unconsumed ordinals at node %016x", c.carrier, len(c.rem), node)
		}
	}
	if a.depth[node] >= bottomDepth {
		return
	}

	groups := make(map[uint64][]sb3Code)
	for _, c := range codes {
		if len(c.path) == 0 {
			continue
		}
		child := c.path[0]
		n := cloneCode(c)
		n.path = n.path[1:]
		groups[child] = append(groups[child], n)
	}
	groupChildren := make([]uint64, 0, len(groups))
	for child := range groups {
		groupChildren = append(groupChildren, child)
	}
	sort.Slice(groupChildren, func(i, j int) bool { return groupChildren[i] < groupChildren[j] })
	wantChildren := a.children[node]
	if len(groups) != len(wantChildren) {
		a.conflictAt(node, "node %016x has %d recovered children but %d carrier child groups", node, len(wantChildren), len(groups))
	}
	for _, child := range wantChildren {
		if len(groups[child]) == 0 {
			a.conflictAt(node, "node %016x child %016x has no carrier in its window", node, child)
		}
	}
	if len(groups) == 0 {
		return
	}
	if len(groups) == 1 {
		child := groupChildren[0]
		a.ord[child] = 1
		a.implicit++
		a.alignNode(child, groups[child], bottomDepth)
		return
	}

	a.fanouts++
	var first uint64
	firstCandidates := 0
	for _, child := range groupChildren {
		g := groups[child]
		hasEmpty := false
		for _, c := range g {
			if len(c.rem) == 0 {
				hasEmpty = true
				break
			}
		}
		if hasEmpty {
			first, firstCandidates = child, firstCandidates+1
		}
	}
	if firstCandidates != 1 {
		a.conflictAt(node, "fanout %016x has %d implicit-first child candidates", node, firstCandidates)
	}
	used := make(map[int]uint64)
	if firstCandidates == 1 {
		a.ord[first] = 1
		a.implicit++
		used[1] = first
	}

	for _, child := range groupChildren {
		g := groups[child]
		if child == first && firstCandidates == 1 {
			continue
		}
		ord := 0
		var ee []int
		valid := true
		for i := range g {
			if len(g[i].rem) == 0 {
				valid = false
				break
			}
			b := g[i].rem[0]
			if ord == 0 {
				ord = b.Ord
				ee = append([]int(nil), b.EE...)
			} else if b.Ord != ord || !equalInts(b.EE, ee) {
				valid = false
				break
			}
		}
		if !valid || ord < 2 || ord > len(groups) {
			a.conflictAt(node, "fanout %016x child %016x has inconsistent/out-of-range sparse ordinal %d", node, child, ord)
			continue
		}
		if prev := used[ord]; prev != 0 && prev != child {
			a.conflictAt(node, "fanout %016x assigns ordinal %d to both %016x and %016x", node, ord, prev, child)
			continue
		}
		used[ord] = child
		a.ord[child] = ord
		a.ee[child] = ee
		a.placed++
		for i := range g {
			g[i].rem = g[i].rem[1:]
		}
		groups[child] = g
	}

	for _, child := range groupChildren {
		a.alignNode(child, groups[child], bottomDepth)
	}
}

// runSB3Alignment performs the topology/ordinal consistency pass but does not
// materialize the final ordered SB tree. Greedy candidate trials need only the
// score and conflict sites; building tens of thousands of identical SBNodes
// for each trial dominated reconstruction time on large traces.
func runSB3Alignment(survivors []Span, cfg Config, topo Result) (*sb3Aligner, uint64) {
	cpd := cfg.CPD
	if cpd < 1 {
		cpd = 1
	}
	a := &sb3Aligner{
		surv: make(map[uint64]bool, len(survivors)), parent: topo.ReconParent,
		children: make(map[uint64][]uint64), depth: make(map[uint64]int),
		ord: make(map[uint64]int), ee: make(map[uint64][]int),
		conflictNodes: make(map[uint64]bool),
	}
	var root uint64
	for i := range survivors {
		s := &survivors[i]
		a.surv[s.SpanID] = true
		if s.ParentID == 0 && (root == 0 || s.SpanID < root) {
			root = s.SpanID
		}
	}
	if root == 0 {
		a.conflictAt(0, "no surviving trace root")
		return a, root
	}
	for child, parent := range topo.ReconParent {
		a.children[parent] = append(a.children[parent], child)
	}
	for p := range a.children {
		sort.Slice(a.children[p], func(i, j int) bool { return a.children[p][i] < a.children[p][j] })
	}

	// Recovered edges are unit-depth. Assign depths from the surviving root and
	// verify each surviving span lands at its emitted absolute depth.
	a.depth[root] = 0
	seen := map[uint64]bool{root: true}
	queue := []uint64{root}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, child := range a.children[n] {
			if seen[child] {
				a.conflictAt(child, "cycle or duplicate parent at reconstructed node %016x", child)
				continue
			}
			seen[child] = true
			a.depth[child] = a.depth[n] + 1
			queue = append(queue, child)
		}
	}
	for i := range survivors {
		s := &survivors[i]
		if !seen[s.SpanID] {
			a.conflictAt(s.SpanID, "survivor %016x is disconnected from root", s.SpanID)
		} else if a.depth[s.SpanID] != s.Depth {
			a.conflictAt(s.SpanID, "survivor %016x reconstructed at depth %d, payload says %d", s.SpanID, a.depth[s.SpanID], s.Depth)
		}
	}

	windows := make(map[uint64][]sb3Code)
	for i := range survivors {
		s := &survivors[i]
		if s.BloomBits == nil { // only _br carriers have a sparse chain
			continue
		}
		if s.Depth == 0 { // root describes no parent-window edge
			continue
		}
		floor := (s.Depth / cpd) * cpd
		if s.Depth%cpd == 0 {
			floor = s.Depth - cpd
		}
		cur := s.SpanID
		rev := make([]uint64, 0, s.Depth-floor)
		ok := true
		for a.depth[cur] > floor {
			rev = append(rev, cur)
			p, exists := a.parent[cur]
			if !exists {
				ok = false
				break
			}
			cur = p
		}
		if !ok || a.depth[cur] != floor {
			a.conflictAt(s.SpanID, "carrier %016x cannot reach checkpoint floor depth %d", s.SpanID, floor)
			continue
		}
		path := make([]uint64, len(rev))
		for j := range rev {
			path[j] = rev[len(rev)-1-j]
		}
		windows[cur] = append(windows[cur], sb3Code{
			carrier: s.SpanID, path: path, rem: append([]bridge.SB3Branch(nil), s.SparseOrdinals...),
		})
	}
	windowRoots := make([]uint64, 0, len(windows))
	for windowRoot := range windows {
		windowRoots = append(windowRoots, windowRoot)
	}
	sort.Slice(windowRoots, func(i, j int) bool { return windowRoots[i] < windowRoots[j] })
	for _, windowRoot := range windowRoots {
		a.alignNode(windowRoot, windows[windowRoot], a.depth[windowRoot]+cpd)
	}
	return a, root
}

func alignSB3(survivors []Span, cfg Config, topo Result) SB3Result {
	t0 := time.Now()
	a, root := runSB3Alignment(survivors, cfg, topo)
	if os.Getenv("TRACE_RECON_SB3DIAG") != "" {
		fmt.Fprintf(os.Stderr, "SB3STAGE final_align conflicts=%d elapsed=%s\n", a.conflicts, time.Since(t0))
	}
	t0 = time.Now()
	out := finishSB3Alignment(SB3Result{Topology: topo}, a, root, cfg)
	if os.Getenv("TRACE_RECON_SB3DIAG") != "" {
		fmt.Fprintf(os.Stderr, "SB3STAGE final_materialize elapsed=%s\n", time.Since(t0))
	}
	return out
}

func finishSB3Alignment(out SB3Result, a *sb3Aligner, root uint64, cfg Config) SB3Result {
	// Give any conflicted/unconstrained fanout a deterministic total labeling so
	// callers can inspect the tree, while Compatible prevents structure scoring.
	for parent, kids := range a.children {
		if len(kids) == 1 && a.ord[kids[0]] == 0 {
			a.ord[kids[0]] = 1
			a.implicit++
		}
		used := make(map[int]bool)
		for _, child := range kids {
			if a.ord[child] > 0 {
				used[a.ord[child]] = true
			}
		}
		next := 1
		for _, child := range kids {
			if a.ord[child] != 0 {
				continue
			}
			for used[next] {
				next++
			}
			a.ord[child] = next
			used[next] = true
		}
		_ = parent
	}

	fpBits := cfg.FPBits
	if fpBits <= 0 {
		fpBits = 16
	}
	var build func(uint64) *SBNode
	building := make(map[uint64]bool)
	build = func(id uint64) *SBNode {
		n := newSBNode(a.ord[id])
		if building[id] {
			a.conflictAt(id, "cycle while materializing reconstructed node %016x", id)
			return n
		}
		building[id] = true
		defer delete(building, id)
		if a.surv[id] {
			n.RealID = id
		}
		if !out.Topology.ReconAnon[id] {
			n.FP = id >> uint(64-fpBits)
			n.FPBits = fpBits
		}
		n.EE = append([]int(nil), a.ee[id]...)
		for _, child := range a.children[id] {
			cn := build(child)
			n.Children[cn.Ord] = cn
		}
		return n
	}
	var sbRoot *SBNode
	if root != 0 {
		sbRoot = build(root)
		sbRoot.Ord = 0
	}
	out.Structure = SBResult{Root: sbRoot, FPBits: fpBits, LehmerEE: cfg.SBridgeLehmer}
	out.Compatible = a.conflicts == 0
	out.Reason = a.firstError
	out.OrdinalPlaced = a.placed
	out.ImplicitOrdinals = a.implicit
	out.Fanouts = a.fanouts
	out.Conflicts = a.conflicts
	out.conflictNodes = a.conflictNodes
	return out
}
