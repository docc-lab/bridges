package loader

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Raw shapes match Jaeger's JSON exactly. We only decode the fields we need.
type rawSpan struct {
	SpanID     string   `json:"spanID"`
	References []rawRef `json:"references"`
	StartTime  int64    `json:"startTime"` // microseconds
	Duration   int64    `json:"duration"`  // microseconds
	ProcessID  string   `json:"processID"`
}

type rawRef struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
}

type rawProcess struct {
	ServiceName string `json:"serviceName"`
}

type rawTrace struct {
	TraceID   string                `json:"traceID"`
	Spans     []rawSpan             `json:"spans"`
	Processes map[string]rawProcess `json:"processes"`
}

type rawWrapper struct {
	Data []rawTrace `json:"data"`
}

// Span is the per-span representation the simulator consumes.
type Span struct {
	SpanID    uint64
	ParentID  uint64 // 0 = root
	StartNS   int64
	EndNS     int64
	ServiceID uint16
	Depth     int // BFS tree depth (sort tie-break only)
}

// Trace is one normalized trace.
type Trace struct {
	TraceID    uint64
	Spans      []Span
	SourceFile string // optional, for log output
	Flags      uint8  // lenient-mode repair markers (see FlagXxx)
}

// Lenient-mode repair markers, recorded on Trace.Flags when requireClean is
// false. They let downstream consumers include or exclude repaired traces on
// demand, instead of those traces being silently dropped or silently kept.
const (
	// FlagPrunedDangling: one or more dangling-parent subtrees were dropped — a
	// span whose parent is absent from the trace, and everything beneath it.
	// (A trace containing a cycle is rejected outright, not flagged.)
	FlagPrunedDangling uint8 = 1 << 0
	// FlagDedupedSpans: identical duplicate span IDs were collapsed to one copy.
	FlagDedupedSpans uint8 = 1 << 1
)

// ServiceTable interns service names to compact uint16 IDs.
type ServiceTable struct {
	NameToID map[string]uint16
	IDToName []string
}

func NewServiceTable() *ServiceTable {
	return &ServiceTable{NameToID: make(map[string]uint16)}
}

func (s *ServiceTable) Intern(name string) uint16 {
	if id, ok := s.NameToID[name]; ok {
		return id
	}
	id := uint16(len(s.IDToName))
	s.NameToID[name] = id
	s.IDToName = append(s.IDToName, name)
	return id
}

func (s *ServiceTable) Name(id uint16) string {
	if int(id) >= len(s.IDToName) {
		return ""
	}
	return s.IDToName[id]
}

// LoadTraceFile reads one Jaeger trace JSON file and returns 0+ normalized
// Trace structs. Handles both wrapper {"data": [...]} and bare-trace shapes.
//
// services is shared across files so the same name maps to the same id.
// requireClean applies the cleanliness filter (Python _clean_trace parity):
// dirty traces are dropped, except multi-root traces, which are salvaged by
// keeping only the largest tree rooted at one of the roots.
func LoadTraceFile(path string, services *ServiceTable, requireClean bool) ([]Trace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	// Try wrapper first.
	var w rawWrapper
	if err := json.Unmarshal(data, &w); err == nil && len(w.Data) > 0 {
		out := make([]Trace, 0, len(w.Data))
		for _, rt := range w.Data {
			t, ok, err := normalizeTrace(rt, services, requireClean)
			if err != nil {
				return nil, err
			}
			if ok {
				t.SourceFile = path
				out = append(out, t)
			}
		}
		return out, nil
	}

	// Bare trace.
	var rt rawTrace
	if err := json.Unmarshal(data, &rt); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	t, ok, err := normalizeTrace(rt, services, requireClean)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	t.SourceFile = path
	return []Trace{t}, nil
}

// normalizeTrace converts raw fields to the runtime Span shape, computing
// parent_id from the first CHILD_OF reference, start/end nanos from
// startTime/duration microseconds, service_id from processes table, and tree
// depth via BFS for the sort tie-break.
//
// Returns (trace, true, nil) on success; (zero, false, nil) if the trace is
// dropped by the cleanliness filter; (zero, false, err) on hard parse errors.
func normalizeTrace(rt rawTrace, services *ServiceTable, requireClean bool) (Trace, bool, error) {
	if len(rt.Spans) == 0 {
		return Trace{}, false, nil
	}

	// Pre-compute string parent_span_id per span (matching Python normalize_span).
	type sx struct {
		raw      *rawSpan
		spanID   uint64
		parentID uint64
		parentH  string // hex parent string for cleanliness check
		spanH    string
		coRefs   []string // distinct CHILD_OF refs, for clean check
	}
	xs := make([]sx, len(rt.Spans))
	idSet := make(map[string]struct{}, len(rt.Spans))
	for i := range rt.Spans {
		s := &rt.Spans[i]
		if s.SpanID == "" {
			if requireClean {
				return Trace{}, false, nil // dirty: missing span id
			}
		}
		if requireClean {
			if _, dup := idSet[s.SpanID]; dup {
				// Duplicate span ID. OpenTracing/Jaeger requires uniqueness
				// within a trace; reject the whole trace as untrustworthy.
				return Trace{}, false, nil
			}
		}
		idSet[s.SpanID] = struct{}{}
	}

	for i := range rt.Spans {
		s := &rt.Spans[i]
		var parentH string
		var coRefs []string
		// Dedupe CHILD_OF refs by spanID — duplicate refs to the same parent
		// are an instrumentation artifact (the same parent listed multiple
		// times), not real DAG topology. Only count distinct parents for the
		// cleanliness check.
		seenCO := make(map[string]struct{})
		for _, r := range s.References {
			if r.RefType != "CHILD_OF" || r.SpanID == "" {
				continue
			}
			if _, dup := seenCO[r.SpanID]; dup {
				continue
			}
			seenCO[r.SpanID] = struct{}{}
			coRefs = append(coRefs, r.SpanID)
		}
		// First CHILD_OF wins (Python uses the first one found).
		if len(coRefs) > 0 {
			parentH = coRefs[0]
		}

		spanID, err := parseHex64(s.SpanID)
		if err != nil {
			if requireClean {
				return Trace{}, false, nil
			}
			return Trace{}, false, fmt.Errorf("bad spanID %q: %w", s.SpanID, err)
		}
		var parentID uint64
		if parentH != "" {
			parentID, err = parseHex64(parentH)
			if err != nil {
				if requireClean {
					return Trace{}, false, nil
				}
				return Trace{}, false, fmt.Errorf("bad parent spanID %q: %w", parentH, err)
			}
		}
		xs[i] = sx{raw: s, spanID: spanID, parentID: parentID, parentH: parentH, spanH: s.SpanID, coRefs: coRefs}
	}

	// Cleanliness filter (Python _clean_trace). Order matters:
	// C5 (duplicate span IDs, checked above) and C2 (dangling parent
	// reference anywhere in the trace) are trace-fatal first. Only then is
	// multi-root salvage considered: instead of rejecting a multi-root
	// trace, keep the largest tree rooted at one of the roots and drop
	// every other span. The kept tree must itself pass the per-span
	// C3/C4 checks. Single-root traces get the original strict filter:
	// any dirty span anywhere rejects the trace.
	if requireClean {
		var roots []int
		children := make(map[string][]int)
		for i, x := range xs {
			if x.parentH == "" {
				if len(x.coRefs) > 0 {
					return Trace{}, false, nil
				}
				roots = append(roots, i)
			} else {
				if _, ok := idSet[x.parentH]; !ok {
					// C2: dangling parent reference rejects the whole
					// trace, before any multi-root salvage.
					return Trace{}, false, nil
				}
				children[x.parentH] = append(children[x.parentH], i)
			}
		}
		if len(roots) == 0 {
			return Trace{}, false, nil
		}
		if len(roots) == 1 {
			for _, x := range xs {
				if len(x.coRefs) > 1 {
					return Trace{}, false, nil
				}
				if len(x.coRefs) > 0 && x.coRefs[0] != x.parentH {
					return Trace{}, false, nil
				}
			}
		} else {
			// Size each root's tree (spans reachable via parent links);
			// ties broken by lowest numeric root span ID for determinism.
			best, bestSize := -1, 0
			var bestKeep []bool
			for _, r := range roots {
				keep := make([]bool, len(xs))
				size := 0
				stack := []int{r}
				for len(stack) > 0 {
					cur := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					if keep[cur] {
						continue
					}
					keep[cur] = true
					size++
					stack = append(stack, children[xs[cur].spanH]...)
				}
				if size > bestSize || (size == bestSize && xs[r].spanID < xs[best].spanID) {
					best, bestSize, bestKeep = r, size, keep
				}
			}
			kept := make([]sx, 0, bestSize)
			for i, x := range xs {
				if !bestKeep[i] {
					continue
				}
				if len(x.coRefs) > 1 {
					return Trace{}, false, nil
				}
				if len(x.coRefs) > 0 && x.coRefs[0] != x.parentH {
					return Trace{}, false, nil
				}
				kept = append(kept, x)
			}
			xs = kept
		}
	}

	// Resolve service IDs.
	traceIDU64, err := parseHex64(rt.TraceID)
	if err != nil {
		if requireClean {
			return Trace{}, false, nil
		}
		return Trace{}, false, fmt.Errorf("bad traceID %q: %w", rt.TraceID, err)
	}

	out := Trace{TraceID: traceIDU64, Spans: make([]Span, len(xs))}
	for i, x := range xs {
		svcName := "missing_service"
		if proc, ok := rt.Processes[x.raw.ProcessID]; ok && proc.ServiceName != "" {
			svcName = proc.ServiceName
		}
		serviceID := services.Intern(svcName)

		// Python normalize_span treats startTime as microseconds when < 1e15
		// and divides by 1000 otherwise (assumes nanoseconds). For Uber's
		// 2024-era microsecond timestamps (~1.7e15) this is the divide branch:
		// `start_us = startTime // 1000`, then `start_time_ns = start_us * 1000`,
		// effectively floor-rounding away the lowest-order microsecond digits.
		// Replicating that exact lossy transform so the event sort tie-breaks
		// match Python's.
		startUS := x.raw.StartTime
		if startUS >= 1_000_000_000_000_000 {
			startUS = startUS / 1000
		}
		durUS := x.raw.Duration
		out.Spans[i] = Span{
			SpanID:    x.spanID,
			ParentID:  x.parentID,
			StartNS:   startUS * 1000,
			EndNS:     startUS*1000 + durUS*1000,
			ServiceID: serviceID,
		}
	}

	// Lenient (non-strict) guards. Strict mode already rejected dirty traces
	// above (C2/C5/...), so these only fire when requireClean is false. We keep
	// the trace but discard its bad fragments rather than dropping the whole
	// thing:
	//   1. Collapse identical duplicate span IDs (same parent/timing/service)
	//      to a single copy; reject the trace only if a duplicate ID carries
	//      conflicting data.
	//   2. Drop dangling-parent subtrees and any cyclic component — i.e. keep
	//      only spans reachable from a real root (ParentID==0). This is also
	//      what makes computeDepths' BFS safe on otherwise-cyclic input.
	if !requireClean {
		before := len(out.Spans)
		var ok bool
		out.Spans, ok = dedupSpans(out.Spans)
		if !ok {
			return Trace{}, false, nil // duplicate span ID with conflicting data
		}
		if len(out.Spans) != before {
			out.Flags |= FlagDedupedSpans
		}
		// A cycle in the parent links is a corrupt trace, not a repairable
		// fragment — reject the whole thing.
		if hasParentCycle(out.Spans) {
			return Trace{}, false, nil
		}
		before = len(out.Spans)
		out.Spans = pruneToReachable(out.Spans)
		if len(out.Spans) != before {
			out.Flags |= FlagPrunedDangling
		}
		if len(out.Spans) == 0 {
			return Trace{}, false, nil // nothing reachable from a real root
		}
	}

	// CRISP-style preprocessing: clip children to parent windows and drop
	// subtrees fully outside parent. Mirrors trace_simulator.py
	// crisp_normalize_spans (Zhang et al., USENIX ATC '22, §5.2).
	out.Spans = crispNormalizeSpans(out.Spans)

	// BFS depth.
	computeDepths(out.Spans)

	return out, true, nil
}

// crispNormalizeSpans applies CRISP's three rules to enforce strict nesting:
//
//  1. child.start < parent.start  ->  child.start = parent.start
//  2. child.end   > parent.end    ->  child.end   = parent.end
//  3. child fully outside parent's window  ->  drop entire subtree
//
// Pre-order: each child sees its parent's *post-clip* bounds, so cascading
// truncation through descendants happens in one pass. Returns the surviving
// span slice; modifies StartNS/EndNS in place on retained spans.
func crispNormalizeSpans(spans []Span) []Span {
	if len(spans) == 0 {
		return spans
	}
	idIdx := make(map[uint64]int, len(spans))
	for i := range spans {
		idIdx[spans[i].SpanID] = i
	}
	children := make(map[uint64][]int, len(spans))
	var roots []int
	for i, s := range spans {
		if s.ParentID == 0 {
			roots = append(roots, i)
			continue
		}
		if _, ok := idIdx[s.ParentID]; !ok {
			roots = append(roots, i)
			continue
		}
		children[s.ParentID] = append(children[s.ParentID], i)
	}

	dropped := make(map[uint64]struct{})
	dropSubtree := func(rootIdx int) {
		s := []int{rootIdx}
		for len(s) > 0 {
			cur := s[len(s)-1]
			s = s[:len(s)-1]
			sid := spans[cur].SpanID
			if _, seen := dropped[sid]; seen {
				continue
			}
			dropped[sid] = struct{}{}
			for _, ch := range children[sid] {
				s = append(s, ch)
			}
		}
	}

	// Iterative pre-order: stack of (idx, parent_bounds_known, ps, pe).
	type frame struct {
		idx        int
		hasBounds  bool
		ps, pe     int64
	}
	stack := make([]frame, 0, len(spans))
	for i := len(roots) - 1; i >= 0; i-- {
		stack = append(stack, frame{roots[i], false, 0, 0})
	}
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		sid := spans[top.idx].SpanID
		if _, seen := dropped[sid]; seen {
			continue
		}
		if top.hasBounds {
			if spans[top.idx].EndNS < top.ps || spans[top.idx].StartNS > top.pe {
				dropSubtree(top.idx)
				continue
			}
			if spans[top.idx].StartNS < top.ps {
				spans[top.idx].StartNS = top.ps
			}
			if spans[top.idx].EndNS > top.pe {
				spans[top.idx].EndNS = top.pe
			}
		}
		for _, ch := range children[sid] {
			stack = append(stack, frame{ch, true, spans[top.idx].StartNS, spans[top.idx].EndNS})
		}
	}

	if len(dropped) == 0 {
		return spans
	}
	out := spans[:0]
	for i := range spans {
		if _, gone := dropped[spans[i].SpanID]; gone {
			continue
		}
		out = append(out, spans[i])
	}
	return out
}

// computeDepths fills Span.Depth via BFS from roots.
func computeDepths(spans []Span) {
	idIdx := make(map[uint64]int, len(spans))
	for i := range spans {
		idIdx[spans[i].SpanID] = i
	}
	children := make(map[uint64][]int, len(spans))
	var roots []int
	for i, s := range spans {
		if s.ParentID == 0 {
			roots = append(roots, i)
			continue
		}
		if _, ok := idIdx[s.ParentID]; !ok {
			// Dangling parent — treat as root for depth purposes.
			roots = append(roots, i)
			continue
		}
		children[s.ParentID] = append(children[s.ParentID], i)
	}
	for _, r := range roots {
		spans[r].Depth = 0
	}
	// visited guard: lenient-mode input can still contain a cyclic component;
	// without this the BFS would revisit it forever (the hang we hit on the
	// unfiltered build). pruneToReachable normally strips cycles before we get
	// here, so this is belt-and-suspenders.
	visited := make(map[uint64]struct{}, len(spans))
	for _, r := range roots {
		visited[spans[r].SpanID] = struct{}{}
	}
	queue := append([]int(nil), roots...)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		curID := spans[cur].SpanID
		curD := spans[cur].Depth
		for _, ci := range children[curID] {
			if _, seen := visited[spans[ci].SpanID]; seen {
				continue
			}
			visited[spans[ci].SpanID] = struct{}{}
			spans[ci].Depth = curD + 1
			queue = append(queue, ci)
		}
	}
}

// dedupSpans collapses duplicate span IDs (lenient mode only). Two spans
// sharing an ID with otherwise identical data are an instrumentation artifact:
// keep the first, drop the rest. A duplicate ID whose data conflicts makes the
// trace untrustworthy -> ok=false, and the caller drops the whole trace.
func dedupSpans(spans []Span) (out []Span, ok bool) {
	seen := make(map[uint64]int, len(spans)) // spanID -> index in out
	out = make([]Span, 0, len(spans))
	for _, s := range spans {
		if j, dup := seen[s.SpanID]; dup {
			if out[j] != s {
				return nil, false
			}
			continue
		}
		seen[s.SpanID] = len(out)
		out = append(out, s)
	}
	return out, true
}

// hasParentCycle reports whether following ParentID links (over parents present
// in the trace) ever forms a cycle. The parent relation is single-valued, so
// this is a linear functional-graph walk: mark each node on the current chain;
// a revisit of a marked node is a back-edge, hence a cycle. Run after dedupSpans
// so idIdx is unambiguous.
func hasParentCycle(spans []Span) bool {
	idIdx := make(map[uint64]int, len(spans))
	for i := range spans {
		idIdx[spans[i].SpanID] = i
	}
	const (
		unvisited int8 = iota
		onPath
		done
	)
	state := make([]int8, len(spans))
	for start := range spans {
		if state[start] != unvisited {
			continue
		}
		var path []int
		i := start
		for {
			if state[i] == onPath {
				return true // back-edge onto the current chain: cycle
			}
			if state[i] == done {
				break // merges into an already-cleared chain
			}
			state[i] = onPath
			path = append(path, i)
			pid := spans[i].ParentID
			if pid == 0 {
				break
			}
			pj, ok := idIdx[pid]
			if !ok {
				break // dangling parent: chain ends cleanly
			}
			i = pj
		}
		for _, n := range path {
			state[n] = done
		}
	}
	return false
}

// pruneToReachable keeps only spans reachable from a real root (ParentID==0)
// by walking parent->child edges (lenient mode only). This discards
// dangling-parent subtrees — a span whose parent is absent from the trace, and
// everything beneath it. Callers reject cyclic traces first (hasParentCycle),
// so every unreached span here is dangling-caused. Run after dedupSpans so
// idIdx is unambiguous.
func pruneToReachable(spans []Span) []Span {
	if len(spans) == 0 {
		return spans
	}
	idIdx := make(map[uint64]int, len(spans))
	for i := range spans {
		idIdx[spans[i].SpanID] = i
	}
	children := make(map[uint64][]int, len(spans))
	var roots []int
	for i, s := range spans {
		if s.ParentID == 0 {
			roots = append(roots, i)
			continue
		}
		if _, ok := idIdx[s.ParentID]; !ok {
			continue // dangling parent: unreachable, and not a root
		}
		children[s.ParentID] = append(children[s.ParentID], i)
	}
	reached := make([]bool, len(spans))
	stack := make([]int, 0, len(roots))
	for _, r := range roots {
		reached[r] = true
		stack = append(stack, r)
	}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, ci := range children[spans[cur].SpanID] {
			if reached[ci] {
				continue
			}
			reached[ci] = true
			stack = append(stack, ci)
		}
	}
	nReached := 0
	for _, ok := range reached {
		if ok {
			nReached++
		}
	}
	if nReached == len(spans) {
		return spans // nothing pruned; keep the slice as-is
	}
	out := spans[:0]
	for i := range spans {
		if reached[i] {
			out = append(out, spans[i])
		}
	}
	return out
}

func parseHex64(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("empty hex")
	}
	return strconv.ParseUint(s, 16, 64)
}
