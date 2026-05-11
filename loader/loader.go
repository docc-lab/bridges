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
}

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
// requireClean drops traces that fail _trace_is_clean (Python parity).
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

	// Cleanliness filter (Python _trace_is_clean).
	if requireClean {
		var roots int
		for _, x := range xs {
			if len(x.coRefs) > 1 {
				return Trace{}, false, nil
			}
			if x.parentH == "" {
				if len(x.coRefs) > 0 {
					return Trace{}, false, nil
				}
				roots++
			} else {
				if _, ok := idSet[x.parentH]; !ok {
					return Trace{}, false, nil
				}
				if len(x.coRefs) > 0 && x.coRefs[0] != x.parentH {
					return Trace{}, false, nil
				}
			}
		}
		if roots != 1 {
			return Trace{}, false, nil
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
	queue := append([]int(nil), roots...)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		curID := spans[cur].SpanID
		curD := spans[cur].Depth
		for _, ci := range children[curID] {
			spans[ci].Depth = curD + 1
			queue = append(queue, ci)
		}
	}
}

func parseHex64(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("empty hex")
	}
	return strconv.ParseUint(s, 16, 64)
}
