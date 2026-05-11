// Command trace_clean_report samples random Uber trace JSON files and
// reports how many violate each individual cleanliness-filter condition,
// independently. A trace can violate more than one condition; conditions
// are tallied separately so we can see which one drives the rejection
// rate.
//
// Conditions tracked (mirrors trace_simulator.py _trace_is_clean):
//
//	C1. number of root spans != 1 (zero or multiple roots)
//	C2. some CHILD_OF reference points to a span ID not present in the trace
//	C3. some span has more than one CHILD_OF reference
//	C4. some span's CHILD_OF reference disagrees with parentSpanID (trivial
//	    on Uber data: no parentSpanID field, so parent_span_id is *defined*
//	    as the first CHILD_OF)
//	C5. some span ID appears more than once in the trace
//
// Usage:
//
//	go run ./cmd/trace_clean_report --src /mydata/uber/traces/traces-sanitized/ --n 10000
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

type rawRef struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
}

type rawSpan struct {
	SpanID     string   `json:"spanID"`
	TraceID    string   `json:"traceID"`
	References []rawRef `json:"references"`
	ProcessID  string   `json:"processID"`
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

type violations struct {
	C1, C2, C3, C4, C5 bool
}

func checkConditions(spans []rawSpan) violations {
	v := violations{}
	if len(spans) == 0 {
		v.C1 = true
		return v
	}

	idCounts := make(map[string]int, len(spans))
	for _, s := range spans {
		idCounts[s.SpanID]++
	}
	for _, c := range idCounts {
		if c > 1 {
			v.C5 = true
			break
		}
	}
	idSet := make(map[string]struct{}, len(idCounts))
	for sid := range idCounts {
		idSet[sid] = struct{}{}
	}

	roots := 0
	for _, s := range spans {
		// Dedupe CHILD_OF refs by spanID — duplicate refs to the same parent
		// are instrumentation artifacts, not real multi-parent topology.
		var childOf []string
		seen := make(map[string]struct{})
		for _, r := range s.References {
			if r.RefType != "CHILD_OF" || r.SpanID == "" {
				continue
			}
			if _, dup := seen[r.SpanID]; dup {
				continue
			}
			seen[r.SpanID] = struct{}{}
			childOf = append(childOf, r.SpanID)
		}
		if len(childOf) > 1 {
			v.C3 = true
		}
		// Normalized parent_span_id = first CHILD_OF (Uber has no parentSpanID).
		var pid string
		if len(childOf) > 0 {
			pid = childOf[0]
		}
		if pid == "" {
			roots++
		} else if _, ok := idSet[pid]; !ok {
			v.C2 = true
		}
		// C4 is trivially satisfied for Uber (no parentSpanID); leave false.
	}
	if roots != 1 {
		v.C1 = true
	}
	return v
}

// multiParentStats characterizes the multi-CHILD_OF spans found in a trace.
// We compute these only when the trace has at least one such span (C3 hit).
//
// For each multi-parent span we ask: is it a leaf (no other span lists it as
// a CHILD_OF parent), or an internal node? Internal multi-parents drag a
// subtree along; leaves are typical fan-in/coalescing/batch endpoints.
type multiParentStats struct {
	totalMultiParent int
	leafCount        int
	internalCount    int
	subtreeSizes     []int // descendants for each internal multi-parent
	directChildren   []int // direct child count for each internal multi-parent

	// Per-multi-parent classification of CHILD_OF references:
	allPresent  int // all N referenced parent IDs exist in this trace
	someMissing int // at least one parent is missing (some present, some not)
	allMissing  int // all referenced parents are absent (full cross-trace ref)

	// Per-CHILD_OF-reference tally: total refs across all multi-parent spans,
	// and how many of those refs resolve within the trace.
	totalRefs   int
	presentRefs int

	// Fan-in factor: per multi-parent span, how many CHILD_OF refs does it
	// have (always >= 2, since 1 ref is just a normal parent).
	refCounts []int

	// Service triples for each multi-parent span:
	//   joinService      — service of the multi-parent (fan-in) span itself
	//   parentServices   — the set of services owning each CHILD_OF parent,
	//                       canonicalized as a sorted "svcA|svcB|..." string
	// Missing-service cases are recorded as "?".
	joinServices  []string
	parentSvcSets []string

	// LCA (lowest common ancestor of the multi-parent span's two parents,
	// walking up via the first CHILD_OF reference at each step). Tells us
	// whether the fan-OUT point that branched into the two paths converging
	// at this fan-IN is in the same service or a different service.
	// "lcaSameService" = LCA service matches the join span's service.
	lcaSameService  int
	lcaCrossService int
	lcaNotFound     int            // P1 and P2 didn't share an ancestor
	lcaSvcCounts    map[string]int // LCA service -> count

	// For cross-service fan-ins only: the "LCA_svc -> join_svc" edge.
	crossSvcEdges []string

	// Branch-length characterization (intra-service fan-ins only). For each
	// fan-in we record (distA, distB) where distX = number of hops from
	// parent X up to the LCA via firstParentOf. Perfect diamond = both = 1.
	// minBranch/maxBranch flatten this for quantile output.
	branchPairs []string // e.g. "(1,1)", "(1,3)" — sorted within pair
	minBranches []int
	maxBranches []int
	diamondCount int
}

// analyzeMultiParent runs the multi-parent leaf/internal classification on a
// trace's span list. Cheap: O(N) once we've built the parent->children map.
func analyzeMultiParent(spans []rawSpan, processes map[string]rawProcess) multiParentStats {
	st := multiParentStats{}
	if len(spans) == 0 {
		return st
	}
	// Build spanID -> serviceName via processID -> processes table.
	svcOf := make(map[string]string, len(spans))
	for _, s := range spans {
		svc := "?"
		if p, ok := processes[s.ProcessID]; ok && p.ServiceName != "" {
			svc = p.ServiceName
		}
		svcOf[s.SpanID] = svc
	}
	// Build span_id set so we can ask whether referenced parent IDs are
	// present in this trace.
	idSet := make(map[string]struct{}, len(spans))
	for _, s := range spans {
		idSet[s.SpanID] = struct{}{}
	}

	// Build parent_id -> []child_id map by reading the FIRST CHILD_OF of each
	// span (matching how the simulator's normalizer derives parent_span_id),
	// and capture full ref lists for spans with multiple CHILD_OFs. Also
	// build a child -> first-parent map for LCA walks.
	children := make(map[string][]string, len(spans))
	firstParentOf := make(map[string]string, len(spans))
	type multiParentEntry struct {
		spanID string
		refs   []string // all CHILD_OF parent IDs for this span (>1 entries)
	}
	var multiParents []multiParentEntry
	for _, s := range spans {
		var firstParent string
		var coRefs []string
		for _, r := range s.References {
			if r.RefType == "CHILD_OF" && r.SpanID != "" {
				if len(coRefs) == 0 {
					firstParent = r.SpanID
				}
				coRefs = append(coRefs, r.SpanID)
			}
		}
		if len(coRefs) > 1 {
			multiParents = append(multiParents, multiParentEntry{spanID: s.SpanID, refs: coRefs})
		}
		if firstParent != "" {
			children[firstParent] = append(children[firstParent], s.SpanID)
			firstParentOf[s.SpanID] = firstParent
		}
	}
	st.totalMultiParent = len(multiParents)
	st.lcaSvcCounts = make(map[string]int)

	// Classify each multi-parent span by how many of its referenced parents
	// are actually present in the trace.
	for _, mp := range multiParents {
		present := 0
		for _, pid := range mp.refs {
			if _, ok := idSet[pid]; ok {
				present++
			}
		}
		n := len(mp.refs)
		st.totalRefs += n
		st.presentRefs += present
		st.refCounts = append(st.refCounts, n)
		switch {
		case present == n:
			st.allPresent++
		case present == 0:
			st.allMissing++
		default:
			st.someMissing++
		}

		// Service triple: the join (fan-in) span's service + the canonicalized
		// set of its parent services.
		joinSvc := svcOf[mp.spanID]
		st.joinServices = append(st.joinServices, joinSvc)
		parentSvcs := make([]string, 0, len(mp.refs))
		for _, pid := range mp.refs {
			if s, ok := svcOf[pid]; ok {
				parentSvcs = append(parentSvcs, s)
			} else {
				parentSvcs = append(parentSvcs, "?")
			}
		}
		sort.Strings(parentSvcs)
		st.parentSvcSets = append(st.parentSvcSets, strings.Join(parentSvcs, "|"))

		// LCA of the parents (the fan-OUT point above this fan-IN). Walk up
		// from each parent following the first-CHILD_OF chain, recording
		// distance from each parent to its ancestor so we can characterize
		// branch length (perfect diamond vs asymmetric vs long).
		if len(mp.refs) >= 2 {
			a, b := mp.refs[0], mp.refs[1]
			distA := map[string]int{a: 0}
			cur, d := a, 0
			for {
				p, ok := firstParentOf[cur]
				if !ok {
					break
				}
				d++
				if _, exists := distA[p]; !exists {
					distA[p] = d
				}
				cur = p
			}
			lca := ""
			distFromB := -1
			cur, d = b, 0
			for {
				if _, hit := distA[cur]; hit {
					lca = cur
					distFromB = d
					break
				}
				p, ok := firstParentOf[cur]
				if !ok {
					break
				}
				cur = p
				d++
			}
			if lca == "" {
				st.lcaNotFound++
			} else {
				lcaSvc := svcOf[lca]
				st.lcaSvcCounts[lcaSvc]++
				if lcaSvc == joinSvc {
					st.lcaSameService++
				} else {
					st.lcaCrossService++
					st.crossSvcEdges = append(st.crossSvcEdges,
						lcaSvc+" -> "+joinSvc)
				}
				// Branch lengths: distance from each parent to the LCA.
				dA := distA[lca]
				dB := distFromB
				lo, hi := dA, dB
				if lo > hi {
					lo, hi = hi, lo
				}
				st.minBranches = append(st.minBranches, lo)
				st.maxBranches = append(st.maxBranches, hi)
				if dA == 1 && dB == 1 {
					st.diamondCount++
				}
				// Pair string for histogramming, sorted (lo, hi).
				st.branchPairs = append(st.branchPairs, fmt.Sprintf("(%d,%d)", lo, hi))
			}
		}
	}

	// For each multi-parent span, check leaf/internal and subtree size.
	for _, mp := range multiParents {
		direct := children[mp.spanID]
		if len(direct) == 0 {
			st.leafCount++
			continue
		}
		st.internalCount++
		st.directChildren = append(st.directChildren, len(direct))
		// BFS subtree size.
		size := 0
		stack := append([]string(nil), direct...)
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			size++
			stack = append(stack, children[cur]...)
		}
		st.subtreeSizes = append(st.subtreeSizes, size)
	}
	return st
}

// multiRootStats characterizes traces that have more than one root span.
// Computed only for C1-violating traces.
type multiRootStats struct {
	numRoots             int   // number of root spans
	numComponents        int   // connected components in the deduped CHILD_OF graph (undirected)
	componentSizes       []int // size of each connected component
	rootsInDistinctComps bool  // true if every root is in its own component
	mismatchedTraceIDs   bool  // true if any span's per-span traceID differs from wrapper's
	wrapperTraceID       string
}

// analyzeMultiRoot runs on a C1-violating trace.
func analyzeMultiRoot(rt rawTrace) multiRootStats {
	st := multiRootStats{wrapperTraceID: rt.TraceID, rootsInDistinctComps: true}
	if len(rt.Spans) == 0 {
		return st
	}

	// Adjacency: undirected edges from each span to its CHILD_OF parents
	// (deduped). We only add edges where the parent is in this trace's
	// spanID set, so dangling refs don't connect anything.
	idSet := make(map[string]struct{}, len(rt.Spans))
	for _, s := range rt.Spans {
		idSet[s.SpanID] = struct{}{}
		// per-span traceID consistency
		if s.TraceID != "" && rt.TraceID != "" && s.TraceID != rt.TraceID {
			st.mismatchedTraceIDs = true
		}
	}
	adj := make(map[string][]string, len(rt.Spans))
	rootSet := make(map[string]struct{})
	for _, s := range rt.Spans {
		seen := make(map[string]struct{})
		hasParent := false
		for _, r := range s.References {
			if r.RefType != "CHILD_OF" || r.SpanID == "" {
				continue
			}
			if _, dup := seen[r.SpanID]; dup {
				continue
			}
			seen[r.SpanID] = struct{}{}
			if _, ok := idSet[r.SpanID]; ok {
				adj[s.SpanID] = append(adj[s.SpanID], r.SpanID)
				adj[r.SpanID] = append(adj[r.SpanID], s.SpanID)
				hasParent = true
			}
		}
		if !hasParent {
			rootSet[s.SpanID] = struct{}{}
		}
	}
	st.numRoots = len(rootSet)

	// Connected components via BFS on the undirected adjacency.
	visited := make(map[string]struct{}, len(rt.Spans))
	for _, s := range rt.Spans {
		if _, seen := visited[s.SpanID]; seen {
			continue
		}
		// New component
		size := 0
		rootsHere := 0
		queue := []string{s.SpanID}
		visited[s.SpanID] = struct{}{}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			size++
			if _, isRoot := rootSet[cur]; isRoot {
				rootsHere++
			}
			for _, nb := range adj[cur] {
				if _, ok := visited[nb]; !ok {
					visited[nb] = struct{}{}
					queue = append(queue, nb)
				}
			}
		}
		st.componentSizes = append(st.componentSizes, size)
		st.numComponents++
		if rootsHere > 1 {
			st.rootsInDistinctComps = false
		}
	}
	return st
}

type result struct {
	v   violations
	mp  multiParentStats
	mr  multiRootStats
	hasMR bool
	ok  bool
	err error
}

func parseFile(path string, out chan<- result) {
	data, err := os.ReadFile(path)
	if err != nil {
		out <- result{err: err}
		return
	}
	var w rawWrapper
	var traces []rawTrace
	if err := json.Unmarshal(data, &w); err == nil && len(w.Data) > 0 {
		traces = w.Data
	} else {
		var rt rawTrace
		if err := json.Unmarshal(data, &rt); err != nil {
			out <- result{err: err}
			return
		}
		traces = []rawTrace{rt}
	}
	for _, t := range traces {
		v := checkConditions(t.Spans)
		var mp multiParentStats
		var mr multiRootStats
		var hasMR bool
		if v.C3 && !v.C1 && !v.C2 && !v.C5 {
			mp = analyzeMultiParent(t.Spans, t.Processes)
		}
		if v.C1 {
			mr = analyzeMultiRoot(t)
			hasMR = true
		}
		out <- result{v: v, mp: mp, mr: mr, hasMR: hasMR, ok: true}
	}
}

func parseWorker(tasks <-chan string, out chan<- result, wg *sync.WaitGroup) {
	defer wg.Done()
	for p := range tasks {
		parseFile(p, out)
	}
}

func main() {
	var (
		src     = flag.String("src", "/mydata/uber/traces/traces-sanitized/", "Directory of trace JSON files")
		n       = flag.Int("n", 10000, "Number of files to sample")
		seed    = flag.Uint64("seed", 42, "Random seed for the file shuffle")
		workers = flag.Int("workers", runtime.NumCPU(), "Parallel parse workers")
	)
	flag.Parse()

	entries, err := os.ReadDir(*src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read dir: %v\n", err)
		os.Exit(1)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	rng := rand.New(rand.NewPCG(*seed, *seed))
	rng.Shuffle(len(files), func(i, j int) { files[i], files[j] = files[j], files[i] })
	if *n > 0 && len(files) > *n {
		files = files[:*n]
	}
	if *workers < 1 {
		*workers = 1
	}

	tasks := make(chan string, *workers*2)
	resultsCh := make(chan result, *workers*4)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go parseWorker(tasks, resultsCh, &wg)
	}
	go func() {
		for _, name := range files {
			tasks <- filepath.Join(*src, name)
		}
		close(tasks)
	}()
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Aggregate.
	var total, pass, fail, parseErrors int64
	condCounts := map[string]int64{"C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0}
	numViolDist := map[int]int64{}
	comboCounts := map[string]int64{}
	// Multi-parent (C3) span statistics, aggregated across all C3 traces.
	var mpTotal, mpLeafTotal, mpInternalTotal int64
	var mpSubtreeSizes []int
	var mpDirectChildren []int
	var mpAllPresent, mpSomeMissing, mpAllMissing int64
	var mpTotalRefs, mpPresentRefs int64
	var mpRefCounts []int
	joinSvcCounts := map[string]int64{}    // service of multi-parent span
	parentSvcSetCounts := map[string]int64{} // canonical "svcA|svcB" of parent services
	parentSvcAppearances := map[string]int64{} // how many fan-ins involve each service as a parent
	var lcaSame, lcaCross, lcaNotFound int64
	lcaSvcCounts := map[string]int64{}
	crossSvcEdgeCounts := map[string]int64{}
	var diamondCount int64
	var allMinBranches, allMaxBranches []int
	branchPairCounts := map[string]int64{}

	// Multi-root characterization aggregates.
	var mrTotal int64
	var mrRootsInDistinctComps int64
	var mrMismatchedTraceIDs int64
	var mrAllNumRoots []int
	var mrAllNumComps []int
	var mrAllCompSizes []int
	mrRootsCompsPair := map[string]int64{}

	for r := range resultsCh {
		if r.err != nil {
			parseErrors++
			continue
		}
		if !r.ok {
			continue
		}
		total++
		var violated []string
		if r.v.C1 {
			condCounts["C1"]++
			violated = append(violated, "C1")
		}
		if r.v.C2 {
			condCounts["C2"]++
			violated = append(violated, "C2")
		}
		if r.v.C3 {
			condCounts["C3"]++
			violated = append(violated, "C3")
			mpTotal += int64(r.mp.totalMultiParent)
			mpLeafTotal += int64(r.mp.leafCount)
			mpInternalTotal += int64(r.mp.internalCount)
			mpSubtreeSizes = append(mpSubtreeSizes, r.mp.subtreeSizes...)
			mpDirectChildren = append(mpDirectChildren, r.mp.directChildren...)
			mpAllPresent += int64(r.mp.allPresent)
			mpSomeMissing += int64(r.mp.someMissing)
			mpAllMissing += int64(r.mp.allMissing)
			mpTotalRefs += int64(r.mp.totalRefs)
			mpPresentRefs += int64(r.mp.presentRefs)
			mpRefCounts = append(mpRefCounts, r.mp.refCounts...)
			for _, js := range r.mp.joinServices {
				joinSvcCounts[js]++
			}
			for _, pset := range r.mp.parentSvcSets {
				parentSvcSetCounts[pset]++
				// Count each distinct service in the set once per fan-in.
				seen := map[string]bool{}
				for _, sv := range strings.Split(pset, "|") {
					if !seen[sv] {
						seen[sv] = true
						parentSvcAppearances[sv]++
					}
				}
			}
			lcaSame += int64(r.mp.lcaSameService)
			lcaCross += int64(r.mp.lcaCrossService)
			lcaNotFound += int64(r.mp.lcaNotFound)
			for k, c := range r.mp.lcaSvcCounts {
				lcaSvcCounts[k] += int64(c)
			}
			for _, e := range r.mp.crossSvcEdges {
				crossSvcEdgeCounts[e]++
			}
			diamondCount += int64(r.mp.diamondCount)
			allMinBranches = append(allMinBranches, r.mp.minBranches...)
			allMaxBranches = append(allMaxBranches, r.mp.maxBranches...)
			for _, p := range r.mp.branchPairs {
				branchPairCounts[p]++
			}
		}
		if r.hasMR {
			mrTotal++
			if r.mr.rootsInDistinctComps {
				mrRootsInDistinctComps++
			}
			if r.mr.mismatchedTraceIDs {
				mrMismatchedTraceIDs++
			}
			mrAllNumRoots = append(mrAllNumRoots, r.mr.numRoots)
			mrAllNumComps = append(mrAllNumComps, r.mr.numComponents)
			mrAllCompSizes = append(mrAllCompSizes, r.mr.componentSizes...)
			pair := fmt.Sprintf("roots=%d comps=%d", r.mr.numRoots, r.mr.numComponents)
			mrRootsCompsPair[pair]++
		}
		if r.v.C4 {
			condCounts["C4"]++
			violated = append(violated, "C4")
		}
		if r.v.C5 {
			condCounts["C5"]++
			violated = append(violated, "C5")
		}
		if len(violated) == 0 {
			pass++
		} else {
			fail++
			numViolDist[len(violated)]++
			comboCounts[strings.Join(violated, "+")]++
		}
	}

	pct := func(num, denom int64) float64 {
		if denom == 0 {
			return 0
		}
		return 100 * float64(num) / float64(denom)
	}

	fmt.Printf("Sampled files:  %d (seed=%d)\n", len(files), *seed)
	fmt.Printf("Workers:        %d\n", *workers)
	fmt.Printf("Parse errors:   %d\n", parseErrors)
	fmt.Printf("Traces parsed:  %d\n", total)
	fmt.Printf("  PASS:         %d (%.2f%%)\n", pass, pct(pass, total))
	fmt.Printf("  FAIL (>=1):   %d (%.2f%%)\n", fail, pct(fail, total))

	fmt.Println()
	fmt.Println("Per-condition violations (a trace can violate multiple):")
	desc := map[string]string{
		"C1": "root count != 1",
		"C2": "dangling CHILD_OF reference",
		"C3": "span has > 1 CHILD_OF reference",
		"C4": "CHILD_OF disagrees with parentSpanID (trivial on Uber)",
		"C5": "duplicate span ID within trace",
	}
	fmt.Printf("  %-4s %10s %10s  %s\n", "cond", "count", "% of all", "description")
	for _, k := range []string{"C1", "C2", "C3", "C4", "C5"} {
		c := condCounts[k]
		fmt.Printf("  %-4s %10d %9.2f%%  %s\n", k, c, pct(c, total), desc[k])
	}

	fmt.Println()
	fmt.Println("Distribution of #violations per failing trace:")
	for n := 1; n <= 5; n++ {
		if c, ok := numViolDist[n]; ok && c > 0 {
			fmt.Printf("  %d violation(s): %d (%.2f%% of failing)\n", n, c, pct(c, fail))
		}
	}

	fmt.Println()
	fmt.Println("Top co-occurrence patterns (failing traces only):")
	type kv struct {
		k string
		c int64
	}
	var combos []kv
	for k, c := range comboCounts {
		combos = append(combos, kv{k, c})
	}
	sort.Slice(combos, func(i, j int) bool { return combos[i].c > combos[j].c })
	limit := 10
	if len(combos) < limit {
		limit = len(combos)
	}
	fmt.Printf("  %8s  %7s  conditions\n", "count", "% fail")
	for _, x := range combos[:limit] {
		fmt.Printf("  %8d  %6.2f%%  %s\n", x.c, pct(x.c, fail), x.k)
	}

	fmt.Println()
	fmt.Println("Multi-parent (C3) span characterization")
	fmt.Println("(restricted to traces that pass C1, C2, C5 — i.e., otherwise structurally clean):")
	if mpTotal == 0 {
		fmt.Println("  (no multi-parent spans observed in this sample)")
		return
	}
	fmt.Printf("  total multi-parent spans seen:  %d\n", mpTotal)
	fmt.Printf("    leaves (no children):         %d (%.2f%%)\n", mpLeafTotal, pct(mpLeafTotal, mpTotal))
	fmt.Printf("    internal (have children):     %d (%.2f%%)\n", mpInternalTotal, pct(mpInternalTotal, mpTotal))
	if mpInternalTotal > 0 {
		fmt.Println()
		fmt.Println("  For internal multi-parent spans, distribution of:")
		fmt.Println("  direct child count")
		printQuantiles(mpDirectChildren)
		fmt.Println("  full subtree size (descendants)")
		printQuantiles(mpSubtreeSizes)
	}

	fmt.Println()
	fmt.Println("  Per-multi-parent span: are the referenced parents in this trace?")
	fmt.Printf("    all parents present:   %d (%.2f%%) — true intra-trace DAG\n",
		mpAllPresent, pct(mpAllPresent, mpTotal))
	fmt.Printf("    some parents missing:  %d (%.2f%%) — partial cross-trace ref\n",
		mpSomeMissing, pct(mpSomeMissing, mpTotal))
	fmt.Printf("    all parents missing:   %d (%.2f%%) — orphan / fully cross-trace\n",
		mpAllMissing, pct(mpAllMissing, mpTotal))
	fmt.Println()
	fmt.Printf("  Per-reference: total CHILD_OF refs across multi-parent spans = %d\n", mpTotalRefs)
	fmt.Printf("    refs that resolve in this trace:   %d (%.2f%%)\n",
		mpPresentRefs, pct(mpPresentRefs, mpTotalRefs))
	fmt.Printf("    refs that don't (cross-trace?):    %d (%.2f%%)\n",
		mpTotalRefs-mpPresentRefs, pct(mpTotalRefs-mpPresentRefs, mpTotalRefs))

	fmt.Println()
	fmt.Println("  Fan-in factor (CHILD_OF count per multi-parent span, always >=2):")
	printQuantiles(mpRefCounts)
	fmt.Println()
	fmt.Println("  Fan-in histogram (count of multi-parent spans with this fan-in):")
	fanInHist := map[int]int{}
	for _, n := range mpRefCounts {
		fanInHist[n]++
	}
	keys := make([]int, 0, len(fanInHist))
	for k := range fanInHist {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	const showFirstN = 15
	shown, otherCount := 0, 0
	for _, k := range keys {
		if shown < showFirstN {
			fmt.Printf("    %4d-way: %d (%.2f%%)\n", k, fanInHist[k], pct(int64(fanInHist[k]), int64(len(mpRefCounts))))
			shown++
		} else {
			otherCount += fanInHist[k]
		}
	}
	if otherCount > 0 {
		fmt.Printf("    >%d-way: %d (%.2f%%)\n", keys[showFirstN-1], otherCount, pct(int64(otherCount), int64(len(mpRefCounts))))
	}

	// Service-level aggregates.
	totalMPI := int64(len(mpRefCounts))
	type svckv struct {
		k string
		c int64
	}
	topN := func(m map[string]int64, n int) []svckv {
		var s []svckv
		for k, c := range m {
			s = append(s, svckv{k, c})
		}
		sort.Slice(s, func(i, j int) bool { return s[i].c > s[j].c })
		if len(s) > n {
			s = s[:n]
		}
		return s
	}

	fmt.Println()
	fmt.Println("  Top-15 services that appear AS A PARENT in multi-parent spans:")
	fmt.Println("  (a fan-in is counted once per distinct service involved)")
	fmt.Printf("    %8s  %7s  service\n", "count", "% fanin")
	for _, x := range topN(parentSvcAppearances, 15) {
		fmt.Printf("    %8d  %6.2f%%  %s\n", x.c, pct(x.c, totalMPI), x.k)
	}

	// Same-vs-cross service parent pair tally.
	var sameSvcPair, crossSvcPair, anyMissing int64
	for k, c := range parentSvcSetCounts {
		parts := strings.Split(k, "|")
		hasMissing := false
		for _, p := range parts {
			if p == "?" || p == "" {
				hasMissing = true
				break
			}
		}
		if hasMissing {
			anyMissing += c
			continue
		}
		// All non-missing — check if all parts are equal.
		allSame := true
		for _, p := range parts[1:] {
			if p != parts[0] {
				allSame = false
				break
			}
		}
		if allSame {
			sameSvcPair += c
		} else {
			crossSvcPair += c
		}
	}
	fmt.Println()
	fmt.Println("  Parent-pair service tally (across ALL fan-ins, not just top-15):")
	fmt.Printf("    same-service pair (both parents same svc):  %d (%.2f%%)\n", sameSvcPair, pct(sameSvcPair, totalMPI))
	fmt.Printf("    cross-service pair (parents in diff svcs):  %d (%.2f%%)\n", crossSvcPair, pct(crossSvcPair, totalMPI))
	fmt.Printf("    pair has missing/unknown service:           %d (%.2f%%)\n", anyMissing, pct(anyMissing, totalMPI))

	fmt.Println()
	fmt.Println("  Top-15 (parent_a | parent_b) service pairs (sorted within pair):")
	fmt.Printf("    %8s  %7s  parents\n", "count", "% fanin")
	for _, x := range topN(parentSvcSetCounts, 15) {
		fmt.Printf("    %8d  %6.2f%%  %s\n", x.c, pct(x.c, totalMPI), x.k)
	}

	fmt.Println()
	fmt.Println("  Top-15 services that ARE the multi-parent (join / fan-in) span:")
	fmt.Printf("    %8s  %7s  service\n", "count", "% fanin")
	for _, x := range topN(joinSvcCounts, 15) {
		fmt.Printf("    %8d  %6.2f%%  %s\n", x.c, pct(x.c, totalMPI), x.k)
	}

	// Fan-OUT analysis: where did the two paths converging at this fan-in
	// originally split? LCA = lowest common ancestor of the two parents,
	// walking up via first-CHILD_OF.
	fmt.Println()
	fmt.Println("  Fan-OUT (parent paths' lowest common ancestor) analysis:")
	lcaTotal := lcaSame + lcaCross + lcaNotFound
	fmt.Printf("    LCA in same service as fan-in: %d (%.2f%%)\n", lcaSame, pct(lcaSame, lcaTotal))
	fmt.Printf("    LCA in different service:      %d (%.2f%%)\n", lcaCross, pct(lcaCross, lcaTotal))
	fmt.Printf("    LCA not found (broken chain):  %d (%.2f%%)\n", lcaNotFound, pct(lcaNotFound, lcaTotal))

	fmt.Println()
	fmt.Println("  Top-15 services that ARE the fan-OUT point (LCA):")
	fmt.Printf("    %8s  %7s  service\n", "count", "% fanin")
	for _, x := range topN(lcaSvcCounts, 15) {
		fmt.Printf("    %8d  %6.2f%%  %s\n", x.c, pct(x.c, totalMPI), x.k)
	}

	if lcaCross > 0 {
		fmt.Println()
		fmt.Printf("  Cross-service fan-ins (%d total): top-15 (LCA_svc -> join_svc) edges:\n", lcaCross)
		fmt.Printf("    %8s  %8s  edge\n", "count", "% xsvc")
		for _, x := range topN(crossSvcEdgeCounts, 15) {
			fmt.Printf("    %8d  %7.2f%%  %s\n", x.c, pct(x.c, lcaCross), x.k)
		}
	}

	// Branch-length characterization: how often is the diamond a "perfect"
	// 1-step diamond vs longer?
	if len(allMinBranches) > 0 {
		fmt.Println()
		fmt.Println("  Branch-length characterization (intra-trace fan-ins with a found LCA):")
		fmt.Printf("    perfect diamond (both branches len 1): %d (%.2f%%)\n",
			diamondCount, pct(diamondCount, int64(len(allMinBranches))))
		fmt.Println("  shorter branch length (parent -> LCA hops, min over the two branches):")
		printQuantiles(allMinBranches)
		fmt.Println("  longer branch length (max over the two branches):")
		printQuantiles(allMaxBranches)

		fmt.Println()
		fmt.Println("  Top-15 branch-length pairs (lo, hi) sorted:")
		fmt.Printf("    %8s  %7s  pair\n", "count", "% fanin")
		for _, x := range topN(branchPairCounts, 15) {
			fmt.Printf("    %8d  %6.2f%%  %s\n", x.c, pct(x.c, int64(len(allMinBranches))), x.k)
		}
	}

	if mrTotal > 0 {
		fmt.Println()
		fmt.Println("Multi-root (C1) trace characterization:")
		fmt.Printf("  total C1-violating traces:                 %d\n", mrTotal)
		fmt.Printf("  every root in its own connected component: %d (%.2f%%)\n",
			mrRootsInDistinctComps, pct(mrRootsInDistinctComps, mrTotal))
		fmt.Printf("  trace has spans whose traceID != wrapper:  %d (%.2f%%)\n",
			mrMismatchedTraceIDs, pct(mrMismatchedTraceIDs, mrTotal))
		fmt.Println()
		fmt.Println("  Number of roots per multi-root trace:")
		printQuantiles(mrAllNumRoots)
		fmt.Println("  Number of connected components per multi-root trace:")
		printQuantiles(mrAllNumComps)
		fmt.Println("  Span count per component (across all components):")
		printQuantiles(mrAllCompSizes)
		fmt.Println()
		fmt.Println("  Top-15 (roots, components) pairs:")
		fmt.Printf("    %8s  %7s  shape\n", "count", "% C1")
		for _, x := range topN(mrRootsCompsPair, 15) {
			fmt.Printf("    %8d  %6.2f%%  %s\n", x.c, pct(x.c, mrTotal), x.k)
		}
	}
}

func printQuantiles(xs []int) {
	if len(xs) == 0 {
		fmt.Println("    (empty)")
		return
	}
	sort.Ints(xs)
	n := len(xs)
	q := func(p float64) int {
		idx := int(p * float64(n))
		if idx >= n {
			idx = n - 1
		}
		return xs[idx]
	}
	sum := 0
	for _, x := range xs {
		sum += x
	}
	fmt.Printf("    n=%d  p50=%d  p90=%d  p99=%d  max=%d  mean=%.1f\n",
		n, q(0.5), q(0.9), q(0.99), xs[n-1], float64(sum)/float64(n))
}
