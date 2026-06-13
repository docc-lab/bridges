// Command depth_profile measures the structural quantities that govern
// PCRB/PCRS reconstruction difficulty, independent of any reconstruction
// or drop policy: how the checkpoint modulus (cpd) aliases against the
// corpus's true depth structure.
//
// The driver of per-bloom false-positive risk is window OCCUPANCY — how
// many true ancestors a payload-bearing span carries since its last
// checkpoint — which for a span at depth d is exactly d mod cpd. A leaf
// landing just below its checkpoint (small residue) rides an under-
// filled bloom (sub-spec FP); a leaf near the window floor (residue
// cpd-1) rides a full bloom. This tool reports, per cpd in 2..6:
//
//   - leaf-depth-mod-cpd histogram, mean, and fill ratio mean/(cpd-1)
//   - branch points (>=2 children): fraction landing AT a checkpoint
//     (absorbed by exact prefix anchoring, no threading ambiguity) vs
//     in a window interior (exposed to same-depth threading ambiguity)
//
// It reads events.bin + meta.bin and derives depth/children by walking
// parents, mirroring the handler — but runs no solver. Pure structure.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"bridges/corpus"
)

type traceState struct {
	parent map[uint64]uint64
	open   int
}

func main() {
	corpusDir := flag.String("corpus", "", "corpus dir (events.bin + meta.bin)")
	traceCount := flag.Int("trace-count", 0, "limit to first N traces (0 = all)")
	flag.Parse()
	if *corpusDir == "" {
		fmt.Fprintln(os.Stderr, "usage: depth_profile --corpus DIR [--trace-count N]")
		os.Exit(2)
	}

	eventsPath, metaPath := corpus.Paths(*corpusDir)
	meta, err := corpus.ReadMeta(metaPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer er.Close()

	order := meta.TraceOrder
	counts := meta.SpanCounts
	if *traceCount > 0 && *traceCount < len(order) {
		order = order[:*traceCount]
		counts = counts[:*traceCount]
	}
	open := make(map[uint64]int, len(order))
	for i, tid := range order {
		open[tid] = 2 * int(counts[i])
	}

	const minCPD, maxCPD = 2, 6
	// per-cpd accumulators
	var (
		leafResidue [maxCPD + 1][]int64 // [cpd][residue]
		leafCount   [maxCPD + 1]int64
		leafSum     [maxCPD + 1]int64 // sum of residues
		branchAtCP  [maxCPD + 1]int64
		branchInner [maxCPD + 1]int64
	)
	for cpd := minCPD; cpd <= maxCPD; cpd++ {
		leafResidue[cpd] = make([]int64, cpd)
	}

	live := make(map[uint64]*traceState, 1024)
	get := func(tid uint64) *traceState {
		ts := live[tid]
		if ts == nil {
			ts = &traceState{parent: make(map[uint64]uint64)}
			live[tid] = ts
		}
		return ts
	}

	traces := 0
	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if _, ok := open[ce.TraceID]; !ok {
			continue
		}
		ts := get(ce.TraceID)
		if bridgeKindIsStart(ce.Kind) {
			ts.parent[ce.SpanID] = ce.ParentID
		}
		open[ce.TraceID]--
		if open[ce.TraceID] == 0 {
			profileTrace(ts, &leafResidue, &leafCount, &leafSum, &branchAtCP, &branchInner, minCPD, maxCPD)
			delete(live, ce.TraceID)
			delete(open, ce.TraceID)
			traces++
		}
	}

	fmt.Printf("depth_profile: %d traces\n\n", traces)
	fmt.Printf("%-4s %-8s %-8s %s\n", "cpd", "meanRes", "fillRat", "leaf residue histogram (mod cpd, normalized %)")
	for cpd := minCPD; cpd <= maxCPD; cpd++ {
		mean := float64(leafSum[cpd]) / float64(leafCount[cpd])
		fill := mean / float64(cpd-1)
		fmt.Printf("%-4d %-8.3f %-8.3f [", cpd, mean, fill)
		for r := 0; r < cpd; r++ {
			pct := 100 * float64(leafResidue[cpd][r]) / float64(leafCount[cpd])
			if r > 0 {
				fmt.Print(" ")
			}
			fmt.Printf("%d:%.1f", r, pct)
		}
		fmt.Println("]")
	}

	fmt.Printf("\n%-4s %-12s %-12s %s\n", "cpd", "branch@ckpt", "branchInner", "exposed%")
	for cpd := minCPD; cpd <= maxCPD; cpd++ {
		tot := branchAtCP[cpd] + branchInner[cpd]
		exposed := 100 * float64(branchInner[cpd]) / float64(tot)
		fmt.Printf("%-4d %-12d %-12d %.2f%%\n", cpd, branchAtCP[cpd], branchInner[cpd], exposed)
	}
}

func bridgeKindIsStart(k uint8) bool { return k == 0 }

func profileTrace(ts *traceState,
	leafResidue *[7][]int64, leafCount, leafSum, branchAtCP, branchInner *[7]int64,
	minCPD, maxCPD int) {

	// depth via memoized parent walk; children counts for leaf/branch.
	depth := make(map[uint64]int, len(ts.parent))
	var dof func(id uint64) int
	dof = func(id uint64) int {
		if d, ok := depth[id]; ok {
			return d
		}
		p, hasParent := ts.parent[id]
		if !hasParent || p == 0 {
			depth[id] = 0
			return 0
		}
		if _, inTrace := ts.parent[p]; !inTrace {
			depth[id] = 0 // parent absent from trace: treat as root
			return 0
		}
		d := dof(p) + 1
		depth[id] = d
		return d
	}
	childCount := make(map[uint64]int, len(ts.parent))
	for sid := range ts.parent {
		dof(sid)
		p, ok := ts.parent[sid]
		if ok && p != 0 {
			if _, inTrace := ts.parent[p]; inTrace {
				childCount[p]++
			}
		}
	}
	for sid := range ts.parent {
		d := depth[sid]
		isLeaf := childCount[sid] == 0
		isBranch := childCount[sid] >= 2
		for cpd := minCPD; cpd <= maxCPD; cpd++ {
			if isLeaf {
				r := d % cpd
				leafResidue[cpd][r]++
				leafCount[cpd]++
				leafSum[cpd] += int64(r)
			}
			if isBranch {
				if d%cpd == 0 {
					branchAtCP[cpd]++
				} else {
					branchInner[cpd]++
				}
			}
		}
	}
}
