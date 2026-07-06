// cgp2_replay loads dumped traces (cmd/trace_recon --dump-survivors) and runs the
// from-scratch CGP reconstructor (recon.ReconstructCGP2) over them, scoring with
// ScoreCGP2Iso. Build with -tags cpsat so the solver is wired.
//
//	cgp2_replay dump.gob [limit]
//
// Set CGP2_WORKERS to the number of parallel scoring goroutines (default NumCPU).
// Each trace is reconstructed + scored independently and the per-trace solve pins
// one CP-SAT search worker, so N goroutines use N cores; the aggregate is
// order-independent so results match a single-threaded run exactly.
package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"

	"bridges/bloom"
	"bridges/recon"
)

// acc holds the additive per-run tallies (merged across worker shards).
type acc struct {
	nt, feas, clean, empty                                                   int
	realNodes, edgeExact, edgeWrong, survN, survEx, named, namedEx, totWrong int
	emptySurvSum, emptyNoDrop, emptyTiny                                     int
}

func (a *acc) merge(b *acc) {
	a.nt += b.nt
	a.feas += b.feas
	a.clean += b.clean
	a.empty += b.empty
	a.realNodes += b.realNodes
	a.edgeExact += b.edgeExact
	a.edgeWrong += b.edgeWrong
	a.survN += b.survN
	a.survEx += b.survEx
	a.named += b.named
	a.namedEx += b.namedEx
	a.totWrong += b.totWrong
	a.emptySurvSum += b.emptySurvSum
	a.emptyNoDrop += b.emptyNoDrop
	a.emptyTiny += b.emptyTiny
}

// score reconstructs + scores one trace into a (the worker-local tally).
func score(dt *recon.DumpedTrace, a *acc) {
	res := recon.ReconstructCGP2(dt.Survivors, dt.Cfg)
	a.nt++
	if len(res.ReconParent) == 0 {
		a.empty++
		a.emptySurvSum += len(dt.Survivors)
		if len(dt.Dropped) == 0 {
			a.emptyNoDrop++ // nothing dropped -> nothing to reconstruct (trivially correct)
		}
		if len(dt.Survivors) <= 2 {
			a.emptyTiny++
		}
		return
	}
	a.feas++
	iso := recon.ScoreCGP2Iso(res, dt.Truth, dt.DroppedSet())
	a.realNodes += iso.RealNodes
	a.edgeExact += iso.EdgeExact
	a.edgeWrong += iso.EdgeWrong
	a.totWrong += iso.EdgeWrong
	a.survN += iso.SurvNodes
	a.survEx += iso.SurvExact
	a.named += iso.NamedSyn
	a.namedEx += iso.NamedExact
	if iso.EdgeWrong == 0 {
		a.clean++
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cgp2_replay dump.gob [limit]   (CGP2_WORKERS=N for parallelism)")
		os.Exit(2)
	}
	if os.Getenv("CGP2_PRIME_M") != "" {
		bloom.PrimeM = true
	}
	traces, err := recon.LoadDumpedTraces(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(1)
	}
	if len(os.Args) >= 3 { // optional limit: first N traces
		var lim int
		fmt.Sscanf(os.Args[2], "%d", &lim)
		if lim > 0 && lim < len(traces) {
			traces = traces[:lim]
		}
	}

	workers := runtime.NumCPU()
	if w := os.Getenv("CGP2_WORKERS"); w != "" {
		if n, e := strconv.Atoi(w); e == nil && n > 0 {
			workers = n
		}
	}
	if workers > len(traces) {
		workers = len(traces)
	}
	if workers < 1 {
		workers = 1
	}

	var total acc
	if workers <= 1 {
		for i := range traces {
			score(&traces[i], &total)
		}
	} else {
		parts := make([]acc, workers)
		chunk := (len(traces) + workers - 1) / workers
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			lo := w * chunk
			if lo >= len(traces) {
				break
			}
			hi := lo + chunk
			if hi > len(traces) {
				hi = len(traces)
			}
			wg.Add(1)
			go func(w, lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					score(&traces[i], &parts[w])
				}
			}(w, lo, hi)
		}
		wg.Wait()
		for w := range parts {
			total.merge(&parts[w])
		}
	}

	pct := func(a, b int) float64 {
		if b == 0 {
			return 0
		}
		return 100 * float64(a) / float64(b)
	}
	fmt.Printf("traces=%d feasible=%d empty=%d | correct=%.2f%% (clean=%d) | correctExclEmpty=%.2f%% | correctCreditEmpty=%.2f%%\n",
		total.nt, total.feas, total.empty, pct(total.clean, total.nt), total.clean,
		pct(total.clean, total.feas), pct(total.clean+total.empty, total.nt))
	if total.empty > 0 {
		fmt.Printf("empties: n=%d avgSurv=%.1f noDrop(trivial)=%d tiny(<=2 surv)=%d\n",
			total.empty, float64(total.emptySurvSum)/float64(total.empty), total.emptyNoDrop, total.emptyTiny)
	}
	fmt.Printf("per-edge: exact=%.3f%% (%d/%d) wrong/trace=%.2f | survivor edges=%.3f%% | named-syn=%.2f%%\n",
		pct(total.edgeExact, total.realNodes), total.edgeExact, total.realNodes, func() float64 {
			if total.feas == 0 {
				return 0
			}
			return float64(total.totWrong) / float64(total.feas)
		}(), pct(total.survEx, total.survN), pct(total.namedEx, total.named))
}
