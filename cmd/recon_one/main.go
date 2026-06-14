// Command recon_one replays one (or more) gob-dumped trace(s) through CGPRB
// reconstruction WITHOUT walking the corpus. Capture a trace once:
//
//	trace_recon --corpus DIR --mode cgprb ... --only-traces <hex> --dump-survivors /tmp/t.gob -o /dev/null
//
// then iterate on reconstruction in microseconds:
//
//	TRACE_RECON_CPSAT=1 recon_one /tmp/t.gob
//
// It prints per-trace timing plus the standard PCR score and the CGP topology
// score, so you can confirm a perf change leaves correctness untouched.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"bridges/recon"
)

func main() {
	reps := flag.Int("reps", 1, "reconstruct each trace this many times (report the fastest) to measure steady-state latency")
	tidHex := flag.String("tid", "", "only replay this hex trace ID")
	cpuprof := flag.String("cpuprofile", "", "write a CPU profile of the reconstruction(s) here")
	profSecs := flag.Int("profsecs", 0, "with --cpuprofile: flush the profile and exit after this many seconds (capture a grind mid-flight)")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: recon_one [--reps N] [--tid HEX] [--cpuprofile FILE] <dump.gob>")
		os.Exit(2)
	}

	traces, err := recon.LoadDumpedTraces(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(1)
	}
	var want uint64
	if *tidHex != "" {
		want, _ = strconv.ParseUint(*tidHex, 16, 64)
	}
	fmt.Fprintf(os.Stderr, "loaded %d trace(s) from %s\n", len(traces), flag.Arg(0))

	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cpuprofile: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		// Timed flush: the Go CPU profile is written only at StopCPUProfile, so
		// a trace that grinds for minutes never flushes if we wait for it. After
		// --profsecs, stop + flush + exit so we capture the hot path mid-grind.
		if *profSecs > 0 {
			time.AfterFunc(time.Duration(*profSecs)*time.Second, func() {
				pprof.StopCPUProfile()
				f.Close()
				fmt.Fprintf(os.Stderr, "profsecs: flushed after %ds\n", *profSecs)
				os.Exit(0)
			})
		}
	}

	for _, dt := range traces {
		if want != 0 && dt.TID != want {
			continue
		}
		dropped := dt.DroppedSet()
		var res recon.Result
		best := time.Duration(1<<62 - 1)
		c0, ok0, bt0, cms0, btms0 := recon.ClusterStats()
		for r := 0; r < *reps; r++ {
			t0 := time.Now()
			res = recon.ReconstructCGPRB(dt.Survivors, dt.Cfg)
			if d := time.Since(t0); d < best {
				best = d
			}
		}
		c1, ok1, bt1, cms1, btms1 := recon.ClusterStats()
		fmt.Printf("  clusters: %d solved (cpsat_ok=%d bt_fallback=%d) | cpsat=%dms bt=%dms\n",
			(c1-c0)/int64(*reps), (ok1-ok0)/int64(*reps), (bt1-bt0)/int64(*reps),
			(cms1-cms0)/int64(*reps), (btms1-btms0)/int64(*reps))
		score := recon.ScorePCR(res, dt.Truth, dropped)
		topo := recon.ScoreCGPTopology(res, dt.Truth, dropped, dt.TID)
		fmt.Printf("tid=%016x survivors=%d dropped=%d recon=%s | bridges=%d\n",
			dt.TID, len(dt.Survivors), len(dt.Dropped), best.Round(time.Microsecond), len(res.Bridges))
		fmt.Printf("  score: %+v\n", score)
		fmt.Printf("  topo:  conn=%t topo=%t sibPairs=%d recall=%d reconSib=%d prec=%d\n",
			topo.ConnCorrect, topo.TopoCorrect, topo.TrueSibPairs, topo.RecallPairs, topo.ReconSibPairs, topo.PrecisionPairs)
	}
}
