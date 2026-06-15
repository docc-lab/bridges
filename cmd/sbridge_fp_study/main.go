// Command sbridge_fp_study measures how wide a truncated fingerprint must be to
// disambiguate non-checkpoint spans when placing them back into a reconstructed
// S-Bridge structure.
//
// Within a single trace, a span is keyed by (depth, start-ordinal) — start
// ordinal = its start-order index among its parent's children (bumpEvent). Spans
// sharing a (depth, ordinal) key inside one trace are mutually ambiguous unless
// their fingerprints differ. We sweep TWO fingerprint sources, both available
// for free when placing an orphan along its ordinal chain:
//   - own bytes: top-Bo big-endian bytes of the span's own ID
//   - parent bytes: top-Bp big-endian bytes of the span's parent ID (the parent
//     is already identified upstream in the chain)
// and report, per cpd, the collision rate over the (Bp, Bo) grid. Checkpoints
// (depth % cpd == 0) carry their own 4-byte anchor and are excluded.
//
//	sbridge_fp_study --archive corpus.arc [--cpds 2,6] [--sample N] [--parent-bytes 4] [--own-bytes 5] [--workers N]
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bridges/corpus"
)

type acc struct {
	collided [][]int64 // [cpdIdx][comboIdx]  comboIdx = bp*(nO)+bo
	nonckpt  []int64   // [cpdIdx]
}

func newAcc(nCpd, nCombo int) *acc {
	a := &acc{collided: make([][]int64, nCpd), nonckpt: make([]int64, nCpd)}
	for i := range a.collided {
		a.collided[i] = make([]int64, nCombo)
	}
	return a
}

var (
	cpds            []int
	parentMax       int
	ownMax          int
	nO              int // ownMax+1
)

var diagonal bool

func main() {
	archivePath := flag.String("archive", "", "archive(s) (corpus.Archive), comma-separated for multiple")
	cpdsArg := flag.String("cpds", "2,6", "comma-separated checkpoint distances")
	sample := flag.Int("sample", 0, "limit to first N traces total (0 = all)")
	pMax := flag.Int("parent-bytes", 4, "max parent-ID fingerprint width to sweep")
	oMax := flag.Int("own-bytes", 5, "max own-ID fingerprint width to sweep")
	diag := flag.Bool("diagonal", false, "only compute symmetric (k parent, k own) configs — much faster")
	workers := flag.Int("workers", runtime.NumCPU(), "parallel workers")
	flag.Parse()
	if *archivePath == "" {
		fmt.Fprintln(os.Stderr, "usage: sbridge_fp_study --archive corpus.arc[,corpus2.arc] [...]")
		os.Exit(2)
	}
	diagonal = *diag
	for _, s := range strings.Split(*cpdsArg, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && v >= 1 {
			cpds = append(cpds, v)
		}
	}
	parentMax, ownMax = *pMax, *oMax
	nO = ownMax + 1
	nCombo := (parentMax + 1) * nO

	jobs := make(chan []corpus.ArchiveSpan, *workers*2)
	var wg sync.WaitGroup
	accs := make([]*acc, *workers)
	for w := 0; w < *workers; w++ {
		accs[w] = newAcc(len(cpds), nCombo)
		wg.Add(1)
		go func(a *acc) {
			defer wg.Done()
			key := make(map[[2]uint64]int)
			for spans := range jobs {
				processTrace(spans, a, key)
			}
		}(accs[w])
	}

	t0 := time.Now()
	var nTraces, nSpans int64
	for _, ap := range strings.Split(*archivePath, ",") {
		ap = strings.TrimSpace(ap)
		if ap == "" {
			continue
		}
		r, err := corpus.OpenArchive(ap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open archive %s: %v\n", ap, err)
			os.Exit(1)
		}
		for {
			tr, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "read %s: %v\n", ap, err)
				os.Exit(1)
			}
			if *sample > 0 && nTraces >= int64(*sample) {
				break
			}
			if len(tr.Spans) == 0 {
				continue
			}
			nTraces++
			nSpans += int64(len(tr.Spans))
			jobs <- tr.Spans
		}
		r.Close()
	}
	close(jobs)
	wg.Wait()

	// Merge worker accumulators.
	tot := newAcc(len(cpds), nCombo)
	for _, a := range accs {
		for ci := range cpds {
			tot.nonckpt[ci] += a.nonckpt[ci]
			for k := range tot.collided[ci] {
				tot.collided[ci][k] += a.collided[ci][k]
			}
		}
	}

	fmt.Printf("studied %d traces, %d spans in %s with %d workers\n",
		nTraces, nSpans, time.Since(t0).Round(time.Millisecond), *workers)
	fmt.Printf("key = (depth, start-ordinal) + top-Bp bytes of parentID + top-Bo bytes of own spanID\n")
	for ci, cpd := range cpds {
		n := tot.nonckpt[ci]
		fmt.Printf("\n=== cpd=%d : %d non-checkpoint spans ===\n", cpd, n)
		if diagonal {
			maxK := parentMax
			if ownMax < maxK {
				maxK = ownMax
			}
			fmt.Printf("symmetric (k parent + k own bytes):\n  k  collided_spans   collision%%\n")
			for k := 0; k <= maxK; k++ {
				c := tot.collided[ci][k*nO+k]
				pct := 0.0
				if n > 0 {
					pct = 100 * float64(c) / float64(n)
				}
				fmt.Printf("%3d  %14d   %11.7f%%\n", k, c, pct)
			}
			continue
		}
		fmt.Printf("collision%% grid  (rows = own bytes Bo, cols = parent bytes Bp)\n")
		fmt.Printf("Bo\\Bp")
		for bp := 0; bp <= parentMax; bp++ {
			fmt.Printf("%12d", bp)
		}
		fmt.Println()
		for bo := 0; bo <= ownMax; bo++ {
			fmt.Printf("%4d ", bo)
			for bp := 0; bp <= parentMax; bp++ {
				c := tot.collided[ci][bp*nO+bo]
				pct := 0.0
				if n > 0 {
					pct = 100 * float64(c) / float64(n)
				}
				fmt.Printf("%11.5f%%", pct)
			}
			fmt.Println()
		}
	}
}

func countColl(spans []corpus.ArchiveSpan, members []int, bp, bo int, key map[[2]uint64]int) int64 {
	for k := range key {
		delete(key, k)
	}
	for _, i := range members {
		var pk, ok uint64
		if bp > 0 {
			pk = spans[i].ParentID >> uint(64-8*bp)
		}
		if bo > 0 {
			ok = spans[i].SpanID >> uint(64-8*bo)
		}
		key[[2]uint64{pk, ok}]++
	}
	var coll int64
	for _, c := range key {
		if c > 1 {
			coll += int64(c)
		}
	}
	return coll
}

func processTrace(spans []corpus.ArchiveSpan, a *acc, key map[[2]uint64]int) {
	n := len(spans)
	idx := make(map[uint64]int, n)
	for i := range spans {
		idx[spans[i].SpanID] = i
	}
	depth := make([]int, n)
	for i := range depth {
		depth[i] = -1
	}
	var d func(i int) int
	d = func(i int) int {
		if depth[i] >= 0 {
			return depth[i]
		}
		dv := 0
		if pid := spans[i].ParentID; pid != 0 {
			if pj, ok := idx[pid]; ok {
				dv = d(pj) + 1
			}
		}
		depth[i] = dv
		return dv
	}
	for i := range spans {
		d(i)
	}
	childrenOf := make(map[uint64][]int)
	for i := range spans {
		childrenOf[spans[i].ParentID] = append(childrenOf[spans[i].ParentID], i)
	}
	ordinal := make([]int, n)
	for _, kids := range childrenOf {
		sort.Slice(kids, func(x, y int) bool {
			ix, iy := kids[x], kids[y]
			if spans[ix].StartTS != spans[iy].StartTS {
				return spans[ix].StartTS < spans[iy].StartTS
			}
			return spans[ix].SpanID < spans[iy].SpanID
		})
		for rank, ci := range kids {
			ordinal[ci] = rank + 1
		}
	}

	for ci, cpd := range cpds {
		groups := make(map[uint64][]int) // (depth,ord) -> span indices
		for i := range spans {
			if depth[i]%cpd == 0 {
				continue
			}
			gk := uint64(depth[i])<<32 | uint64(uint32(ordinal[i]))
			groups[gk] = append(groups[gk], i)
			a.nonckpt[ci]++
		}
		for _, members := range groups {
			if len(members) < 2 {
				continue
			}
			if diagonal {
				maxK := parentMax
				if ownMax < maxK {
					maxK = ownMax
				}
				for k := 0; k <= maxK; k++ {
					a.collided[ci][k*nO+k] += countColl(spans, members, k, k, key)
				}
			} else {
				for bp := 0; bp <= parentMax; bp++ {
					for bo := 0; bo <= ownMax; bo++ {
						a.collided[ci][bp*nO+bo] += countColl(spans, members, bp, bo, key)
					}
				}
			}
		}
	}
}
