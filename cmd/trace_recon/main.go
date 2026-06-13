// Command trace_recon is the reconstruction test harness (P-Bridge only for
// now). It runs spans through the bridge handler with payload capture on,
// applies a seeded drop policy to non-checkpoint spans, reconstructs each
// trace from the surviving spans + payloads, and scores the reconstruction
// against the pre-drop ground truth.
//
// The whole pipeline is streaming and per-trace: a trace is dropped,
// reconstructed, and scored the moment its last event is processed, then its
// state is freed. Two input modes, mirroring trace_sim:
//
//	--corpus DIR   read events.bin + meta.bin produced by trace_prep
//	(no flag)      read JSON files from <input_dir> directly
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/loader"
	"bridges/recon"
	"bridges/verify"
)

type config struct {
	inputDir           string
	corpusDir          string
	outputPath         string
	mode               string // "pb" or "pcr"
	checkpointDistance int
	dropRate           float64
	seed               int64
	traceCount         int
	requireClean       bool
	bottomUp           bool
	bloomFP            float64
	chainCheck         bool
	prefixLen          int    // PCR truncated checkpoint-root ID length
	classifyWrong      string // JSONL dump path for wrong-edge classification (pcr/pcrb)
	stopOnGap          bool   // PCRB: stop threading at first zero-candidate level
	tiePolicy          string // PCRS: thread-item tie resolution (aware|id|stop)
	workers            int    // reconstruction worker goroutines (0 = NumCPU)
	scoreAudit         bool   // fault-injection sensitivity audit of the scorer
	dumpRecon          string // serialize per-trace reconstruction artifacts (JSONL.gz) for recon_verify
	verifyInline       bool   // run the independent checker in-process after every reconstruction
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.outputPath, "o", "", "Output JSON file (required)")
	flag.StringVar(&c.outputPath, "output", "", "Output JSON file (required)")
	flag.StringVar(&c.corpusDir, "corpus", "", "Read events.bin + meta.bin from this corpus dir")
	flag.StringVar(&c.mode, "mode", "pb", "Bridge mode: pb (bloom membership), pcr (truncated checkpoint-root ID), or pcrb (checkpoint root + window bloom for in-window threading)")
	flag.IntVar(&c.prefixLen, "prefix-len", bridge.DefaultPCRPrefixLen, "PCR mode: truncated checkpoint-root span ID length in bytes (1-8)")
	flag.IntVar(&c.checkpointDistance, "checkpoint-distance", 1, "Checkpoint distance")
	flag.Float64Var(&c.bloomFP, "bloom-fp", bridge.DefaultBloomFPRate, "Target bloom false-positive rate (sets bloom geometry on both the emit and reconstruction sides)")
	flag.Float64Var(&c.dropRate, "drop-rate", 1.0, "Probability of dropping each non-checkpoint span (1.0 = drop all)")
	order := ""
	flag.StringVar(&order, "order", "bottom-up", "Orphan processing order: bottom-up (deepest-first, default) or independent")
	consistency := ""
	flag.StringVar(&consistency, "consistency", "chain", "Anchor candidate consistency check: chain (test the candidate's nameable ancestry against the bloom) or none")
	flag.Int64Var(&c.seed, "seed", 42, "RNG seed for the drop policy")
	flag.IntVar(&c.traceCount, "trace-count", 0, "Max number of traces to process (0 = all). Corpus mode: first N of the corpus trace order; events of other traces are skipped during streaming")
	flag.BoolVar(&c.requireClean, "require-clean", true, "Cleanliness filter (JSON mode only)")
	flag.StringVar(&c.classifyWrong, "classify-wrong", "", "Write one JSONL record per wrong-edge bridge (anchor not a true ancestor) locating the divergence against truth")
	flag.IntVar(&c.workers, "workers", 0, "Reconstruction worker goroutines (0 = NumCPU). Drop decisions stay serial, so results are bit-identical at any worker count")
	gapPolicy := ""
	flag.StringVar(&gapPolicy, "gap-policy", "continue", "PCRB threading at a zero-candidate level: continue (default) or stop (maximally FP-averse)")
	flag.StringVar(&c.tiePolicy, "tie-policy", "aware", "PCRS threading tie resolution: aware (global bit-accounting, default), id (span-ID coin), or stop (abstain)")
	flag.BoolVar(&c.scoreAudit, "score-audit", false, "Fault-injection sensitivity audit: per trace, inject known scoring errors into a copy of the reconstruction and require ScorePCR to detect each with the exact predicted counter delta (report on stderr)")
	flag.StringVar(&c.dumpRecon, "dump-recon", "", "Write per-trace reconstruction artifacts (JSONL.gz) for standalone auditing by recon_verify")
	flag.BoolVar(&c.verifyInline, "verify", false, "Run the independent structural/evidence checker (package verify) in-process after every reconstruction; violations reported at exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <input_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s --corpus <corpus_dir> [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if c.mode != "pb" && c.mode != "pcr" && c.mode != "pcrb" && c.mode != "pcrs" {
		fmt.Fprintf(os.Stderr, "error: -mode must be pb, pcr, pcrb, or pcrs (got %q)\n", c.mode)
		os.Exit(2)
	}
	switch gapPolicy {
	case "continue":
	case "stop":
		c.stopOnGap = true
	default:
		fmt.Fprintf(os.Stderr, "error: -gap-policy must be continue or stop (got %q)\n", gapPolicy)
		os.Exit(2)
	}
	if c.prefixLen < 1 || c.prefixLen > 8 {
		fmt.Fprintln(os.Stderr, "error: --prefix-len must be in [1,8]")
		os.Exit(2)
	}
	switch order {
	case "independent":
	case "bottom-up":
		c.bottomUp = true
	default:
		fmt.Fprintf(os.Stderr, "error: -order must be independent or bottom-up (got %q)\n", order)
		os.Exit(2)
	}
	switch consistency {
	case "chain":
		c.chainCheck = true
	case "none":
	default:
		fmt.Fprintf(os.Stderr, "error: -consistency must be chain or none (got %q)\n", consistency)
		os.Exit(2)
	}
	if c.corpusDir == "" {
		if flag.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "error: input_dir or --corpus required")
			flag.Usage()
			os.Exit(2)
		}
		c.inputDir = flag.Arg(0)
	}
	if c.outputPath == "" {
		fmt.Fprintln(os.Stderr, "error: -o/--output required")
		os.Exit(2)
	}
	if c.dropRate < 0 || c.dropRate > 1 {
		fmt.Fprintln(os.Stderr, "error: --drop-rate must be in [0,1]")
		os.Exit(2)
	}
	return c
}

// collSpan is one span's collected record: standard fields plus captured
// bridge attributes.
type collSpan struct {
	spanID   uint64
	parentID uint64
	depth    int    // handler-derived absolute depth (ground truth for scoring)
	br       []byte // _br payload; nil = the span carried only _d
}

// harness drives the per-trace collect -> drop -> reconstruct -> score loop.
type harness struct {
	h       bridge.Handler
	mode    string // "pb" or "pcr": selects payload decode + reconstruction
	cfg     recon.Config
	rng     *rand.Rand
	dropAll bool
	rate    float64

	spansByTID map[uint64][]collSpan
	idxByKey   map[spanKey]int
	openByTID  map[uint64]int
	// nextSeq is nested per trace so a closed trace's counters can be freed;
	// a flat (traceID, parentID) map grows with every parent span ever seen
	// (gigabytes over the full corpus).
	nextSeq map[uint64]map[uint64]int

	scores map[uint64]recon.Score

	cpd      int
	wrongLog *os.File // non-nil: wrong-edge classification dump

	// Per-trace reconstruction is embarrassingly parallel (no cross-trace
	// state in pb/pcr/pcrb/pcrs). Drop decisions are made SERIALLY at
	// dispatch (preserving the global RNG sequence, hence bit-identical
	// results at any worker count); decode+reconstruct+score fan out.
	jobs chan finishJob
	wg   sync.WaitGroup
	mu   sync.Mutex

	// prog: cumulative counters for the PROGRESS row printed every 10k
	// traces (guarded by mu; trace-completion order, not corpus order).
	prog struct {
		start                                           time.Time
		spans, c, a, w, oe, oem, lost, placed, affected int
	}

	// audit: fault-injection sensitivity accumulator (guarded by mu;
	// nil unless --score-audit).
	audit *auditStats

	// dumper/verify: artifact emission and in-process checking (package
	// verify); violations guarded by mu.
	dumper     *reconDumper
	verifyOn   bool
	verifyCfg  verify.Config
	violations int
}

type finishJob struct {
	tid     uint64
	spans   []collSpan
	dropped map[uint64]struct{}
}

// wrongRec locates one wrong-edge bridge against the pre-drop truth.
type wrongRec struct {
	Trace     string `json:"trace"`
	Root      string `json:"root"` // fragment root span
	RootDepth int    `json:"root_depth"`
	Anchor    string `json:"anchor"` // chosen (wrong) anchor
	AnchDepth int    `json:"anchor_depth"`
	WDepth    int    `json:"w_depth"`    // window checkpoint depth
	DivDepth  int    `json:"div_depth"`  // depth where anchor's chain meets the root's true window path; -1 = never (wrong window)
	TrueDepth int    `json:"true_depth"` // depth of true nearest surviving ancestor
}

type spanKey struct{ traceID, spanID uint64 }

func newHarness(c config) *harness {
	var h bridge.Handler
	cfg := recon.NewConfig(c.checkpointDistance, c.bloomFP)
	cfg.BottomUp = c.bottomUp
	cfg.ChainCheck = c.chainCheck
	switch c.mode {
	case "pcr":
		ph := bridge.NewPCRBridgeHandler(c.checkpointDistance, c.prefixLen)
		ph.Capture = true
		cfg.PrefixLen = c.prefixLen
		h = ph
	case "pcrb", "pcrs":
		ph := bridge.NewPCRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
		ph.Capture = true
		// PCRB bloom geometry differs from PB's for the same --bloom-fp
		// (sized for cpd-1; see bridge.PCRBBloomCapacity) — rebuild the
		// config with the matching constructor.
		pcrbCfg := recon.NewPCRBConfig(c.checkpointDistance, c.prefixLen, c.bloomFP)
		pcrbCfg.BottomUp, pcrbCfg.ChainCheck = cfg.BottomUp, cfg.ChainCheck
		pcrbCfg.StopOnGap = c.stopOnGap
		pcrbCfg.TiePolicy = c.tiePolicy
		cfg = pcrbCfg
		h = ph
	default: // pb
		pb := bridge.NewPathBridgeHandler(c.checkpointDistance, c.bloomFP)
		pb.EmitDepth = true
		pb.Capture = true
		h = pb
	}
	var wlog *os.File
	if c.classifyWrong != "" {
		f, err := os.Create(c.classifyWrong)
		if err != nil {
			fmt.Fprintf(os.Stderr, "classify-wrong: %v\n", err)
			os.Exit(1)
		}
		wlog = f
	}
	workers := c.workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	ha := &harness{
		h:          h,
		mode:       c.mode,
		cpd:        c.checkpointDistance,
		wrongLog:   wlog,
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(c.seed)),
		dropAll:    c.dropRate >= 1.0,
		rate:       c.dropRate,
		spansByTID: make(map[uint64][]collSpan),
		idxByKey:   make(map[spanKey]int),
		openByTID:  make(map[uint64]int),
		nextSeq:    make(map[uint64]map[uint64]int),
		jobs:       make(chan finishJob, workers*2),
	}
	ha.prog.start = time.Now()
	if c.scoreAudit {
		ha.audit = newAuditStats()
	}
	if c.dumpRecon != "" {
		d, err := newReconDumper(c.dumpRecon)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		ha.dumper = d
	}
	if c.verifyInline {
		ha.verifyOn = true
		ha.verifyCfg = verify.Config{PrefixLen: cfg.PrefixLen, BloomM: cfg.BloomM, BloomK: cfg.BloomK, PCRBTypeByte: byte(bridge.PCRBBridgeTypeID), CPD: cfg.CPD}
	}
	for i := 0; i < workers; i++ {
		ha.wg.Add(1)
		go func() {
			defer ha.wg.Done()
			for j := range ha.jobs {
				ha.process(j)
			}
		}()
	}
	return ha
}

// drain closes the job channel and waits for in-flight reconstructions.
func (ha *harness) drain() {
	close(ha.jobs)
	ha.wg.Wait()
	if ha.audit != nil {
		ha.audit.report()
	}
	if ha.dumper != nil {
		ha.dumper.close()
	}
	if ha.verifyOn {
		fmt.Fprintf(os.Stderr, "VERIFY violations=%d\n", ha.violations)
	}
	if ha.mode == "pcrs" && os.Getenv("TRACE_RECON_DEBUG") == "1" {
		recon.DumpPCRSAmbiguity()
	}
}

func (ha *harness) beginTrace(tid uint64, spanCount int) {
	ha.openByTID[tid] = 2 * spanCount
}

func (ha *harness) onEvent(ts int64, kind bridge.Kind, tid, sid, pid uint64, serviceID uint16) {
	ev := &bridge.Event{TraceID: tid, SpanID: sid, ParentID: pid, ServiceID: serviceID}
	if kind == bridge.KindStart {
		seqNum := 0
		if pid != 0 {
			m := ha.nextSeq[tid]
			if m == nil {
				m = make(map[uint64]int)
				ha.nextSeq[tid] = m
			}
			seqNum = m[pid] + 1
			m[pid] = seqNum
		}
		r := ha.h.OnStart(ev, seqNum)
		ha.idxByKey[spanKey{tid, sid}] = len(ha.spansByTID[tid])
		ha.spansByTID[tid] = append(ha.spansByTID[tid], collSpan{
			spanID: sid, parentID: pid, depth: -1, br: r.Payload,
		})
	} else {
		r := ha.h.OnEnd(ev)
		if idx, ok := ha.idxByKey[spanKey{tid, sid}]; ok {
			s := &ha.spansByTID[tid][idx]
			s.depth = r.Depth
			if r.Payload != nil {
				s.br = r.Payload
			}
		}
	}

	ha.openByTID[tid]--
	if ha.openByTID[tid] == 0 {
		ha.h.EvictTrace(tid)
		ha.finishTrace(tid)
		delete(ha.openByTID, tid)
	}
}

// finishTrace frees the trace's collection state, makes the drop decisions
// SERIALLY (global RNG order preserved -> bit-identical results regardless
// of worker count), and hands reconstruction+scoring to the worker pool.
func (ha *harness) finishTrace(tid uint64) {
	spans := ha.spansByTID[tid]
	delete(ha.spansByTID, tid)
	delete(ha.nextSeq, tid)
	for _, s := range spans {
		delete(ha.idxByKey, spanKey{tid, s.spanID})
	}

	// Drop policy: only non-checkpoint spans (no _br) are candidates;
	// _br carriers ride the high-priority queue and are never dropped.
	dropped := make(map[uint64]struct{})
	for _, s := range spans {
		if s.br != nil {
			continue
		}
		if ha.dropAll || ha.rng.Float64() < ha.rate {
			dropped[s.spanID] = struct{}{}
		}
	}
	ha.jobs <- finishJob{tid: tid, spans: spans, dropped: dropped}
}

// process is the per-trace parallel work: decode, reconstruct, score.
func (ha *harness) process(j finishJob) {
	tid, spans, dropped := j.tid, j.spans, j.dropped

	truth := make([]recon.TruthSpan, len(spans))
	for i, s := range spans {
		truth[i] = recon.TruthSpan{SpanID: s.spanID, ParentID: s.parentID, Depth: s.depth}
	}

	survivors := make([]recon.Span, 0, len(spans)-len(dropped))
	for _, s := range spans {
		if _, gone := dropped[s.spanID]; gone {
			continue
		}
		sp := recon.Span{SpanID: s.spanID, ParentID: s.parentID, Depth: s.depth}
		if s.br != nil {
			if ha.mode == "pcr" {
				d, prefix, err := recon.DecodePCRPayload(s.br, ha.cfg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "trace %016x span %016x: %v\n", tid, s.spanID, err)
					os.Exit(1)
				}
				sp.Depth = d
				sp.CkptPrefix = prefix
				sp.LeafCarrier = d%ha.cpd != 0
			} else if ha.mode == "pcrb" || ha.mode == "pcrs" {
				d, prefix, bits, err := recon.DecodePCRBPayload(s.br, ha.cfg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "trace %016x span %016x: %v\n", tid, s.spanID, err)
					os.Exit(1)
				}
				sp.Depth = d
				sp.CkptPrefix = prefix
				sp.BloomBits = bits
				sp.LeafCarrier = d%ha.cpd != 0
			} else {
				d, bits, err := recon.DecodePBPayload(s.br, ha.cfg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "trace %016x span %016x: %v\n", tid, s.spanID, err)
					os.Exit(1)
				}
				sp.Depth = d
				sp.BloomBits = bits
			}
		}
		survivors = append(survivors, sp)
	}

	if ha.mode == "pcr" {
		res := recon.ReconstructPCR(survivors, ha.cfg)
		ha.scoresStore(tid, recon.ScorePCR(res, truth, dropped))
		ha.classifyWrong(tid, res, truth, dropped)
	} else if ha.mode == "pcrb" {
		res := recon.ReconstructPCRB(survivors, ha.cfg)
		ha.scoresStore(tid, recon.ScorePCR(res, truth, dropped))
		ha.classifyWrong(tid, res, truth, dropped)
	} else if ha.mode == "pcrs" {
		res := recon.ReconstructPCRS(survivors, ha.cfg)
		ha.scoresStore(tid, recon.ScorePCR(res, truth, dropped))
		ha.classifyWrong(tid, res, truth, dropped)
		if ha.audit != nil {
			st := auditScores(res, truth, dropped, int64(tid))
			ha.mu.Lock()
			ha.audit.add(st)
			ha.mu.Unlock()
		}
		if ha.dumper != nil || ha.verifyOn {
			brOf := make(map[uint64][]byte)
			for _, s := range spans {
				if s.br != nil {
					if _, gone := dropped[s.spanID]; !gone {
						brOf[s.spanID] = s.br
					}
				}
			}
			if ha.dumper != nil {
				ha.dumper.emit(buildArtifact(tid, survivors, brOf, res))
			}
			if ha.verifyOn {
				vs := verify.Check(toVerifyTrace(tid, survivors, brOf, res), ha.verifyCfg)
				if len(vs) > 0 {
					ha.mu.Lock()
					ha.violations += len(vs)
					ha.mu.Unlock()
					for _, v := range vs {
						fmt.Fprintln(os.Stderr, v)
					}
				}
			}
		}
	} else {
		res := recon.ReconstructPB(survivors, ha.cfg)
		ha.scoresStore(tid, recon.ScorePB(res, truth, dropped))
	}
}

// classifyWrong writes one JSONL record per bridge whose anchor is not a
// true ancestor of the fragment root, locating where the chosen anchor's
// real ancestry diverges from the fragment's true window path.
func (ha *harness) classifyWrong(tid uint64, res recon.Result, truth []recon.TruthSpan, dropped map[uint64]struct{}) {
	if ha.wrongLog == nil || len(res.Bridges) == 0 {
		return
	}
	ha.mu.Lock()
	defer ha.mu.Unlock()
	parent := make(map[uint64]uint64, len(truth))
	depth := make(map[uint64]int, len(truth))
	for _, t := range truth {
		parent[t.SpanID] = t.ParentID
		depth[t.SpanID] = t.Depth
	}
	enc := json.NewEncoder(ha.wrongLog)
	for _, b := range res.Bridges {
		// True nearest surviving ancestor + ancestor test (as in ScorePB).
		isAncestor := false
		trueDepth := -1
		for p := parent[b.OrphanID]; p != 0; p = parent[p] {
			if p == b.AnchorID {
				isAncestor = true
				break
			}
			if trueDepth < 0 {
				if _, gone := dropped[p]; !gone {
					trueDepth = depth[p]
				}
			}
		}
		if isAncestor {
			continue
		}
		// Window path: the fragment root's true ancestors down to the window
		// checkpoint level, inclusive.
		wDepth := ((depth[b.OrphanID] - 1) / ha.cpd) * ha.cpd
		onPath := make(map[uint64]struct{})
		for p := parent[b.OrphanID]; p != 0; p = parent[p] {
			onPath[p] = struct{}{}
			if depth[p] <= wDepth {
				break
			}
		}
		// Walk the anchor's own chain upward to the divergence point.
		divDepth := -1
		if _, ok := onPath[b.AnchorID]; ok {
			divDepth = depth[b.AnchorID] // defensive; cannot happen (isAncestor)
		} else {
			for p := parent[b.AnchorID]; p != 0; p = parent[p] {
				if _, ok := onPath[p]; ok {
					divDepth = depth[p]
					break
				}
				if depth[p] < wDepth {
					break // left the window without meeting the path: wrong window
				}
			}
		}
		enc.Encode(wrongRec{
			Trace:     fmt.Sprintf("%016x", tid),
			Root:      fmt.Sprintf("%016x", b.OrphanID),
			RootDepth: depth[b.OrphanID],
			Anchor:    fmt.Sprintf("%016x", b.AnchorID),
			AnchDepth: depth[b.AnchorID],
			WDepth:    wDepth,
			DivDepth:  divDepth,
			TrueDepth: trueDepth,
		})
	}
}

func (ha *harness) scoresStore(tid uint64, sc recon.Score) {
	ha.mu.Lock()
	defer ha.mu.Unlock()
	if ha.scores == nil {
		ha.scores = make(map[uint64]recon.Score)
	}
	ha.scores[tid] = sc
	// Cumulative progress row every 10k traces (stderr, monitor-friendly).
	ha.prog.spans += sc.Spans
	ha.prog.c += sc.AnchorCorrect
	ha.prog.a += sc.AnchorAncestor
	ha.prog.w += sc.Misattached
	ha.prog.oe += sc.OpenEnds
	ha.prog.oem += sc.OpenEndsMatched
	ha.prog.lost += sc.SpansLost
	ha.prog.placed += sc.OrphansPlaced
	if sc.Misattached > 0 {
		ha.prog.affected++
	}
	n := len(ha.scores)
	if n%10000 == 0 {
		p := &ha.prog
		r := p.c + p.w
		den := func(x, y int) float64 {
			if y == 0 {
				return 100
			}
			return 100 * float64(x) / float64(y)
		}
		fmt.Fprintf(os.Stderr, "PROGRESS traces=%d elapsed=%ds exact=%.2f%% benign=%.2f%% wrong=%.4f%% tr=%.2f%% oe=%.4f%% lost=%.3f%% placed=%d\n",
			n, int(time.Since(ha.prog.start).Seconds()),
			den(p.c, r), den(p.a-p.c, r), den(p.w, r), den(p.affected, n),
			den(p.oem, p.oe), den(p.lost, p.spans), p.placed)
	}
}

// output is the per-trace score arrays, in trace load order.
type output struct {
	Mode               string  `json:"mode"`
	CheckpointDistance int     `json:"checkpoint_distance"`
	DropRate           float64 `json:"drop_rate"`
	BloomFP            float64 `json:"bloom_fp,omitempty"`
	PrefixLen          int     `json:"prefix_len,omitempty"`
	Order              string  `json:"order"`
	Consistency        string  `json:"consistency"`
	Seed               int64   `json:"seed"`
	NumTraces          int     `json:"num_traces"`
	NumSpans           []int   `json:"num_spans"`
	NumDropped         []int   `json:"num_dropped"`
	NumOrphans         []int   `json:"num_orphans"`
	NumReconnected     []int   `json:"num_reconnected"`
	NumAnchorCorrect   []int   `json:"num_anchor_correct"`
	NumAnchorAncestor  []int   `json:"num_anchor_ancestor"`
	NumGapCorrect      []int   `json:"num_gap_correct"`
	NumMisattached     []int   `json:"num_misattached"`
	NumUnanchored      []int   `json:"num_unanchored"`
	NumSynthetic       []int   `json:"num_synthetic"`
	NumBorrowed        []int   `json:"num_borrowed_bloom"`
	NumFragmentsLost   []int   `json:"num_fragments_lost,omitempty"`    // PCR only
	NumSpansLost       []int   `json:"num_spans_lost,omitempty"`        // PCR only
	NumAncestorsSkip   []int   `json:"num_ancestors_skipped,omitempty"` // PCR only
	NumSpansInSkipped  []int   `json:"num_spans_in_skipped,omitempty"`  // PCR only
	NumOpenEnds        []int   `json:"num_open_ends,omitempty"`         // PCRB only
	NumOpenEndsMatched []int   `json:"num_open_ends_matched,omitempty"` // PCRB only
	NumForcedMatches   []int   `json:"num_forced_matches,omitempty"`    // PCRB only
	NumOrphansPlaced   []int   `json:"num_orphans_placed,omitempty"`    // PCRB only
	NumOrphanOpenEnds  []int   `json:"num_orphan_open_ends,omitempty"`  // PCRB only
}

func main() {
	c := parseFlags()
	ha := newHarness(c)

	var traceOrder []uint64
	if c.corpusDir != "" {
		traceOrder = runFromCorpus(c, ha)
	} else {
		traceOrder = runFromJSON(c, ha)
	}
	ha.drain() // wait for in-flight parallel reconstructions

	orderName := "independent"
	if c.bottomUp {
		orderName = "bottom-up"
	}
	out := output{
		Mode:               c.mode,
		CheckpointDistance: c.checkpointDistance,
		DropRate:           c.dropRate,
		Order:              orderName,
		Consistency:        map[bool]string{true: "chain", false: "none"}[c.chainCheck],
		Seed:               c.seed,
		NumTraces:          len(traceOrder),
	}
	switch c.mode {
	case "pcr":
		out.PrefixLen = c.prefixLen
	case "pcrb", "pcrs":
		out.PrefixLen = c.prefixLen
		out.BloomFP = c.bloomFP
	default:
		out.BloomFP = c.bloomFP
	}
	var agg recon.Score
	for _, tid := range traceOrder {
		sc := ha.scores[tid]
		out.NumSpans = append(out.NumSpans, sc.Spans)
		out.NumDropped = append(out.NumDropped, sc.Dropped)
		out.NumOrphans = append(out.NumOrphans, sc.Orphans)
		out.NumReconnected = append(out.NumReconnected, sc.Reconnected)
		out.NumAnchorCorrect = append(out.NumAnchorCorrect, sc.AnchorCorrect)
		out.NumAnchorAncestor = append(out.NumAnchorAncestor, sc.AnchorAncestor)
		out.NumGapCorrect = append(out.NumGapCorrect, sc.GapCorrect)
		out.NumMisattached = append(out.NumMisattached, sc.Misattached)
		out.NumUnanchored = append(out.NumUnanchored, sc.Unanchored)
		out.NumSynthetic = append(out.NumSynthetic, sc.Synthetic)
		out.NumBorrowed = append(out.NumBorrowed, sc.Borrowed)
		if c.mode == "pcr" || c.mode == "pcrb" || c.mode == "pcrs" {
			out.NumFragmentsLost = append(out.NumFragmentsLost, sc.FragmentsLost)
			out.NumSpansLost = append(out.NumSpansLost, sc.SpansLost)
			out.NumAncestorsSkip = append(out.NumAncestorsSkip, sc.AncestorsSkipped)
			out.NumSpansInSkipped = append(out.NumSpansInSkipped, sc.SpansInSkipped)
			out.NumOpenEnds = append(out.NumOpenEnds, sc.OpenEnds)
			out.NumOpenEndsMatched = append(out.NumOpenEndsMatched, sc.OpenEndsMatched)
			out.NumForcedMatches = append(out.NumForcedMatches, sc.ForcedMatches)
			out.NumOrphansPlaced = append(out.NumOrphansPlaced, sc.OrphansPlaced)
			out.NumOrphanOpenEnds = append(out.NumOrphanOpenEnds, sc.OrphanOpenEnds)
		}
		agg.Spans += sc.Spans
		agg.Dropped += sc.Dropped
		agg.Orphans += sc.Orphans
		agg.Reconnected += sc.Reconnected
		agg.AnchorCorrect += sc.AnchorCorrect
		agg.AnchorAncestor += sc.AnchorAncestor
		agg.GapCorrect += sc.GapCorrect
		agg.Misattached += sc.Misattached
		agg.Unanchored += sc.Unanchored
		agg.Synthetic += sc.Synthetic
		agg.Borrowed += sc.Borrowed
		agg.FragmentsLost += sc.FragmentsLost
		agg.SpansLost += sc.SpansLost
		agg.AncestorsSkipped += sc.AncestorsSkipped
		agg.SpansInSkipped += sc.SpansInSkipped
		agg.OpenEnds += sc.OpenEnds
		agg.OpenEndsMatched += sc.OpenEndsMatched
		agg.ForcedMatches += sc.ForcedMatches
		agg.OrphansPlaced += sc.OrphansPlaced
		agg.OrphanOpenEnds += sc.OrphanOpenEnds
	}

	f, err := os.Create(c.outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output: %v\n", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "write output: %v\n", err)
		os.Exit(1)
	}
	f.Close()

	pct := func(n, d int) float64 {
		if d == 0 {
			return 0
		}
		return 100 * float64(n) / float64(d)
	}
	fmt.Fprintf(os.Stderr, "traces=%d spans=%d dropped=%d orphans=%d\n",
		len(traceOrder), agg.Spans, agg.Dropped, agg.Orphans)
	fmt.Fprintf(os.Stderr, "reconnected=%d (%.2f%%) anchor_correct=%d (%.2f%%) anchor_ancestor=%d (%.2f%%) gap_correct=%d (%.2f%%)\n",
		agg.Reconnected, pct(agg.Reconnected, agg.Orphans),
		agg.AnchorCorrect, pct(agg.AnchorCorrect, agg.Orphans),
		agg.AnchorAncestor, pct(agg.AnchorAncestor, agg.Orphans),
		agg.GapCorrect, pct(agg.GapCorrect, agg.Orphans))
	// "via_carrier": PB = bloom borrowed across reconstructed structure or
	// by membership scan (a verified guess); PCR = exact inheritance from a
	// real-edge in-fragment descendant carrier (not an error channel). The
	// JSON field keeps its legacy name num_borrowed_bloom for script compat.
	fmt.Fprintf(os.Stderr, "misattached=%d unanchored=%d synthetic=%d via_carrier=%d\n",
		agg.Misattached, agg.Unanchored, agg.Synthetic, agg.Borrowed)
	fmt.Fprintf(os.Stderr, "ambiguous=%d (of which misattached=%d); non-ambiguous misattached=%d\n",
		agg.Ambiguous, agg.AmbiguousBad, agg.Misattached-agg.AmbiguousBad)
	if c.mode == "pcr" || c.mode == "pcrb" || c.mode == "pcrs" {
		fmt.Fprintf(os.Stderr, "fragments_lost=%d spans_lost=%d (%.2f%% of survivors thrown away)\n",
			agg.FragmentsLost, agg.SpansLost, pct(agg.SpansLost, agg.Spans-agg.Dropped))
		fmt.Fprintf(os.Stderr, "ancestors_skipped=%d spans_in_skipped_fragments=%d (%.2f%% of survivors)\n",
			agg.AncestorsSkipped, agg.SpansInSkipped, pct(agg.SpansInSkipped, agg.Spans-agg.Dropped))
		fmt.Fprintf(os.Stderr, "open_ends=%d matched=%d (%.2f%%) forced=%d\n",
			agg.OpenEnds, agg.OpenEndsMatched, pct(agg.OpenEndsMatched, agg.OpenEnds), agg.ForcedMatches)
		fmt.Fprintf(os.Stderr, "orphans_placed=%d orphan_open_ends_pending=%d\n",
			agg.OrphansPlaced, agg.OrphanOpenEnds)
	}
	fmt.Fprintf(os.Stderr, "Wrote %d traces to %s\n", len(traceOrder), c.outputPath)
}

func runFromJSON(c config, ha *harness) []uint64 {
	t0 := time.Now()
	traces, _, err := loadTracesFromDir(c.inputDir, c.traceCount, c.requireClean)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading traces: %v\n", err)
		os.Exit(1)
	}
	if len(traces) == 0 {
		fmt.Fprintln(os.Stderr, "No traces loaded.")
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Loaded %d traces in %s\n", len(traces), time.Since(t0).Round(time.Millisecond))

	traceOrder := make([]uint64, len(traces))
	for i, t := range traces {
		traceOrder[i] = t.TraceID
		ha.beginTrace(t.TraceID, len(t.Spans))
	}

	// Same global event sort as trace_sim (ts, kind, depth-rank, tid, sid).
	type ev struct {
		ts        int64
		kind      bridge.Kind
		depth     int
		tid, sid  uint64
		pid       uint64
		serviceID uint16
	}
	var events []ev
	for _, t := range traces {
		for _, s := range t.Spans {
			events = append(events, ev{s.StartNS, bridge.KindStart, s.Depth, t.TraceID, s.SpanID, s.ParentID, s.ServiceID})
			events = append(events, ev{s.EndNS, bridge.KindEnd, s.Depth, t.TraceID, s.SpanID, s.ParentID, s.ServiceID})
		}
	}
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		var ar, br int
		if a.kind == bridge.KindStart {
			ar, br = a.depth, b.depth
		} else {
			ar, br = -a.depth, -b.depth
		}
		if ar != br {
			return ar < br
		}
		if a.tid != b.tid {
			return a.tid < b.tid
		}
		return a.sid < b.sid
	})

	for _, e := range events {
		ha.onEvent(e.ts, e.kind, e.tid, e.sid, e.pid, e.serviceID)
	}
	return traceOrder
}

func runFromCorpus(c config, ha *harness) []uint64 {
	eventsPath, metaPath := corpus.Paths(c.corpusDir)
	meta, err := corpus.ReadMeta(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading meta: %v\n", err)
		os.Exit(1)
	}
	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening events: %v\n", err)
		os.Exit(1)
	}
	defer er.Close()
	traceOrder := append([]uint64(nil), meta.TraceOrder...)
	spanCounts := meta.SpanCounts
	if c.traceCount > 0 && c.traceCount < len(traceOrder) {
		traceOrder = traceOrder[:c.traceCount]
		spanCounts = spanCounts[:c.traceCount]
	}
	fmt.Fprintf(os.Stderr, "Opened corpus (%d traces, processing %d)\n",
		len(meta.TraceOrder), len(traceOrder))

	selected := make(map[uint64]struct{}, len(traceOrder))
	for i, tid := range traceOrder {
		selected[tid] = struct{}{}
		ha.beginTrace(tid, int(spanCounts[i]))
	}

	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "corpus read error: %v\n", err)
			os.Exit(1)
		}
		// Skip events of unselected traces: the handler never sees them, so
		// the selected traces' simulation matches a corpus containing only
		// the selection (PB state is strictly per-trace).
		if _, ok := selected[ce.TraceID]; !ok {
			continue
		}
		ha.onEvent(ce.TS, bridge.Kind(ce.Kind), ce.TraceID, ce.SpanID, ce.ParentID, ce.ServiceID)
	}
	return traceOrder
}

// loadTracesFromDir mirrors cmd/trace_sim's loader: all *.json files in dir,
// alphabetical order.
func loadTracesFromDir(dir string, traceCount int, requireClean bool) ([]loader.Trace, *loader.ServiceTable, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > 5 && name[len(name)-5:] == ".json" {
			files = append(files, name)
		}
	}
	sort.Strings(files)

	services := loader.NewServiceTable()
	out := make([]loader.Trace, 0, len(files))
	for _, name := range files {
		ts, err := loader.LoadTraceFile(filepath.Join(dir, name), services, requireClean)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", name, err)
			continue
		}
		out = append(out, ts...)
		if traceCount > 0 && len(out) >= traceCount {
			out = out[:traceCount]
			break
		}
	}
	return out, services, nil
}
