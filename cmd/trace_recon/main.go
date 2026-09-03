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
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bridges/bloom"
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
	cgrp               bool   // pcrs + HA: run the PCRS solver on CGPRB (HA-bearing) payloads, with named dropped fan-outs as constraints
	fullsatPB          bool   // pcrs: solve each cluster as a general declarative CP-SAT model (full-SAT parity baseline; no HA)
	fullsatCGPB        bool   // pcrs + HA: full-SAT model with named-synthetic identities (CGP) injected into the cluster occupancy
	fullsatEngine      bool   // pcrs: the single-pass ReconstructFullSAT engine (anchoring + all rules as one CP-SAT model)
	noParents          bool   // ablation: ignore the intrinsic ParentID naming (no sibling coalescing) even though it is available
	timingPath         string // non-empty: write per-trace reconstruction timing CSV here
	checkpointDistance int
	dropRate           float64
	dropRates          string // comma list: one decode, all rates reconstructed per pass (CGP/PB/SB3 families)
	seed               int64
	traceCount         int
	requireClean       bool
	bottomUp           bool
	bloomFP            float64
	fpBits             int  // SB3 DEE-owner fingerprint width
	topoOnly           bool // SB3: omit EE/DEE while retaining sparse ordinals
	lehmerEE           bool // SB3: Lehmer-code EE/DEE groups
	sb3IgnoreOrdinals  bool // SB3 ablation: reconstruct without ordinal-guided choices
	cgp0Legacy         bool // CGP0 ablation: original first-match/anonymous-gap implementation
	pb0Legacy          bool // PB0 ablation: original first-deepest-match implementation
	greedyNoGrouped    bool // greedy ablation: no exact-parent/HA-group Bloom intersection
	greedyNoHardHA     bool // greedy ablation: HA is optional naming evidence, not a hard carrier constraint
	greedyNoFallback   bool // greedy ablation: evaluate only the ordinary first route
	chainCheck         bool
	prefixLen          int    // PCR truncated checkpoint-root ID length
	classifyWrong      string // JSONL dump path for wrong-edge classification (pcr/pcrb)
	stopOnGap          bool   // PCRB: stop threading at first zero-candidate level
	tiePolicy          string // PCRS: thread-item tie resolution (aware|id|stop)
	workers            int    // reconstruction worker goroutines (0 = NumCPU)
	scoreAudit         bool   // fault-injection sensitivity audit of the scorer
	compareScorers     bool   // CGP0 diagnostic: evaluate canonical and historical score/denominator contracts together
	dumpRecon          string // serialize per-trace reconstruction artifacts (JSONL.gz) for recon_verify
	verifyInline       bool   // run the independent checker in-process after every reconstruction
	onlyTraces         string // comma-separated hex trace IDs: reconstruct ONLY these (drop RNG still advances for all -> exact survivors). Diagnostic.
	dumpSurvivors      string // gob-dump each reconstructed trace's survivors+truth+dropped+cfg here (pair with --only-traces to capture one trace for offline replay via cmd/recon_one).
	dumpOnly           bool   // with --dump-survivors: capture only, skip reconstruction (fast capture of slow traces)
	traceStore         string // read a per-trace store (cmd/corpus_split output) instead of --corpus, running handler+drop+reconstruct per-trace in PARALLEL (bit-identical to streaming).
	perTraceDropSeed   bool   // seed the drop RNG per-trace (from traceID+seed) instead of one global stream, so drops are independent of trace order/partitioning.
	sampleCount        int    // if >0, process a RANDOM sample of this many traces (seeded) instead of the first traceCount; avoids prefix bias.
	sampleSeed         int64  // seed for the random sample selection (independent of the drop seed)
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.outputPath, "o", "", "Output JSON file (required)")
	flag.StringVar(&c.outputPath, "output", "", "Output JSON file (required)")
	flag.StringVar(&c.corpusDir, "corpus", "", "Read events.bin + meta.bin from this corpus dir")
	flag.BoolVar(&c.dumpOnly, "dump-only", false, "With --dump-survivors: dump the trace inputs and SKIP reconstruction (fast capture of slow traces for offline replay).")
	flag.StringVar(&c.traceStore, "trace-store", "", "Read a per-trace store (cmd/corpus_split output) instead of --corpus. Runs handler+drop+reconstruct per-trace in PARALLEL (no single-threaded event streaming); bit-identical results. Needs --corpus too (for meta: trace order + service names).")
	flag.StringVar(&c.mode, "mode", "pb", "Bridge mode: pb, pcr, pcrb, pcrs, cgprb, cgp*/pb*, or sb3")
	flag.BoolVar(&c.cgrp, "cgrp", false, "PCRS+CGP: run the PCRS solver on CGPRB (HA-bearing) payloads, ingesting the hash array so named dropped fan-outs become constraints (carriers hard, bloom-confirmers soft). Requires --mode pcrs.")
	flag.BoolVar(&c.fullsatPB, "fullsat-pb", false, "PCRS: solve each cluster as a general declarative CP-SAT model (docs/fullsat_shim.md) instead of the Go B&B — the full-SAT parity baseline (no HA). Requires --mode pcrs and the cpsat build tag.")
	flag.StringVar(&c.timingPath, "timing", "", "Write a per-trace reconstruction-timing CSV here (tid,survivors,spans,dropped,recon_ns) and print a TIMING summary. (TRACE_RECON_TIMING=1 prints the summary without the CSV.)")
	flag.BoolVar(&c.fullsatCGPB, "fullsat-cgpb", false, "PCRS + HA: the --fullsat-pb model plus named-synthetic identities from the hash array (CGP) injected into the cluster occupancy — hard fan-out coalescing + exclusivity. Requires --mode pcrs and the cpsat build tag.")
	flag.BoolVar(&c.fullsatEngine, "fullsat", false, "PCRS: the single-pass ReconstructFullSAT engine — anchoring + all invariants compiled into one CP-SAT model per cluster, solved once (no pass-1 commit, no post-processing). Requires --mode pcrs and the cpsat build tag.")
	flag.BoolVar(&c.noParents, "no-parents", false, "Ablation (--fullsat): ignore the intrinsic ParentID naming, so fragment roots get private anonymous synthetic parents and siblings never coalesce (pure connectivity, no recovered fan-out topology).")
	flag.IntVar(&c.prefixLen, "prefix-len", bridge.DefaultPCRPrefixLen, "PCR mode: truncated checkpoint-root span ID length in bytes (1-8)")
	flag.StringVar(&c.onlyTraces, "only-traces", "", "Diagnostic: comma-separated hex trace IDs to reconstruct ONLY (corpus still streams and drop RNG still advances for all traces, so survivors are bit-identical to a full run). Lets you repro a single slow trace in seconds.")
	flag.StringVar(&c.dumpSurvivors, "dump-survivors", "", "Gob-dump each reconstructed trace's decoded survivors+truth+dropped+cfg to this file. Pair with --only-traces to capture one trace from a single corpus walk, then replay it offline with cmd/recon_one (no corpus walk).")
	flag.IntVar(&c.checkpointDistance, "checkpoint-distance", 1, "Checkpoint distance")
	flag.Float64Var(&c.bloomFP, "bloom-fp", bridge.DefaultBloomFPRate, "Target bloom false-positive rate (sets bloom geometry on both the emit and reconstruction sides)")
	flag.IntVar(&c.fpBits, "fp-bits", 16, "SB3 delayed-end owner fingerprint width in bits")
	flag.BoolVar(&c.topoOnly, "topo-only", false, "SB3: omit lateral EE/DEE evidence while retaining topology and sparse ordinals")
	flag.BoolVar(&c.lehmerEE, "lehmer-ee", false, "SB3: Lehmer-code EE/DEE groups on emit and decode")
	flag.BoolVar(&c.sb3IgnoreOrdinals, "sb3-ignore-ordinals", false, "SB3 ablation: retain the same payload and hard/Bloom evidence but do not use sparse ordinals to select topology")
	flag.BoolVar(&c.cgp0Legacy, "cgp0-legacy", false, "CGP0 ablation: use the original lean first-match reconstructor (no exact-parent route units or global hard-evidence pruning)")
	flag.BoolVar(&c.pb0Legacy, "pb0-legacy", false, "PB0 ablation: use the original lean first-deepest-match path reconstructor")
	flag.BoolVar(&c.greedyNoGrouped, "greedy-no-grouped-evidence", false, "CGP0/SB3 ablation: do not intersect Bloom evidence across exact-parent or HA fanout groups")
	flag.BoolVar(&c.greedyNoHardHA, "greedy-no-hard-ha", false, "CGP0/SB3 ablation: do not use HA carrier ancestry as a hard candidate constraint")
	flag.BoolVar(&c.greedyNoFallback, "greedy-no-route-fallback", false, "CGP0/SB3 ablation: evaluate only the ordinary first deepest/named greedy route")
	flag.Float64Var(&c.dropRate, "drop-rate", 1.0, "Probability of dropping each non-checkpoint span (1.0 = drop all)")
	flag.StringVar(&c.dropRates, "drop-rates", "", "Comma-separated drop rates run on one decoded trace per pass for CGP/PB/SB3 family modes. Drops are per-trace-seeded and bit-identical to separate runs. --timing must contain {dc}.")
	order := ""
	flag.StringVar(&order, "order", "bottom-up", "Orphan processing order: bottom-up (deepest-first, default) or independent")
	consistency := ""
	flag.StringVar(&consistency, "consistency", "chain", "Anchor candidate consistency check: chain (test the candidate's nameable ancestry against the bloom) or none")
	flag.Int64Var(&c.seed, "seed", 42, "RNG seed for the drop policy")
	flag.BoolVar(&c.perTraceDropSeed, "per-trace-drop-seed", false, "Seed the drop RNG per-trace from (traceID, seed) and draw over spans in spanID order, so each trace drops the same spans regardless of processing order or corpus partitioning. Required for partition-invariant parallel sweeps; differs bit-wise from the legacy global-stream drops.")
	flag.IntVar(&c.traceCount, "trace-count", 0, "Max number of traces to process (0 = all). Corpus mode: first N of the corpus trace order; events of other traces are skipped during streaming")
	flag.IntVar(&c.sampleCount, "sample", 0, "If >0, process a RANDOM sample of this many traces (uniform over the trace order, seeded by --sample-seed) instead of the first --trace-count. Avoids prefix bias; same seed => same sample across runs/cells.")
	flag.Int64Var(&c.sampleSeed, "sample-seed", 1, "Seed for --sample trace selection (independent of the drop --seed)")
	flag.BoolVar(&c.requireClean, "require-clean", true, "Cleanliness filter (JSON mode only)")
	flag.StringVar(&c.classifyWrong, "classify-wrong", "", "Write one JSONL record per wrong-edge bridge (anchor not a true ancestor) locating the divergence against truth")
	flag.IntVar(&c.workers, "workers", 0, "Reconstruction worker goroutines (0 = NumCPU). Drop decisions stay serial, so results are bit-identical at any worker count")
	gapPolicy := ""
	flag.StringVar(&gapPolicy, "gap-policy", "continue", "PCRB threading at a zero-candidate level: continue (default) or stop (maximally FP-averse)")
	flag.StringVar(&c.tiePolicy, "tie-policy", "aware", "PCRS threading tie resolution: aware (global bit-accounting, default), id (span-ID coin), or stop (abstain)")
	flag.BoolVar(&c.scoreAudit, "score-audit", false, "Fault-injection sensitivity audit: per trace, inject known scoring errors into a copy of the reconstruction and require ScorePCR to detect each with the exact predicted counter delta (report on stderr)")
	flag.BoolVar(&c.compareScorers, "compare-scorers", false, "CGP0 diagnostic: apply canonical evidence-bounded and historical permissive scorers to the same reconstruction and report both input-obligation and emitted-output denominators")
	flag.StringVar(&c.dumpRecon, "dump-recon", "", "Write per-trace reconstruction artifacts (JSONL.gz) for standalone auditing by recon_verify")
	flag.BoolVar(&c.verifyInline, "verify", false, "Run the independent structural/evidence checker (package verify) in-process after every reconstruction; violations reported at exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <input_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s --corpus <corpus_dir> [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}
	var primeM, primeMByteCap bool
	flag.BoolVar(&primeM, "prime-m", false, "size blooms with a prime bit count (fixes small-m double-hash clustering; affects emit + recon together)")
	flag.BoolVar(&primeMByteCap, "prime-m-bytecap", false, "with --prime-m: keep the prime within the raw size's byte budget")
	flag.Parse()
	bloom.PrimeM = primeM // set before any handler/config sizing (emit + recon must match)
	bloom.PrimeMByteCap = primeMByteCap
	if c.mode != "pb" && c.mode != "pcr" && c.mode != "pcrb" && c.mode != "pcrs" && c.mode != "cgprb" && c.mode != "cgp2" && c.mode != "pb2" && c.mode != "cgp1" && c.mode != "pb1" && c.mode != "cgp0" && c.mode != "pb0" && c.mode != "sb3" {
		fmt.Fprintf(os.Stderr, "error: unsupported reconstruction mode %q\n", c.mode)
		os.Exit(2)
	}
	if c.cgrp && c.mode != "pcrs" {
		fmt.Fprintf(os.Stderr, "error: --cgrp requires --mode pcrs (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.cgp0Legacy && c.mode != "cgp0" {
		fmt.Fprintf(os.Stderr, "error: --cgp0-legacy requires --mode cgp0 (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.compareScorers && c.mode != "cgp0" {
		fmt.Fprintf(os.Stderr, "error: --compare-scorers requires --mode cgp0 (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.pb0Legacy && c.mode != "pb0" {
		fmt.Fprintf(os.Stderr, "error: --pb0-legacy requires --mode pb0 (got %q)\n", c.mode)
		os.Exit(2)
	}
	if (c.greedyNoGrouped || c.greedyNoFallback) && c.mode != "cgp0" && c.mode != "pb0" && c.mode != "sb3" {
		fmt.Fprintf(os.Stderr, "error: grouped-evidence/route-fallback ablations require --mode cgp0, pb0, or sb3 (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.greedyNoHardHA && c.mode != "cgp0" && c.mode != "sb3" {
		fmt.Fprintf(os.Stderr, "error: --greedy-no-hard-ha requires --mode cgp0 or --mode sb3 (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.fullsatPB && c.mode != "pcrs" {
		fmt.Fprintf(os.Stderr, "error: --fullsat-pb requires --mode pcrs (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.fullsatCGPB && c.mode != "pcrs" {
		fmt.Fprintf(os.Stderr, "error: --fullsat-cgpb requires --mode pcrs (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.fullsatPB || c.fullsatCGPB || c.fullsatEngine {
		recon.EnableFullsatPB(c.seed, 0) // fixed seed for machine-independent results
	}
	recon.SetFullsatNoParents(c.noParents) // ablation: drop intrinsic ParentID naming
	if c.fullsatEngine && c.mode != "pcrs" {
		fmt.Fprintf(os.Stderr, "error: --fullsat requires --mode pcrs (got %q)\n", c.mode)
		os.Exit(2)
	}
	if c.fullsatCGPB {
		recon.EnableFullsatCGPB() // inject named-synthetic identities into the cluster occupancy
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
	ordinal  int    // ground-truth start ordinal under parent
	endPos   int64  // ground-truth END position in the trace event total-order
}

// harness drives the per-trace collect -> drop -> reconstruct -> score loop.
type harness struct {
	h              bridge.Handler
	mode           string // "pb" or "pcr": selects payload decode + reconstruction
	fullsatEngine  bool   // pcrs mode: use the single-pass ReconstructFullSAT engine
	cfg            recon.Config
	rng            *rand.Rand
	dropAll        bool
	rate           float64
	seed           int64 // base drop seed (for per-trace reseeding)
	perTraceSeed   bool  // reseed the drop RNG per-trace (order/partition-invariant)
	compareScorers bool  // emit a controlled canonical-vs-historical score comparison

	spansByTID map[uint64][]collSpan
	idxByKey   map[spanKey]int
	openByTID  map[uint64]int
	// nextSeq is nested per trace so a closed trace's counters can be freed;
	// a flat (traceID, parentID) map grows with every parent span ever seen
	// (gigabytes over the full corpus).
	nextSeq       map[uint64]map[uint64]int
	eventPosByTID map[uint64]int64
	// SB3 delayed-end records are captured when generated and keyed by their
	// origin trace. This models the collector routing the embedded trace ID even
	// when the record is physically picked up by a later trace.
	deesByTID map[uint64][]bridge.DEEQuad

	scores map[uint64]recon.Score

	// cg2: inline from-scratch reconstruction + canonical evidence-bounded
	// topology-score tallies, guarded by mu.
	// Additive across the parallel workers; reported at drain.
	cg2 cgp2acc

	// CGP call-graph-topology accounting (TRACE_RECON_TOPO=1), guarded by mu.
	topoOn        bool
	topoTotal     int64
	topoConn      int64 // connectivity-correct traces
	topoCorrect   int64 // topology-correct traces (conn + fan-out)
	topoStrict    int64 // strict shape-correct traces (singleton routing checked too)
	topoSingleBP  int64 // lone-orphan branch-point groups tested by the strict check
	topoSingleMis int64 // of those, misrouted (caught only by the strict check)
	topoFanTraces int64 // traces with >=1 dropped fan-out
	topoCorrectF  int64 // topology-correct among the dropped-fan-out traces
	topoTrueSib   int64
	topoRecall    int64
	topoReconSib  int64
	topoPrec      int64

	cpd      int
	wrongLog *os.File // non-nil: wrong-edge classification dump

	// Per-trace reconstruction timing (TRACE_RECON_TIMING=1, or --timing FILE
	// for a per-trace CSV). Guarded by mu; recon_ns is measured around the
	// ReconstructX call only (decode/score excluded). Buffered in memory and
	// written/summarized at the end.
	timingOn   bool
	timingPath string
	timingRecs []traceTiming

	// Multi-drop: decode each trace ONCE, then reconstruct+score it under every
	// one of these drop rates in the same pass (cgp2/pb2 only). Empty => the
	// legacy single --drop-rate path. Per-rate accumulators + timing, guarded by
	// mu. Drops are per-trace-seeded so each rate is bit-identical to a separate
	// --drop-rate run.
	mdRates  []float64
	mdDC     []string        // drop code per rate ("d05", "d10", ...), for CSV names + labels
	mdAcc    []cgp2acc       // per-rate correctness accumulator
	mdTiming [][]traceTiming // per-rate per-trace timing (if timingOn)

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

	// only: if non-nil, reconstruct ONLY these trace IDs (--only-traces). The
	// drop RNG still advances for every trace, so survivors are bit-identical.
	only map[uint64]bool

	dumpOnly bool // capture-only: skip reconstruction after dumping

	// dumpW: if non-nil (--dump-survivors), each reconstructed trace's decoded
	// input (survivors+truth+dropped+cfg) is gob-streamed here for offline replay.
	dumpW *recon.DumpWriter

	// inflight: tids currently being reconstructed -> start time (watchdog,
	// TRACE_RECON_WATCHDOG=<secs>). A background goroutine logs any tid in
	// flight longer than the threshold, so a single grinding monster trace
	// self-identifies at the tail without waiting for it to finish.
	inflight   map[uint64]time.Time
	inflightMu sync.Mutex
}

type finishJob struct {
	tid          uint64
	spans        []collSpan
	dropped      map[uint64]struct{}   // single-drop path
	droppedMulti []map[uint64]struct{} // multi-drop path: one set per ha.mdRates
	dees         []bridge.DEEQuad      // SB3 DEEs already grouped by origin trace
}

// cgp2acc accumulates the inline cgp2 per-trace scores (mirrors cmd/cgp2_replay).
// All fields are additive so the parallel workers merge under harness.mu.
type cgp2acc struct {
	nt, feas, clean, empty, emptyClean                                                                    int
	realNodes, edgeExact, edgeAnonOK, edgeWrong, constraintWrong, survN, survEx, named, namedEx, totWrong int
	emptySurvSum, emptyNoDrop, emptyTiny                                                                  int
	greedyChecked, greedyCandidates, greedyHardOverrides                                                  int64
	greedyHardConflicts, greedyParentConflicts, greedyHAConflicts                                         int64
	sb3                                                                                                   sb3acc
	scoreComparison                                                                                       scorerComparisonAcc
	chainEvidence                                                                                         chainEvidenceAcc
	fanoutEvidence                                                                                        fanoutEvidenceAcc
}

type fanoutTraceBinAcc struct {
	traces, clean int64
}

type fanoutWindowBinAcc struct {
	windows, correct int64
}

type fanoutEvidenceAcc struct {
	traces, clean, haEnabled, tracesWithHA int64
	haEntries, haCarriers, distinctHA      int64
	recoveredHA, routeUnits                int64
	unitsWithRequiredHA, requiredHA        int64
	evidenceGroups, witnessedGroups        int64
	multiBloomGroups                       int64
	groupedAnchorsPruned                   int64
	groupedFanoutsPruned                   int64
	hardRoutesRejected                     int64
	sumDistinct, sumDistinctSq             float64
	sumDistinctClean                       float64
	byDistinctBucket                       map[int]*fanoutTraceBinAcc
	windows, correctWindows                int64
	haEntriesOnWindowPaths                 int64
	haEntriesOffWindowPaths                int64
	truthFanoutOccurrences                 int64
	knownFanoutOccurrences                 int64
	windowByHACount                        map[int]*fanoutWindowBinAcc
	windowByKnownFanoutCount               map[int]*fanoutWindowBinAcc
	windowByTruthFanoutCount               map[int]*fanoutWindowBinAcc
	windowsWithFanouts                     int64
	correctWindowsWithFanouts              int64
	sumWindowHA, sumWindowHASq             float64
	sumWindowHACorrect                     float64
	sumWindowKnown, sumWindowKnownSq       float64
	sumWindowKnownCorrect                  float64
	sumWindowFanouts, sumWindowFanoutsSq   float64
	sumWindowFanoutsCorrect                float64
	sumCoverage, sumCoverageSq             float64
	sumCoverageCorrect                     float64
	sumKnownCoverage, sumKnownCoverageSq   float64
	sumKnownCoverageCorrect                float64
	coverageByBucket                       map[int]*fanoutWindowBinAcc
	knownCoverageByBucket                  map[int]*fanoutWindowBinAcc
	tracesWithWindows                      int64
	cleanTracesWithWindows                 int64
	sumTraceMeanHA, sumTraceMeanHASq       float64
	sumTraceMeanHAClean                    float64
	sumTraceMeanKnown, sumTraceMeanKnownSq float64
	sumTraceMeanKnownClean                 float64
	tracesWithFanoutOpportunities          int64
	cleanTracesWithFanoutOpportunities     int64
	sumTraceCoverage, sumTraceCoverageSq   float64
	sumTraceCoverageClean                  float64
	sumTraceKnownCoverage                  float64
	sumTraceKnownCoverageSq                float64
	sumTraceKnownCoverageClean             float64
	routedUnits, correctAnchorRoutes       int64
	correctTopologyRoutes                  int64
	sumRouteRequiredHA                     float64
	sumRouteRequiredHASq                   float64
	sumRouteRequiredHACorrect              float64
	sumRouteFanoutGroups                   float64
	sumRouteFanoutGroupsSq                 float64
	sumRouteFanoutGroupsCorrect            float64
	sumRouteMultiBloomGroups               float64
	sumRouteMultiBloomGroupsSq             float64
	sumRouteMultiBloomGroupsCorrect        float64
	sumRouteFanoutTests                    float64
	sumRouteFanoutTestsSq                  float64
	sumRouteFanoutTestsCorrect             float64
	routeByRequiredHA                      map[int]*fanoutWindowBinAcc
	routeByFanoutGroups                    map[int]*fanoutWindowBinAcc
	routeByMultiBloomGroups                map[int]*fanoutWindowBinAcc
	routeByFanoutTests                     map[int]*fanoutWindowBinAcc
}

func fanoutBucket(n int) int {
	switch {
	case n == 0:
		return 0
	case n == 1:
		return 1
	case n == 2:
		return 2
	case n <= 4:
		return 3
	case n <= 8:
		return 4
	case n <= 16:
		return 5
	case n <= 32:
		return 6
	default:
		return 7
	}
}

func windowHABucket(n int) int {
	if n >= 5 {
		return 5
	}
	return n
}

func coverageBucket(x float64) int {
	switch {
	case x == 0:
		return 0
	case x <= 0.25:
		return 1
	case x <= 0.50:
		return 2
	case x <= 0.75:
		return 3
	case x < 1:
		return 4
	default:
		return 5
	}
}

func (a *fanoutEvidenceAcc) add(s recon.GreedyFanoutStats, res recon.Result, survivors []recon.Span, truth []recon.TruthSpan, cpd int, clean bool) {
	a.traces++
	if clean {
		a.clean++
	}
	if s.HAEnabled {
		a.haEnabled++
	}
	if s.DistinctHAFanoutsAvailable > 0 {
		a.tracesWithHA++
	}
	a.haEntries += int64(s.HAEntriesAvailable)
	a.haCarriers += int64(s.HACarriersAvailable)
	a.distinctHA += int64(s.DistinctHAFanoutsAvailable)
	a.recoveredHA += int64(s.RecoveredHAFanoutsUsed)
	a.routeUnits += int64(s.RouteUnits)
	a.unitsWithRequiredHA += int64(s.RouteUnitsWithRequiredHA)
	a.requiredHA += int64(s.RequiredHAConstraints)
	a.evidenceGroups += int64(s.EvidenceGroups)
	a.witnessedGroups += int64(s.WitnessedFanoutGroups)
	a.multiBloomGroups += int64(s.MultiBloomEvidenceGroups)
	a.groupedAnchorsPruned += int64(s.GroupedAnchorCandidatesPruned)
	a.groupedFanoutsPruned += int64(s.GroupedFanoutCandidatesPruned)
	a.hardRoutesRejected += int64(s.HardRouteCandidatesRejected)
	x := float64(s.DistinctHAFanoutsAvailable)
	a.sumDistinct += x
	a.sumDistinctSq += x * x
	if clean {
		a.sumDistinctClean += x
	}
	if a.byDistinctBucket == nil {
		a.byDistinctBucket = make(map[int]*fanoutTraceBinAcc)
		a.windowByHACount = make(map[int]*fanoutWindowBinAcc)
		a.windowByKnownFanoutCount = make(map[int]*fanoutWindowBinAcc)
		a.windowByTruthFanoutCount = make(map[int]*fanoutWindowBinAcc)
		a.coverageByBucket = make(map[int]*fanoutWindowBinAcc)
		a.knownCoverageByBucket = make(map[int]*fanoutWindowBinAcc)
		a.routeByRequiredHA = make(map[int]*fanoutWindowBinAcc)
		a.routeByFanoutGroups = make(map[int]*fanoutWindowBinAcc)
		a.routeByMultiBloomGroups = make(map[int]*fanoutWindowBinAcc)
		a.routeByFanoutTests = make(map[int]*fanoutWindowBinAcc)
	}
	bucket := fanoutBucket(s.DistinctHAFanoutsAvailable)
	b := a.byDistinctBucket[bucket]
	if b == nil {
		b = &fanoutTraceBinAcc{}
		a.byDistinctBucket[bucket] = b
	}
	b.traces++
	if clean {
		b.clean++
	}

	trueParent := make(map[uint64]uint64, len(truth))
	trueDepth := make(map[uint64]int, len(truth))
	trueChildren := make(map[uint64]int, len(truth))
	for _, span := range truth {
		trueParent[span.SpanID] = span.ParentID
		trueDepth[span.SpanID] = span.Depth
		if span.ParentID != 0 {
			trueChildren[span.ParentID]++
		}
	}
	nameable := make(map[uint64]bool, len(survivors)*2)
	survived := make(map[uint64]bool, len(survivors))
	missingParentReferences := make(map[uint64]int)
	knownFanouts := make(map[uint64]bool)
	for _, span := range survivors {
		survived[span.SpanID] = true
		nameable[span.SpanID] = true
		if span.ParentID != 0 {
			nameable[span.ParentID] = true
		}
		for _, entry := range span.HA {
			nameable[entry.ParentID] = true
			knownFanouts[entry.ParentID] = true
		}
	}
	for _, span := range survivors {
		if span.ParentID != 0 && !survived[span.ParentID] {
			missingParentReferences[span.ParentID]++
		}
	}
	for parentID, references := range missingParentReferences {
		if references >= 2 {
			knownFanouts[parentID] = true
		}
	}
	trueAnchor := func(id uint64) uint64 {
		seen := make(map[uint64]bool)
		for parent := trueParent[id]; parent != 0 && !seen[parent]; parent = trueParent[parent] {
			seen[parent] = true
			if survived[parent] {
				return parent
			}
		}
		return 0
	}
	routeTopologyCorrect := func(orphanID, anchorID uint64) bool {
		truthNode, reconNode := orphanID, orphanID
		seenTruth := make(map[uint64]bool)
		for truthNode != anchorID && !seenTruth[truthNode] {
			seenTruth[truthNode] = true
			truthNext := trueParent[truthNode]
			reconNext, exists := res.ReconParent[reconNode]
			if truthNext == 0 || !exists {
				return false
			}
			if nameable[truthNext] {
				if reconNext != truthNext || res.ReconAnon[reconNext] {
					return false
				}
			} else if !res.ReconAnon[reconNext] {
				return false
			}
			truthNode, reconNode = truthNext, reconNext
		}
		return truthNode == anchorID && reconNode == anchorID
	}
	for _, route := range res.GreedyChain.Routes {
		if !route.Routed {
			continue
		}
		anchorCorrect := len(route.OrphanIDs) > 0
		topologyCorrect := len(route.OrphanIDs) > 0
		for _, orphanID := range route.OrphanIDs {
			if route.AnchorID != trueAnchor(orphanID) {
				anchorCorrect = false
			}
			if !routeTopologyCorrect(orphanID, trueAnchor(orphanID)) {
				topologyCorrect = false
			}
		}
		a.routedUnits++
		if anchorCorrect {
			a.correctAnchorRoutes++
		}
		if topologyCorrect {
			a.correctTopologyRoutes++
		}
		addRouteMetric := func(x int, sum, sumSq, sumCorrect *float64, bins map[int]*fanoutWindowBinAcc) {
			xf := float64(x)
			*sum += xf
			*sumSq += xf * xf
			if topologyCorrect {
				*sumCorrect += xf
			}
			bucket := windowHABucket(x)
			b := bins[bucket]
			if b == nil {
				b = &fanoutWindowBinAcc{}
				bins[bucket] = b
			}
			b.windows++
			if topologyCorrect {
				b.correct++
			}
		}
		addRouteMetric(route.RequiredHAFanouts, &a.sumRouteRequiredHA, &a.sumRouteRequiredHASq, &a.sumRouteRequiredHACorrect, a.routeByRequiredHA)
		addRouteMetric(route.ApplicableFanoutGroups, &a.sumRouteFanoutGroups, &a.sumRouteFanoutGroupsSq, &a.sumRouteFanoutGroupsCorrect, a.routeByFanoutGroups)
		addRouteMetric(route.ApplicableMultiBloomGroups, &a.sumRouteMultiBloomGroups, &a.sumRouteMultiBloomGroupsSq, &a.sumRouteMultiBloomGroupsCorrect, a.routeByMultiBloomGroups)
		addRouteMetric(route.FanoutCandidateTests, &a.sumRouteFanoutTests, &a.sumRouteFanoutTestsSq, &a.sumRouteFanoutTestsCorrect, a.routeByFanoutTests)
	}
	if cpd < 1 {
		cpd = 1
	}
	traceWindows, traceHA, traceKnown, traceFanouts := 0, 0, 0, 0
	for _, carrier := range survivors {
		if carrier.Depth == 0 || carrier.BloomBits == nil {
			continue
		}
		windowTop := (carrier.Depth / cpd) * cpd
		if carrier.Depth%cpd == 0 {
			// A periodic checkpoint closes the preceding full window. A leaf
			// checkpoint closes the partial window below its periodic checkpoint.
			windowTop -= cpd
		}
		steps := carrier.Depth - windowTop
		pathIDs := make(map[uint64]bool, cpd)
		fanouts := 0
		known := 0
		cur := carrier.SpanID
		for {
			parent := trueParent[cur]
			if parent == 0 || trueDepth[parent] < windowTop {
				break
			}
			pathIDs[parent] = true
			if trueChildren[parent] >= 2 {
				fanouts++
				if knownFanouts[parent] {
					known++
				}
			}
			if trueDepth[parent] == windowTop {
				break
			}
			cur = parent
		}
		haOnPath := make(map[uint64]bool)
		for _, entry := range carrier.HA {
			if pathIDs[entry.ParentID] {
				haOnPath[entry.ParentID] = true
			} else {
				a.haEntriesOffWindowPaths++
			}
		}
		nha := len(haOnPath)
		localCorrect := true
		truthNode, reconNode := carrier.SpanID, carrier.SpanID
		for step := 0; step < steps; step++ {
			truthNext := trueParent[truthNode]
			reconNext, exists := res.ReconParent[reconNode]
			if truthNext == 0 || !exists {
				localCorrect = false
				break
			}
			if nameable[truthNext] {
				if reconNext != truthNext || res.ReconAnon[reconNext] {
					localCorrect = false
					break
				}
			} else if !res.ReconAnon[reconNext] {
				localCorrect = false
				break
			}
			truthNode, reconNode = truthNext, reconNext
		}

		a.windows++
		a.haEntriesOnWindowPaths += int64(nha)
		a.truthFanoutOccurrences += int64(fanouts)
		a.knownFanoutOccurrences += int64(known)
		traceWindows++
		traceHA += nha
		traceKnown += known
		traceFanouts += fanouts
		if localCorrect {
			a.correctWindows++
		}
		xHA := float64(nha)
		a.sumWindowHA += xHA
		a.sumWindowHASq += xHA * xHA
		if localCorrect {
			a.sumWindowHACorrect += xHA
		}
		xKnown := float64(known)
		a.sumWindowKnown += xKnown
		a.sumWindowKnownSq += xKnown * xKnown
		if localCorrect {
			a.sumWindowKnownCorrect += xKnown
		}
		xFanouts := float64(fanouts)
		a.sumWindowFanouts += xFanouts
		a.sumWindowFanoutsSq += xFanouts * xFanouts
		if localCorrect {
			a.sumWindowFanoutsCorrect += xFanouts
		}
		hb := a.windowByHACount[windowHABucket(nha)]
		if hb == nil {
			hb = &fanoutWindowBinAcc{}
			a.windowByHACount[windowHABucket(nha)] = hb
		}
		hb.windows++
		if localCorrect {
			hb.correct++
		}
		kb := a.windowByKnownFanoutCount[windowHABucket(known)]
		if kb == nil {
			kb = &fanoutWindowBinAcc{}
			a.windowByKnownFanoutCount[windowHABucket(known)] = kb
		}
		kb.windows++
		if localCorrect {
			kb.correct++
		}
		fb := a.windowByTruthFanoutCount[windowHABucket(fanouts)]
		if fb == nil {
			fb = &fanoutWindowBinAcc{}
			a.windowByTruthFanoutCount[windowHABucket(fanouts)] = fb
		}
		fb.windows++
		if localCorrect {
			fb.correct++
		}
		if fanouts > 0 {
			a.windowsWithFanouts++
			if localCorrect {
				a.correctWindowsWithFanouts++
			}
			coverage := float64(nha) / float64(fanouts)
			a.sumCoverage += coverage
			a.sumCoverageSq += coverage * coverage
			if localCorrect {
				a.sumCoverageCorrect += coverage
			}
			knownCoverage := float64(known) / float64(fanouts)
			a.sumKnownCoverage += knownCoverage
			a.sumKnownCoverageSq += knownCoverage * knownCoverage
			if localCorrect {
				a.sumKnownCoverageCorrect += knownCoverage
			}
			cb := a.coverageByBucket[coverageBucket(coverage)]
			if cb == nil {
				cb = &fanoutWindowBinAcc{}
				a.coverageByBucket[coverageBucket(coverage)] = cb
			}
			cb.windows++
			if localCorrect {
				cb.correct++
			}
			kcb := a.knownCoverageByBucket[coverageBucket(knownCoverage)]
			if kcb == nil {
				kcb = &fanoutWindowBinAcc{}
				a.knownCoverageByBucket[coverageBucket(knownCoverage)] = kcb
			}
			kcb.windows++
			if localCorrect {
				kcb.correct++
			}
		}
	}
	if traceWindows > 0 {
		a.tracesWithWindows++
		if clean {
			a.cleanTracesWithWindows++
		}
		meanHA := float64(traceHA) / float64(traceWindows)
		a.sumTraceMeanHA += meanHA
		a.sumTraceMeanHASq += meanHA * meanHA
		if clean {
			a.sumTraceMeanHAClean += meanHA
		}
		meanKnown := float64(traceKnown) / float64(traceWindows)
		a.sumTraceMeanKnown += meanKnown
		a.sumTraceMeanKnownSq += meanKnown * meanKnown
		if clean {
			a.sumTraceMeanKnownClean += meanKnown
		}
	}
	if traceFanouts > 0 {
		a.tracesWithFanoutOpportunities++
		if clean {
			a.cleanTracesWithFanoutOpportunities++
		}
		coverage := float64(traceHA) / float64(traceFanouts)
		a.sumTraceCoverage += coverage
		a.sumTraceCoverageSq += coverage * coverage
		if clean {
			a.sumTraceCoverageClean += coverage
		}
		knownCoverage := float64(traceKnown) / float64(traceFanouts)
		a.sumTraceKnownCoverage += knownCoverage
		a.sumTraceKnownCoverageSq += knownCoverage * knownCoverage
		if clean {
			a.sumTraceKnownCoverageClean += knownCoverage
		}
	}
}

type chainLengthAcc struct {
	units, correctUnits, wrongUnits       int64
	orphans, correctOrphans, wrongOrphans int64
}

type chainTraceAcc struct {
	traces, clean int64
}

// chainEvidenceAcc correlates reconstructor-side chain evidence with
// evaluator-side outcomes.  Route correctness is nearest-surviving-anchor
// correctness; trace correctness is the canonical topology score.
type chainEvidenceAcc struct {
	candidateInitialHits, candidateAccepted, candidateRejected int64
	candidatePositiveBloomChecks                               int64
	acceptedByLevels, rejectedAfterLevels                      map[int]int64
	routedUnits, unroutedUnits                                 int64
	routedOrphans, unroutedOrphans                             int64
	correctUnits, wrongUnits                                   int64
	correctOrphans, wrongOrphans                               int64
	sumMatchedLevels, sumPositiveChecks                        int64
	byLevels                                                   map[int]*chainLengthAcc
	allRoutedTraces, allRoutedClean                            int64
	tracesWithUnrouted                                         int64
	traceByMin                                                 map[int]*chainTraceAcc
	sumMean, sumMeanSq, sumMeanClean                           float64
	sumMin, sumMinSq, sumMinClean                              float64
	sumChecks, sumChecksSq, sumChecksClean                     float64
}

func (a *chainEvidenceAcc) add(chain recon.GreedyChainStats, truth []recon.TruthSpan, dropped map[uint64]struct{}, traceClean bool) {
	a.candidateInitialHits += int64(chain.CandidateInitialHits)
	a.candidateAccepted += int64(chain.CandidateAccepted)
	a.candidateRejected += int64(chain.CandidateRejected)
	a.candidatePositiveBloomChecks += int64(chain.CandidatePositiveBloomChecks)
	if a.acceptedByLevels == nil {
		a.acceptedByLevels = make(map[int]int64)
		a.rejectedAfterLevels = make(map[int]int64)
		a.byLevels = make(map[int]*chainLengthAcc)
		a.traceByMin = make(map[int]*chainTraceAcc)
	}
	for levels, count := range chain.AcceptedByMatchedLevels {
		a.acceptedByLevels[levels] += int64(count)
	}
	for levels, count := range chain.RejectedAfterMatchedLevels {
		a.rejectedAfterLevels[levels] += int64(count)
	}

	trueParent := make(map[uint64]uint64, len(truth))
	for _, s := range truth {
		trueParent[s.SpanID] = s.ParentID
	}
	trueAnchor := func(id uint64) uint64 {
		seen := make(map[uint64]bool)
		for p := trueParent[id]; p != 0 && !seen[p]; p = trueParent[p] {
			seen[p] = true
			if _, gone := dropped[p]; !gone {
				return p
			}
		}
		return 0
	}

	routed, unrouted := 0, 0
	minLevels, sumLevels, sumChecks := 0, 0, 0
	for _, route := range chain.Routes {
		if !route.Routed {
			a.unroutedUnits++
			a.unroutedOrphans += int64(len(route.OrphanIDs))
			unrouted++
			continue
		}
		a.routedUnits++
		a.routedOrphans += int64(len(route.OrphanIDs))
		a.sumMatchedLevels += int64(route.MatchedLevels)
		a.sumPositiveChecks += int64(route.PositiveBloomChecks)
		routed++
		sumLevels += route.MatchedLevels
		sumChecks += route.PositiveBloomChecks
		if routed == 1 || route.MatchedLevels < minLevels {
			minLevels = route.MatchedLevels
		}
		bin := a.byLevels[route.MatchedLevels]
		if bin == nil {
			bin = &chainLengthAcc{}
			a.byLevels[route.MatchedLevels] = bin
		}
		bin.units++
		bin.orphans += int64(len(route.OrphanIDs))
		unitCorrect := len(route.OrphanIDs) > 0
		for _, orphan := range route.OrphanIDs {
			if route.AnchorID == trueAnchor(orphan) {
				a.correctOrphans++
				bin.correctOrphans++
			} else {
				a.wrongOrphans++
				bin.wrongOrphans++
				unitCorrect = false
			}
		}
		if unitCorrect {
			a.correctUnits++
			bin.correctUnits++
		} else {
			a.wrongUnits++
			bin.wrongUnits++
		}
	}
	if unrouted > 0 {
		a.tracesWithUnrouted++
	}
	if routed == 0 || unrouted != 0 {
		return
	}
	a.allRoutedTraces++
	y := 0.0
	if traceClean {
		a.allRoutedClean++
		y = 1
	}
	mean := float64(sumLevels) / float64(routed)
	meanChecks := float64(sumChecks) / float64(routed)
	a.sumMean += mean
	a.sumMeanSq += mean * mean
	a.sumMeanClean += mean * y
	a.sumMin += float64(minLevels)
	a.sumMinSq += float64(minLevels * minLevels)
	a.sumMinClean += float64(minLevels) * y
	a.sumChecks += meanChecks
	a.sumChecksSq += meanChecks * meanChecks
	a.sumChecksClean += meanChecks * y
	tb := a.traceByMin[minLevels]
	if tb == nil {
		tb = &chainTraceAcc{}
		a.traceByMin[minLevels] = tb
	}
	tb.traces++
	if traceClean {
		tb.clean++
	}
}

// scorerComparisonAcc records the two independent changes between the old and
// canonical CGP0 evaluation contracts: the scorer and the trace denominator.
// "obligation" is determined solely from surviving records; "emitted" is the
// historical res.Reconnected > 0 inclusion rule and therefore depends on the
// algorithm's output.
type scorerComparisonAcc struct {
	traces, obligations, emitted, obligationAndEmitted, obligationNoEmission, emissionNoObligation int
	canonicalCleanObligations, historicalCleanObligations                                          int
	canonicalCleanEmitted, historicalCleanEmitted                                                  int
	bothCleanObligations, canonicalOnlyCleanObligations                                            int
	historicalOnlyCleanObligations, bothWrongObligations                                           int
	canonicalWrongSegments, historicalWrongUnits                                                   int
}

func (a *scorerComparisonAcc) add(obligation, emitted bool, canonical, historical recon.CGP2Iso) {
	a.traces++
	if obligation {
		a.obligations++
		a.canonicalWrongSegments += canonical.Wrong()
		a.historicalWrongUnits += historical.Wrong()
		cc, hc := canonical.Clean(), historical.Clean()
		if cc {
			a.canonicalCleanObligations++
		}
		if hc {
			a.historicalCleanObligations++
		}
		switch {
		case cc && hc:
			a.bothCleanObligations++
		case cc:
			a.canonicalOnlyCleanObligations++
		case hc:
			a.historicalOnlyCleanObligations++
		default:
			a.bothWrongObligations++
		}
	}
	if emitted {
		a.emitted++
		if canonical.Clean() {
			a.canonicalCleanEmitted++
		}
		if historical.Clean() {
			a.historicalCleanEmitted++
		}
	}
	if obligation && emitted {
		a.obligationAndEmitted++
	} else if obligation {
		a.obligationNoEmission++
	} else if emitted {
		a.emissionNoObligation++
	}
}

// sb3acc preserves the evidence-specific telemetry returned by the dedicated
// SB3 greedy reconstructor. Keeping it beside the shared topology tally makes
// a long corpus run report whether sparse ordinals actually changed any
// greedy decisions, rather than only reporting the resulting edge accuracy.
type sb3acc struct {
	checked, compatible                    int64
	candidateEvaluations, ordinalOverrides int64
	hardOverrides                          int64
	ordinalPlaced, implicitOrdinals        int64
	fanouts, conflicts                     int64
	hardConflicts                          int64
	parentConflicts, haConflicts           int64
	structureChecked, structureComplete    int64
	structureIncomplete                    int64
	deePlaced, deeAmbiguous, deeNoPlace    int64
	eventOK, criticalPathOK                int64
	structureParents, endOrderOK           int64
}

func (a *sb3acc) add(r *recon.SB3Result) {
	if r == nil {
		return
	}
	a.checked++
	if r.Compatible {
		a.compatible++
	}
	a.candidateEvaluations += int64(r.CandidateEvaluations)
	a.ordinalOverrides += int64(r.OrdinalOverrides)
	a.hardOverrides += int64(r.HardOverrides)
	a.ordinalPlaced += int64(r.OrdinalPlaced)
	a.implicitOrdinals += int64(r.ImplicitOrdinals)
	a.fanouts += int64(r.Fanouts)
	a.conflicts += int64(r.Conflicts)
	a.hardConflicts += int64(r.HardConflicts)
	a.parentConflicts += int64(r.ParentConflicts)
	a.haConflicts += int64(r.HAConflicts)
}

func (a *sb3acc) addStructure(r recon.StructureResult, status recon.SBStructureStatus) {
	a.structureChecked++
	a.deePlaced += int64(status.DEEPlaced)
	a.deeAmbiguous += int64(status.DEEAmbiguous)
	a.deeNoPlace += int64(status.DEENoPlace)
	if r.Incomplete {
		a.structureIncomplete++
		return
	}
	a.structureComplete++
	a.structureParents += int64(r.NParents)
	a.endOrderOK += int64(r.EndOrderOK)
	if r.EventOrderOK {
		a.eventOK++
	}
	if r.CriticalPath {
		a.criticalPathOK++
	}
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

// makeHandler builds a fresh bridge handler and the matching reconstruction
// Config for the given mode. Factored out of newHarness so the trace-store
// pipeline can give each parallel replay worker its own handler instance
// (handler state is strictly per-trace, so a per-worker handler reused across
// traces with EvictTrace is identical to the shared streaming handler).
func makeHandler(c config) (bridge.Handler, recon.Config) {
	cfg := recon.NewConfig(c.checkpointDistance, c.bloomFP)
	cfg.BottomUp = c.bottomUp
	cfg.ChainCheck = c.chainCheck
	switch c.mode {
	case "pcr":
		ph := bridge.NewPCRBridgeHandler(c.checkpointDistance, c.prefixLen)
		ph.Capture = true
		cfg.PrefixLen = c.prefixLen
		return ph, cfg
	case "pcrb", "pcrs":
		// PCRB bloom geometry differs from PB's for the same --bloom-fp
		// (sized for cpd-1; see bridge.PCRBBloomCapacity) — rebuild the
		// config with the matching constructor.
		pcrbCfg := recon.NewPCRBConfig(c.checkpointDistance, c.prefixLen, c.bloomFP)
		pcrbCfg.BottomUp, pcrbCfg.ChainCheck = cfg.BottomUp, cfg.ChainCheck
		pcrbCfg.StopOnGap = c.stopOnGap
		pcrbCfg.TiePolicy = c.tiePolicy
		// --cgrp (pcrs only): emit the CGPRB payload so the PCRS solver gets
		// the hash array. Same checkpoint-root + bloom geometry as PCRB, so it
		// shares the PCRB config; the solver keys off cfg.CGRP.
		if c.cgrp || c.fullsatCGPB {
			ch := bridge.NewCGPRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
			ch.Capture = true
			pcrbCfg.CGRP = true
			return ch, pcrbCfg
		}
		ph := bridge.NewPCRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
		ph.Capture = true
		return ph, pcrbCfg
	case "pb2", "pb1", "pb0":
		// P-Bridge substrate: checkpoint prefix + window Bloom, with no HA
		// topology extension. ParentID remains part of each ordinary span record
		// and is fully available to the maximal-evidence path reconstructor.
		ph := bridge.NewPCRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
		ph.Capture = true
		pcrbCfg := recon.NewPCRBConfig(c.checkpointDistance, c.prefixLen, c.bloomFP)
		pcrbCfg.BottomUp, pcrbCfg.ChainCheck = cfg.BottomUp, cfg.ChainCheck
		pcrbCfg.StopOnGap = c.stopOnGap
		pcrbCfg.TiePolicy = c.tiePolicy
		pcrbCfg.PB0Legacy = c.pb0Legacy
		pcrbCfg.GreedyNoGroupedEvidence = c.greedyNoGrouped
		pcrbCfg.GreedyNoRouteFallback = c.greedyNoFallback
		return ph, pcrbCfg
	case "cgprb", "cgp2", "cgp1", "cgp0":
		// Call-graph-preserving: PCRB payload + window-local hash array. Same
		// checkpoint-root + bloom geometry as PCRB, so it shares the PCRB
		// config; reconstruction additionally consumes the HA. cgp2 reconstructs
		// the SAME emitted payloads with the from-scratch ReconstructCGP2.
		ph := bridge.NewCGPRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
		ph.Capture = true
		pcrbCfg := recon.NewPCRBConfig(c.checkpointDistance, c.prefixLen, c.bloomFP)
		pcrbCfg.BottomUp, pcrbCfg.ChainCheck = cfg.BottomUp, cfg.ChainCheck
		pcrbCfg.StopOnGap = c.stopOnGap
		pcrbCfg.TiePolicy = c.tiePolicy
		pcrbCfg.CGP0Legacy = c.cgp0Legacy
		pcrbCfg.PB0Legacy = c.pb0Legacy
		pcrbCfg.GreedyNoGroupedEvidence = c.greedyNoGrouped
		pcrbCfg.GreedyNoHardHA = c.greedyNoHardHA
		pcrbCfg.GreedyNoRouteFallback = c.greedyNoFallback
		return ph, pcrbCfg
	case "sb3":
		// SB3 carries the same CGPRB topology substrate plus sparse branch
		// ordinals and the lateral S-Bridge EE/DEE records.
		ph := bridge.NewSB3Handler(c.checkpointDistance, c.prefixLen, c.bloomFP, nil)
		ph.Capture = true
		ph.TopoOnly = c.topoOnly
		ph.LehmerEE = c.lehmerEE
		ph.FPBits = c.fpBits
		pcrbCfg := recon.NewPCRBConfig(c.checkpointDistance, c.prefixLen, c.bloomFP)
		pcrbCfg.FPBits = c.fpBits
		pcrbCfg.SBridgeLehmer = c.lehmerEE
		pcrbCfg.SB3IgnoreOrdinals = c.sb3IgnoreOrdinals
		pcrbCfg.SB3TopoOnly = c.topoOnly
		pcrbCfg.GreedyNoGroupedEvidence = c.greedyNoGrouped
		pcrbCfg.GreedyNoHardHA = c.greedyNoHardHA
		pcrbCfg.GreedyNoRouteFallback = c.greedyNoFallback
		return ph, pcrbCfg
	default: // pb
		pb := bridge.NewPathBridgeHandler(c.checkpointDistance, c.bloomFP)
		pb.EmitDepth = true
		pb.Capture = true
		return pb, cfg
	}
}

func newHarness(c config) *harness {
	h, cfg := makeHandler(c)
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
		h:              h,
		mode:           c.mode,
		topoOn:         os.Getenv("TRACE_RECON_TOPO") == "1",
		cpd:            c.checkpointDistance,
		wrongLog:       wlog,
		cfg:            cfg,
		rng:            rand.New(rand.NewSource(c.seed)),
		dropAll:        c.dropRate >= 1.0,
		rate:           c.dropRate,
		seed:           c.seed,
		perTraceSeed:   c.perTraceDropSeed,
		compareScorers: c.compareScorers,
		spansByTID:     make(map[uint64][]collSpan),
		idxByKey:       make(map[spanKey]int),
		openByTID:      make(map[uint64]int),
		nextSeq:        make(map[uint64]map[uint64]int),
		eventPosByTID:  make(map[uint64]int64),
		deesByTID:      make(map[uint64][]bridge.DEEQuad),
		jobs:           make(chan finishJob, workers*2),
	}
	if sh, ok := h.(*bridge.SB3Handler); ok {
		sh.DEESink = func(tid uint64, raw []byte) {
			var quads []bridge.DEEQuad
			var err error
			if cfg.SBridgeLehmer {
				quads, err = bridge.DecodeDEEQuadsLehmer(raw, cfg.FPBits)
			} else {
				quads, err = bridge.DecodeDEEQuads(raw, cfg.FPBits)
			}
			if err != nil {
				panic(fmt.Sprintf("decode generated SB3 DEE for trace %016x: %v", tid, err))
			}
			ha.deesByTID[tid] = append(ha.deesByTID[tid], quads...)
		}
	}
	ha.fullsatEngine = c.fullsatEngine
	ha.timingPath = c.timingPath
	ha.timingOn = c.timingPath != "" || os.Getenv("TRACE_RECON_TIMING") == "1"
	if c.dropRates != "" {
		if !cg2Family(c.mode) {
			fmt.Fprintln(os.Stderr, "--drop-rates supports only CGP/PB/SB3 family modes")
			os.Exit(2)
		}
		for _, tok := range strings.Split(c.dropRates, ",") {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}
			r, err := strconv.ParseFloat(tok, 64)
			if err != nil || r <= 0 || r > 1 {
				fmt.Fprintf(os.Stderr, "--drop-rates: bad rate %q\n", tok)
				os.Exit(2)
			}
			ha.mdRates = append(ha.mdRates, r)
			ha.mdDC = append(ha.mdDC, dropCode(r))
		}
		ha.mdAcc = make([]cgp2acc, len(ha.mdRates))
		ha.mdTiming = make([][]traceTiming, len(ha.mdRates))
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
	if c.onlyTraces != "" {
		ha.only = make(map[uint64]bool)
		for _, h := range strings.Split(c.onlyTraces, ",") {
			h = strings.TrimSpace(h)
			if h == "" {
				continue
			}
			id, err := strconv.ParseUint(h, 16, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bad --only-traces hex %q: %v\n", h, err)
				os.Exit(2)
			}
			ha.only[id] = true
		}
		fmt.Fprintf(os.Stderr, "only-traces: reconstructing %d trace(s) only\n", len(ha.only))
	}
	if c.dumpSurvivors != "" {
		dw, err := recon.NewDumpWriter(c.dumpSurvivors)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dump-survivors: %v\n", err)
			os.Exit(1)
		}
		ha.dumpW = dw
		ha.dumpOnly = c.dumpOnly
	}
	// Watchdog: log any trace in flight longer than TRACE_RECON_WATCHDOG secs.
	if w := os.Getenv("TRACE_RECON_WATCHDOG"); w != "" {
		if secs, err := strconv.Atoi(w); err == nil && secs > 0 {
			ha.inflight = make(map[uint64]time.Time)
			go func() {
				thresh := time.Duration(secs) * time.Second
				for {
					time.Sleep(time.Duration(secs) * time.Second)
					now := time.Now()
					ha.inflightMu.Lock()
					for tid, t0 := range ha.inflight {
						if d := now.Sub(t0); d >= thresh {
							fmt.Fprintf(os.Stderr, "WATCHDOG tid=%016x in-flight %.0fs\n", tid, d.Seconds())
						}
					}
					ha.inflightMu.Unlock()
				}
			}()
		}
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
	if ha.dumpW != nil {
		ha.dumpW.Close()
	}
	if ha.audit != nil {
		ha.audit.report()
	}
	if ha.dumper != nil {
		ha.dumper.close()
	}
	if ha.verifyOn {
		fmt.Fprintf(os.Stderr, "VERIFY violations=%d\n", ha.violations)
	}
	// Ambiguity counters increment unconditionally; surface them either under
	// full debug or via the dedicated TRACE_RECON_AMBIG flag (which avoids the
	// per-cluster CLUSTER spam that TRACE_RECON_DEBUG also enables).
	if ha.mode == "pcrs" && (os.Getenv("TRACE_RECON_DEBUG") == "1" || os.Getenv("TRACE_RECON_AMBIG") == "1") {
		recon.DumpPCRSAmbiguity()
	}
	if ha.topoOn {
		pct := func(num, den int64) float64 {
			if den == 0 {
				return 0
			}
			return 100 * float64(num) / float64(den)
		}
		fmt.Fprintf(os.Stderr,
			"TOPO traces=%d topo_correct=%.4f%% strict_shape=%.4f%% conn_correct=%.4f%% | dropfanout_traces=%d topo_correct_among_them=%.4f%% | sib_recall=%.4f%% (%d/%d) sib_precision=%.4f%% (%d/%d)\n",
			ha.topoTotal, pct(ha.topoCorrect, ha.topoTotal), pct(ha.topoStrict, ha.topoTotal), pct(ha.topoConn, ha.topoTotal),
			ha.topoFanTraces, pct(ha.topoCorrectF, ha.topoFanTraces),
			pct(ha.topoRecall, ha.topoTrueSib), ha.topoRecall, ha.topoTrueSib,
			pct(ha.topoPrec, ha.topoReconSib), ha.topoPrec, ha.topoReconSib)
		fmt.Fprintf(os.Stderr, "STRICT singleton-branchpoint orphans tested=%d misrouted=%d\n",
			ha.topoSingleBP, ha.topoSingleMis)
	}
	if cg2Family(ha.mode) {
		label := strings.ToUpper(ha.mode) // CGP2 / PB2 / CGP1 / PB1
		cgpFamily := ha.mode == "cgp0" || ha.mode == "cgp1" || ha.mode == "cgp2" || ha.mode == "sb3"
		pct := func(x, y int) float64 {
			if y == 0 {
				return 0
			}
			return 100 * float64(x) / float64(y)
		}
		emit := func(tag string, a cgp2acc) {
			wt := 0.0
			if a.feas > 0 {
				wt = float64(a.totWrong) / float64(a.feas)
			}
			fmt.Fprintf(os.Stderr, "%s%s traces=%d nontrivial=%d empty=%d | cleanNontrivial=%d (%.2f%%) | cleanAll=%d (%.2f%%)\n",
				label, tag, a.nt, a.feas, a.empty, a.clean, pct(a.clean, a.feas), a.clean+a.emptyClean, pct(a.clean+a.emptyClean, a.nt))
			if a.empty > 0 {
				fmt.Fprintf(os.Stderr, "%s%s empties: n=%d score-clean=%d avgSurv=%.1f noDrop(trivial)=%d tiny(<=2 surv)=%d\n",
					label, tag, a.empty, a.emptyClean, float64(a.emptySurvSum)/float64(a.empty), a.emptyNoDrop, a.emptyTiny)
			}
			if cgpFamily {
				fmt.Fprintf(os.Stderr, "%s%s per-edge: exact=%.3f%% (%d/%d) wrong/trace=%.2f | survivor edges=%.3f%% | named-syn=%.2f%%\n",
					label, tag, pct(a.edgeExact, a.realNodes), a.edgeExact, a.realNodes, wt, pct(a.survEx, a.survN), pct(a.namedEx, a.named))
			} else {
				fmt.Fprintf(os.Stderr, "%s%s per-fragment: reconnect-exact=%.3f%% (%d/%d) wrong/trace=%.2f\n",
					label, tag, pct(a.edgeExact, a.realNodes), a.edgeExact, a.realNodes, wt)
			}
		}
		if len(ha.mdRates) > 0 {
			for r := range ha.mdRates {
				emit("["+ha.mdDC[r]+"]", ha.mdAcc[r])
			}
		} else {
			emit("", ha.cg2)
		}
		if ha.mode == "sb3" {
			emitSB3 := func(tag string, s sb3acc) {
				fmt.Fprintf(os.Stderr,
					"SB3%s greedy=sb3-greedy checked=%d compatible=%d (%.4f%%) | candidate_evaluations=%d ordinal_overrides=%d hard_overrides=%d explicit_ordinals=%d implicit_first_labels=%d fanouts=%d ordinal_conflicts=%d hard_conflicts=%d (parent=%d ha=%d)\n",
					tag, s.checked, s.compatible, pct(int(s.compatible), int(s.checked)),
					s.candidateEvaluations, s.ordinalOverrides, s.hardOverrides,
					s.ordinalPlaced, s.implicitOrdinals, s.fanouts, s.conflicts, s.hardConflicts, s.parentConflicts, s.haConflicts)
				if !ha.cfg.SB3TopoOnly {
					fmt.Fprintf(os.Stderr,
						"SB3%s structure topo-clean-checked=%d complete=%d (%.4f%%) incomplete=%d | DEE placed=%d ambiguous=%d no_place=%d | event_order=%d (%.4f%%) critical_path=%d (%.4f%%) parent_end_order=%d/%d (%.4f%%)\n",
						tag, s.structureChecked, s.structureComplete, pct(int(s.structureComplete), int(s.structureChecked)), s.structureIncomplete,
						s.deePlaced, s.deeAmbiguous, s.deeNoPlace,
						s.eventOK, pct(int(s.eventOK), int(s.structureComplete)),
						s.criticalPathOK, pct(int(s.criticalPathOK), int(s.structureComplete)),
						s.endOrderOK, s.structureParents, pct(int(s.endOrderOK), int(s.structureParents)))
				}
			}
			if len(ha.mdRates) > 0 {
				for r := range ha.mdRates {
					emitSB3("["+ha.mdDC[r]+"]", ha.mdAcc[r].sb3)
				}
			} else {
				emitSB3("", ha.cg2.sb3)
			}
		} else if ha.mode == "cgp0" {
			emitGreedy := func(tag string, a cgp2acc) {
				mode := "full-evidence"
				if ha.cfg.CGP0Legacy {
					mode = "legacy-lean"
				}
				fmt.Fprintf(os.Stderr,
					"CGP0%s greedy=%s checked=%d | candidate_evaluations=%d hard_overrides=%d hard_conflicts=%d (parent=%d ha=%d)\n",
					tag, mode, a.greedyChecked, a.greedyCandidates, a.greedyHardOverrides,
					a.greedyHardConflicts, a.greedyParentConflicts, a.greedyHAConflicts)
			}
			if len(ha.mdRates) > 0 {
				for r := range ha.mdRates {
					emitGreedy("["+ha.mdDC[r]+"]", ha.mdAcc[r])
				}
			} else {
				emitGreedy("", ha.cg2)
			}
		}
	}
}

func (ha *harness) beginTrace(tid uint64, spanCount int) {
	ha.openByTID[tid] = 2 * spanCount
	ha.eventPosByTID[tid] = 0
}

func (ha *harness) onEvent(ts int64, kind bridge.Kind, tid, sid, pid uint64, serviceID uint16) {
	pos := ha.eventPosByTID[tid]
	ha.eventPosByTID[tid] = pos + 1
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
			spanID: sid, parentID: pid, depth: -1, br: r.Payload, ordinal: seqNum,
		})
	} else {
		r := ha.h.OnEnd(ev)
		if idx, ok := ha.idxByKey[spanKey{tid, sid}]; ok {
			s := &ha.spansByTID[tid][idx]
			s.depth = r.Depth
			s.endPos = pos
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

// mixSeed derives a per-trace drop seed from the trace ID and the base seed
// (splitmix64), so a trace's drop set depends only on its own ID — not on where
// it falls in the stream or which partition it lands in.
func mixSeed(tid uint64, base int64) int64 {
	x := tid + uint64(base)*0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return int64(x)
}

// computeDropped applies the drop policy to a trace's spans. Only spans without
// _br are candidates; every _br emitter is a checkpoint and is never dropped. With
// perTraceSeed, draws come from a per-trace RNG over spans in spanID order, so
// the result is independent of processing order / corpus partitioning;
// otherwise it advances the single global RNG stream (legacy, order-dependent).
func (ha *harness) computeDropped(tid uint64, spans []collSpan) map[uint64]struct{} {
	dropped := make(map[uint64]struct{})
	if ha.dropAll {
		for _, s := range spans {
			if s.br == nil {
				dropped[s.spanID] = struct{}{}
			}
		}
		return dropped
	}
	if ha.perTraceSeed {
		cands := make([]uint64, 0, len(spans))
		for _, s := range spans {
			if s.br == nil {
				cands = append(cands, s.spanID)
			}
		}
		sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
		rng := rand.New(rand.NewSource(mixSeed(tid, ha.seed)))
		for _, id := range cands {
			if rng.Float64() < ha.rate {
				dropped[id] = struct{}{}
			}
		}
		return dropped
	}
	for _, s := range spans {
		if s.br != nil {
			continue
		}
		if ha.rng.Float64() < ha.rate {
			dropped[s.spanID] = struct{}{}
		}
	}
	return dropped
}

// computeDroppedMulti returns one drop set per ha.mdRates, from a SINGLE
// per-span uniform draw thresholded at each rate. This is bit-identical to
// running each rate as a separate --per-trace-drop-seed --drop-rate job (same
// seed, same spanID order, same draws), and the sets are nested. Rate 1.0
// thresholds every candidate (Float64 in [0,1) < 1.0 always), matching dropAll.
func (ha *harness) computeDroppedMulti(tid uint64, spans []collSpan) []map[uint64]struct{} {
	out := make([]map[uint64]struct{}, len(ha.mdRates))
	for i := range out {
		out[i] = make(map[uint64]struct{})
	}
	cands := make([]uint64, 0, len(spans))
	for _, s := range spans {
		if s.br == nil {
			cands = append(cands, s.spanID)
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	rng := rand.New(rand.NewSource(mixSeed(tid, ha.seed)))
	for _, id := range cands {
		u := rng.Float64()
		for r, rate := range ha.mdRates {
			if u < rate {
				out[r][id] = struct{}{}
			}
		}
	}
	return out
}

// dropCode maps a drop rate to the sweep's file/label code (d005, d05, d10 ...).
func dropCode(r float64) string {
	switch r {
	case 0.05:
		return "d005"
	case 0.25:
		return "d025"
	case 0.5:
		return "d05"
	case 0.75:
		return "d075"
	case 0.95:
		return "d095"
	case 1.0:
		return "d10"
	}
	return "d" + strings.ReplaceAll(strconv.FormatFloat(r, 'f', -1, 64), ".", "")
}

// finishTrace frees the trace's collection state, makes the drop decisions, and
// hands reconstruction+scoring to the worker pool. Drops are bit-identical
// regardless of worker count; with --per-trace-drop-seed they are also identical
// across corpus partitionings (see computeDropped).
func (ha *harness) finishTrace(tid uint64) {
	spans := ha.spansByTID[tid]
	delete(ha.spansByTID, tid)
	delete(ha.nextSeq, tid)
	delete(ha.eventPosByTID, tid)
	for _, s := range spans {
		delete(ha.idxByKey, spanKey{tid, s.spanID})
	}

	// Drop policy: only spans without _br are candidates. Every _br emitter is
	// a checkpoint; checkpoints ride the high-priority queue and are never dropped.
	if len(ha.mdRates) > 0 {
		dm := ha.computeDroppedMulti(tid, spans)
		dees := append([]bridge.DEEQuad(nil), ha.deesByTID[tid]...)
		delete(ha.deesByTID, tid)
		if ha.only != nil && !ha.only[tid] {
			return
		}
		ha.jobs <- finishJob{tid: tid, spans: spans, droppedMulti: dm, dees: dees}
		return
	}
	dropped := ha.computeDropped(tid, spans)
	dees := append([]bridge.DEEQuad(nil), ha.deesByTID[tid]...)
	delete(ha.deesByTID, tid)
	// --only-traces: the drop loop above still ran (RNG advanced identically),
	// so survivors match a full run; we just skip the expensive reconstruct for
	// non-target traces by not enqueueing them.
	if ha.only != nil && !ha.only[tid] {
		return
	}
	ha.jobs <- finishJob{tid: tid, spans: spans, dropped: dropped, dees: dees}
}

// process is the per-trace parallel work: decode, reconstruct, score.
func (ha *harness) process(j finishJob) {
	tid, spans, dropped := j.tid, j.spans, j.dropped

	if ha.inflight != nil {
		ha.inflightMu.Lock()
		ha.inflight[tid] = time.Now()
		ha.inflightMu.Unlock()
		defer func() {
			ha.inflightMu.Lock()
			delete(ha.inflight, tid)
			ha.inflightMu.Unlock()
		}()
	}

	truth := make([]recon.TruthSpan, len(spans))
	for i, s := range spans {
		truth[i] = recon.TruthSpan{SpanID: s.spanID, ParentID: s.parentID, Depth: s.depth}
	}

	if len(ha.mdRates) > 0 {
		ha.processMulti(j, truth) // decode once, reconstruct under every rate
		return
	}

	survivors := make([]recon.Span, 0, len(spans)-len(dropped))
	for _, s := range spans {
		if _, gone := dropped[s.spanID]; gone {
			continue
		}
		survivors = append(survivors, ha.decodeSpan(tid, s))
	}

	if ha.dumpW != nil {
		dropIDs := make([]uint64, 0, len(dropped))
		for id := range dropped {
			dropIDs = append(dropIDs, id)
		}
		if err := ha.dumpW.Write(recon.DumpedTrace{
			TID: tid, Survivors: survivors, Truth: truth, Dropped: dropIDs, Cfg: ha.cfg,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "dump-survivors: %v\n", err)
			os.Exit(1)
		}
		if ha.dumpOnly {
			return // capture only: skip the (possibly slow) reconstruction
		}
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
		t0 := time.Now()
		var res recon.Result
		if ha.fullsatEngine {
			res = recon.ReconstructFullSAT(survivors, ha.cfg)
		} else {
			res = recon.ReconstructPCRS(survivors, ha.cfg)
		}
		ha.recRecon(tid, len(survivors), len(truth), len(dropped), true, time.Since(t0))
		ha.scoresStore(tid, recon.ScorePCR(res, truth, dropped))
		ha.classifyWrong(tid, res, truth, dropped)
		if ha.topoOn {
			ha.addTopo(recon.ScoreCGPTopology(res, truth, dropped, tid))
		}
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
	} else if ha.mode == "cgprb" {
		t0 := time.Now()
		res := recon.ReconstructCGPRB(survivors, ha.cfg)
		d := time.Since(t0)
		ha.recRecon(tid, len(survivors), len(truth), len(dropped), true, d)
		if slowReconMS > 0 && d.Milliseconds() >= slowReconMS {
			fmt.Fprintf(os.Stderr, "SLOWRECON tid=%016x survivors=%d ms=%d\n", tid, len(survivors), d.Milliseconds())
		}
		ha.scoresStore(tid, recon.ScorePCR(res, truth, dropped))
		ha.classifyWrong(tid, res, truth, dropped)
		if ha.topoOn {
			ha.addTopo(recon.ScoreCGPTopology(res, truth, dropped, tid))
		}
	} else if cg2Family(ha.mode) {
		// From-scratch reconstructors over the same CGPRB payloads: cgp2/pb2
		// (solver-based) and cgp1/pb1 (greedy baselines), all scored into the
		// shared cg2 tally. Reconstruct+score off-lock; only the tally is guarded.
		t0 := time.Now()
		res, sb3 := ha.cg2Reconstruct(survivors, j.dees)
		d := time.Since(t0)
		if ha.mode == "sb3" && os.Getenv("TRACE_RECON_SB3DIAG") != "" {
			fmt.Fprintf(os.Stderr, "SB3STAGE harness_reconstruct elapsed=%s parents=%d bridges=%d\n", d, len(res.ReconParent), len(res.Bridges))
		}
		empty := !hasReconstructionObligation(survivors)
		ha.recRecon(tid, len(survivors), len(truth), len(dropped), !empty, d)
		scoreStart := time.Now()
		iso := ha.cg2ScoreOf(res, survivors, truth, dropped)
		var historical recon.CGP2Iso
		if ha.compareScorers {
			historical = recon.ScoreCGP2HistoricalStrict(res, truth, dropped)
		}
		var structure *recon.StructureResult
		if sb3 != nil && iso.Clean() && sb3.Compatible && !ha.cfg.SB3TopoOnly {
			stTruth, endPos := sb3StructureTruth(spans)
			sr := recon.ScoreStructureQuads(sb3.Structure, stTruth, endPos, j.dees)
			structure = &sr
		}
		if ha.mode == "sb3" && os.Getenv("TRACE_RECON_SB3DIAG") != "" {
			fmt.Fprintf(os.Stderr, "SB3STAGE harness_score elapsed=%s\n", time.Since(scoreStart))
		}
		if sb3 != nil && sb3.HardConflicts > 0 && os.Getenv("TRACE_RECON_SB3_HARD_TIDS") != "" {
			fmt.Fprintf(os.Stderr, "SB3HARTRACE tid=%016x survivors=%d parent_conflicts=%d ha_conflicts=%d\n",
				tid, len(survivors), sb3.ParentConflicts, sb3.HAConflicts)
		}
		if ha.mode == "cgp0" && res.GreedyHardConflicts > 0 && os.Getenv("TRACE_RECON_GREEDY_HARD_TIDS") != "" {
			fmt.Fprintf(os.Stderr, "CGP0HARDTRACE tid=%016x survivors=%d parent_conflicts=%d ha_conflicts=%d\n",
				tid, len(survivors), res.GreedyParentConflicts, res.GreedyHAConflicts)
		}
		ha.mu.Lock()
		ha.cg2.nt++
		if ha.compareScorers {
			ha.cg2.scoreComparison.add(!empty, res.Reconnected > 0, iso, historical)
		}
		ha.cg2.sb3.add(sb3)
		ha.cg2.chainEvidence.add(res.GreedyChain, truth, dropped, iso.Clean())
		if res.GreedyMode == "full-evidence" {
			ha.cg2.fanoutEvidence.add(res.GreedyFanout, res, survivors, truth, ha.cpd, iso.Clean())
		}
		if structure != nil {
			ha.cg2.sb3.addStructure(*structure, sb3.StructureStatus)
		}
		if res.GreedyMode != "" {
			ha.cg2.greedyChecked++
			ha.cg2.greedyCandidates += int64(res.GreedyCandidateEvaluations)
			ha.cg2.greedyHardOverrides += int64(res.GreedyHardOverrides)
			ha.cg2.greedyHardConflicts += int64(res.GreedyHardConflicts)
			ha.cg2.greedyParentConflicts += int64(res.GreedyParentConflicts)
			ha.cg2.greedyHAConflicts += int64(res.GreedyHAConflicts)
		}
		if empty {
			ha.cg2.empty++
			if iso.Clean() {
				ha.cg2.emptyClean++
			}
			ha.cg2.emptySurvSum += len(survivors)
			if len(dropped) == 0 {
				ha.cg2.emptyNoDrop++
			}
			if len(survivors) <= 2 {
				ha.cg2.emptyTiny++
			}
		} else {
			ha.cg2.feas++
			ha.cg2.realNodes += iso.RealNodes
			ha.cg2.edgeExact += iso.EdgeExact
			ha.cg2.edgeAnonOK += iso.EdgeAnonOK
			ha.cg2.edgeWrong += iso.EdgeWrong
			ha.cg2.constraintWrong += iso.ConstraintWrong
			ha.cg2.totWrong += iso.Wrong()
			ha.cg2.survN += iso.SurvNodes
			ha.cg2.survEx += iso.SurvExact
			ha.cg2.named += iso.NamedSyn
			ha.cg2.namedEx += iso.NamedExact
			if iso.Clean() {
				ha.cg2.clean++
			}
		}
		// Progress row every 10k traces (cgp2 bypasses scoresStore, so emit here).
		if a := ha.cg2; a.nt%10000 == 0 {
			ex, pe := 100.0, 100.0
			if a.feas > 0 {
				ex = 100 * float64(a.clean) / float64(a.feas)
			}
			if a.realNodes > 0 {
				pe = 100 * float64(a.edgeExact) / float64(a.realNodes)
			}
			fmt.Fprintf(os.Stderr, "PROGRESS traces=%d elapsed=%ds feasible=%d empty=%d exclEmpty=%.2f%% per-edge-exact=%.3f%%\n",
				a.nt, int(time.Since(ha.prog.start).Seconds()), a.feas, a.empty, ex, pe)
		}
		ha.mu.Unlock()
	} else {
		res := recon.ReconstructPB(survivors, ha.cfg)
		ha.scoresStore(tid, recon.ScorePB(res, truth, dropped))
	}
}

// cg2Family reports whether the mode uses the shared cgp2-style tally/scoring:
// the from-scratch reconstructors (cgp2/pb2 solver-based, cgp1/pb1 greedy).
func cg2Family(mode string) bool {
	switch mode {
	case "cgp2", "pb2", "cgp1", "pb1", "cgp0", "pb0", "sb3":
		return true
	}
	return false
}

// hasReconstructionObligation classifies the input, not whether the chosen
// algorithm emitted an answer. A survivor whose literal parent is absent is an
// observable fragment root that must be reconnected; emitting zero bridges for
// such a trace is a failed nontrivial reconstruction, not an empty trace.
func hasReconstructionObligation(survivors []recon.Span) bool {
	surviving := make(map[uint64]bool, len(survivors))
	for i := range survivors {
		surviving[survivors[i].SpanID] = true
	}
	for i := range survivors {
		s := &survivors[i]
		if s.ParentID != 0 && !surviving[s.ParentID] {
			return true
		}
	}
	return false
}

// sb3StructureTruth builds the scorer-only ordered truth tree and END-event
// positions collected during the same trace replay. Neither is visible to the
// reconstructor; they are consulted only after topology+structure finish.
func sb3StructureTruth(spans []collSpan) (recon.SBTruth, map[uint64]int64) {
	truth := recon.SBTruth{ChildByOrd: make(map[uint64]map[int]uint64)}
	endPos := make(map[uint64]int64, len(spans))
	for _, s := range spans {
		endPos[s.spanID] = s.endPos
		if s.parentID == 0 {
			truth.RootID = s.spanID
			continue
		}
		kids := truth.ChildByOrd[s.parentID]
		if kids == nil {
			kids = make(map[int]uint64)
			truth.ChildByOrd[s.parentID] = kids
		}
		kids[s.ordinal] = s.spanID
	}
	return truth, endPos
}

// cg2Reconstruct dispatches to the right from-scratch reconstructor by mode.
func (ha *harness) cg2Reconstruct(survivors []recon.Span, dees []bridge.DEEQuad) (recon.Result, *recon.SB3Result) {
	switch ha.mode {
	case "pb2":
		return recon.ReconstructPB2(survivors, ha.cfg), nil
	case "cgp1":
		return recon.ReconstructCGP1(survivors, ha.cfg), nil
	case "pb1":
		return recon.ReconstructPB1(survivors, ha.cfg), nil
	case "cgp0":
		return recon.ReconstructCGP0(survivors, ha.cfg), nil
	case "pb0":
		return recon.ReconstructPB0(survivors, ha.cfg), nil
	case "sb3":
		r := recon.ReconstructSB3WithDEE(survivors, dees, ha.cfg)
		return r.Topology, &r
	default: // cgp2
		return recon.ReconstructCGP2(survivors, ha.cfg), nil
	}
}

// cg2ScoreOf scores a from-scratch result: ancestor/path (pb-family) or strict
// connectivity+topology (cgp-family), matching how the solver versions score.
func (ha *harness) cg2ScoreOf(res recon.Result, survivors []recon.Span, truth []recon.TruthSpan, dropped map[uint64]struct{}) recon.CGP2Iso {
	switch ha.mode {
	case "pb0", "pb1", "pb2":
		return recon.ScorePBPathStrict(res, survivors, truth, dropped)
	}
	iso := recon.ScoreCGP2Strict(res, survivors, truth, dropped) // cgp: evidence-bounded topology
	if ha.mode == "sb3" && res.SB3OrdinalChecked && !res.SB3OrdinalCompatible {
		iso.ConstraintWrong++ // topology cannot support the emitted sparse structure
	}
	return iso
}

// decodeSpan turns one collected span into a recon.Span, decoding its carried
// payload per mode. Drop-independent, so multi-drop decodes each span once and
// reuses it across every rate.
func (ha *harness) decodeSpan(tid uint64, s collSpan) recon.Span {
	sp := recon.Span{SpanID: s.spanID, ParentID: s.parentID, Depth: s.depth}
	if s.br == nil {
		return sp
	}
	die := func(err error) {
		fmt.Fprintf(os.Stderr, "trace %016x span %016x: %v\n", tid, s.spanID, err)
		os.Exit(1)
	}
	switch {
	case ha.mode == "pcr":
		d, prefix, err := recon.DecodePCRPayload(s.br, ha.cfg)
		if err != nil {
			die(err)
		}
		sp.Depth = d
		sp.CkptPrefix = prefix
		sp.LeafCarrier = d%ha.cpd != 0
	case (ha.mode == "pcrb" || ha.mode == "pcrs" || ha.mode == "pb2" || ha.mode == "pb1" || ha.mode == "pb0") && !ha.cfg.CGRP:
		d, prefix, bits, err := recon.DecodePCRBPayload(s.br, ha.cfg)
		if err != nil {
			die(err)
		}
		sp.Depth = d
		sp.CkptPrefix = prefix
		sp.BloomBits = bits
		sp.LeafCarrier = d%ha.cpd != 0
	case ha.mode == "sb3":
		d, prefix, bits, haEntries, ords, err := recon.DecodeSB3SpanPayload(s.br, ha.cfg)
		if err != nil {
			die(err)
		}
		sp.Depth = d
		sp.CkptPrefix = prefix
		sp.BloomBits = bits
		sp.LeafCarrier = d%ha.cpd != 0
		sp.HA = haEntries
		sp.SparseOrdinals = ords
	case ha.mode == "cgprb" || ha.mode == "cgp2" || ha.mode == "cgp1" || ha.mode == "cgp0" || ha.cfg.CGRP:
		d, prefix, bits, haEntries, err := recon.DecodeCGPRBPayload(s.br, ha.cfg)
		if err != nil {
			die(err)
		}
		sp.Depth = d
		sp.CkptPrefix = prefix
		sp.BloomBits = bits
		sp.LeafCarrier = d%ha.cpd != 0
		sp.HA = haEntries
	default:
		d, bits, err := recon.DecodePBPayload(s.br, ha.cfg)
		if err != nil {
			die(err)
		}
		sp.Depth = d
		sp.BloomBits = bits
	}
	return sp
}

// processMulti reconstructs one already-decoded trace under every ha.mdRates
// drop rate (cgp2 or pb2 per ha.mode), accumulating into the per-rate cells.
// Reconstruction runs off-lock; only the additive tally is mutex-guarded.
func (ha *harness) processMulti(j finishJob, truth []recon.TruthSpan) {
	tid, spans := j.tid, j.spans
	decoded := make([]recon.Span, len(spans))
	for i, s := range spans {
		decoded[i] = ha.decodeSpan(tid, s)
	}
	type cell struct {
		empty                 bool
		emitted               bool
		iso                   recon.CGP2Iso
		historical            recon.CGP2Iso
		sb3                   *recon.SB3Result
		structure             *recon.StructureResult
		nsurv, ndrop          int
		ns                    int64
		greedyMode            string
		greedyCandidates      int
		greedyHardOverrides   int
		greedyHardConflicts   int
		greedyParentConflicts int
		greedyHAConflicts     int
		greedyChain           recon.GreedyChainStats
		greedyFanout          recon.GreedyFanoutStats
		result                recon.Result
		survivors             []recon.Span
	}
	cells := make([]cell, len(ha.mdRates))
	for r := range ha.mdRates {
		dropped := j.droppedMulti[r]
		survivors := make([]recon.Span, 0, len(spans))
		for i, s := range spans {
			if _, gone := dropped[s.spanID]; gone {
				continue
			}
			survivors = append(survivors, decoded[i])
		}
		t0 := time.Now()
		res, sb3 := ha.cg2Reconstruct(survivors, j.dees)
		ns := time.Since(t0).Nanoseconds()
		empty := !hasReconstructionObligation(survivors)
		iso := ha.cg2ScoreOf(res, survivors, truth, dropped)
		var historical recon.CGP2Iso
		if ha.compareScorers {
			historical = recon.ScoreCGP2HistoricalStrict(res, truth, dropped)
		}
		var structure *recon.StructureResult
		if sb3 != nil && iso.Clean() && sb3.Compatible && !ha.cfg.SB3TopoOnly {
			stTruth, endPos := sb3StructureTruth(spans)
			sr := recon.ScoreStructureQuads(sb3.Structure, stTruth, endPos, j.dees)
			structure = &sr
		}
		cells[r] = cell{
			empty: empty, emitted: res.Reconnected > 0, iso: iso, historical: historical,
			nsurv: len(survivors), ndrop: len(dropped), ns: ns, sb3: sb3, structure: structure,
			greedyMode: res.GreedyMode, greedyCandidates: res.GreedyCandidateEvaluations,
			greedyHardOverrides: res.GreedyHardOverrides, greedyHardConflicts: res.GreedyHardConflicts,
			greedyParentConflicts: res.GreedyParentConflicts, greedyHAConflicts: res.GreedyHAConflicts,
			greedyChain:  res.GreedyChain,
			greedyFanout: res.GreedyFanout,
			result:       res, survivors: survivors,
		}
	}
	for r, c := range cells {
		if c.sb3 != nil && c.sb3.HardConflicts > 0 && os.Getenv("TRACE_RECON_SB3_HARD_TIDS") != "" {
			fmt.Fprintf(os.Stderr, "SB3HARTRACE tid=%016x drop=%s survivors=%d parent_conflicts=%d ha_conflicts=%d\n",
				tid, ha.mdDC[r], c.nsurv, c.sb3.ParentConflicts, c.sb3.HAConflicts)
		}
		if ha.mode == "cgp0" && c.greedyHardConflicts > 0 && os.Getenv("TRACE_RECON_GREEDY_HARD_TIDS") != "" {
			fmt.Fprintf(os.Stderr, "CGP0HARDTRACE tid=%016x drop=%s survivors=%d parent_conflicts=%d ha_conflicts=%d\n",
				tid, ha.mdDC[r], c.nsurv, c.greedyParentConflicts, c.greedyHAConflicts)
		}
	}
	ha.mu.Lock()
	for r := range cells {
		c := cells[r]
		a := &ha.mdAcc[r]
		a.nt++
		if ha.compareScorers {
			a.scoreComparison.add(!c.empty, c.emitted, c.iso, c.historical)
		}
		a.sb3.add(c.sb3)
		a.chainEvidence.add(c.greedyChain, truth, j.droppedMulti[r], c.iso.Clean())
		if c.greedyMode == "full-evidence" {
			a.fanoutEvidence.add(c.greedyFanout, c.result, c.survivors, truth, ha.cpd, c.iso.Clean())
		}
		if c.structure != nil {
			a.sb3.addStructure(*c.structure, c.sb3.StructureStatus)
		}
		if c.greedyMode != "" {
			a.greedyChecked++
			a.greedyCandidates += int64(c.greedyCandidates)
			a.greedyHardOverrides += int64(c.greedyHardOverrides)
			a.greedyHardConflicts += int64(c.greedyHardConflicts)
			a.greedyParentConflicts += int64(c.greedyParentConflicts)
			a.greedyHAConflicts += int64(c.greedyHAConflicts)
		}
		if c.empty {
			a.empty++
			if c.iso.Clean() {
				a.emptyClean++
			}
			a.emptySurvSum += c.nsurv
			if c.ndrop == 0 {
				a.emptyNoDrop++
			}
			if c.nsurv <= 2 {
				a.emptyTiny++
			}
		} else {
			a.feas++
			a.realNodes += c.iso.RealNodes
			a.edgeExact += c.iso.EdgeExact
			a.edgeAnonOK += c.iso.EdgeAnonOK
			a.edgeWrong += c.iso.EdgeWrong
			a.constraintWrong += c.iso.ConstraintWrong
			a.totWrong += c.iso.Wrong()
			if c.iso.Clean() {
				a.clean++
			}
		}
		if ha.timingOn {
			ha.mdTiming[r] = append(ha.mdTiming[r], traceTiming{tid, c.nsurv, len(truth), c.ndrop, !c.empty, c.ns})
		}
	}
	if ha.mdAcc[0].nt%10000 == 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "PROGRESS traces=%d elapsed=%ds |", ha.mdAcc[0].nt, int(time.Since(ha.prog.start).Seconds()))
		for r := range ha.mdRates {
			a := ha.mdAcc[r]
			ex := 100.0
			if a.feas > 0 {
				ex = 100 * float64(a.clean) / float64(a.feas)
			}
			fmt.Fprintf(&b, " %s:excl=%.2f%%", ha.mdDC[r], ex)
		}
		fmt.Fprintln(os.Stderr, b.String())
	}
	ha.mu.Unlock()
}

// traceTiming is one trace's reconstruction wall-time and size, for --timing.
// feasible = the reconstruction produced output (cgp2: non-empty, i.e. the
// exclEmpty subset); for modes without an empty/feasible notion it is true.
type traceTiming struct {
	tid                 uint64
	nsurv, nspan, ndrop int
	feasible            bool
	ns                  int64
}

// recRecon records one trace's reconstruction time (no-op unless timing is on).
func (ha *harness) recRecon(tid uint64, nsurv, nspan, ndrop int, feasible bool, d time.Duration) {
	if !ha.timingOn {
		return
	}
	ha.mu.Lock()
	ha.timingRecs = append(ha.timingRecs, traceTiming{tid, nsurv, nspan, ndrop, feasible, d.Nanoseconds()})
	ha.mu.Unlock()
}

// addTopo accumulates one trace's call-graph-topology verdict (guarded by mu).
func (ha *harness) addTopo(ts recon.TopoScore) {
	ha.mu.Lock()
	ha.topoTotal++
	if ts.ConnCorrect {
		ha.topoConn++
	}
	if ts.TopoCorrect {
		ha.topoCorrect++
	}
	if ts.StrictShapeCorrect {
		ha.topoStrict++
	}
	ha.topoSingleBP += int64(ts.SingletonBP)
	ha.topoSingleMis += int64(ts.SingletonBPMisrouted)
	if ts.HasDropFanout {
		ha.topoFanTraces++
		if ts.TopoCorrect {
			ha.topoCorrectF++
		}
	}
	ha.topoTrueSib += int64(ts.TrueSibPairs)
	ha.topoRecall += int64(ts.RecallPairs)
	ha.topoReconSib += int64(ts.ReconSibPairs)
	ha.topoPrec += int64(ts.PrecisionPairs)
	ha.mu.Unlock()
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
		topoStr := ""
		if ha.topoOn {
			tc, tcf := 100.0, 100.0
			if ha.topoTotal > 0 {
				tc = 100 * float64(ha.topoCorrect) / float64(ha.topoTotal)
			}
			if ha.topoFanTraces > 0 {
				tcf = 100 * float64(ha.topoCorrectF) / float64(ha.topoFanTraces)
			}
			topoStr = fmt.Sprintf(" topo_correct=%.4f%% topo_df=%.4f%%", tc, tcf)
		}
		fmt.Fprintf(os.Stderr, "PROGRESS traces=%d elapsed=%ds exact=%.2f%% benign=%.2f%% wrong=%.4f%% tr=%.2f%% oe=%.4f%% lost=%.3f%% placed=%d%s\n",
			n, int(time.Since(ha.prog.start).Seconds()),
			den(p.c, r), den(p.a-p.c, r), den(p.w, r), den(p.affected, n),
			den(p.oem, p.oe), den(p.lost, p.spans), p.placed, topoStr)
	}
}

type topologySummary struct {
	ScorePolicy     string `json:"score_policy"`
	EvidenceProfile string `json:"evidence_profile"`
	Traces          int    `json:"traces"`
	Feasible        int    `json:"feasible"`
	Empty           int    `json:"empty"`
	EmptyClean      int    `json:"empty_clean"`
	Clean           int    `json:"clean"`
	CleanAll        int    `json:"clean_all"`
	RealNodes       int    `json:"real_nodes"`
	EdgeExact       int    `json:"edge_exact"`
	EdgeAnonOK      int    `json:"edge_anonymous_valid"`
	EdgeWrong       int    `json:"edge_wrong"`
	ConstraintWrong int    `json:"constraint_wrong"`
	SurvivorEdges   int    `json:"survivor_edges"`
	SurvivorExact   int    `json:"survivor_exact"`
	NamedSyn        int    `json:"named_synthetic"`
	NamedExact      int    `json:"named_exact"`
}

type sb3Summary struct {
	GreedyMode           string `json:"greedy_mode"`
	Checked              int64  `json:"checked"`
	Compatible           int64  `json:"compatible"`
	CandidateEvaluations int64  `json:"candidate_evaluations"`
	OrdinalOverrides     int64  `json:"ordinal_overrides"`
	HardOverrides        int64  `json:"hard_overrides"`
	OrdinalPlaced        int64  `json:"ordinal_placed"`
	ImplicitOrdinals     int64  `json:"implicit_first_labels"`
	Fanouts              int64  `json:"fanouts"`
	Conflicts            int64  `json:"conflicts"`
	HardConflicts        int64  `json:"hard_conflicts"`
	ParentConflicts      int64  `json:"parent_conflicts"`
	HAConflicts          int64  `json:"ha_conflicts"`
	StructureChecked     int64  `json:"structure_checked"`
	StructureComplete    int64  `json:"structure_complete"`
	StructureIncomplete  int64  `json:"structure_incomplete"`
	DEEPlaced            int64  `json:"dee_placed"`
	DEEAmbiguous         int64  `json:"dee_ambiguous"`
	DEENoPlace           int64  `json:"dee_no_place"`
	EventOrderOK         int64  `json:"event_order_ok"`
	CriticalPathOK       int64  `json:"critical_path_ok"`
	StructureParents     int64  `json:"structure_parents"`
	EndOrderOK           int64  `json:"end_order_ok"`
}

type greedySummary struct {
	Mode                 string                 `json:"mode"`
	Checked              int64                  `json:"checked"`
	CandidateEvaluations int64                  `json:"candidate_evaluations"`
	HardOverrides        int64                  `json:"hard_overrides"`
	HardConflicts        int64                  `json:"hard_conflicts"`
	ParentConflicts      int64                  `json:"parent_conflicts"`
	HAConflicts          int64                  `json:"ha_conflicts"`
	FanoutEvidence       *fanoutEvidenceSummary `json:"fanout_evidence,omitempty"`
}

type fanoutTraceBin struct {
	DistinctHAFanouts string  `json:"distinct_ha_fanouts"`
	Traces            int64   `json:"traces"`
	CanonicalClean    int64   `json:"canonical_clean"`
	CleanRate         float64 `json:"canonical_clean_rate"`
}

type fanoutWindowBin struct {
	Bucket      string  `json:"bucket"`
	Windows     int64   `json:"windows"`
	Correct     int64   `json:"locally_correct"`
	CorrectRate float64 `json:"local_correct_rate"`
}

type fanoutRouteBin struct {
	Bucket     string  `json:"bucket"`
	RouteUnits int64   `json:"route_units"`
	Correct    int64   `json:"topology_correct"`
	Accuracy   float64 `json:"topology_accuracy"`
}

type fanoutEvidenceSummary struct {
	Traces                               int64             `json:"traces"`
	CanonicalClean                       int64             `json:"canonical_clean"`
	HAEnabled                            bool              `json:"ha_enabled"`
	TracesWithHA                         int64             `json:"traces_with_ha"`
	HAEntriesAvailable                   int64             `json:"ha_entries_available"`
	HACarriersAvailable                  int64             `json:"ha_carriers_available"`
	DistinctHAFanoutsAvailable           int64             `json:"distinct_ha_fanouts_available"`
	MeanHAEntriesPerTrace                float64           `json:"mean_ha_entries_per_trace"`
	MeanDistinctHAFanoutsPerTrace        float64           `json:"mean_distinct_ha_fanouts_per_trace"`
	RecoveredHAFanoutsUsed               int64             `json:"recovered_ha_fanouts_used"`
	RouteUnits                           int64             `json:"route_units"`
	RouteUnitsWithRequiredHA             int64             `json:"route_units_with_required_ha"`
	RequiredHAConstraints                int64             `json:"required_ha_constraints"`
	EvidenceGroups                       int64             `json:"exact_parent_or_ha_evidence_groups"`
	WitnessedFanoutGroups                int64             `json:"witnessed_fanout_groups"`
	MultiBloomEvidenceGroups             int64             `json:"multi_bloom_evidence_groups"`
	GroupedAnchorCandidatesPruned        int64             `json:"grouped_anchor_candidates_pruned"`
	GroupedFanoutCandidatesPruned        int64             `json:"grouped_fanout_candidates_pruned"`
	HardRouteCandidatesRejected          int64             `json:"hard_route_candidates_rejected"`
	PearsonDistinctHAVsCanonicalClean    *float64          `json:"pearson_distinct_ha_fanouts_vs_canonical_clean"`
	TracePerformanceByDistinctHA         []fanoutTraceBin  `json:"trace_performance_by_distinct_ha_fanouts"`
	CarrierWindows                       int64             `json:"carrier_windows"`
	LocallyCorrectWindows                int64             `json:"locally_correct_carrier_windows"`
	HAEntriesOnWindowPaths               int64             `json:"ha_entries_on_window_paths"`
	HAEntriesOffWindowPaths              int64             `json:"ha_entries_off_window_paths"`
	TruthFanoutOccurrences               int64             `json:"truth_fanout_occurrences_on_window_paths"`
	KnownFanoutOccurrences               int64             `json:"known_fanout_occurrences_on_window_paths"`
	MeanHAEntriesPerWindow               float64           `json:"mean_ha_entries_per_carrier_window"`
	WeightedHAPathCoverage               float64           `json:"weighted_ha_path_coverage"`
	WeightedKnownFanoutPathCoverage      float64           `json:"weighted_known_fanout_path_coverage"`
	PearsonWindowHAVsLocalCorrect        *float64          `json:"pearson_window_ha_count_vs_local_path_correct"`
	PearsonWindowKnownVsLocalCorrect     *float64          `json:"pearson_window_known_fanout_count_vs_local_path_correct"`
	PearsonWindowFanoutsVsLocalCorrect   *float64          `json:"pearson_window_truth_fanout_count_vs_local_path_correct"`
	PearsonWindowCoverageVsLocalCorrect  *float64          `json:"pearson_window_ha_coverage_vs_local_path_correct"`
	PearsonKnownCoverageVsLocalCorrect   *float64          `json:"pearson_window_known_fanout_coverage_vs_local_path_correct"`
	PearsonTraceMeanHAVsClean            *float64          `json:"pearson_trace_mean_window_ha_vs_canonical_clean"`
	PearsonTraceMeanKnownVsClean         *float64          `json:"pearson_trace_mean_known_fanouts_vs_canonical_clean"`
	PearsonTraceCoverageVsClean          *float64          `json:"pearson_trace_ha_coverage_vs_canonical_clean"`
	PearsonTraceKnownCoverageVsClean     *float64          `json:"pearson_trace_known_fanout_coverage_vs_canonical_clean"`
	WindowPerformanceByHACount           []fanoutWindowBin `json:"window_performance_by_ha_count"`
	WindowPerformanceByKnownCount        []fanoutWindowBin `json:"window_performance_by_known_fanout_count"`
	WindowPerformanceByTruthFanoutCount  []fanoutWindowBin `json:"window_performance_by_truth_fanout_count"`
	WindowPerformanceByHACoverage        []fanoutWindowBin `json:"window_performance_by_ha_coverage"`
	WindowPerformanceByKnownCoverage     []fanoutWindowBin `json:"window_performance_by_known_fanout_coverage"`
	RoutedUnitsMeasured                  int64             `json:"routed_units_measured"`
	CorrectAnchorRoutes                  int64             `json:"correct_anchor_routes"`
	CorrectTopologyRoutes                int64             `json:"correct_topology_routes"`
	MeanRequiredHAFanoutsPerRoute        float64           `json:"mean_required_ha_fanouts_per_route"`
	MeanApplicableFanoutGroupsPerRoute   float64           `json:"mean_applicable_fanout_groups_per_route"`
	MeanMultiBloomFanoutGroupsPerRoute   float64           `json:"mean_multi_bloom_fanout_groups_per_route"`
	MeanFanoutCandidateTestsPerRoute     float64           `json:"mean_fanout_candidate_tests_per_route"`
	PearsonRequiredHAVsTopologyCorrect   *float64          `json:"pearson_required_ha_fanouts_vs_route_topology_correct"`
	PearsonFanoutGroupsVsTopologyCorrect *float64          `json:"pearson_applicable_fanout_groups_vs_route_topology_correct"`
	PearsonMultiBloomVsTopologyCorrect   *float64          `json:"pearson_multi_bloom_fanout_groups_vs_route_topology_correct"`
	PearsonFanoutTestsVsTopologyCorrect  *float64          `json:"pearson_fanout_candidate_tests_vs_route_topology_correct"`
	RoutePerformanceByRequiredHA         []fanoutRouteBin  `json:"route_performance_by_required_ha_fanouts"`
	RoutePerformanceByFanoutGroups       []fanoutRouteBin  `json:"route_performance_by_applicable_fanout_groups"`
	RoutePerformanceByMultiBloomGroups   []fanoutRouteBin  `json:"route_performance_by_multi_bloom_fanout_groups"`
	RoutePerformanceByFanoutTests        []fanoutRouteBin  `json:"route_performance_by_fanout_candidate_tests"`
}

func summarizeFanoutEvidence(a fanoutEvidenceAcc) *fanoutEvidenceSummary {
	if a.traces == 0 {
		return nil
	}
	s := &fanoutEvidenceSummary{
		Traces: a.traces, CanonicalClean: a.clean, HAEnabled: a.haEnabled == a.traces,
		TracesWithHA: a.tracesWithHA, HAEntriesAvailable: a.haEntries,
		HACarriersAvailable: a.haCarriers, DistinctHAFanoutsAvailable: a.distinctHA,
		MeanHAEntriesPerTrace:         float64(a.haEntries) / float64(a.traces),
		MeanDistinctHAFanoutsPerTrace: float64(a.distinctHA) / float64(a.traces),
		RecoveredHAFanoutsUsed:        a.recoveredHA, RouteUnits: a.routeUnits,
		RouteUnitsWithRequiredHA: a.unitsWithRequiredHA, RequiredHAConstraints: a.requiredHA,
		EvidenceGroups: a.evidenceGroups, WitnessedFanoutGroups: a.witnessedGroups,
		MultiBloomEvidenceGroups:      a.multiBloomGroups,
		GroupedAnchorCandidatesPruned: a.groupedAnchorsPruned,
		GroupedFanoutCandidatesPruned: a.groupedFanoutsPruned,
		HardRouteCandidatesRejected:   a.hardRoutesRejected,
		CarrierWindows:                a.windows, LocallyCorrectWindows: a.correctWindows,
		HAEntriesOnWindowPaths:  a.haEntriesOnWindowPaths,
		HAEntriesOffWindowPaths: a.haEntriesOffWindowPaths,
		TruthFanoutOccurrences:  a.truthFanoutOccurrences,
		KnownFanoutOccurrences:  a.knownFanoutOccurrences,
	}
	if a.windows > 0 {
		s.MeanHAEntriesPerWindow = float64(a.haEntriesOnWindowPaths) / float64(a.windows)
	}
	if a.truthFanoutOccurrences > 0 {
		s.WeightedHAPathCoverage = float64(a.haEntriesOnWindowPaths) / float64(a.truthFanoutOccurrences)
		s.WeightedKnownFanoutPathCoverage = float64(a.knownFanoutOccurrences) / float64(a.truthFanoutOccurrences)
	}
	s.PearsonDistinctHAVsCanonicalClean = pointBiserial(
		a.traces, a.sumDistinct, a.sumDistinctSq, float64(a.clean), a.sumDistinctClean,
	)
	s.PearsonWindowHAVsLocalCorrect = pointBiserial(
		a.windows, a.sumWindowHA, a.sumWindowHASq, float64(a.correctWindows), a.sumWindowHACorrect,
	)
	s.PearsonWindowKnownVsLocalCorrect = pointBiserial(
		a.windows, a.sumWindowKnown, a.sumWindowKnownSq, float64(a.correctWindows), a.sumWindowKnownCorrect,
	)
	s.PearsonWindowFanoutsVsLocalCorrect = pointBiserial(
		a.windows, a.sumWindowFanouts, a.sumWindowFanoutsSq, float64(a.correctWindows), a.sumWindowFanoutsCorrect,
	)
	s.PearsonWindowCoverageVsLocalCorrect = pointBiserial(
		a.windowsWithFanouts, a.sumCoverage, a.sumCoverageSq, float64(a.correctWindowsWithFanouts), a.sumCoverageCorrect,
	)
	s.PearsonKnownCoverageVsLocalCorrect = pointBiserial(
		a.windowsWithFanouts, a.sumKnownCoverage, a.sumKnownCoverageSq, float64(a.correctWindowsWithFanouts), a.sumKnownCoverageCorrect,
	)
	s.PearsonTraceMeanHAVsClean = pointBiserial(
		a.tracesWithWindows, a.sumTraceMeanHA, a.sumTraceMeanHASq, float64(a.cleanTracesWithWindows), a.sumTraceMeanHAClean,
	)
	s.PearsonTraceMeanKnownVsClean = pointBiserial(
		a.tracesWithWindows, a.sumTraceMeanKnown, a.sumTraceMeanKnownSq, float64(a.cleanTracesWithWindows), a.sumTraceMeanKnownClean,
	)
	s.PearsonTraceCoverageVsClean = pointBiserial(
		a.tracesWithFanoutOpportunities, a.sumTraceCoverage, a.sumTraceCoverageSq, float64(a.cleanTracesWithFanoutOpportunities), a.sumTraceCoverageClean,
	)
	s.PearsonTraceKnownCoverageVsClean = pointBiserial(
		a.tracesWithFanoutOpportunities, a.sumTraceKnownCoverage, a.sumTraceKnownCoverageSq, float64(a.cleanTracesWithFanoutOpportunities), a.sumTraceKnownCoverageClean,
	)
	if a.routedUnits > 0 {
		s.RoutedUnitsMeasured = a.routedUnits
		s.CorrectAnchorRoutes = a.correctAnchorRoutes
		s.CorrectTopologyRoutes = a.correctTopologyRoutes
		s.MeanRequiredHAFanoutsPerRoute = a.sumRouteRequiredHA / float64(a.routedUnits)
		s.MeanApplicableFanoutGroupsPerRoute = a.sumRouteFanoutGroups / float64(a.routedUnits)
		s.MeanMultiBloomFanoutGroupsPerRoute = a.sumRouteMultiBloomGroups / float64(a.routedUnits)
		s.MeanFanoutCandidateTestsPerRoute = a.sumRouteFanoutTests / float64(a.routedUnits)
	}
	s.PearsonRequiredHAVsTopologyCorrect = pointBiserial(
		a.routedUnits, a.sumRouteRequiredHA, a.sumRouteRequiredHASq, float64(a.correctTopologyRoutes), a.sumRouteRequiredHACorrect,
	)
	s.PearsonFanoutGroupsVsTopologyCorrect = pointBiserial(
		a.routedUnits, a.sumRouteFanoutGroups, a.sumRouteFanoutGroupsSq, float64(a.correctTopologyRoutes), a.sumRouteFanoutGroupsCorrect,
	)
	s.PearsonMultiBloomVsTopologyCorrect = pointBiserial(
		a.routedUnits, a.sumRouteMultiBloomGroups, a.sumRouteMultiBloomGroupsSq, float64(a.correctTopologyRoutes), a.sumRouteMultiBloomGroupsCorrect,
	)
	s.PearsonFanoutTestsVsTopologyCorrect = pointBiserial(
		a.routedUnits, a.sumRouteFanoutTests, a.sumRouteFanoutTestsSq, float64(a.correctTopologyRoutes), a.sumRouteFanoutTestsCorrect,
	)
	labels := []string{"0", "1", "2", "3-4", "5-8", "9-16", "17-32", "33+"}
	for bucket, label := range labels {
		b := a.byDistinctBucket[bucket]
		if b == nil {
			continue
		}
		s.TracePerformanceByDistinctHA = append(s.TracePerformanceByDistinctHA, fanoutTraceBin{
			DistinctHAFanouts: label, Traces: b.traces, CanonicalClean: b.clean,
			CleanRate: float64(b.clean) / float64(b.traces),
		})
	}
	haLabels := []string{"0", "1", "2", "3", "4", "5+"}
	for bucket, label := range haLabels {
		b := a.windowByHACount[bucket]
		if b == nil {
			continue
		}
		s.WindowPerformanceByHACount = append(s.WindowPerformanceByHACount, fanoutWindowBin{
			Bucket: label, Windows: b.windows, Correct: b.correct,
			CorrectRate: float64(b.correct) / float64(b.windows),
		})
	}
	for bucket, label := range haLabels {
		b := a.windowByKnownFanoutCount[bucket]
		if b == nil {
			continue
		}
		s.WindowPerformanceByKnownCount = append(s.WindowPerformanceByKnownCount, fanoutWindowBin{
			Bucket: label, Windows: b.windows, Correct: b.correct,
			CorrectRate: float64(b.correct) / float64(b.windows),
		})
	}
	for bucket, label := range haLabels {
		b := a.windowByTruthFanoutCount[bucket]
		if b == nil {
			continue
		}
		s.WindowPerformanceByTruthFanoutCount = append(s.WindowPerformanceByTruthFanoutCount, fanoutWindowBin{
			Bucket: label, Windows: b.windows, Correct: b.correct,
			CorrectRate: float64(b.correct) / float64(b.windows),
		})
	}
	coverageLabels := []string{"0", "(0,.25]", "(.25,.50]", "(.50,.75]", "(.75,1)", "1"}
	for bucket, label := range coverageLabels {
		b := a.coverageByBucket[bucket]
		if b == nil {
			continue
		}
		s.WindowPerformanceByHACoverage = append(s.WindowPerformanceByHACoverage, fanoutWindowBin{
			Bucket: label, Windows: b.windows, Correct: b.correct,
			CorrectRate: float64(b.correct) / float64(b.windows),
		})
	}
	for bucket, label := range coverageLabels {
		b := a.knownCoverageByBucket[bucket]
		if b == nil {
			continue
		}
		s.WindowPerformanceByKnownCoverage = append(s.WindowPerformanceByKnownCoverage, fanoutWindowBin{
			Bucket: label, Windows: b.windows, Correct: b.correct,
			CorrectRate: float64(b.correct) / float64(b.windows),
		})
	}
	appendRouteBins := func(dst *[]fanoutRouteBin, bins map[int]*fanoutWindowBinAcc) {
		for bucket, label := range haLabels {
			b := bins[bucket]
			if b == nil {
				continue
			}
			*dst = append(*dst, fanoutRouteBin{
				Bucket: label, RouteUnits: b.windows, Correct: b.correct,
				Accuracy: float64(b.correct) / float64(b.windows),
			})
		}
	}
	appendRouteBins(&s.RoutePerformanceByRequiredHA, a.routeByRequiredHA)
	appendRouteBins(&s.RoutePerformanceByFanoutGroups, a.routeByFanoutGroups)
	appendRouteBins(&s.RoutePerformanceByMultiBloomGroups, a.routeByMultiBloomGroups)
	appendRouteBins(&s.RoutePerformanceByFanoutTests, a.routeByFanoutTests)
	return s
}

type chainCountSummary struct {
	MatchedLevels int   `json:"matched_levels"`
	Count         int64 `json:"count"`
}

type chainPerformanceBin struct {
	MatchedLevels  int     `json:"matched_levels"`
	RouteUnits     int64   `json:"route_units"`
	CorrectUnits   int64   `json:"anchor_correct_units"`
	WrongUnits     int64   `json:"anchor_wrong_units"`
	UnitAccuracy   float64 `json:"anchor_unit_accuracy"`
	Orphans        int64   `json:"orphan_fragments"`
	CorrectOrphans int64   `json:"anchor_correct_fragments"`
	WrongOrphans   int64   `json:"anchor_wrong_fragments"`
	OrphanAccuracy float64 `json:"anchor_fragment_accuracy"`
}

type chainTraceBin struct {
	MinimumMatchedLevels int     `json:"minimum_matched_levels"`
	Traces               int64   `json:"traces"`
	Clean                int64   `json:"canonical_clean"`
	CleanRate            float64 `json:"canonical_clean_rate"`
}

type chainEvidenceSummary struct {
	MatchedLevelDefinition string `json:"matched_level_definition"`
	TraceDomainDefinition  string `json:"trace_domain_definition"`

	CandidateInitialHits         int64               `json:"candidate_initial_bloom_hits"`
	CandidateAccepted            int64               `json:"candidate_chain_accepted"`
	CandidateRejected            int64               `json:"candidate_chain_rejected"`
	CandidatePositiveBloomChecks int64               `json:"candidate_positive_bloom_checks"`
	AcceptedByLevels             []chainCountSummary `json:"accepted_candidates_by_matched_levels"`
	RejectedAfterLevels          []chainCountSummary `json:"rejected_candidates_after_matched_levels"`

	RoutedUnits     int64                 `json:"routed_units"`
	UnroutedUnits   int64                 `json:"unrouted_units"`
	RoutedOrphans   int64                 `json:"routed_orphan_fragments"`
	UnroutedOrphans int64                 `json:"unrouted_orphan_fragments"`
	CorrectUnits    int64                 `json:"anchor_correct_units"`
	WrongUnits      int64                 `json:"anchor_wrong_units"`
	CorrectOrphans  int64                 `json:"anchor_correct_fragments"`
	WrongOrphans    int64                 `json:"anchor_wrong_fragments"`
	ByMatchedLevels []chainPerformanceBin `json:"route_performance_by_matched_levels"`

	AllUnitsRoutedTraces int64           `json:"all_units_routed_traces"`
	AllUnitsRoutedClean  int64           `json:"all_units_routed_canonical_clean"`
	TracesWithUnrouted   int64           `json:"traces_with_unrouted_units"`
	TraceByMinimum       []chainTraceBin `json:"trace_performance_by_minimum_matched_levels"`
	PearsonMeanVsClean   *float64        `json:"pearson_mean_matched_levels_vs_canonical_clean"`
	PearsonMinVsClean    *float64        `json:"pearson_minimum_matched_levels_vs_canonical_clean"`
	PearsonChecksVsClean *float64        `json:"pearson_mean_positive_bloom_checks_vs_canonical_clean"`
}

func sortedChainCounts(m map[int]int64) []chainCountSummary {
	keys := make([]int, 0, len(m))
	for levels := range m {
		keys = append(keys, levels)
	}
	sort.Ints(keys)
	out := make([]chainCountSummary, 0, len(keys))
	for _, levels := range keys {
		out = append(out, chainCountSummary{MatchedLevels: levels, Count: m[levels]})
	}
	return out
}

func pointBiserial(n int64, sumX, sumX2, sumY, sumXY float64) *float64 {
	if n < 2 {
		return nil
	}
	nf := float64(n)
	denX := nf*sumX2 - sumX*sumX
	denY := nf*sumY - sumY*sumY // y is binary, so sum(y^2) == sum(y)
	if denX <= 0 || denY <= 0 {
		return nil
	}
	r := (nf*sumXY - sumX*sumY) / math.Sqrt(denX*denY)
	return &r
}

func summarizeChainEvidence(a chainEvidenceAcc) *chainEvidenceSummary {
	if a.candidateInitialHits == 0 && a.routedUnits == 0 && a.unroutedUnits == 0 {
		return nil
	}
	s := &chainEvidenceSummary{
		MatchedLevelDefinition: "one non-checkpoint ancestor ID on the selected anchor-to-checkpoint chain that passed every applicable carrier Bloom; the minimum across exact-parent sibling fragments is reported per route",
		TraceDomainDefinition:  "correlations include only nonempty traces whose exact-parent route units all selected an anchor; correctness is the canonical evidence-bounded topology verdict",
		CandidateInitialHits:   a.candidateInitialHits, CandidateAccepted: a.candidateAccepted,
		CandidateRejected: a.candidateRejected, CandidatePositiveBloomChecks: a.candidatePositiveBloomChecks,
		AcceptedByLevels: sortedChainCounts(a.acceptedByLevels), RejectedAfterLevels: sortedChainCounts(a.rejectedAfterLevels),
		RoutedUnits: a.routedUnits, UnroutedUnits: a.unroutedUnits,
		RoutedOrphans: a.routedOrphans, UnroutedOrphans: a.unroutedOrphans,
		CorrectUnits: a.correctUnits, WrongUnits: a.wrongUnits,
		CorrectOrphans: a.correctOrphans, WrongOrphans: a.wrongOrphans,
		AllUnitsRoutedTraces: a.allRoutedTraces, AllUnitsRoutedClean: a.allRoutedClean,
		TracesWithUnrouted: a.tracesWithUnrouted,
	}
	keys := make([]int, 0, len(a.byLevels))
	for levels := range a.byLevels {
		keys = append(keys, levels)
	}
	sort.Ints(keys)
	for _, levels := range keys {
		b := a.byLevels[levels]
		unitAccuracy, orphanAccuracy := 0.0, 0.0
		if b.units > 0 {
			unitAccuracy = float64(b.correctUnits) / float64(b.units)
		}
		if b.orphans > 0 {
			orphanAccuracy = float64(b.correctOrphans) / float64(b.orphans)
		}
		s.ByMatchedLevels = append(s.ByMatchedLevels, chainPerformanceBin{
			MatchedLevels: levels, RouteUnits: b.units, CorrectUnits: b.correctUnits,
			WrongUnits: b.wrongUnits, UnitAccuracy: unitAccuracy, Orphans: b.orphans,
			CorrectOrphans: b.correctOrphans, WrongOrphans: b.wrongOrphans, OrphanAccuracy: orphanAccuracy,
		})
	}
	keys = keys[:0]
	for levels := range a.traceByMin {
		keys = append(keys, levels)
	}
	sort.Ints(keys)
	for _, levels := range keys {
		b := a.traceByMin[levels]
		cleanRate := 0.0
		if b.traces > 0 {
			cleanRate = float64(b.clean) / float64(b.traces)
		}
		s.TraceByMinimum = append(s.TraceByMinimum, chainTraceBin{
			MinimumMatchedLevels: levels, Traces: b.traces, Clean: b.clean, CleanRate: cleanRate,
		})
	}
	s.PearsonMeanVsClean = pointBiserial(a.allRoutedTraces, a.sumMean, a.sumMeanSq, float64(a.allRoutedClean), a.sumMeanClean)
	s.PearsonMinVsClean = pointBiserial(a.allRoutedTraces, a.sumMin, a.sumMinSq, float64(a.allRoutedClean), a.sumMinClean)
	s.PearsonChecksVsClean = pointBiserial(a.allRoutedTraces, a.sumChecks, a.sumChecksSq, float64(a.allRoutedClean), a.sumChecksClean)
	return s
}

// scorerComparisonSummary exposes raw counts rather than pre-rounded rates so
// downstream analysis can reproduce every percentage and denominator exactly.
type scorerComparisonSummary struct {
	CanonicalPolicy                string `json:"canonical_policy"`
	HistoricalPolicy               string `json:"historical_policy"`
	CanonicalDenominator           string `json:"canonical_denominator"`
	HistoricalDenominator          string `json:"historical_denominator"`
	Traces                         int    `json:"traces"`
	InputObligations               int    `json:"input_obligations"`
	EmittedReconstructions         int    `json:"emitted_reconstructions"`
	ObligationAndEmitted           int    `json:"obligation_and_emitted"`
	ObligationNoEmission           int    `json:"obligation_no_emission"`
	EmissionNoObligation           int    `json:"emission_no_obligation"`
	CanonicalCleanObligations      int    `json:"canonical_clean_on_input_obligations"`
	HistoricalCleanObligations     int    `json:"historical_clean_on_input_obligations"`
	CanonicalCleanEmitted          int    `json:"canonical_clean_on_emitted_reconstructions"`
	HistoricalCleanEmitted         int    `json:"historical_clean_on_emitted_reconstructions"`
	BothCleanObligations           int    `json:"both_clean_on_input_obligations"`
	CanonicalOnlyCleanObligations  int    `json:"canonical_only_clean_on_input_obligations"`
	HistoricalOnlyCleanObligations int    `json:"historical_only_clean_on_input_obligations"`
	BothWrongObligations           int    `json:"both_wrong_on_input_obligations"`
	CanonicalWrongSegments         int    `json:"canonical_wrong_segments_on_input_obligations"`
	HistoricalWrongUnits           int    `json:"historical_wrong_units_on_input_obligations"`
}

type rateSummary struct {
	DropRate         float64                  `json:"drop_rate"`
	DropCode         string                   `json:"drop_code"`
	TopologySummary  topologySummary          `json:"topology_summary"`
	GreedySummary    *greedySummary           `json:"greedy_summary,omitempty"`
	ChainEvidence    *chainEvidenceSummary    `json:"chain_evidence_summary,omitempty"`
	SB3Summary       *sb3Summary              `json:"sb3_summary,omitempty"`
	ScorerComparison *scorerComparisonSummary `json:"scorer_comparison,omitempty"`
}

func evidenceProfile(c config) string {
	profile := "model-default"
	if c.mode == "cgp0" || c.mode == "pb0" || c.mode == "sb3" {
		profile = "maximal"
		if c.cgp0Legacy || c.pb0Legacy || c.sb3IgnoreOrdinals || c.greedyNoGrouped || c.greedyNoHardHA || c.greedyNoFallback || c.topoOnly {
			profile = "ablation"
		}
	}
	return profile
}

func summariesFor(c config, a cgp2acc) (topologySummary, *greedySummary, *chainEvidenceSummary, *sb3Summary, *scorerComparisonSummary) {
	scorePolicy := "evidence-bounded-v1"
	if c.mode == "pb0" || c.mode == "pb1" || c.mode == "pb2" {
		scorePolicy = "path-evidence-v1"
	}
	topo := topologySummary{
		ScorePolicy: scorePolicy, EvidenceProfile: evidenceProfile(c),
		Traces: a.nt, Feasible: a.feas, Empty: a.empty, EmptyClean: a.emptyClean,
		Clean: a.clean, CleanAll: a.clean + a.emptyClean, RealNodes: a.realNodes,
		EdgeExact: a.edgeExact, EdgeAnonOK: a.edgeAnonOK, EdgeWrong: a.edgeWrong,
		ConstraintWrong: a.constraintWrong, SurvivorEdges: a.survN, SurvivorExact: a.survEx,
		NamedSyn: a.named, NamedExact: a.namedEx,
	}
	var greedy *greedySummary
	if c.mode == "cgp0" || c.mode == "pb0" {
		mode := "full-evidence"
		if c.cgp0Legacy || c.pb0Legacy {
			mode = "legacy-lean"
		}
		greedy = &greedySummary{
			Mode: mode, Checked: a.greedyChecked, CandidateEvaluations: a.greedyCandidates,
			HardOverrides: a.greedyHardOverrides, HardConflicts: a.greedyHardConflicts,
			ParentConflicts: a.greedyParentConflicts, HAConflicts: a.greedyHAConflicts,
			FanoutEvidence: summarizeFanoutEvidence(a.fanoutEvidence),
		}
	}
	var sb3 *sb3Summary
	if c.mode == "sb3" {
		s := a.sb3
		sb3 = &sb3Summary{
			GreedyMode: "sb3-greedy", Checked: s.checked, Compatible: s.compatible,
			CandidateEvaluations: s.candidateEvaluations, OrdinalOverrides: s.ordinalOverrides,
			HardOverrides: s.hardOverrides, OrdinalPlaced: s.ordinalPlaced,
			ImplicitOrdinals: s.implicitOrdinals, Fanouts: s.fanouts, Conflicts: s.conflicts,
			HardConflicts: s.hardConflicts, ParentConflicts: s.parentConflicts, HAConflicts: s.haConflicts,
			StructureChecked: s.structureChecked, StructureComplete: s.structureComplete,
			StructureIncomplete: s.structureIncomplete, DEEPlaced: s.deePlaced,
			DEEAmbiguous: s.deeAmbiguous, DEENoPlace: s.deeNoPlace,
			EventOrderOK: s.eventOK, CriticalPathOK: s.criticalPathOK,
			StructureParents: s.structureParents, EndOrderOK: s.endOrderOK,
		}
	}
	var comparison *scorerComparisonSummary
	if c.compareScorers {
		x := a.scoreComparison
		comparison = &scorerComparisonSummary{
			CanonicalPolicy: "evidence-bounded-v1", HistoricalPolicy: "legacy-node-plus-anchor-v0",
			CanonicalDenominator:  "surviving-record input obligations",
			HistoricalDenominator: "algorithm emitted at least one reconnection",
			Traces:                x.traces, InputObligations: x.obligations, EmittedReconstructions: x.emitted,
			ObligationAndEmitted: x.obligationAndEmitted, ObligationNoEmission: x.obligationNoEmission,
			EmissionNoObligation:           x.emissionNoObligation,
			CanonicalCleanObligations:      x.canonicalCleanObligations,
			HistoricalCleanObligations:     x.historicalCleanObligations,
			CanonicalCleanEmitted:          x.canonicalCleanEmitted,
			HistoricalCleanEmitted:         x.historicalCleanEmitted,
			BothCleanObligations:           x.bothCleanObligations,
			CanonicalOnlyCleanObligations:  x.canonicalOnlyCleanObligations,
			HistoricalOnlyCleanObligations: x.historicalOnlyCleanObligations,
			BothWrongObligations:           x.bothWrongObligations,
			CanonicalWrongSegments:         x.canonicalWrongSegments,
			HistoricalWrongUnits:           x.historicalWrongUnits,
		}
	}
	return topo, greedy, summarizeChainEvidence(a.chainEvidence), sb3, comparison
}

// output is the per-trace score arrays, in trace load order.
type output struct {
	Mode               string                   `json:"mode"`
	Corpus             string                   `json:"corpus,omitempty"`
	TraceStore         string                   `json:"trace_store,omitempty"`
	CheckpointDistance int                      `json:"checkpoint_distance"`
	DropRate           float64                  `json:"drop_rate,omitempty"`
	DropRates          []float64                `json:"drop_rates,omitempty"`
	RateSummaries      []rateSummary            `json:"rate_summaries,omitempty"`
	BloomFP            float64                  `json:"bloom_fp,omitempty"`
	PrimeM             bool                     `json:"prime_m,omitempty"`
	PrimeMByteCap      bool                     `json:"prime_m_bytecap,omitempty"`
	PrefixLen          int                      `json:"prefix_len,omitempty"`
	FPBits             int                      `json:"fp_bits,omitempty"`
	LehmerEE           bool                     `json:"lehmer_ee,omitempty"`
	TopoOnly           bool                     `json:"topo_only,omitempty"`
	SB3IgnoreOrdinals  bool                     `json:"sb3_ignore_ordinals,omitempty"`
	CGP0Legacy         bool                     `json:"cgp0_legacy,omitempty"`
	PB0Legacy          bool                     `json:"pb0_legacy,omitempty"`
	GreedyNoGrouped    bool                     `json:"greedy_no_grouped_evidence,omitempty"`
	GreedyNoHardHA     bool                     `json:"greedy_no_hard_ha,omitempty"`
	GreedyNoFallback   bool                     `json:"greedy_no_route_fallback,omitempty"`
	PerTraceDropSeed   bool                     `json:"per_trace_drop_seed,omitempty"`
	Order              string                   `json:"order"`
	Consistency        string                   `json:"consistency"`
	Seed               int64                    `json:"seed"`
	Sample             int                      `json:"sample,omitempty"`
	SampleSeed         int64                    `json:"sample_seed,omitempty"`
	NumTraces          int                      `json:"num_traces"`
	NumSpans           []int                    `json:"num_spans,omitempty"`
	NumDropped         []int                    `json:"num_dropped,omitempty"`
	NumOrphans         []int                    `json:"num_orphans,omitempty"`
	NumReconnected     []int                    `json:"num_reconnected,omitempty"`
	NumAnchorCorrect   []int                    `json:"num_anchor_correct,omitempty"`
	NumAnchorAncestor  []int                    `json:"num_anchor_ancestor,omitempty"`
	NumGapCorrect      []int                    `json:"num_gap_correct,omitempty"`
	NumMisattached     []int                    `json:"num_misattached,omitempty"`
	NumUnanchored      []int                    `json:"num_unanchored,omitempty"`
	NumSynthetic       []int                    `json:"num_synthetic,omitempty"`
	NumBorrowed        []int                    `json:"num_borrowed_bloom,omitempty"`
	NumFragmentsLost   []int                    `json:"num_fragments_lost,omitempty"`    // PCR only
	NumSpansLost       []int                    `json:"num_spans_lost,omitempty"`        // PCR only
	NumAncestorsSkip   []int                    `json:"num_ancestors_skipped,omitempty"` // PCR only
	NumSpansInSkipped  []int                    `json:"num_spans_in_skipped,omitempty"`  // PCR only
	NumOpenEnds        []int                    `json:"num_open_ends,omitempty"`         // PCRB only
	NumOpenEndsMatched []int                    `json:"num_open_ends_matched,omitempty"` // PCRB only
	NumForcedMatches   []int                    `json:"num_forced_matches,omitempty"`    // PCRB only
	NumOrphansPlaced   []int                    `json:"num_orphans_placed,omitempty"`    // PCRB only
	NumOrphanOpenEnds  []int                    `json:"num_orphan_open_ends,omitempty"`  // PCRB only
	TopologySummary    *topologySummary         `json:"topology_summary,omitempty"`
	GreedySummary      *greedySummary           `json:"greedy_summary,omitempty"`
	ChainEvidence      *chainEvidenceSummary    `json:"chain_evidence_summary,omitempty"`
	SB3Summary         *sb3Summary              `json:"sb3_summary,omitempty"`
	ScorerComparison   *scorerComparisonSummary `json:"scorer_comparison,omitempty"`
	CompareScorers     bool                     `json:"compare_scorers,omitempty"`
}

// slowReconMS: if >0 (set via TRACE_RECON_SLOW=<ms>), log any single cgprb
// trace whose reconstruction takes at least this many ms. Diagnostic only.
var slowReconMS = func() int64 {
	if v := os.Getenv("TRACE_RECON_SLOW"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}()

func main() {
	c := parseFlags()

	// CPU profile (TRACE_RECON_CPUPROF=<path>): diagnostic for the tail grind.
	// TRACE_RECON_PROFAFTER=<n>: delay StartCPUProfile by n seconds so the
	// window lands on the tail monster traces rather than the early bulk.
	if p := os.Getenv("TRACE_RECON_CPUPROF"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cpuprof create: %v\n", err)
			os.Exit(1)
		}
		startProf := func() {
			if err := pprof.StartCPUProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "cpuprof start: %v\n", err)
				os.Exit(1)
			}
		}
		if after := os.Getenv("TRACE_RECON_PROFAFTER"); after != "" {
			if secs, err := strconv.Atoi(after); err == nil && secs > 0 {
				time.AfterFunc(time.Duration(secs)*time.Second, func() {
					fmt.Fprintf(os.Stderr, "PROFAFTER: starting cpu profile at %ds\n", secs)
					startProf()
				})
			} else {
				startProf()
			}
		} else {
			startProf()
		}
		defer pprof.StopCPUProfile()
		// Timer-based flush (TRACE_RECON_PROFSECS=<n>): after n seconds, stop the
		// profile, flush it, and exit cleanly. Reliable where signal delivery
		// races default termination. Slow traces are interspersed throughout the
		// corpus, so a short window captures the hot path.
		if s := os.Getenv("TRACE_RECON_PROFSECS"); s != "" {
			if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
				time.AfterFunc(time.Duration(secs)*time.Second, func() {
					pprof.StopCPUProfile()
					f.Close()
					fmt.Fprintf(os.Stderr, "PROFSECS: cpu profile flushed after %ds, exiting\n", secs)
					os.Exit(0)
				})
			}
		}
	}

	ha := newHarness(c)

	var traceOrder []uint64
	if c.traceStore != "" {
		traceOrder = runFromTraceStore(c, ha)
	} else if c.corpusDir != "" {
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
		Corpus:             c.corpusDir,
		TraceStore:         c.traceStore,
		CheckpointDistance: c.checkpointDistance,
		DropRate:           c.dropRate,
		PrimeM:             bloom.PrimeM,
		PrimeMByteCap:      bloom.PrimeMByteCap,
		PerTraceDropSeed:   c.perTraceDropSeed,
		TopoOnly:           c.topoOnly,
		SB3IgnoreOrdinals:  c.sb3IgnoreOrdinals,
		CGP0Legacy:         c.cgp0Legacy,
		PB0Legacy:          c.pb0Legacy,
		GreedyNoGrouped:    c.greedyNoGrouped,
		GreedyNoHardHA:     c.greedyNoHardHA,
		GreedyNoFallback:   c.greedyNoFallback,
		CompareScorers:     c.compareScorers,
		Order:              orderName,
		Consistency:        map[bool]string{true: "chain", false: "none"}[c.chainCheck],
		Seed:               c.seed,
		Sample:             c.sampleCount,
		NumTraces:          len(traceOrder),
	}
	if c.sampleCount > 0 {
		out.SampleSeed = c.sampleSeed
	}
	if cg2Family(c.mode) {
		if len(ha.mdRates) == 0 {
			topo, greedy, chain, sb3, comparison := summariesFor(c, ha.cg2)
			out.TopologySummary, out.GreedySummary, out.ChainEvidence, out.SB3Summary, out.ScorerComparison = &topo, greedy, chain, sb3, comparison
		} else {
			out.DropRate = 0
			out.DropRates = append([]float64(nil), ha.mdRates...)
			for r, rate := range ha.mdRates {
				topo, greedy, chain, sb3, comparison := summariesFor(c, ha.mdAcc[r])
				out.RateSummaries = append(out.RateSummaries, rateSummary{
					DropRate: rate, DropCode: ha.mdDC[r], TopologySummary: topo,
					GreedySummary: greedy, ChainEvidence: chain, SB3Summary: sb3, ScorerComparison: comparison,
				})
			}
		}
	}
	switch c.mode {
	case "pcr":
		out.PrefixLen = c.prefixLen
	case "pcrb", "pcrs", "cgprb", "cgp2", "pb2", "cgp1", "pb1", "cgp0", "pb0", "sb3":
		out.PrefixLen = c.prefixLen
		out.BloomFP = c.bloomFP
		if c.mode == "sb3" {
			out.FPBits = c.fpBits
			out.LehmerEE = c.lehmerEE
		}
	default:
		out.BloomFP = c.bloomFP
	}
	var agg recon.Score
	if !cg2Family(c.mode) {
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
			if c.mode == "pcr" || c.mode == "pcrb" || c.mode == "pcrs" || c.mode == "cgprb" {
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

	if !cg2Family(c.mode) {
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
	}
	if len(ha.mdRates) > 0 {
		if ha.timingOn {
			reportTimingMulti(ha, c.timingPath)
		}
	} else if ha.timingOn && len(ha.timingRecs) > 0 {
		reportTiming(ha, c.timingPath)
	}
	fmt.Fprintf(os.Stderr, "Wrote %d traces to %s\n", len(traceOrder), c.outputPath)
}

// reportTiming prints a per-trace reconstruction-time summary (percentiles in
// microseconds) and, if path != "", writes the full per-trace CSV.
func reportTiming(ha *harness, path string) {
	reportTimingRecs(ha.timingRecs, "", path)
}

// reportTimingRecs prints a per-trace reconstruction-time summary (percentiles
// in ms, tag-prefixed) and, if path != "", writes the full per-trace CSV.
func reportTimingRecs(recs []traceTiming, tag, path string) {
	// Summarize two subsets: all recorded traces, and the feasible (exclEmpty)
	// subset — the traces that actually reconstructed something.
	summ := func(label string, ns []int64) {
		if len(ns) == 0 {
			fmt.Fprintf(os.Stderr, "TIMING[%s%s] traces=0\n", tag, label)
			return
		}
		sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
		var total int64
		for _, v := range ns {
			total += v
		}
		ms := func(v int64) float64 { return float64(v) / 1e6 }
		pctl := func(p float64) int64 { return ns[int(p*float64(len(ns)-1))] }
		fmt.Fprintf(os.Stderr, "TIMING[%s%s] traces=%d recon_total=%s mean=%.3fms p50=%.3f p90=%.3f p99=%.3f max=%.3f (ms)\n",
			tag, label, len(ns), time.Duration(total).Round(time.Millisecond),
			ms(total/int64(len(ns))), ms(pctl(0.50)), ms(pctl(0.90)), ms(pctl(0.99)), ms(ns[len(ns)-1]))
	}
	all := make([]int64, 0, len(recs))
	feas := make([]int64, 0, len(recs))
	for _, r := range recs {
		all = append(all, r.ns)
		if r.feasible {
			feas = append(feas, r.ns)
		}
	}
	summ("all", all)
	summ("exclEmpty", feas)
	if path == "" {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "timing: %v\n", err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "tid,survivors,spans,dropped,feasible,recon_ns")
	for _, r := range recs {
		fb := 0
		if r.feasible {
			fb = 1
		}
		fmt.Fprintf(w, "%016x,%d,%d,%d,%d,%d\n", r.tid, r.nsurv, r.nspan, r.ndrop, fb, r.ns)
	}
}

// reportTimingMulti writes one summary + per-trace CSV per drop rate. The path
// template should contain {dc}; it is replaced by each rate's drop code (else
// the code is appended).
func reportTimingMulti(ha *harness, tmpl string) {
	for r := range ha.mdRates {
		dc := ha.mdDC[r]
		p := ""
		if tmpl != "" {
			if strings.Contains(tmpl, "{dc}") {
				p = strings.ReplaceAll(tmpl, "{dc}", dc)
			} else {
				p = tmpl + "_" + dc
			}
		}
		reportTimingRecs(ha.mdTiming[r], dc+" ", p)
	}
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
	traceOrder, spanCounts := pickTraces(meta.TraceOrder, meta.SpanCounts, c)
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

// replayTrace reconstructs one trace's collected spans by replaying its event
// block through a per-worker handler — the exact span-building onEvent does in
// the streaming path, but for one isolated trace. EvictTrace clears the
// handler's per-trace state so the same handler can serve the next trace.
// SB3's cross-trace DEE queue is irrelevant to topology payload fields; the
// generated DEE side records are captured separately by origin trace.
func replayTrace(h bridge.Handler, st corpus.StoredTrace) []collSpan {
	spans := make([]collSpan, 0, len(st.Events)/2)
	idx := make(map[uint64]int, len(st.Events)/2)
	nextSeq := make(map[uint64]int)
	for i := range st.Events {
		e := &st.Events[i]
		ev := &bridge.Event{TraceID: st.TraceID, SpanID: e.SpanID, ParentID: e.ParentID, ServiceID: e.ServiceID}
		if e.Kind == corpus.KindStart {
			seqNum := 0
			if e.ParentID != 0 {
				seqNum = nextSeq[e.ParentID] + 1
				nextSeq[e.ParentID] = seqNum
			}
			r := h.OnStart(ev, seqNum)
			idx[e.SpanID] = len(spans)
			spans = append(spans, collSpan{spanID: e.SpanID, parentID: e.ParentID, depth: -1, br: r.Payload, ordinal: seqNum})
		} else {
			r := h.OnEnd(ev)
			if j, ok := idx[e.SpanID]; ok {
				spans[j].depth = r.Depth
				spans[j].endPos = int64(i)
				if r.Payload != nil {
					spans[j].br = r.Payload
				}
			}
		}
	}
	h.EvictTrace(st.TraceID)
	return spans
}

// runFromTraceStore is the parallel pipeline: a per-trace store (in completion
// order) is read sequentially; each trace's handler replay runs on a worker
// pool (the expensive part the single global stream forced to be serial);
// results are re-sequenced into completion order for a cheap serial drop pass
// (reproducing the global RNG exactly), then handed to the existing
// reconstruction worker pool. Results are bit-identical to runFromCorpus.
// pickTraces selects which traces to process: a uniform random sample of
// c.sampleCount (seeded by c.sampleSeed) if set, else the first c.traceCount
// (prefix), else all. counts (pass nil if unused) is kept aligned with the
// returned order. The sample is re-sorted into original order for locality.
func pickTraces(order []uint64, counts []uint32, c config) ([]uint64, []uint32) {
	if c.sampleCount > 0 && c.sampleCount < len(order) {
		idx := make([]int, len(order))
		for i := range idx {
			idx[i] = i
		}
		rng := rand.New(rand.NewSource(c.sampleSeed))
		rng.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })
		idx = idx[:c.sampleCount]
		sort.Ints(idx)
		o := make([]uint64, len(idx))
		var ct []uint32
		if counts != nil {
			ct = make([]uint32, len(idx))
		}
		for k, i := range idx {
			o[k] = order[i]
			if counts != nil {
				ct[k] = counts[i]
			}
		}
		return o, ct
	}
	if c.traceCount > 0 && c.traceCount < len(order) {
		if counts != nil {
			return order[:c.traceCount], counts[:c.traceCount]
		}
		return order[:c.traceCount], nil
	}
	return order, counts
}

func runFromTraceStore(c config, ha *harness) []uint64 {
	if c.corpusDir == "" {
		fmt.Fprintln(os.Stderr, "error: --trace-store also needs --corpus (for meta: trace order)")
		os.Exit(2)
	}
	_, metaPath := corpus.Paths(c.corpusDir)
	meta, err := corpus.ReadMeta(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading meta: %v\n", err)
		os.Exit(1)
	}
	traceOrder, _ := pickTraces(meta.TraceOrder, nil, c)
	// With per-trace seeded drops, a target trace's drop set is independent of
	// every preceding trace. Filter --only-traces before opening/replaying the
	// store so a one-trace diagnostic does not pointlessly simulate all 521k
	// Day-1 traces first. Preserve the legacy full scan for the shared RNG mode,
	// where advancing earlier draws is part of reproducibility.
	if len(ha.only) > 0 && c.perTraceDropSeed {
		filtered := make([]uint64, 0, len(ha.only))
		for _, tid := range traceOrder {
			if ha.only[tid] {
				filtered = append(filtered, tid)
			}
		}
		traceOrder = filtered
	}
	selected := make(map[uint64]bool, len(traceOrder))
	for _, tid := range traceOrder {
		selected[tid] = true
	}

	r, err := corpus.OpenTraceStore(c.traceStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open trace store: %v\n", err)
		os.Exit(1)
	}

	nw := c.workers
	if nw <= 0 {
		nw = runtime.NumCPU()
	}
	fmt.Fprintf(os.Stderr, "Trace store: %d traces selected, %d parallel replay workers\n", len(traceOrder), nw)

	type seqTrace struct {
		seq int
		st  corpus.StoredTrace
	}
	type prepped struct {
		seq   int
		tid   uint64
		spans []collSpan
		dees  []bridge.DEEQuad
	}
	inCh := make(chan seqTrace, nw*2)
	outCh := make(chan prepped, nw*2)

	// Reader: stream blocks in completion order, assign a contiguous sequence
	// number to each SELECTED trace (so the drop pass below can re-impose
	// completion order and consume the RNG exactly as streaming does).
	go func() {
		seq := 0
		for {
			st, err := r.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				fmt.Fprintf(os.Stderr, "trace store read: %v\n", err)
				os.Exit(1)
			}
			if !selected[st.TraceID] {
				continue
			}
			inCh <- seqTrace{seq: seq, st: st}
			seq++
		}
		close(inCh)
		r.Close()
	}()

	// Phase 1 (parallel): handler replay. Each worker owns one handler.
	var p1wg sync.WaitGroup
	for i := 0; i < nw; i++ {
		p1wg.Add(1)
		go func() {
			defer p1wg.Done()
			h, workerCfg := makeHandler(c)
			for stx := range inCh {
				var dees []bridge.DEEQuad
				if sh, ok := h.(*bridge.SB3Handler); ok {
					sh.DEESink = func(tid uint64, raw []byte) {
						var decoded []bridge.DEEQuad
						var err error
						if workerCfg.SBridgeLehmer {
							decoded, err = bridge.DecodeDEEQuadsLehmer(raw, workerCfg.FPBits)
						} else {
							decoded, err = bridge.DecodeDEEQuads(raw, workerCfg.FPBits)
						}
						if err != nil {
							panic(fmt.Sprintf("decode generated SB3 DEE for trace %016x: %v", tid, err))
						}
						dees = append(dees, decoded...)
					}
				}
				spans := replayTrace(h, stx.st)
				outCh <- prepped{seq: stx.seq, tid: stx.st.TraceID, spans: spans, dees: dees}
			}
		}()
	}
	go func() { p1wg.Wait(); close(outCh) }()

	// Serializer (this goroutine): re-impose completion order via a reorder
	// buffer, run the serial drop (global RNG), and dispatch to the existing
	// reconstruction worker pool (ha.jobs). Drop visits br==nil spans in
	// start-order and the RNG advances in completion order — identical to
	// finishTrace, so the dropped set is bit-identical to streaming.
	buf := make(map[int]prepped)
	next := 0
	dispatch := func(p prepped) {
		if len(ha.mdRates) > 0 {
			dm := ha.computeDroppedMulti(p.tid, p.spans)
			if ha.only != nil && !ha.only[p.tid] {
				return
			}
			ha.jobs <- finishJob{tid: p.tid, spans: p.spans, droppedMulti: dm, dees: p.dees}
			return
		}
		dropped := ha.computeDropped(p.tid, p.spans)
		// --only-traces: RNG already advanced above; skip the (expensive)
		// reconstruction for non-target traces, mirroring finishTrace.
		if ha.only != nil && !ha.only[p.tid] {
			return
		}
		ha.jobs <- finishJob{tid: p.tid, spans: p.spans, dropped: dropped, dees: p.dees}
	}
	for p := range outCh {
		buf[p.seq] = p
		for {
			q, ok := buf[next]
			if !ok {
				break
			}
			delete(buf, next)
			next++
			dispatch(q)
		}
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
