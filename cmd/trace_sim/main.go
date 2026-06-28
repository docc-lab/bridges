// Command trace_sim is the Go port of bridges/trace_simulator.py --bagsize.
//
// It runs spans through one of the four bridge handlers (vanilla / pb / cgpb
// / sbridge) and writes per-trace bagsize metrics in the same JSON shape as
// the Python sim. Two input modes:
//
//	--corpus DIR   read events.bin + meta.bin produced by trace_prep
//	(no flag)      read JSON files from <input_dir> directly
//
// Both modes produce byte-identical metrics output for any given trace set.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/loader"
)

type config struct {
	inputDir           string
	corpusDir          string
	outputPath         string
	mode               string
	checkpointDistance int
	bagsize            bool
	traceCount         int
	requireClean       bool
	logDee             bool
	deeLogBytes        int
	emitDepth          bool
	emitOC             bool
	topoOnly           bool
	fpBits             int
	noOrdinal          bool
	prefixLen          int
	bloomFP            float64
	sampleCount        int   // corpus mode: if >0, simulate a RANDOM sample of this many traces (seeded)
	sampleSeed         int64 // seed for the random sample
	first              int   // corpus mode: if >0, simulate only the FIRST N traces (contiguous), stop early
	progressN          int   // corpus mode: print a PROGRESS line every N completed traces (0 = off)
	workers            int   // corpus mode: parallel worker count (trace sharding; non-sbridge modes only)
	streamMetrics      string // corpus mode: stream per-trace metrics CSV here instead of holding all in RAM
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.outputPath, "o", "", "Output JSON file (required)")
	flag.StringVar(&c.outputPath, "output", "", "Output JSON file (required)")
	flag.StringVar(&c.corpusDir, "corpus", "", "Read events.bin + meta.bin from this corpus dir (skips JSON parse)")
	flag.StringVar(&c.mode, "mode", "vanilla", "Bridge mode: vanilla, pb, cgpb, sbridge, pcr, pcrb")
	flag.IntVar(&c.checkpointDistance, "checkpoint-distance", 1, "Checkpoint distance")
	flag.BoolVar(&c.bagsize, "bagsize", false, "Output per-trace bagsize metrics")
	flag.IntVar(&c.traceCount, "trace-count", 0, "Max number of traces to load (0 = all; JSON mode only)")
	flag.IntVar(&c.sampleCount, "sample", 0, "Corpus mode: if >0, simulate a RANDOM sample of this many traces (uniform over the trace order, seeded by --sample-seed). Same seed => same sample (match the recon sweep's --sample/--sample-seed for overhead-vs-accuracy on identical traces).")
	flag.Int64Var(&c.sampleSeed, "sample-seed", 1, "Seed for --sample trace selection")
	flag.IntVar(&c.first, "first", 0, "Corpus mode: simulate only the FIRST N traces (contiguous in trace order), stopping the read once they finalize. Unlike --sample this keeps S-bridge's cross-trace DEE density realistic.")
	flag.IntVar(&c.progressN, "progress", 0, "Corpus mode: print a PROGRESS line to stderr every N completed traces (0 = silent)")
	flag.IntVar(&c.workers, "workers", 1, "Corpus mode: parallel workers, sharding traces by tid%workers (1 = single-threaded). Only for modes with no cross-trace state (pb/cgpb/pcr/pcrb/cgprb/vanilla); sbridge is forced to 1 (its DEE queue is cross-trace).")
	flag.StringVar(&c.streamMetrics, "stream-metrics", "", "Corpus mode: stream per-trace metrics as CSV to this file (one row per trace, written + freed on finalize) instead of holding them all in RAM. Bounds memory to the in-flight trace set; post-process into the bagsize JSON with `trace_sim csv2json <csv> <out.json>`. Replaces -o (no JSON written directly).")
	flag.BoolVar(&c.requireClean, "require-clean", false, "Cleanliness filter: drop dirty traces; multi-root traces keep only the biggest root tree (JSON mode only)")
	flag.BoolVar(&c.emitDepth, "emit-depth", true, "Emit absolute depth: varint(depth) replaces varint(depthMod) in _br payloads, and interior non-checkpoint spans carry a _d attribute (see docs/depth_emission.md). Default on: reconstruction relies on per-span depth, so its cost must be charged. Pass --emit-depth=false for the legacy depthMod accounting.")
	flag.BoolVar(&c.emitOC, "emit-oc", true, "S-bridge: emit an _oc attribute (window-relative ordinal chain) on interior non-checkpoint spans so a surviving non-checkpoint can self-place. Default on: sbridge reconstruction needs per-span ordinal position, so charge it. (No effect on non-sbridge modes.)")
	flag.BoolVar(&c.topoOnly, "topo-only", false, "S-bridge: emit topology only — drop the per-level EE sublists and DEE batches (Phase-2 ordering). Smaller _br payload, no DEE baggage; reconstructs call-graph shape but not event order.")
	flag.IntVar(&c.fpBits, "fp-bits", 16, "S-bridge: non-checkpoint fingerprint width in bits (bit-packed in the _br fp section). Default 16 (legacy 2-byte). Narrower = smaller payload, higher reject rate.")
	flag.BoolVar(&c.noOrdinal, "no-ordinal", false, "S-bridge: drop the ordinal from interior _o (emit depth only); severed survivors self-place by (depth, own-fp, parent-fp).")
	flag.BoolVar(&c.logDee, "log-dee", false, "S-bridge: log DEE pickup/queue events to stderr")
	flag.IntVar(&c.deeLogBytes, "dee-log-bytes", 10000, "Threshold for --log-dee")
	flag.IntVar(&c.prefixLen, "prefix-len", bridge.DefaultPCRPrefixLen, "truncated checkpoint-root span ID length in bytes (1-8); PCR/PCRB/CGPRB prefix and the S-bridge window anchor. 8 = full-width.")
	flag.Float64Var(&c.bloomFP, "bloom-fp", bridge.DefaultBloomFPRate, "PCRB mode: target bloom false-positive rate (pb/cgpb keep the legacy default)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <input_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s --corpus <corpus_dir> [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if c.corpusDir == "" {
		if flag.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "error: input_dir or --corpus required")
			flag.Usage()
			os.Exit(2)
		}
		c.inputDir = flag.Arg(0)
	}
	if c.streamMetrics != "" && c.corpusDir == "" {
		fmt.Fprintln(os.Stderr, "error: --stream-metrics requires corpus mode (--corpus)")
		os.Exit(2)
	}
	if c.outputPath == "" && c.streamMetrics == "" {
		fmt.Fprintln(os.Stderr, "error: -o/--output required (or use --stream-metrics)")
		os.Exit(2)
	}
	if !c.bagsize {
		fmt.Fprintln(os.Stderr, "error: --bagsize is required (only mode supported in this port)")
		os.Exit(2)
	}
	return c
}

func makeHandler(c config, serviceName func(uint16) string, sourceFile func(uint64) string) bridge.Handler {
	switch c.mode {
	case "vanilla":
		return bridge.NewVanillaHandler()
	case "pcr":
		return bridge.NewPCRBridgeHandler(c.checkpointDistance, c.prefixLen)
	case "pcrb":
		return bridge.NewPCRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
	case "pb":
		h := bridge.NewPathBridgeHandler(c.checkpointDistance, bridge.DefaultBloomFPRate)
		h.EmitDepth = c.emitDepth
		return h
	case "cgpb":
		h := bridge.NewCGPBBridgeHandler(c.checkpointDistance, bridge.DefaultBloomFPRate)
		h.EmitDepth = c.emitDepth
		return h
	case "cgprb":
		// The call-graph-preserving PCRB we actually reconstruct in trace_recon
		// --mode cgprb: PCRB payload (truncated ckpt id + in-window bloom) plus
		// the window-local HA. Same constructor args/defaults as the recon path
		// so the byte accounting matches what cpd=8 recon consumed.
		return bridge.NewCGPRBBridgeHandler(c.checkpointDistance, c.prefixLen, c.bloomFP)
	case "sbridge":
		var logger *bridge.DeeSizeLogger
		if c.logDee {
			logger = &bridge.DeeSizeLogger{
				ThresholdBytes: c.deeLogBytes,
				ServiceName:    serviceName,
				SourceFile:     sourceFile,
			}
		}
		h := bridge.NewSBridgeHandler(c.checkpointDistance, logger)
		h.EmitOC = c.emitOC
		h.TopoOnly = c.topoOnly
		h.OmitOrdinal = c.noOrdinal
		if c.fpBits > 0 {
			h.FPBits = c.fpBits
		}
		if c.prefixLen > 0 {
			h.CkptBytes = c.prefixLen // checkpoint-root anchor width (shared --prefix-len knob)
		}
		return h
	}
	fmt.Fprintf(os.Stderr, "unknown mode %q\n", c.mode)
	os.Exit(2)
	return nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "csv2json" {
		runCSV2JSON(os.Args[2:])
		return
	}
	c := parseFlags()

	var metrics []TraceMetrics
	if c.corpusDir != "" {
		metrics = runFromCorpus(c)
	} else {
		metrics = runFromJSON(c)
	}

	if c.streamMetrics != "" {
		// Per-trace records were streamed to the CSV; nothing to dump here.
		// Post-process with cmd/bagsize_csv2json to get the bagsize JSON.
		fmt.Fprintf(os.Stderr, "Streamed per-trace metrics to %s\n", c.streamMetrics)
		return
	}

	if err := writeBagsizeJSON(c.outputPath, c.checkpointDistance, metrics, c.emitDepth, c.emitOC); err != nil {
		fmt.Fprintf(os.Stderr, "write output: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Wrote %d traces to %s\n", len(metrics), c.outputPath)
}

func runFromJSON(c config) []TraceMetrics {
	t0 := time.Now()
	traces, services, err := loadTracesFromDir(c.inputDir, c.traceCount, c.requireClean)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading traces: %v\n", err)
		os.Exit(1)
	}
	if len(traces) == 0 {
		fmt.Fprintln(os.Stderr, "No traces loaded.")
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Loaded %d traces in %s\n", len(traces), time.Since(t0).Round(time.Millisecond))

	if os.Getenv("TRACE_SIM_DUMP_TIMESTAMPS") == "1" {
		for _, t := range traces {
			for _, s := range t.Spans {
				parentHex := "0000000000000000"
				if s.ParentID != 0 {
					parentHex = fmt.Sprintf("%016x", s.ParentID)
				}
				fmt.Printf("%016x %d %d parent=%s\n", s.SpanID, s.StartNS, s.EndNS, parentHex)
			}
		}
		os.Exit(0)
	}

	traceSource := make(map[uint64]string, len(traces))
	for _, t := range traces {
		traceSource[t.TraceID] = t.SourceFile
	}

	t1 := time.Now()
	h := makeHandler(c,
		func(id uint16) string { return services.Name(id) },
		func(tid uint64) string { return traceSource[tid] })
	metrics := runInterleavedJSON(traces, h)
	fmt.Fprintf(os.Stderr, "Simulated in %s\n", time.Since(t1).Round(time.Millisecond))
	return metrics
}

func runFromCorpus(c config) []TraceMetrics {
	t0 := time.Now()
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
	fmt.Fprintf(os.Stderr, "Opened corpus (%d traces, %d services) in %s\n",
		len(meta.TraceOrder), len(meta.Services), time.Since(t0).Round(time.Millisecond))

	t1 := time.Now()
	svcFn := func(id uint16) string {
		if int(id) < len(meta.Services) {
			return meta.Services[id]
		}
		return ""
	}
	// JSON path-style source-file resolution isn't available from corpus alone;
	// fall back to "<traceIDhex>.json". DEE log uses this only for annotation.
	srcFn := func(tid uint64) string { return fmt.Sprintf("%016x.json", tid) }

	W := c.workers
	if W < 1 {
		W = 1
	}
	if W > 1 && c.mode == "sbridge" {
		fmt.Fprintln(os.Stderr, "warning: --workers > 1 ignored for sbridge (cross-trace DEE state is order-dependent); running single-threaded")
		W = 1
	}

	var stream *streamWriter
	if c.streamMetrics != "" {
		sw, err := newStreamWriter(c.streamMetrics, c.checkpointDistance, c.emitDepth, c.emitOC)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open stream-metrics file: %v\n", err)
			os.Exit(1)
		}
		stream = sw
	}

	var metrics []TraceMetrics
	if W > 1 {
		fmt.Fprintf(os.Stderr, "sharded: %d workers (parallel across traces)\n", W)
		metrics = runShardedFromCorpus(er, meta, func() bridge.Handler { return makeHandler(c, svcFn, srcFn) }, c, W, stream)
	} else {
		metrics = runInterleavedFromCorpus(er, meta, makeHandler(c, svcFn, srcFn), c, stream)
	}
	if stream != nil {
		if err := stream.close(); err != nil {
			fmt.Fprintf(os.Stderr, "close stream-metrics file: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "Simulated in %s\n", time.Since(t1).Round(time.Millisecond))
	return metrics
}

// loadTracesFromDir reads all *.json files in dir in alphabetical order
// (matching Python sorted(path.glob("*.json"))) and returns normalized traces.
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
		path := filepath.Join(dir, name)
		ts, err := loader.LoadTraceFile(path, services, requireClean)
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
