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
	prefixLen          int
	bloomFP            float64
	sampleCount        int   // corpus mode: if >0, simulate a RANDOM sample of this many traces (seeded)
	sampleSeed         int64 // seed for the random sample
	progressN          int   // corpus mode: print a PROGRESS line every N completed traces (0 = off)
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
	flag.IntVar(&c.progressN, "progress", 0, "Corpus mode: print a PROGRESS line to stderr every N completed traces (0 = silent)")
	flag.BoolVar(&c.requireClean, "require-clean", false, "Cleanliness filter: drop dirty traces; multi-root traces keep only the biggest root tree (JSON mode only)")
	flag.BoolVar(&c.emitDepth, "emit-depth", false, "Emit absolute depth: varint(depth) replaces varint(depthMod) in _br payloads, and interior non-checkpoint spans carry a _d attribute (see docs/depth_emission.md)")
	flag.BoolVar(&c.emitOC, "emit-oc", false, "S-bridge: emit an _oc attribute (window-relative ordinal chain) on interior non-checkpoint spans, to measure per-span ordinal-chain inflation")
	flag.BoolVar(&c.logDee, "log-dee", false, "S-bridge: log DEE pickup/queue events to stderr")
	flag.IntVar(&c.deeLogBytes, "dee-log-bytes", 10000, "Threshold for --log-dee")
	flag.IntVar(&c.prefixLen, "prefix-len", bridge.DefaultPCRPrefixLen, "PCR mode: truncated checkpoint-root span ID length in bytes (1-8)")
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
	if c.outputPath == "" {
		fmt.Fprintln(os.Stderr, "error: -o/--output required")
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
		h.EmitDepth = c.emitDepth
		h.EmitOC = c.emitOC
		return h
	}
	fmt.Fprintf(os.Stderr, "unknown mode %q\n", c.mode)
	os.Exit(2)
	return nil
}

func main() {
	c := parseFlags()

	var metrics []TraceMetrics
	if c.corpusDir != "" {
		metrics = runFromCorpus(c)
	} else {
		metrics = runFromJSON(c)
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
	h := makeHandler(c,
		func(id uint16) string {
			if int(id) < len(meta.Services) {
				return meta.Services[id]
			}
			return ""
		},
		// JSON path-style source-file resolution isn't available from corpus
		// alone; fall back to "<traceIDhex>.json". DEE log uses this only for
		// human-readable annotation.
		func(tid uint64) string { return fmt.Sprintf("%016x.json", tid) })
	metrics := runInterleavedFromCorpus(er, meta, h, c)
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
