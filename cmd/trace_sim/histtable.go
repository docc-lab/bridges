package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type byteRange struct {
	label string
	min   int
	max   int // -1 means unbounded
}

var comparisonByteRanges = []byteRange{
	{"0-15", 0, 15},
	{"16-23", 16, 23},
	{"24-31", 24, 31},
	{"32-47", 32, 47},
	{"48-63", 48, 63},
	{"64-95", 64, 95},
	{"96-127", 96, 127},
	{"128-191", 128, 191},
	{"192-255", 192, 255},
	{"256-383", 256, 383},
	{"384-511", 384, 511},
	{"512-767", 512, 767},
	{"768-1023", 768, 1023},
	{"1024-1535", 1024, 1535},
	{"1536-2047", 1536, 2047},
	{"2048-3071", 2048, 3071},
	{"3072-4095", 3072, 4095},
	{"4096-6143", 4096, 6143},
	{"6144-8191", 6144, 8191},
	{"8192-12287", 8192, 12287},
	{"12288-16383", 12288, 16383},
	{"16384-24575", 16384, 24575},
	{"24576-32767", 24576, 32767},
	{"32768-49151", 32768, 49151},
	{"49152-65535", 49152, 65535},
	{"65536-98303", 65536, 98303},
	{"98304-131071", 98304, 131071},
	{"131072+", 131072, -1},
}

type labeledHistogram struct {
	label string
	hist  histogramOutput
}

func rebinHistogram(h histogramOutput) ([]uint64, error) {
	counts := make([]uint64, len(comparisonByteRanges))
	var total uint64
	for _, bin := range h.Bins {
		if bin.Bytes < 0 {
			return nil, fmt.Errorf("negative histogram size %d", bin.Bytes)
		}
		found := false
		for i, r := range comparisonByteRanges {
			if bin.Bytes >= r.min && (r.max < 0 || bin.Bytes <= r.max) {
				counts[i] += bin.Count
				total += bin.Count
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("histogram size %d did not match a byte range", bin.Bytes)
		}
	}
	if total != h.Count {
		return nil, fmt.Errorf("histogram bins total %d, declared count %d", total, h.Count)
	}
	return counts, nil
}

func loadLabeledHistogram(spec, metric string) (labeledHistogram, error) {
	label, path, ok := strings.Cut(spec, "=")
	if !ok || label == "" || path == "" {
		return labeledHistogram{}, fmt.Errorf("input %q must be LABEL=PATH", spec)
	}
	f, err := os.Open(path)
	if err != nil {
		return labeledHistogram{}, err
	}
	defer f.Close()
	var in sizeHistogramFile
	if err := json.NewDecoder(f).Decode(&in); err != nil {
		return labeledHistogram{}, err
	}
	var h histogramOutput
	switch metric {
	case "baggage":
		h = in.BaggageCallBytes
	case "payload":
		h = in.BridgePayloadBytes
	default:
		return labeledHistogram{}, fmt.Errorf("unknown metric %q (want baggage or payload)", metric)
	}
	return labeledHistogram{label: label, hist: h}, nil
}

func writeHistogramCSV(w io.Writer, inputs []labeledHistogram, rebinned [][]uint64, precision int) error {
	cw := csv.NewWriter(w)
	header := []string{"range", "min_bytes", "max_bytes"}
	for _, in := range inputs {
		header = append(header, in.label)
	}
	if err := cw.Write(header); err != nil {
		return err
	}
	for i, r := range comparisonByteRanges {
		max := ""
		if r.max >= 0 {
			max = strconv.Itoa(r.max)
		}
		row := []string{r.label, strconv.Itoa(r.min), max}
		for j, in := range inputs {
			pct := 0.0
			if in.hist.Count > 0 {
				pct = 100 * float64(rebinned[j][i]) / float64(in.hist.Count)
			}
			row = append(row, strconv.FormatFloat(pct, 'f', precision, 64))
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func writeHistogramMarkdown(w io.Writer, inputs []labeledHistogram, rebinned [][]uint64, precision int) error {
	if _, err := fmt.Fprint(w, "| Bytes |"); err != nil {
		return err
	}
	for _, in := range inputs {
		fmt.Fprintf(w, " %s |", in.label)
	}
	fmt.Fprint(w, "\n|---:|")
	for range inputs {
		fmt.Fprint(w, "---:|")
	}
	fmt.Fprintln(w)
	for i, r := range comparisonByteRanges {
		fmt.Fprintf(w, "| %s |", r.label)
		for j, in := range inputs {
			pct := 0.0
			if in.hist.Count > 0 {
				pct = 100 * float64(rebinned[j][i]) / float64(in.hist.Count)
			}
			fmt.Fprintf(w, " %.*f%% |", precision, pct)
		}
		fmt.Fprintln(w)
	}
	return nil
}

func runHistogramTable(args []string) {
	fs := flag.NewFlagSet("histtable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	metric := fs.String("metric", "baggage", "Histogram metric: baggage or payload")
	format := fs.String("format", "csv", "Output format: csv or markdown")
	output := fs.String("output", "", "Output path (default stdout)")
	precision := fs.Int("precision", 9, "Decimal places for percentages")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: trace_sim histtable [flags] LABEL=hist.json [LABEL=hist.json ...]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(os.Stderr, "histtable: %v\n", err)
			os.Exit(2)
		}
		return
	}
	if fs.NArg() == 0 || *precision < 0 {
		fs.Usage()
		os.Exit(2)
	}
	if *format != "csv" && *format != "markdown" && *format != "md" {
		fmt.Fprintf(os.Stderr, "histtable: unknown format %q (want csv or markdown)\n", *format)
		os.Exit(2)
	}

	inputs := make([]labeledHistogram, 0, fs.NArg())
	rebinned := make([][]uint64, 0, fs.NArg())
	seen := make(map[string]struct{})
	for _, spec := range fs.Args() {
		in, err := loadLabeledHistogram(spec, *metric)
		if err != nil {
			fmt.Fprintf(os.Stderr, "histtable: %v\n", err)
			os.Exit(1)
		}
		if _, exists := seen[in.label]; exists {
			fmt.Fprintf(os.Stderr, "histtable: duplicate label %q\n", in.label)
			os.Exit(1)
		}
		seen[in.label] = struct{}{}
		counts, err := rebinHistogram(in.hist)
		if err != nil {
			fmt.Fprintf(os.Stderr, "histtable %s: %v\n", in.label, err)
			os.Exit(1)
		}
		inputs = append(inputs, in)
		rebinned = append(rebinned, counts)
	}

	w := io.Writer(os.Stdout)
	var f *os.File
	if *output != "" {
		var err error
		f, err = os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "histtable: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		w = f
	}
	var err error
	switch *format {
	case "csv":
		err = writeHistogramCSV(w, inputs, rebinned, *precision)
	case "markdown", "md":
		err = writeHistogramMarkdown(w, inputs, rebinned, *precision)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "histtable: %v\n", err)
		os.Exit(1)
	}
}
