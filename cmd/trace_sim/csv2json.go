package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// runCSV2JSON converts a --stream-metrics CSV (one row per finalized trace) into
// the bagsize JSON, reusing writeBagsizeJSON so the output format is byte-for-byte
// the same as a direct -o run. Rows arrive in finalization order rather than
// corpus order, but bagsize aggregation is order-independent, so the numbers
// match a direct run exactly.
//
//	trace_sim csv2json <in.csv> <out.json>
func runCSV2JSON(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: trace_sim csv2json <in.csv> <out.json>")
		os.Exit(2)
	}
	in, out := args[0], args[1]
	f, err := os.Open(in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", in, err)
		os.Exit(1)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	cpd := 0
	emitDepth, emitOC := false, false
	var metrics []TraceMetrics
	lineNo := 0
	for sc.Scan() {
		line := sc.Text()
		lineNo++
		switch {
		case strings.HasPrefix(line, "#"): // header: #cpd=4 emit_depth=true emit_oc=true
			for _, kv := range strings.Fields(strings.TrimPrefix(line, "#")) {
				p := strings.SplitN(kv, "=", 2)
				if len(p) != 2 {
					continue
				}
				switch p[0] {
				case "cpd":
					cpd, _ = strconv.Atoi(p[1])
				case "emit_depth":
					emitDepth = p[1] == "true"
				case "emit_oc":
					emitOC = p[1] == "true"
				}
			}
			continue
		case strings.HasPrefix(line, "tid,"): // column header
			continue
		case line == "":
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) != 12 {
			fmt.Fprintf(os.Stderr, "line %d: expected 12 fields, got %d\n", lineNo, len(fields))
			os.Exit(1)
		}
		// tid,num_spans,num_ckpt_spans,ckpt_sum,ckpt_max,n_bag,bag_sum,bag_max,n_depth,depth_sum,n_oc,oc_sum
		ai := func(i int) int { v, _ := strconv.Atoi(fields[i]); return v }
		metrics = append(metrics, TraceMetrics{
			NumSpans:           ai(1),
			NumCheckpointSpans: ai(2),
			CheckpointSum:      ai(3),
			CheckpointMax:      ai(4),
			NumBaggageCalls:    ai(5),
			BaggageSum:         ai(6),
			BaggageMax:         ai(7),
			NumDepthSpans:      ai(8),
			DepthSum:           ai(9),
			NumOcSpans:         ai(10),
			OcSum:              ai(11),
		})
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", in, err)
		os.Exit(1)
	}
	if err := writeBagsizeJSON(out, cpd, metrics, emitDepth, emitOC); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", out, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "csv2json: %d traces -> %s (cpd=%d emit_depth=%t emit_oc=%t)\n",
		len(metrics), out, cpd, emitDepth, emitOC)
}
