package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"bridges/bridge"
)

// writeBagsizeJSON emits a Python-compatible json.dump(out, f, indent=2)
// representation of the bagsize output. Float formatting matches Python's
// repr(): whole-number floats get a trailing ".0", others use the shortest
// roundtrip representation.
func writeBagsizeJSON(path string, checkpointDistance int, m []TraceMetrics, emitDepth, emitOC bool, policies ...*bridge.CheckpointRange) error {
	c := config{checkpointDistance: checkpointDistance, emitDepth: emitDepth, emitOC: emitOC}
	if len(policies) > 0 {
		c.checkpointPolicy = policies[0]
	}
	return writeBagsizeJSONWithConfig(path, c, m)
}

func writeBagsizeJSONWithConfig(path string, c config, m []TraceMetrics) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	w := bw
	io.WriteString(w, "{\n")
	fmt.Fprintf(w, "  \"checkpoint_distance\": %d,\n", c.checkpointDistance)
	if c.checkpointPolicy != nil {
		raw, err := json.Marshal(c.checkpointPolicy)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  \"checkpoint_randomization\": %s,\n", raw)
	}
	if c.reverse != nil {
		fmt.Fprintf(w, "  \"reverse_config\": %s,\n", reverseConfigJSON(c.reverse))
		raw, _ := json.Marshal(reverseAccounting)
		fmt.Fprintf(w, "  \"reverse_accounting\": %s,\n", raw)
	}
	fmt.Fprintf(w, "  \"num_traces\": %d,\n", len(m))

	writeIntArr(w, "num_spans", m, func(t TraceMetrics) int { return t.NumSpans }, true)
	writeIntArr(w, "num_checkpoint_spans", m, func(t TraceMetrics) int { return t.NumCheckpointSpans }, true)
	writeFloatArr(w, "amortized_by_total", m, func(t TraceMetrics) float64 {
		if t.NumSpans == 0 {
			return 0.0
		}
		return float64(t.CheckpointSum) / float64(t.NumSpans)
	}, true)
	writeFloatArr(w, "amortized_by_checkpoint", m, func(t TraceMetrics) float64 {
		if t.NumCheckpointSpans == 0 {
			return 0.0
		}
		return float64(t.CheckpointSum) / float64(t.NumCheckpointSpans)
	}, true)
	writeIntArr(w, "max_checkpoint_payload", m, func(t TraceMetrics) int { return t.CheckpointMax }, true)
	writeIntArr(w, "num_baggage_calls", m, func(t TraceMetrics) int { return t.NumBaggageCalls }, true)
	writeFloatArr(w, "avg_baggage_call", m, func(t TraceMetrics) float64 {
		if t.NumBaggageCalls == 0 {
			return 0.0
		}
		return float64(t.BaggageSum) / float64(t.NumBaggageCalls)
	}, true)
	writeIntArr(w, "max_baggage_call", m, func(t TraceMetrics) int { return t.BaggageMax }, c.emitDepth || c.emitOC || c.reverse != nil)
	if c.emitDepth {
		writeIntArr(w, "num_depth_spans", m, func(t TraceMetrics) int { return t.NumDepthSpans }, true)
		writeIntArr(w, "depth_overhead_sum", m, func(t TraceMetrics) int { return t.DepthSum }, c.emitOC || c.reverse != nil)
	}
	if c.emitOC {
		writeIntArr(w, "num_oc_spans", m, func(t TraceMetrics) int { return t.NumOcSpans }, true)
		writeIntArr(w, "oc_overhead_sum", m, func(t TraceMetrics) int { return t.OcSum }, c.reverse != nil)
	}
	if c.reverse != nil {
		for _, col := range reverseMetricColumns {
			writeIntArr(w, col.name, m, func(t TraceMetrics) int {
				if t.Reverse == nil {
					return 0
				}
				return *col.ptr(t.Reverse)
			}, true)
		}
		writeFloatArr(w, "avg_reverse_raw_baggage", m, func(t TraceMetrics) float64 {
			if t.Reverse == nil || t.Reverse.NumReverseReturnEdges == 0 {
				return 0
			}
			return float64(t.Reverse.ReverseRawSum) / float64(t.Reverse.NumReverseReturnEdges)
		}, true)
		writeFloatArr(w, "avg_reverse_encoded_baggage", m, func(t TraceMetrics) float64 {
			if t.Reverse == nil || t.Reverse.NumReverseReturnEdges == 0 {
				return 0
			}
			return float64(t.Reverse.ReverseEncodedSum) / float64(t.Reverse.NumReverseReturnEdges)
		}, true)
		writeFloatArr(w, "amortized_combined_checkpoint_payload_by_total", m, func(t TraceMetrics) float64 {
			if t.Reverse == nil || t.NumSpans == 0 {
				return 0
			}
			return float64(t.Reverse.CombinedCheckpointSum) / float64(t.NumSpans)
		}, true)
		writeFloatArr(w, "amortized_combined_checkpoint_payload_by_checkpoint", m, func(t TraceMetrics) float64 {
			if t.Reverse == nil || t.NumCheckpointSpans == 0 {
				return 0
			}
			return float64(t.Reverse.CombinedCheckpointSum) / float64(t.NumCheckpointSpans)
		}, false)
	}

	io.WriteString(w, "}")
	return nil
}

func writeIntArr(w io.Writer, key string, m []TraceMetrics, get func(TraceMetrics) int, hasNext bool) {
	fmt.Fprintf(w, "  %q: ", key)
	if len(m) == 0 {
		io.WriteString(w, "[]")
	} else {
		io.WriteString(w, "[\n")
		for i, t := range m {
			io.WriteString(w, "    ")
			io.WriteString(w, strconv.Itoa(get(t)))
			if i < len(m)-1 {
				io.WriteString(w, ",")
			}
			io.WriteString(w, "\n")
		}
		io.WriteString(w, "  ]")
	}
	if hasNext {
		io.WriteString(w, ",")
	}
	io.WriteString(w, "\n")
}

func writeFloatArr(w io.Writer, key string, m []TraceMetrics, get func(TraceMetrics) float64, hasNext bool) {
	fmt.Fprintf(w, "  %q: ", key)
	if len(m) == 0 {
		io.WriteString(w, "[]")
	} else {
		io.WriteString(w, "[\n")
		for i, t := range m {
			io.WriteString(w, "    ")
			io.WriteString(w, formatPythonFloat(get(t)))
			if i < len(m)-1 {
				io.WriteString(w, ",")
			}
			io.WriteString(w, "\n")
		}
		io.WriteString(w, "  ]")
	}
	if hasNext {
		io.WriteString(w, ",")
	}
	io.WriteString(w, "\n")
}

// formatPythonFloat returns a string equivalent to Python's repr(f) for
// finite f. Whole-number floats with no exponent get a trailing ".0".
func formatPythonFloat(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' || c == 'e' || c == 'E' {
			return s
		}
	}
	return s + ".0"
}
