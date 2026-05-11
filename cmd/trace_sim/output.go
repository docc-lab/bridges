package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
)

// writeBagsizeJSON emits a Python-compatible json.dump(out, f, indent=2)
// representation of the bagsize output. Float formatting matches Python's
// repr(): whole-number floats get a trailing ".0", others use the shortest
// roundtrip representation.
func writeBagsizeJSON(path string, checkpointDistance int, m []TraceMetrics) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	w := bw
	io.WriteString(w, "{\n")
	fmt.Fprintf(w, "  \"checkpoint_distance\": %d,\n", checkpointDistance)
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
	writeIntArr(w, "max_baggage_call", m, func(t TraceMetrics) int { return t.BaggageMax }, false)

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
