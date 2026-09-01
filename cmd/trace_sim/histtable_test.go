package main

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
)

func TestRebinHistogramBoundariesAndOverflow(t *testing.T) {
	h := histogramOutput{
		Count: 8,
		Bins: []histogramBin{
			{Bytes: 0, Count: 1},
			{Bytes: 15, Count: 1},
			{Bytes: 16, Count: 1},
			{Bytes: 23, Count: 1},
			{Bytes: 24, Count: 1},
			{Bytes: 131071, Count: 1},
			{Bytes: 131072, Count: 1},
			{Bytes: 200000, Count: 1},
		},
	}
	got, err := rebinHistogram(h)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 2 || got[1] != 2 || got[2] != 1 || got[len(got)-2] != 1 || got[len(got)-1] != 2 {
		t.Fatalf("unexpected rebinned counts: first=%d second=%d third=%d penultimate=%d overflow=%d",
			got[0], got[1], got[2], got[len(got)-2], got[len(got)-1])
	}
}

func TestRebinHistogramRejectsCountMismatch(t *testing.T) {
	_, err := rebinHistogram(histogramOutput{Count: 2, Bins: []histogramBin{{Bytes: 10, Count: 1}}})
	if err == nil || !strings.Contains(err.Error(), "declared count") {
		t.Fatalf("error = %v, want declared-count mismatch", err)
	}
}

func TestWriteHistogramMarkdown(t *testing.T) {
	inputs := []labeledHistogram{{label: "B", hist: histogramOutput{Count: 4}}}
	counts := make([]uint64, len(comparisonByteRanges))
	counts[0] = 1
	var out bytes.Buffer
	if err := writeHistogramMarkdown(&out, inputs, [][]uint64{counts}, 2); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "| 0-15 | 25.00% |") || !strings.Contains(out.String(), "| 131072+ | 0.00% |") {
		t.Fatalf("unexpected markdown:\n%s", out.String())
	}
}

func TestWriteHistogramCSVUsesNumericPercentages(t *testing.T) {
	inputs := []labeledHistogram{{label: "B", hist: histogramOutput{Count: 4}}}
	counts := make([]uint64, len(comparisonByteRanges))
	counts[0] = 1
	counts[len(counts)-1] = 3
	var out bytes.Buffer
	if err := writeHistogramCSV(&out, inputs, [][]uint64{counts}, 3); err != nil {
		t.Fatal(err)
	}
	r := csv.NewReader(&out)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if got := rows[0]; strings.Join(got, ",") != "range,min_bytes,max_bytes,B" {
		t.Fatalf("header = %v", got)
	}
	if got := rows[1]; strings.Join(got, ",") != "0-15,0,15,25.000" {
		t.Fatalf("first row = %v", got)
	}
	if got := rows[len(rows)-1]; strings.Join(got, ",") != "131072+,131072,,75.000" {
		t.Fatalf("overflow row = %v", got)
	}
}
