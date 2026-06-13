// Command recon_verify audits serialized reconstruction artifacts
// (produced by trace_recon --dump-recon) with the independent checker in
// package verify. It never imports the reconstruction engine: structural
// validity and per-bridge payload evidence are re-derived from the
// artifact's raw bytes alone. Exit status 1 if any violation is found.
//
//	recon_verify --cpd 6 --bloom-fp 1e-4 --prefix-len 4 dump.jsonl.gz
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"bridges/bloom"
	"bridges/verify"
)

type artSpan struct {
	ID     string `json:"id"`
	Parent string `json:"parent"`
	Depth  int    `json:"depth"`
	BR     string `json:"br,omitempty"`
}

type artBridge struct {
	Orphan    string `json:"o"`
	Anchor    string `json:"a"`
	Synthetic int    `json:"syn"`
	Via       string `json:"via,omitempty"`
	Forced    bool   `json:"forced,omitempty"`
}

type artTrace struct {
	TID        string      `json:"trace"`
	Survivors  []artSpan   `json:"survivors"`
	Bridges    []artBridge `json:"bridges"`
	Unanchored []string    `json:"unanchored,omitempty"`
}

func id(s string) uint64 {
	v, _ := strconv.ParseUint(s, 16, 64)
	return v
}

func main() {
	cpd := flag.Int("cpd", 0, "checkpoint distance (derives bloom geometry)")
	fp := flag.Float64("bloom-fp", 1e-4, "bloom false-positive target")
	prefixLen := flag.Int("prefix-len", 4, "checkpoint-root prefix bytes")
	flag.Parse()
	if *cpd == 0 || flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: recon_verify --cpd N [--bloom-fp F] [--prefix-len K] dump.jsonl[.gz]")
		os.Exit(2)
	}
	// Bloom geometry restated from the deployment spec: PCRB blooms are
	// sized for the cpd-1 window interior (checkpoints do not self-add).
	cap := *cpd - 1
	if *cpd <= 2 {
		cap = 1
	}
	m, k := bloom.EstimateParameters(cap, *fp)
	cfg := verify.Config{PrefixLen: *prefixLen, BloomM: m, BloomK: k, PCRBTypeByte: 5, CPD: *cpd}

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer f.Close()
	var r io.Reader = f
	if gz, err := gzip.NewReader(f); err == nil {
		r = gz
	} else {
		f.Seek(0, 0)
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<28)

	traces, violations := 0, 0
	for sc.Scan() {
		var at artTrace
		if err := json.Unmarshal(sc.Bytes(), &at); err != nil {
			fmt.Fprintf(os.Stderr, "line %d: %v\n", traces+1, err)
			os.Exit(2)
		}
		t := verify.Trace{TID: id(at.TID)}
		for _, s := range at.Survivors {
			var br []byte
			if s.BR != "" {
				br, _ = hex.DecodeString(s.BR)
			}
			t.Survivors = append(t.Survivors, verify.Span{ID: id(s.ID), Parent: id(s.Parent), Depth: s.Depth, BR: br})
		}
		for _, b := range at.Bridges {
			t.Bridges = append(t.Bridges, verify.Bridge{Orphan: id(b.Orphan), Anchor: id(b.Anchor), Synthetic: b.Synthetic, ViaCarrier: id(b.Via), Forced: b.Forced})
		}
		for _, u := range at.Unanchored {
			t.Unanchored = append(t.Unanchored, id(u))
		}
		traces++
		for _, v := range verify.Check(t, cfg) {
			violations++
			fmt.Println(v)
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "recon_verify: %d traces, %d violations\n", traces, violations)
	if violations > 0 {
		os.Exit(1)
	}
}
