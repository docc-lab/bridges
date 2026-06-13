package main

import (
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"bridges/recon"
	"bridges/verify"
)

// Reconstruction artifact emission (--dump-recon) and in-process
// verification (--verify). Both feed the independent checker in package
// verify: --verify runs it inline after every reconstruction (cheap,
// always-on guard); --dump-recon serializes one JSONL line per trace so
// the standalone recon_verify binary can audit the artifact cold (the
// headline mode: the checker never sees the engine).

type dumpSpan struct {
	ID     string `json:"id"`
	Parent string `json:"parent"`
	Depth  int    `json:"depth"`
	BR     string `json:"br,omitempty"`
}

type dumpBridge struct {
	Orphan    string `json:"o"`
	Anchor    string `json:"a"`
	Synthetic int    `json:"syn"`
	Via       string `json:"via,omitempty"`
	Forced    bool   `json:"forced,omitempty"`
}

type dumpTrace struct {
	TID        string       `json:"trace"`
	Survivors  []dumpSpan   `json:"survivors"`
	Bridges    []dumpBridge `json:"bridges"`
	Unanchored []string     `json:"unanchored,omitempty"`
}

type reconDumper struct {
	mu sync.Mutex
	f  *os.File
	gz *gzip.Writer
	je *json.Encoder
}

func newReconDumper(path string) (*reconDumper, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	gz := gzip.NewWriter(f)
	return &reconDumper{f: f, gz: gz, je: json.NewEncoder(gz)}, nil
}

func (d *reconDumper) emit(t dumpTrace) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.je.Encode(t); err != nil {
		fmt.Fprintf(os.Stderr, "dump-recon: %v\n", err)
		os.Exit(1)
	}
}

func (d *reconDumper) close() {
	d.gz.Close()
	d.f.Close()
}

// buildArtifact assembles the per-trace artifact from the harness's
// in-memory state. brOf maps surviving span ID -> raw payload bytes.
func buildArtifact(tid uint64, survivors []recon.Span, brOf map[uint64][]byte, res recon.Result) dumpTrace {
	h := func(v uint64) string { return fmt.Sprintf("%x", v) }
	dt := dumpTrace{TID: h(tid)}
	for i := range survivors {
		s := &survivors[i]
		ds := dumpSpan{ID: h(s.SpanID), Parent: h(s.ParentID), Depth: s.Depth}
		if br := brOf[s.SpanID]; br != nil {
			ds.BR = hex.EncodeToString(br)
		}
		dt.Survivors = append(dt.Survivors, ds)
	}
	for _, b := range res.Bridges {
		dt.Bridges = append(dt.Bridges, dumpBridge{
			Orphan: h(b.OrphanID), Anchor: h(b.AnchorID),
			Synthetic: b.Synthetic, Via: h(b.ViaCarrier), Forced: b.Forced,
		})
	}
	for _, u := range res.Unanchored {
		dt.Unanchored = append(dt.Unanchored, h(u))
	}
	return dt
}

// toVerifyTrace converts the same state for the in-process checker.
func toVerifyTrace(tid uint64, survivors []recon.Span, brOf map[uint64][]byte, res recon.Result) verify.Trace {
	t := verify.Trace{TID: tid}
	for i := range survivors {
		s := &survivors[i]
		t.Survivors = append(t.Survivors, verify.Span{ID: s.SpanID, Parent: s.ParentID, Depth: s.Depth, BR: brOf[s.SpanID]})
	}
	for _, b := range res.Bridges {
		t.Bridges = append(t.Bridges, verify.Bridge{Orphan: b.OrphanID, Anchor: b.AnchorID, Synthetic: b.Synthetic, ViaCarrier: b.ViaCarrier, Forced: b.Forced})
	}
	t.Unanchored = append(t.Unanchored, res.Unanchored...)
	return t
}
