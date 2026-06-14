package recon

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// DumpedTrace is a single trace's fully-decoded reconstruction input, captured
// after one corpus walk so the (expensive) walk never has to be repeated to
// iterate on reconstruction. It holds everything ReconstructCGPRB/ScorePCR/
// ScoreCGPTopology need — survivors, ground truth, the dropped set, and the
// config — so a replay tool (cmd/recon_one) can rerun a single slow trace in
// microseconds. All fields are plain exported types, so gob handles it.
type DumpedTrace struct {
	TID       uint64
	Survivors []Span
	Truth     []TruthSpan
	Dropped   []uint64 // dropped span IDs (gob-friendly form of the dropped set)
	Cfg       Config
}

// DroppedSet rebuilds the map form expected by the scorers.
func (d *DumpedTrace) DroppedSet() map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(d.Dropped))
	for _, id := range d.Dropped {
		m[id] = struct{}{}
	}
	return m
}

// DumpWriter streams DumpedTrace records to a single file through ONE gob
// encoder, so the gob type definitions are emitted exactly once (a fresh
// encoder per record would re-emit them and the reader would reject the
// stream with "duplicate type received"). Write is safe for concurrent
// callers — the reconstruction workers dump from multiple goroutines.
type DumpWriter struct {
	mu  sync.Mutex
	f   *os.File
	enc *gob.Encoder
}

// NewDumpWriter creates/truncates path and returns a writer over it.
func NewDumpWriter(path string) (*DumpWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &DumpWriter{f: f, enc: gob.NewEncoder(f)}, nil
}

// Write gob-appends one trace.
func (w *DumpWriter) Write(dt DumpedTrace) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(&dt); err != nil {
		return fmt.Errorf("encode dumped trace: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying file.
func (w *DumpWriter) Close() error { return w.f.Close() }

// LoadDumpedTraces reads every gob-encoded DumpedTrace from path in order.
func LoadDumpedTraces(path string) ([]DumpedTrace, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var out []DumpedTrace
	for {
		var dt DumpedTrace
		err := dec.Decode(&dt)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out, err
		}
		out = append(out, dt)
	}
	return out, nil
}
