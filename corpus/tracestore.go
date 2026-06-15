package corpus

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// A trace store is the corpus re-grouped into self-contained per-trace event
// blocks, written in TRACE-COMPLETION order (the order each trace's last event
// appears in the global timestamp-sorted stream — i.e. the order the streaming
// harness calls finishTrace). Because bridge handler state is strictly
// per-trace, replaying one trace's events against a fresh handler reproduces
// its payloads bit-for-bit; emitting in completion order lets a consumer
// reproduce the global drop RNG sequence exactly with a cheap serial pass,
// while the expensive handler + reconstruction run per-trace in parallel.
//
// Stored per event: Kind, SpanID, ParentID, ServiceID, and TS (the start/end
// timestamp). Recon's onEvent ignores ts and derives depth from the handler,
// but TS is retained so the store is a complete per-trace record (S-Bridge /
// critical-path analysis need it). Event order is implicit in block order.
//
// File header (little-endian): Magic uint32 ("TSTR") + Version uint32.
// Block layout: TraceID uint64, NEvents uint32, then NEvents ×
// { Kind uint8, SpanID uint64, ParentID uint64, ServiceID uint16, TS int64 }.

// StoredEvent is one start/end event in a trace's block, in stream order.
type StoredEvent struct {
	Kind      uint8
	SpanID    uint64
	ParentID  uint64
	ServiceID uint16
	TS        int64 // start/end timestamp (store v2+)
}

const storedEventBytes = 1 + 8 + 8 + 2 + 8 // 27

// Trace-store file header. v2 added per-event TS; a v1 (headerless, 19-byte
// record) store is rejected on open so it can't be silently misread at the new
// record size.
const (
	traceStoreMagic    uint32 = 0x54535452 // "TSTR"
	traceStoreVersion  uint32 = 2
	traceStoreHdrBytes        = 8
)

// StoredTrace is one decoded per-trace block.
type StoredTrace struct {
	TraceID uint64
	Events  []StoredEvent
}

// TraceStoreWriter streams per-trace blocks to a file.
type TraceStoreWriter struct {
	f   *os.File
	w   *bufio.Writer
	buf []byte
	n   int
}

// NewTraceStoreWriter creates/truncates path.
func NewTraceStoreWriter(path string) (*TraceStoreWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &TraceStoreWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), buf: make([]byte, 0, 4096)}
	var h [traceStoreHdrBytes]byte
	binary.LittleEndian.PutUint32(h[0:], traceStoreMagic)
	binary.LittleEndian.PutUint32(h[4:], traceStoreVersion)
	if _, err := w.w.Write(h[:]); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// WriteTrace appends one trace's block.
func (w *TraceStoreWriter) WriteTrace(tid uint64, events []StoredEvent) error {
	need := 8 + 4 + len(events)*storedEventBytes
	if cap(w.buf) < need {
		w.buf = make([]byte, need)
	}
	b := w.buf[:need]
	binary.LittleEndian.PutUint64(b[0:], tid)
	binary.LittleEndian.PutUint32(b[8:], uint32(len(events)))
	off := 12
	for i := range events {
		e := &events[i]
		b[off] = e.Kind
		binary.LittleEndian.PutUint64(b[off+1:], e.SpanID)
		binary.LittleEndian.PutUint64(b[off+9:], e.ParentID)
		binary.LittleEndian.PutUint16(b[off+17:], e.ServiceID)
		binary.LittleEndian.PutUint64(b[off+19:], uint64(e.TS))
		off += storedEventBytes
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	w.n++
	return nil
}

// Count returns how many traces have been written.
func (w *TraceStoreWriter) Count() int { return w.n }

// Close flushes and closes.
func (w *TraceStoreWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// TraceStoreReader reads per-trace blocks sequentially (completion order).
type TraceStoreReader struct {
	f   *os.File
	r   *bufio.Reader
	hdr []byte
}

// OpenTraceStore opens a trace store for sequential reading.
func OpenTraceStore(path string) (*TraceStoreReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &TraceStoreReader{f: f, r: bufio.NewReaderSize(f, 1<<20), hdr: make([]byte, 12)}
	var h [traceStoreHdrBytes]byte
	if _, err := io.ReadFull(r.r, h[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("trace store header: %w", err)
	}
	if binary.LittleEndian.Uint32(h[0:]) != traceStoreMagic || binary.LittleEndian.Uint32(h[4:]) != traceStoreVersion {
		f.Close()
		return nil, fmt.Errorf("trace store %s: bad magic/version (need v%d — rebuild it)", path, traceStoreVersion)
	}
	return r, nil
}

// Next reads the next trace block. Returns io.EOF cleanly at end-of-stream.
func (r *TraceStoreReader) Next() (StoredTrace, error) {
	if _, err := io.ReadFull(r.r, r.hdr); err != nil {
		return StoredTrace{}, err // io.EOF at a clean block boundary
	}
	tid := binary.LittleEndian.Uint64(r.hdr[0:])
	n := binary.LittleEndian.Uint32(r.hdr[8:])
	body := make([]byte, int(n)*storedEventBytes)
	if _, err := io.ReadFull(r.r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return StoredTrace{}, fmt.Errorf("trace %016x: %w", tid, err)
	}
	events := make([]StoredEvent, n)
	off := 0
	for i := range events {
		events[i] = StoredEvent{
			Kind:      body[off],
			SpanID:    binary.LittleEndian.Uint64(body[off+1:]),
			ParentID:  binary.LittleEndian.Uint64(body[off+9:]),
			ServiceID: binary.LittleEndian.Uint16(body[off+17:]),
			TS:        int64(binary.LittleEndian.Uint64(body[off+19:])),
		}
		off += storedEventBytes
	}
	return StoredTrace{TraceID: tid, Events: events}, nil
}

// Close closes the underlying file.
func (r *TraceStoreReader) Close() error { return r.f.Close() }
