package corpus

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// A trace ARCHIVE is the minimal lossless per-trace representation: just the
// fields the classic corpus can't recompute. It is meant to replace the raw
// JSON as the source of truth — from it you can regenerate the classic
// events.bin corpus (depth is recomputed from the ParentID topology) and
// re-apply filtering, without ever touching the original traces.
//
// Stored per SPAN (not per event): SpanID, ParentID, ServiceID, and the start
// and end timestamps. Depth is omitted (recomputable from topology); Kind is
// omitted (a span is its start/end pair); operation names / tags are not kept
// (the corpus doesn't use them). 34 bytes/span.
//
// File header (little-endian): Magic uint32 ("TARC") + Version uint32.
// Block: TraceID uint64, NSpans uint32, Flags uint8, then NSpans ×
//   { SpanID uint64, ParentID uint64, ServiceID uint16, StartTS int64, EndTS int64 }.
// Flags are the loader's lenient-mode repair markers (loader.FlagXxx): they let
// consumers include or exclude repaired traces on demand.
// Service-id -> name and trace order live in the sidecar meta.bin (corpus.Meta).

// ArchiveSpan is one span's minimal record.
type ArchiveSpan struct {
	SpanID    uint64
	ParentID  uint64
	ServiceID uint16
	StartTS   int64
	EndTS     int64
}

const archiveSpanBytes = 8 + 8 + 2 + 8 + 8 // 34

const (
	archiveMagic    uint32 = 0x54415243 // "TARC"
	archiveVersion  uint32 = 2          // v2 added the per-trace Flags byte
	archiveHdrBytes        = 8
	archiveBlkHdr          = 13 // TraceID(8) + NSpans(4) + Flags(1)
)

// ArchiveTrace is one decoded per-trace block.
type ArchiveTrace struct {
	TraceID uint64
	Flags   uint8 // loader.FlagXxx repair markers
	Spans   []ArchiveSpan
}

// ArchiveWriter streams per-trace span blocks to a file.
type ArchiveWriter struct {
	f   *os.File
	w   *bufio.Writer
	buf []byte
	n   int
}

// NewArchiveWriter creates/truncates path and writes the header.
func NewArchiveWriter(path string) (*ArchiveWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &ArchiveWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), buf: make([]byte, 0, 4096)}
	var h [archiveHdrBytes]byte
	binary.LittleEndian.PutUint32(h[0:], archiveMagic)
	binary.LittleEndian.PutUint32(h[4:], archiveVersion)
	if _, err := w.w.Write(h[:]); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// WriteTrace appends one trace's span block.
func (w *ArchiveWriter) WriteTrace(tid uint64, flags uint8, spans []ArchiveSpan) error {
	need := archiveBlkHdr + len(spans)*archiveSpanBytes
	if cap(w.buf) < need {
		w.buf = make([]byte, need)
	}
	b := w.buf[:need]
	binary.LittleEndian.PutUint64(b[0:], tid)
	binary.LittleEndian.PutUint32(b[8:], uint32(len(spans)))
	b[12] = flags
	off := archiveBlkHdr
	for i := range spans {
		s := &spans[i]
		binary.LittleEndian.PutUint64(b[off:], s.SpanID)
		binary.LittleEndian.PutUint64(b[off+8:], s.ParentID)
		binary.LittleEndian.PutUint16(b[off+16:], s.ServiceID)
		binary.LittleEndian.PutUint64(b[off+18:], uint64(s.StartTS))
		binary.LittleEndian.PutUint64(b[off+26:], uint64(s.EndTS))
		off += archiveSpanBytes
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	w.n++
	return nil
}

// Count returns how many traces have been written.
func (w *ArchiveWriter) Count() int { return w.n }

// Close flushes and closes.
func (w *ArchiveWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// ArchiveReader reads per-trace span blocks sequentially.
type ArchiveReader struct {
	f   *os.File
	r   *bufio.Reader
	hdr []byte
}

// OpenArchive opens an archive for sequential reading, verifying the header.
func OpenArchive(path string) (*ArchiveReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &ArchiveReader{f: f, r: bufio.NewReaderSize(f, 1<<20), hdr: make([]byte, archiveBlkHdr)}
	var h [archiveHdrBytes]byte
	if _, err := io.ReadFull(r.r, h[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("archive header: %w", err)
	}
	if binary.LittleEndian.Uint32(h[0:]) != archiveMagic || binary.LittleEndian.Uint32(h[4:]) != archiveVersion {
		f.Close()
		return nil, fmt.Errorf("archive %s: bad magic/version (need v%d)", path, archiveVersion)
	}
	return r, nil
}

// Next reads the next trace block; io.EOF cleanly at end-of-stream.
func (r *ArchiveReader) Next() (ArchiveTrace, error) {
	if _, err := io.ReadFull(r.r, r.hdr); err != nil {
		return ArchiveTrace{}, err
	}
	tid := binary.LittleEndian.Uint64(r.hdr[0:])
	n := binary.LittleEndian.Uint32(r.hdr[8:])
	flags := r.hdr[12]
	body := make([]byte, int(n)*archiveSpanBytes)
	if _, err := io.ReadFull(r.r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return ArchiveTrace{}, fmt.Errorf("trace %016x: %w", tid, err)
	}
	spans := make([]ArchiveSpan, n)
	off := 0
	for i := range spans {
		spans[i] = ArchiveSpan{
			SpanID:    binary.LittleEndian.Uint64(body[off:]),
			ParentID:  binary.LittleEndian.Uint64(body[off+8:]),
			ServiceID: binary.LittleEndian.Uint16(body[off+16:]),
			StartTS:   int64(binary.LittleEndian.Uint64(body[off+18:])),
			EndTS:     int64(binary.LittleEndian.Uint64(body[off+26:])),
		}
		off += archiveSpanBytes
	}
	return ArchiveTrace{TraceID: tid, Flags: flags, Spans: spans}, nil
}

// Close closes the underlying file.
func (r *ArchiveReader) Close() error { return r.f.Close() }
