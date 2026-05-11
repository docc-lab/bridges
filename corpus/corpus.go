// Package corpus defines the on-disk binary format for the preprocessed
// trace corpus produced by cmd/trace_prep and consumed by cmd/trace_sim's
// corpus mode.
//
// Two files per corpus:
//
//   events.bin  - 8-byte header + N x 40-byte event records, globally sorted
//                 by Python's sort_events tie-break (ts, kind, depth-rank,
//                 trace_id, span_id).
//   meta.bin    - small metadata: service intern table, trace load order,
//                 span count per trace.
//
// The event stream has been: cleanliness-filtered, CRISP-normalized
// (per Zhang et al., USENIX ATC '22, §5.2), and depth-tagged. The simulator
// streams events.bin sequentially and dispatches to a bridge.Handler — no
// further loading or sorting required.
package corpus

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Versioning + sentinels.
const (
	EventsMagic = uint32(0x42524447) // "BRDG"
	MetaMagic   = uint32(0x4252444D) // "BRDM"
	Version     = uint32(1)

	EventRecordSize = 40
	HeaderSize      = 8 // 4 bytes magic + 4 bytes version
)

// EventKind: 0 = start, 1 = end. Mirrors bridge.Kind so they can be cast.
const (
	KindStart uint8 = 0
	KindEnd   uint8 = 1
)

// Event is the in-memory form of one event record. Field order matches the
// on-disk layout exactly (40 bytes packed, little-endian).
type Event struct {
	TS        int64  // 8
	SpanID    uint64 // 8
	ParentID  uint64 // 8
	TraceID   uint64 // 8
	Depth     uint16 // 2
	ServiceID uint16 // 2
	Kind      uint8  // 1
	_         [3]uint8
}

// Sanity check on the field byte layout — kept narrow on purpose: the on-wire
// format is fixed at 40 bytes regardless of what unsafe.Sizeof reports for
// the in-memory struct (Go may add alignment padding).
const _wireSize = 8 + 8 + 8 + 8 + 2 + 2 + 1 + 3
var _ = [1]struct{}{}[EventRecordSize-_wireSize]

// EventsWriter writes an events.bin file with header + records.
type EventsWriter struct {
	f   *os.File
	bw  *bufio.Writer
	buf [EventRecordSize]byte
	n   uint64
}

func CreateEvents(path string) (*EventsWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], EventsMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], Version)
	if _, err := bw.Write(hdr[:]); err != nil {
		f.Close()
		return nil, err
	}
	return &EventsWriter{f: f, bw: bw}, nil
}

func (w *EventsWriter) Write(e Event) error {
	encodeEvent(e, w.buf[:])
	if _, err := w.bw.Write(w.buf[:]); err != nil {
		return err
	}
	w.n++
	return nil
}

func (w *EventsWriter) Count() uint64 { return w.n }

func (w *EventsWriter) Close() error {
	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// EventsReader streams events.bin record-by-record.
type EventsReader struct {
	f   *os.File
	br  *bufio.Reader
	buf [EventRecordSize]byte
}

func OpenEvents(path string) (*EventsReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	mg := binary.LittleEndian.Uint32(hdr[0:4])
	vr := binary.LittleEndian.Uint32(hdr[4:8])
	if mg != EventsMagic {
		f.Close()
		return nil, fmt.Errorf("bad events magic 0x%x (want 0x%x)", mg, EventsMagic)
	}
	if vr != Version {
		f.Close()
		return nil, fmt.Errorf("unsupported events version %d (want %d)", vr, Version)
	}
	return &EventsReader{f: f, br: br}, nil
}

// Next reads the next event. Returns io.EOF cleanly at end-of-stream.
func (r *EventsReader) Next() (Event, error) {
	if _, err := io.ReadFull(r.br, r.buf[:]); err != nil {
		return Event{}, err
	}
	return decodeEvent(r.buf[:]), nil
}

func (r *EventsReader) Close() error { return r.f.Close() }

func encodeEvent(e Event, buf []byte) {
	_ = buf[39] // bounds hint
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.TS))
	binary.LittleEndian.PutUint64(buf[8:16], e.SpanID)
	binary.LittleEndian.PutUint64(buf[16:24], e.ParentID)
	binary.LittleEndian.PutUint64(buf[24:32], e.TraceID)
	binary.LittleEndian.PutUint16(buf[32:34], e.Depth)
	binary.LittleEndian.PutUint16(buf[34:36], e.ServiceID)
	buf[36] = e.Kind
	buf[37] = 0
	buf[38] = 0
	buf[39] = 0
}

func decodeEvent(buf []byte) Event {
	_ = buf[39]
	return Event{
		TS:        int64(binary.LittleEndian.Uint64(buf[0:8])),
		SpanID:    binary.LittleEndian.Uint64(buf[8:16]),
		ParentID:  binary.LittleEndian.Uint64(buf[16:24]),
		TraceID:   binary.LittleEndian.Uint64(buf[24:32]),
		Depth:     binary.LittleEndian.Uint16(buf[32:34]),
		ServiceID: binary.LittleEndian.Uint16(buf[34:36]),
		Kind:      buf[36],
	}
}

// Meta is the corpus metadata: service intern table + per-trace order/sizes.
type Meta struct {
	// Services is the intern table: index = service_id, value = service name.
	Services []string

	// TraceOrder is the load order of trace IDs. Per-trace metric output is
	// emitted in this order, matching Python's run_traces output.
	TraceOrder []uint64

	// SpanCounts is parallel to TraceOrder: pre-CRISP-prep span count is
	// what Python reports as num_spans. After our prep this equals the count
	// of surviving spans (post-cleanliness, post-CRISP-drop).
	SpanCounts []uint32
}

// WriteMeta serializes Meta into a self-describing little-endian binary file.
func WriteMeta(path string, m *Meta) error {
	if len(m.TraceOrder) != len(m.SpanCounts) {
		return errors.New("meta: TraceOrder and SpanCounts length mismatch")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)

	// Header.
	if err := writeUint32(bw, MetaMagic); err != nil {
		return err
	}
	if err := writeUint32(bw, Version); err != nil {
		return err
	}

	// Services.
	if err := writeUint32(bw, uint32(len(m.Services))); err != nil {
		return err
	}
	for _, s := range m.Services {
		b := []byte(s)
		if err := writeUint32(bw, uint32(len(b))); err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
	}

	// Traces.
	if err := writeUint64(bw, uint64(len(m.TraceOrder))); err != nil {
		return err
	}
	for i := range m.TraceOrder {
		if err := writeUint64(bw, m.TraceOrder[i]); err != nil {
			return err
		}
		if err := writeUint32(bw, m.SpanCounts[i]); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadMeta loads Meta from a meta.bin file.
func ReadMeta(path string) (*Meta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)

	mg, err := readUint32(br)
	if err != nil {
		return nil, fmt.Errorf("read meta magic: %w", err)
	}
	if mg != MetaMagic {
		return nil, fmt.Errorf("bad meta magic 0x%x (want 0x%x)", mg, MetaMagic)
	}
	vr, err := readUint32(br)
	if err != nil {
		return nil, fmt.Errorf("read meta version: %w", err)
	}
	if vr != Version {
		return nil, fmt.Errorf("unsupported meta version %d", vr)
	}

	nServices, err := readUint32(br)
	if err != nil {
		return nil, err
	}
	services := make([]string, nServices)
	for i := uint32(0); i < nServices; i++ {
		nameLen, err := readUint32(br)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, nameLen)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		services[i] = string(buf)
	}

	nTraces, err := readUint64(br)
	if err != nil {
		return nil, err
	}
	traceOrder := make([]uint64, nTraces)
	spanCounts := make([]uint32, nTraces)
	for i := uint64(0); i < nTraces; i++ {
		tid, err := readUint64(br)
		if err != nil {
			return nil, err
		}
		sc, err := readUint32(br)
		if err != nil {
			return nil, err
		}
		traceOrder[i] = tid
		spanCounts[i] = sc
	}
	return &Meta{Services: services, TraceOrder: traceOrder, SpanCounts: spanCounts}, nil
}

func writeUint32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeUint64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

// Paths returns the conventional paths for events.bin and meta.bin within a
// corpus directory.
func Paths(dir string) (events, meta string) {
	return filepath.Join(dir, "events.bin"), filepath.Join(dir, "meta.bin")
}
