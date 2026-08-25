package corpus

// This file defines the optional DEE queue-ID sidecar consumed by S-Bridge.
// It is deliberately separate from events.bin so existing corpora and bridge
// modes remain byte-for-byte compatible.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	DEEQueueMagic      = uint32(0x44495144) // bytes "DQID" in little endian
	DEEQueueVersion    = uint32(1)
	DEEQueueHeaderSize = 16 // magic + version + event count
)

// DEEQueueWriter writes one uint32 queue ID for every corpus event. Starts and
// ends for the same span must carry the same ID.
type DEEQueueWriter struct {
	f        *os.File
	bw       *bufio.Writer
	expected uint64
	written  uint64
	buf      [4]byte
}

func CreateDEEQueueIDs(path string, eventCount uint64) (*DEEQueueWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var hdr [DEEQueueHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], DEEQueueMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], DEEQueueVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], eventCount)
	if _, err := bw.Write(hdr[:]); err != nil {
		f.Close()
		return nil, err
	}
	return &DEEQueueWriter{f: f, bw: bw, expected: eventCount}, nil
}

func (w *DEEQueueWriter) Write(id uint32) error {
	if w.written >= w.expected {
		return fmt.Errorf("DEE queue IDs: too many records (expected %d)", w.expected)
	}
	binary.LittleEndian.PutUint32(w.buf[:], id)
	if _, err := w.bw.Write(w.buf[:]); err != nil {
		return err
	}
	w.written++
	return nil
}

func (w *DEEQueueWriter) Count() uint64 { return w.written }

func (w *DEEQueueWriter) Close() error {
	if w.written != w.expected {
		w.bw.Flush()
		w.f.Close()
		return fmt.Errorf("DEE queue IDs: wrote %d records, expected %d", w.written, w.expected)
	}
	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// DEEQueueReader streams queue IDs in lockstep with events.bin.
type DEEQueueReader struct {
	f        *os.File
	br       *bufio.Reader
	expected uint64
	read     uint64
	buf      [4]byte
}

func OpenDEEQueueIDs(path string) (*DEEQueueReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var hdr [DEEQueueHeaderSize]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("read DEE queue-ID header: %w", err)
	}
	magic := binary.LittleEndian.Uint32(hdr[0:4])
	version := binary.LittleEndian.Uint32(hdr[4:8])
	if magic != DEEQueueMagic {
		f.Close()
		return nil, fmt.Errorf("bad DEE queue-ID magic 0x%x (want 0x%x)", magic, DEEQueueMagic)
	}
	if version != DEEQueueVersion {
		f.Close()
		return nil, fmt.Errorf("unsupported DEE queue-ID version %d (want %d)", version, DEEQueueVersion)
	}
	return &DEEQueueReader{
		f:        f,
		br:       br,
		expected: binary.LittleEndian.Uint64(hdr[8:16]),
	}, nil
}

func (r *DEEQueueReader) Expected() uint64 { return r.expected }
func (r *DEEQueueReader) Count() uint64    { return r.read }

func (r *DEEQueueReader) Next() (uint32, error) {
	if r.read == r.expected {
		return 0, io.EOF
	}
	if _, err := io.ReadFull(r.br, r.buf[:]); err != nil {
		return 0, fmt.Errorf("read DEE queue ID %d/%d: %w", r.read, r.expected, err)
	}
	r.read++
	return binary.LittleEndian.Uint32(r.buf[:]), nil
}

// ValidateEOF verifies that the declared record count was consumed and that
// no undeclared bytes follow it.
func (r *DEEQueueReader) ValidateEOF() error {
	if r.read != r.expected {
		return fmt.Errorf("DEE queue IDs: consumed %d records, expected %d", r.read, r.expected)
	}
	_, err := r.br.ReadByte()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("DEE queue IDs: trailing data after %d records", r.expected)
}

func (r *DEEQueueReader) Close() error { return r.f.Close() }
