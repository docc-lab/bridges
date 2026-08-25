package corpus

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func TestDEEQueueIDsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instances.bin")
	w, err := CreateDEEQueueIDs(path, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint32{7, 0, 0xffffffff} {
		if err := w.Write(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenDEEQueueIDs(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Expected() != 3 {
		t.Fatalf("expected count = %d, want 3", r.Expected())
	}
	for i, want := range []uint32{7, 0, 0xffffffff} {
		got, err := r.Next()
		if err != nil || got != want {
			t.Fatalf("record %d = (%d, %v), want (%d, nil)", i, got, err, want)
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end error = %v, want EOF", err)
	}
	if err := r.ValidateEOF(); err != nil {
		t.Fatal(err)
	}
}

func TestDEEQueueIDsRejectShortWrite(t *testing.T) {
	w, err := CreateDEEQueueIDs(filepath.Join(t.TempDir(), "short.bin"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after a short write")
	}
}
