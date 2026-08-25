package bridge

import (
	"bytes"
	"testing"
)

func TestSBridgeDEEQueueCanBeKeyedBySimulatedInstance(t *testing.T) {
	h := NewSBridgeHandler(8, nil)
	h.UseDEEQueueID = true
	a := &Event{ServiceID: 4, DEEQueueID: 100}
	b := &Event{ServiceID: 4, DEEQueueID: 101}
	if h.deeQueueKey(a) == h.deeQueueKey(b) {
		t.Fatal("distinct instances of one service resolved to the same DEE queue")
	}
	h.enqueueDEE(h.deeQueueKey(a), a.ServiceID, []byte{1, 2, 3}, 1)
	if got := h.drainDEE(h.deeQueueKey(b), b.ServiceID, 2); got != nil {
		t.Fatalf("instance B drained instance A's DEE: %v", got)
	}
	if got := h.drainDEE(h.deeQueueKey(a), a.ServiceID, 2); !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("instance A drain = %v, want [1 2 3]", got)
	}
}

func TestSBridgeDEEQueueDefaultsToService(t *testing.T) {
	h := NewSBridgeHandler(8, nil)
	a := &Event{ServiceID: 4, DEEQueueID: 100}
	b := &Event{ServiceID: 4, DEEQueueID: 101}
	if h.deeQueueKey(a) != h.deeQueueKey(b) {
		t.Fatal("legacy mode did not retain its per-service DEE queue")
	}
}
