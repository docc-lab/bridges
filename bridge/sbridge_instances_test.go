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

func TestSBridgeCanDequeueOneDEEAtATime(t *testing.T) {
	h := NewSBridgeHandler(8, nil)
	h.DequeueOneDEE = true
	const queueID = uint32(4)
	h.enqueueDEE(queueID, 4, []byte{1, 2}, 1)
	h.enqueueDEE(queueID, 4, []byte{3, 4, 5}, 1)

	if got := h.drainDEE(queueID, 4, 2); !bytes.Equal(got, []byte{1, 2}) {
		t.Fatalf("first drain = %v, want oldest DEE [1 2]", got)
	}
	if got := len(h.deeQueue[queueID]); got != 1 {
		t.Fatalf("queue length after first drain = %d, want 1", got)
	}
	if got := h.drainDEE(queueID, 4, 3); !bytes.Equal(got, []byte{3, 4, 5}) {
		t.Fatalf("second drain = %v, want remaining DEE [3 4 5]", got)
	}
	if _, exists := h.deeQueue[queueID]; exists {
		t.Fatal("empty queue was not removed")
	}
}

func TestSBridgeStillDrainsEntireDEEQueueByDefault(t *testing.T) {
	h := NewSBridgeHandler(8, nil)
	h.enqueueDEE(4, 4, []byte{1, 2}, 1)
	h.enqueueDEE(4, 4, []byte{3, 4, 5}, 1)

	if got := h.drainDEE(4, 4, 2); !bytes.Equal(got, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("default drain = %v, want concatenated queue", got)
	}
	if _, exists := h.deeQueue[4]; exists {
		t.Fatal("default drain did not remove the entire queue")
	}
}

func TestSBridgeDequeueOneLeavesBacklog(t *testing.T) {
	h := NewSBridgeHandler(8, nil)
	h.DequeueOneDEE = true
	h.DEEStats = NewDEEQueueStats()
	h.enqueueDEE(7, 4, []byte{1}, 1)
	h.enqueueDEE(7, 4, []byte{2}, 1)
	h.enqueueDEE(7, 4, []byte{3}, 1)

	_ = h.drainDEE(7, 4, 2)
	if got := len(h.deeQueue[7]); got != 2 {
		t.Fatalf("backlog length = %d, want 2", got)
	}
	// Evicting the final trace deliberately leaves the cross-trace queue alone.
	h.EvictTrace(1)
	if got := len(h.deeQueue[7]); got != 2 {
		t.Fatalf("backlog after final trace eviction = %d, want 2", got)
	}
	got := h.DEEStats.Snapshot()
	if got.PickupAttempts != 1 || got.EmptyPickups != 0 || got.PickupCalls != 1 ||
		got.EnqueuedRecords != 3 || got.EnqueuedBytes != 3 ||
		got.DequeuedRecords != 1 || got.DequeuedBytes != 1 ||
		got.BacklogQueues != 1 || got.BacklogRecords != 2 || got.BacklogBytes != 2 ||
		got.MaxQueueRecords != 3 || got.MaxQueueBytes != 3 {
		t.Fatalf("unexpected DEE stats: %+v", got)
	}
}

func TestSBridgeDEEStatsCountEmptyPickups(t *testing.T) {
	h := NewSBridgeHandler(8, nil)
	h.DEEStats = NewDEEQueueStats()
	if got := h.drainDEE(99, 4, 1); got != nil {
		t.Fatalf("empty drain = %v, want nil", got)
	}
	got := h.DEEStats.Snapshot()
	if got.PickupAttempts != 1 || got.EmptyPickups != 1 || got.PickupCalls != 0 {
		t.Fatalf("unexpected empty-pickup stats: %+v", got)
	}
}
