package bridge

// DEEQueueStatsSnapshot is a point-in-time summary of S-Bridge's delayed-end
// queue lifecycle. A non-empty pickup is one call that removed one or more
// records; an empty pickup is a call that found its selected queue empty.
type DEEQueueStatsSnapshot struct {
	PickupAttempts  uint64 `json:"pickup_attempts"`
	EmptyPickups    uint64 `json:"empty_pickups"`
	PickupCalls     uint64 `json:"pickup_calls"`
	EnqueuedRecords uint64 `json:"enqueued_records"`
	EnqueuedBytes   uint64 `json:"enqueued_bytes"`
	DequeuedRecords uint64 `json:"dequeued_records"`
	DequeuedBytes   uint64 `json:"dequeued_bytes"`
	BacklogQueues   uint64 `json:"backlog_queues"`
	BacklogRecords  uint64 `json:"backlog_records"`
	BacklogBytes    uint64 `json:"backlog_bytes"`
	MaxQueueRecords uint64 `json:"max_queue_records"`
	MaxQueueBytes   uint64 `json:"max_queue_bytes"`
}

type deeQueueTotals struct {
	records uint64
	bytes   uint64
}

// DEEQueueStats incrementally tracks one handler's queues without rescanning
// their payload slices. S-Bridge is intentionally single-threaded because its
// queue state is event-order dependent, so this tracker needs no locking.
type DEEQueueStats struct {
	snapshot DEEQueueStatsSnapshot
	queues   map[uint32]deeQueueTotals
}

func NewDEEQueueStats() *DEEQueueStats {
	return &DEEQueueStats{queues: make(map[uint32]deeQueueTotals)}
}

func (s *DEEQueueStats) recordPickupAttempt(found bool) {
	if s == nil {
		return
	}
	s.snapshot.PickupAttempts++
	if !found {
		s.snapshot.EmptyPickups++
	}
}

func (s *DEEQueueStats) recordEnqueue(queueID uint32, bytes int) {
	if s == nil {
		return
	}
	q := s.queues[queueID]
	if q.records == 0 {
		s.snapshot.BacklogQueues++
	}
	q.records++
	q.bytes += uint64(bytes)
	s.queues[queueID] = q
	s.snapshot.EnqueuedRecords++
	s.snapshot.EnqueuedBytes += uint64(bytes)
	s.snapshot.BacklogRecords++
	s.snapshot.BacklogBytes += uint64(bytes)
	if q.records > s.snapshot.MaxQueueRecords {
		s.snapshot.MaxQueueRecords = q.records
	}
	if q.bytes > s.snapshot.MaxQueueBytes {
		s.snapshot.MaxQueueBytes = q.bytes
	}
}

func (s *DEEQueueStats) recordDequeue(queueID uint32, records int, bytes int) {
	if s == nil {
		return
	}
	q := s.queues[queueID]
	r := uint64(records)
	b := uint64(bytes)
	if r > q.records || b > q.bytes {
		panic("S-Bridge DEE queue statistics underflow")
	}
	s.snapshot.PickupCalls++
	s.snapshot.DequeuedRecords += r
	s.snapshot.DequeuedBytes += b
	s.snapshot.BacklogRecords -= r
	s.snapshot.BacklogBytes -= b
	q.records -= r
	q.bytes -= b
	if q.records == 0 {
		delete(s.queues, queueID)
		s.snapshot.BacklogQueues--
	} else {
		s.queues[queueID] = q
	}
}

func (s *DEEQueueStats) Snapshot() DEEQueueStatsSnapshot {
	if s == nil {
		return DEEQueueStatsSnapshot{}
	}
	return s.snapshot
}
