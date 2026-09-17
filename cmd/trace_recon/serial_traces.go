package main

import (
	"errors"
	"fmt"
	"io"

	"bridges/corpus"
)

// runSerialTraceStore deliberately has no reader, replay, or reconstruction
// goroutines. Even the next trace's read waits until all rates of the current
// trace have been reconstructed and scored. Separate pinned processes provide
// parallelism without making traces compete for the same physical core.
func runSerialTraceStore(next func() (corpus.StoredTrace, error), selected map[uint64]bool, ha *harness) error {
	remaining := len(selected)
	seen := make(map[uint64]bool, remaining)
	for remaining > 0 {
		st, err := next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("ended with %d selected traces missing", remaining)
			}
			return err
		}
		if !selected[st.TraceID] {
			continue
		}
		if seen[st.TraceID] {
			return fmt.Errorf("duplicate selected trace %016x", st.TraceID)
		}
		seen[st.TraceID] = true
		remaining--
		spans := replayTrace(ha.h, st)
		dees := ha.deesByTID[st.TraceID]
		delete(ha.deesByTID, st.TraceID)
		j := finishJob{tid: st.TraceID, spans: spans, dees: dees}
		if len(ha.mdRates) > 0 {
			j.droppedMulti = ha.computeDroppedMulti(st.TraceID, spans)
		} else {
			j.dropped = ha.computeDropped(st.TraceID, spans)
		}
		// Preserve global-RNG advancement for --only-traces, just as the
		// ordinary trace-store dispatcher does.
		if ha.only != nil && !ha.only[st.TraceID] {
			continue
		}
		ha.process(j)
	}
	return nil
}
