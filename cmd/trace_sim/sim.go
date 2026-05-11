package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/loader"
)

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// streamEvent is one start or end event in the simulator's sorted order.
// Same shape as corpus.Event but in-memory; conversions are trivial.
type streamEvent struct {
	ts        int64
	kind      bridge.Kind
	depth     int // sort tie-break, not consumed by handlers
	traceID   uint64
	spanID    uint64
	parentID  uint64
	serviceID uint16
}

// buildAndSortEvents takes traces in load order and returns the globally
// time-sorted event stream that Python's sort_events would produce. Tie-break
// matches Python: (ts, kind, depth-rank, trace_id, span_id), where start
// events come before end events, starts tie-break shallower-first, ends
// tie-break deeper-first.
func buildAndSortEvents(traces []loader.Trace) []streamEvent {
	total := 0
	for _, t := range traces {
		total += len(t.Spans) * 2
	}
	out := make([]streamEvent, 0, total)
	for _, t := range traces {
		for _, s := range t.Spans {
			out = append(out, streamEvent{
				ts: s.StartNS, kind: bridge.KindStart, depth: s.Depth,
				traceID: t.TraceID, spanID: s.SpanID, parentID: s.ParentID,
				serviceID: s.ServiceID,
			})
			out = append(out, streamEvent{
				ts: s.EndNS, kind: bridge.KindEnd, depth: s.Depth,
				traceID: t.TraceID, spanID: s.SpanID, parentID: s.ParentID,
				serviceID: s.ServiceID,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		var ar, br int
		if a.kind == bridge.KindStart {
			ar, br = a.depth, b.depth
		} else {
			ar, br = -a.depth, -b.depth
		}
		if ar != br {
			return ar < br
		}
		if a.traceID != b.traceID {
			return a.traceID < b.traceID
		}
		return a.spanID < b.spanID
	})
	return out
}

// TraceMetrics is the per-trace bagsize accumulator. Field semantics match
// Python's run_traces output exactly.
type TraceMetrics struct {
	NumSpans           int
	NumCheckpointSpans int
	CheckpointSum      int
	CheckpointMax      int
	NumBaggageCalls    int
	BaggageSum         int
	BaggageMax         int
	emitted            map[uint64]struct{} // (span_id) — dedupe Start vs End emit
}

func newTraceMetrics(numSpans int) *TraceMetrics {
	return &TraceMetrics{
		NumSpans: numSpans,
		emitted:  make(map[uint64]struct{}),
	}
}

// simState holds the per-trace accumulators and seq counters used by both
// JSON-direct and corpus event loops.
type simState struct {
	metricsByTID map[uint64]*TraceMetrics
	traceOrder   []uint64
	openByTID    map[uint64]int
	nextSeq      map[seqKey]int
}

type seqKey struct{ traceID, parentID uint64 }

func newSimState(traceOrder []uint64, spanCounts []int) *simState {
	s := &simState{
		metricsByTID: make(map[uint64]*TraceMetrics, len(traceOrder)),
		traceOrder:   traceOrder,
		openByTID:    make(map[uint64]int, len(traceOrder)),
		nextSeq:      make(map[seqKey]int),
	}
	for i, tid := range traceOrder {
		s.metricsByTID[tid] = newTraceMetrics(spanCounts[i])
		s.openByTID[tid] = 2 * spanCounts[i]
	}
	return s
}

var dumpPerSpans = os.Getenv("TRACE_SIM_DUMP_PERSPANS") == "1"

// onEvent dispatches one event through the handler and updates per-trace
// metric accumulators. Same logic for JSON-direct and corpus modes.
func (s *simState) onEvent(h bridge.Handler, e streamEvent) {
	ev := &bridge.Event{
		TraceID:   e.traceID,
		SpanID:    e.spanID,
		ParentID:  e.parentID,
		ServiceID: e.serviceID,
	}
	m := s.metricsByTID[e.traceID]

	if e.kind == bridge.KindStart {
		seqNum := 0
		if e.parentID != 0 {
			k := seqKey{e.traceID, e.parentID}
			seqNum = s.nextSeq[k] + 1
			s.nextSeq[k] = seqNum
		}
		r := h.OnStart(ev, seqNum)
		if dumpPerSpans {
			ne := r.EmitBytes
			if _, seen := m.emitted[e.spanID]; seen {
				ne = 0
			}
			fmt.Fprintf(os.Stdout, "start %016x seq=%d bag=%d bag_b=%d emit=%d\n",
				e.spanID, seqNum, btoi(r.BaggageFound), r.BaggageBytes, ne)
		}
		if r.BaggageFound && m != nil {
			m.NumBaggageCalls++
			m.BaggageSum += r.BaggageBytes
			if r.BaggageBytes > m.BaggageMax {
				m.BaggageMax = r.BaggageBytes
			}
		}
		if r.EmitBytes > 0 && m != nil {
			if _, seen := m.emitted[e.spanID]; !seen {
				m.emitted[e.spanID] = struct{}{}
				m.NumCheckpointSpans++
				m.CheckpointSum += r.EmitBytes
				if r.EmitBytes > m.CheckpointMax {
					m.CheckpointMax = r.EmitBytes
				}
			}
		}
	} else {
		r := h.OnEnd(ev)
		if dumpPerSpans {
			ne := r.EmitBytes
			if _, seen := m.emitted[e.spanID]; seen {
				ne = 0
			}
			fmt.Fprintf(os.Stdout, "end   %016x emit=%d\n", e.spanID, ne)
		}
		if r.EmitBytes > 0 && m != nil {
			if _, seen := m.emitted[e.spanID]; !seen {
				m.emitted[e.spanID] = struct{}{}
				m.NumCheckpointSpans++
				m.CheckpointSum += r.EmitBytes
				if r.EmitBytes > m.CheckpointMax {
					m.CheckpointMax = r.EmitBytes
				}
			}
		}
	}

	s.openByTID[e.traceID]--
	if s.openByTID[e.traceID] == 0 {
		h.EvictTrace(e.traceID)
		delete(s.openByTID, e.traceID)
	}
}

func (s *simState) finalize() []TraceMetrics {
	out := make([]TraceMetrics, len(s.traceOrder))
	for i, tid := range s.traceOrder {
		out[i] = *s.metricsByTID[tid]
	}
	return out
}

// runInterleavedJSON drives an in-memory traces slice through the handler.
func runInterleavedJSON(traces []loader.Trace, h bridge.Handler) []TraceMetrics {
	events := buildAndSortEvents(traces)

	traceOrder := make([]uint64, len(traces))
	spanCounts := make([]int, len(traces))
	for i, t := range traces {
		traceOrder[i] = t.TraceID
		spanCounts[i] = len(t.Spans)
	}
	s := newSimState(traceOrder, spanCounts)

	for _, e := range events {
		s.onEvent(h, e)
	}
	return s.finalize()
}

// runInterleavedFromCorpus streams events.bin and dispatches each event.
// Corpus is already globally sorted by the same key as Python's sort_events,
// so no sort happens here — pure streaming dispatch.
func runInterleavedFromCorpus(er *corpus.EventsReader, meta *corpus.Meta, h bridge.Handler) []TraceMetrics {
	traceOrder := append([]uint64(nil), meta.TraceOrder...)
	spanCounts := make([]int, len(meta.SpanCounts))
	for i, c := range meta.SpanCounts {
		spanCounts[i] = int(c)
	}
	s := newSimState(traceOrder, spanCounts)

	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "corpus read error: %v\n", err)
			os.Exit(1)
		}
		s.onEvent(h, streamEvent{
			ts:        ce.TS,
			kind:      bridge.Kind(ce.Kind),
			depth:     int(ce.Depth),
			traceID:   ce.TraceID,
			spanID:    ce.SpanID,
			parentID:  ce.ParentID,
			serviceID: ce.ServiceID,
		})
	}
	return s.finalize()
}
