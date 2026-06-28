package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"bridges/bridge"
	"bridges/corpus"
	"bridges/loader"
)

// streamWriter appends one CSV record per finalized trace to disk, so the
// per-trace metrics never have to be held in memory all at once. It is safe for
// concurrent use (the sharded runner has W workers writing through it). The
// leading header documents the schema and run parameters for the post-processor.
type streamWriter struct {
	mu sync.Mutex
	w  *bufio.Writer
	f  *os.File
}

func newStreamWriter(path string, cpd int, emitDepth, emitOC bool) (*streamWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintf(w, "#cpd=%d emit_depth=%t emit_oc=%t\n", cpd, emitDepth, emitOC)
	fmt.Fprintln(w, "tid,num_spans,num_ckpt_spans,ckpt_sum,ckpt_max,n_bag,bag_sum,bag_max,n_depth,depth_sum,n_oc,oc_sum")
	return &streamWriter{w: w, f: f}, nil
}

func (sw *streamWriter) writeRec(tid uint64, m *TraceMetrics) {
	sw.mu.Lock()
	fmt.Fprintf(sw.w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
		tid, m.NumSpans, m.NumCheckpointSpans, m.CheckpointSum, m.CheckpointMax,
		m.NumBaggageCalls, m.BaggageSum, m.BaggageMax,
		m.NumDepthSpans, m.DepthSum, m.NumOcSpans, m.OcSum)
	sw.mu.Unlock()
}

func (sw *streamWriter) close() error {
	if err := sw.w.Flush(); err != nil {
		sw.f.Close()
		return err
	}
	return sw.f.Close()
}

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
	NumDepthSpans      int // spans carrying a _d attribute (--emit-depth only)
	DepthSum           int // total _d attribute bytes (--emit-depth only)
	NumOcSpans         int // spans carrying an _oc ordinal chain (--emit-oc only)
	OcSum              int // total _oc attribute bytes (--emit-oc only)
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
	// nextSeq[traceID][parentID] = child start-ordinal counter. Nested by trace
	// so a trace's counters can be freed in one delete on finalize — otherwise
	// they accumulate one entry per (trace, parent) for the whole run.
	nextSeq map[uint64]map[uint64]int

	progressN int       // print a PROGRESS line every N completed traces (0 = off)
	t0        time.Time // start time for the progress elapsed
	completed int       // traces finished so far

	// Sharded mode only: when gCompleted != nil, completions are counted into
	// this shared atomic (across all workers) and PROGRESS prints off it at
	// gProgressN, instead of the per-state `completed` counter. nil in the
	// single-threaded path, so its behavior is unchanged.
	gCompleted *int64
	gProgressN int

	// Streaming mode: when stream != nil, per-trace metrics are allocated lazily
	// (on first event) and written + freed on finalize instead of being retained
	// for a final JSON dump — bounding resident memory to the in-flight trace
	// set. spanCountByTID supplies each trace's span count for that lazy alloc.
	stream         *streamWriter
	spanCountByTID map[uint64]int
}


func newSimState(traceOrder []uint64, spanCounts []int, stream *streamWriter) *simState {
	s := &simState{
		metricsByTID: make(map[uint64]*TraceMetrics, len(traceOrder)),
		traceOrder:   traceOrder,
		openByTID:    make(map[uint64]int, len(traceOrder)),
		nextSeq:      make(map[uint64]map[uint64]int),
		stream:       stream,
	}
	if stream != nil {
		// Lazy: keep only a tid->span-count index up front; per-trace metrics and
		// openByTID entries are created on first event and freed on finalize.
		s.spanCountByTID = make(map[uint64]int, len(traceOrder))
		for i, tid := range traceOrder {
			s.spanCountByTID[tid] = spanCounts[i]
		}
		return s
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
	if m == nil && s.stream != nil { // streaming: allocate on first event seen
		sc := s.spanCountByTID[e.traceID]
		m = newTraceMetrics(sc)
		s.metricsByTID[e.traceID] = m
		s.openByTID[e.traceID] = 2 * sc
	}

	if e.kind == bridge.KindStart {
		seqNum := 0
		if e.parentID != 0 {
			pm := s.nextSeq[e.traceID]
			if pm == nil {
				pm = make(map[uint64]int)
				s.nextSeq[e.traceID] = pm
			}
			seqNum = pm[e.parentID] + 1
			pm[e.parentID] = seqNum
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
		if r.DepthBytes > 0 && m != nil {
			m.NumDepthSpans++
			m.DepthSum += r.DepthBytes
		}
		if r.OcBytes > 0 && m != nil {
			m.NumOcSpans++
			m.OcSum += r.OcBytes
		}
	}

	s.openByTID[e.traceID]--
	if s.openByTID[e.traceID] == 0 {
		h.EvictTrace(e.traceID)
		delete(s.openByTID, e.traceID)
		delete(s.nextSeq, e.traceID) // free this trace's child-ordinal counters
		if s.stream != nil { // streaming: emit this trace's record and free it
			s.stream.writeRec(e.traceID, m)
			delete(s.metricsByTID, e.traceID)
		}
		if s.gCompleted != nil { // sharded: count into the shared atomic
			n := atomic.AddInt64(s.gCompleted, 1)
			if s.gProgressN > 0 && n%int64(s.gProgressN) == 0 {
				fmt.Fprintf(os.Stderr, "PROGRESS traces=%d elapsed=%ds\n",
					n, int(time.Since(s.t0).Seconds()))
			}
		} else {
			s.completed++
			if s.progressN > 0 && s.completed%s.progressN == 0 {
				fmt.Fprintf(os.Stderr, "PROGRESS traces=%d elapsed=%ds\n",
					s.completed, int(time.Since(s.t0).Seconds()))
			}
		}
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
	s := newSimState(traceOrder, spanCounts, nil)

	for _, e := range events {
		s.onEvent(h, e)
	}
	return s.finalize()
}

// runInterleavedFromCorpus streams events.bin and dispatches each event.
// Corpus is already globally sorted by the same key as Python's sort_events,
// so no sort happens here — pure streaming dispatch.
// selectTraces resolves which traces to simulate (and in what order) from the
// corpus meta and the --first/--sample flags, returning the trace order, their
// span counts, and a membership set (nil = all traces).
func selectTraces(meta *corpus.Meta, cfg config) (traceOrder []uint64, spanCounts []int, selected map[uint64]struct{}) {
	traceOrder = append([]uint64(nil), meta.TraceOrder...)
	spanCounts = make([]int, len(meta.SpanCounts))
	for i, c := range meta.SpanCounts {
		spanCounts[i] = int(c)
	}
	// Optional uniform random sample: simulate only a selected subset (events of
	// other traces are skipped). NOTE: S-bridge's DEE queue is cross-trace, so
	// its baggage amortization depends on trace density — a sample under-amortizes
	// it; pb/cgpb/vanilla are per-trace and unbiased under sampling.
	if cfg.first > 0 && cfg.first < len(traceOrder) {
		// First N contiguous traces: keeps S-bridge's cross-trace DEE density
		// realistic (consecutive traces feed each other's queues), and we stop
		// reading as soon as all N finalize.
		traceOrder = traceOrder[:cfg.first]
		spanCounts = spanCounts[:cfg.first]
		selected = make(map[uint64]struct{}, len(traceOrder))
		for _, tid := range traceOrder {
			selected[tid] = struct{}{}
		}
		fmt.Fprintf(os.Stderr, "first: simulating first %d traces (contiguous)\n", len(traceOrder))
	} else if cfg.sampleCount > 0 && cfg.sampleCount < len(traceOrder) {
		idx := make([]int, len(traceOrder))
		for i := range idx {
			idx[i] = i
		}
		rng := rand.New(rand.NewSource(cfg.sampleSeed))
		rng.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })
		idx = idx[:cfg.sampleCount]
		sort.Ints(idx)
		so := make([]uint64, len(idx))
		sc := make([]int, len(idx))
		selected = make(map[uint64]struct{}, len(idx))
		for k, i := range idx {
			so[k], sc[k] = traceOrder[i], spanCounts[i]
			selected[traceOrder[i]] = struct{}{}
		}
		traceOrder, spanCounts = so, sc
		fmt.Fprintf(os.Stderr, "sample: simulating %d random traces (seed %d)\n", len(traceOrder), cfg.sampleSeed)
	}
	return
}

func runInterleavedFromCorpus(er *corpus.EventsReader, meta *corpus.Meta, h bridge.Handler, cfg config, stream *streamWriter) []TraceMetrics {
	traceOrder, spanCounts, selected := selectTraces(meta, cfg)
	s := newSimState(traceOrder, spanCounts, stream)
	s.progressN, s.t0 = cfg.progressN, time.Now()

	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "corpus read error: %v\n", err)
			os.Exit(1)
		}
		if selected != nil {
			if _, ok := selected[ce.TraceID]; !ok {
				continue
			}
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
		// --first: stop as soon as all selected traces have finalized.
		if cfg.first > 0 && s.completed >= len(traceOrder) {
			break
		}
	}
	if stream != nil {
		return nil // records were streamed to disk; nothing retained to dump
	}
	return s.finalize()
}

// runShardedFromCorpus is the parallel-across-traces variant of
// runInterleavedFromCorpus, for modes with NO cross-trace state (pb, cgpb, pcr,
// pcrb, cgprb, vanilla — anything without S-bridge's DEE queue). Traces are
// sharded by tid % workers; each worker owns its shard end-to-end with its own
// handler instance and its own simState (own openByTID/nextSeq/metrics), so
// nothing mutable is shared — only the per-trace metrics, partitioned across
// shards, summing to the same total memory as the single-threaded path. A single
// reader routes each event to its owner's channel (bounded → backpressure).
// makeH must return a FRESH, independent handler on each call.
func runShardedFromCorpus(er *corpus.EventsReader, meta *corpus.Meta, makeH func() bridge.Handler, cfg config, workers int, stream *streamWriter) []TraceMetrics {
	traceOrder, spanCounts, selected := selectTraces(meta, cfg)
	W := workers
	if W < 1 {
		W = 1
	}
	wof := func(tid uint64) int { return int(tid % uint64(W)) }

	// Per-shard trace order + span counts (preserves global order within a shard).
	subOrder := make([][]uint64, W)
	subCounts := make([][]int, W)
	for i, tid := range traceOrder {
		w := wof(tid)
		subOrder[w] = append(subOrder[w], tid)
		subCounts[w] = append(subCounts[w], spanCounts[i])
	}

	var gCompleted int64
	t0 := time.Now()
	states := make([]*simState, W)
	handlers := make([]bridge.Handler, W)
	chans := make([]chan streamEvent, W)
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		s := newSimState(subOrder[w], subCounts[w], stream)
		s.gCompleted, s.gProgressN, s.t0 = &gCompleted, cfg.progressN, t0
		states[w] = s
		handlers[w] = makeH()
		chans[w] = make(chan streamEvent, 4096)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s, h := states[w], handlers[w]
			for e := range chans[w] {
				s.onEvent(h, e)
			}
		}(w)
	}

	target := int64(len(traceOrder))
	early := cfg.first > 0 || cfg.sampleCount > 0
	for {
		ce, err := er.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "corpus read error: %v\n", err)
			os.Exit(1)
		}
		if selected != nil {
			if _, ok := selected[ce.TraceID]; !ok {
				continue
			}
		}
		chans[wof(ce.TraceID)] <- streamEvent{
			ts:        ce.TS,
			kind:      bridge.Kind(ce.Kind),
			depth:     int(ce.Depth),
			traceID:   ce.TraceID,
			spanID:    ce.SpanID,
			parentID:  ce.ParentID,
			serviceID: ce.ServiceID,
		}
		// Early stop for --first/--sample: once every selected trace has
		// finalized, all their events are consumed, so nothing else matters.
		if early && atomic.LoadInt64(&gCompleted) >= target {
			break
		}
	}
	for w := 0; w < W; w++ {
		close(chans[w])
	}
	wg.Wait()

	if stream != nil {
		return nil // streamed to disk; per-worker maps were emptied on finalize
	}
	// Reassemble metrics in the global selected order (the JSON's row order).
	out := make([]TraceMetrics, len(traceOrder))
	for i, tid := range traceOrder {
		out[i] = *states[wof(tid)].metricsByTID[tid]
	}
	return out
}
