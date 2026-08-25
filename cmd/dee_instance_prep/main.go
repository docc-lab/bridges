// Command dee_instance_prep assigns simulated service instances to a corpus.
//
// It consumes the endpoint sidecar recovered with the endpoint-enhanced day-1
// corpus. Pass 1 computes, for every (service, endpoint), the maximum number of
// simultaneously active calls made by any one (trace, parent). Pass 2 assigns
// the smallest available instance slot within each such concurrent group and
// writes a queue ID alongside every event. Calls that overlap in a group can
// therefore never share a simulated instance; slots are reused after a call
// ends. Queue IDs are globally unique across service+endpoint pools.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"bridges/corpus"
)

type pairKey struct {
	Service  uint16
	Endpoint uint32
}

type groupKey struct {
	Trace  uint64
	Parent uint64
	Pair   pairKey
}

type spanKey struct {
	Trace uint64
	Span  uint64
}

type analysis struct {
	MaxConcurrent map[pairKey]uint32
	EventCount    uint64
	SpanCount     uint64
}

type rawEndpointReader struct {
	f   *os.File
	br  *bufio.Reader
	buf [4]byte
	n   uint64
}

func openRawEndpoints(path string) (*rawEndpointReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &rawEndpointReader{f: f, br: bufio.NewReaderSize(f, 1<<20)}, nil
}

func (r *rawEndpointReader) next() (uint32, error) {
	_, err := io.ReadFull(r.br, r.buf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("endpoint record %d: %w", r.n, err)
	}
	r.n++
	return binary.LittleEndian.Uint32(r.buf[:]), nil
}

func (r *rawEndpointReader) close() error { return r.f.Close() }

type progress struct {
	pass  int
	every uint64
	next  uint64
	start time.Time
	out   io.Writer
}

func newProgress(pass int, every uint64, out io.Writer) *progress {
	return &progress{pass: pass, every: every, next: every, start: time.Now(), out: out}
}

func (p *progress) tick(n uint64) {
	if p.every == 0 || n < p.next {
		return
	}
	secs := time.Since(p.start).Seconds()
	rate := float64(n)
	if secs > 0 {
		rate /= secs
	}
	fmt.Fprintf(p.out, "PASS%d events=%d elapsed=%s rate=%.0f_events/s\n",
		p.pass, n, time.Since(p.start).Round(time.Second), rate)
	for p.next <= n {
		p.next += p.every
	}
}

func finishEndpointStream(r *rawEndpointReader) error {
	_, err := r.next()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("endpoint sidecar has more records than events.bin")
	}
	return err
}

func analyzeCorpus(eventsPath, endpointsPath string, progressEvery uint64, progressOut io.Writer) (analysis, error) {
	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		return analysis{}, err
	}
	defer er.Close()
	epr, err := openRawEndpoints(endpointsPath)
	if err != nil {
		return analysis{}, err
	}
	defer epr.close()

	out := analysis{MaxConcurrent: make(map[pairKey]uint32)}
	active := make(map[groupKey]uint32)
	p := newProgress(1, progressEvery, progressOut)
	for {
		ev, readErr := er.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return analysis{}, fmt.Errorf("events record %d: %w", out.EventCount, readErr)
		}
		endpoint, endpointErr := epr.next()
		if endpointErr != nil {
			return analysis{}, fmt.Errorf("event %d: %w", out.EventCount, endpointErr)
		}
		pair := pairKey{Service: ev.ServiceID, Endpoint: endpoint}
		group := groupKey{Trace: ev.TraceID, Parent: ev.ParentID, Pair: pair}
		switch ev.Kind {
		case corpus.KindStart:
			out.SpanCount++
			n := active[group] + 1
			active[group] = n
			if n > out.MaxConcurrent[pair] {
				out.MaxConcurrent[pair] = n
			}
		case corpus.KindEnd:
			n := active[group]
			if n == 0 {
				return analysis{}, fmt.Errorf("event %d: end without active matching group (trace=%016x span=%016x service=%d endpoint=%d)",
					out.EventCount, ev.TraceID, ev.SpanID, ev.ServiceID, endpoint)
			}
			if n == 1 {
				delete(active, group)
			} else {
				active[group] = n - 1
			}
		default:
			return analysis{}, fmt.Errorf("event %d: invalid event kind %d", out.EventCount, ev.Kind)
		}
		out.EventCount++
		p.tick(out.EventCount)
	}
	if err := finishEndpointStream(epr); err != nil {
		return analysis{}, err
	}
	if len(active) != 0 {
		return analysis{}, fmt.Errorf("events ended with %d active trace/parent/service/endpoint groups", len(active))
	}
	return out, nil
}

type pool struct {
	Pair      pairKey
	Base      uint32
	Instances uint32
}

func buildPools(maxima map[pairKey]uint32) ([]pool, map[pairKey]pool, uint64, error) {
	keys := make([]pairKey, 0, len(maxima))
	for pair, n := range maxima {
		if n == 0 {
			return nil, nil, 0, fmt.Errorf("zero-sized pool for service=%d endpoint=%d", pair.Service, pair.Endpoint)
		}
		keys = append(keys, pair)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Service != keys[j].Service {
			return keys[i].Service < keys[j].Service
		}
		return keys[i].Endpoint < keys[j].Endpoint
	})

	pools := make([]pool, 0, len(keys))
	byPair := make(map[pairKey]pool, len(keys))
	var total uint64
	for _, pair := range keys {
		n := uint64(maxima[pair])
		if total+n > uint64(math.MaxUint32)+1 {
			return nil, nil, 0, fmt.Errorf("instance pools require %d queue IDs; uint32 supports at most %d", total+n, uint64(math.MaxUint32)+1)
		}
		p := pool{Pair: pair, Base: uint32(total), Instances: uint32(n)}
		pools = append(pools, p)
		byPair[pair] = p
		total += n
	}
	return pools, byPair, total, nil
}

type slotHeap []uint32

func (h *slotHeap) push(v uint32) {
	a := append(*h, v)
	i := len(a) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if a[parent] <= v {
			break
		}
		a[i] = a[parent]
		i = parent
	}
	a[i] = v
	*h = a
}

func (h *slotHeap) pop() uint32 {
	a := *h
	out := a[0]
	last := a[len(a)-1]
	a = a[:len(a)-1]
	if len(a) > 0 {
		i := 0
		for {
			left := 2*i + 1
			if left >= len(a) {
				break
			}
			child := left
			right := left + 1
			if right < len(a) && a[right] < a[left] {
				child = right
			}
			if a[child] >= last {
				break
			}
			a[i] = a[child]
			i = child
		}
		a[i] = last
	}
	*h = a
	return out
}

type groupSlots struct {
	next   uint32
	active uint32
	free   slotHeap
}

func (g *groupSlots) acquire() uint32 {
	var slot uint32
	if len(g.free) > 0 {
		slot = g.free.pop()
	} else {
		slot = g.next
		g.next++
	}
	g.active++
	return slot
}

func (g *groupSlots) release(slot uint32) error {
	if g.active == 0 {
		return errors.New("release from inactive group")
	}
	g.active--
	g.free.push(slot)
	return nil
}

type spanAssignment struct {
	group   groupKey
	slot    uint32
	queueID uint32
}

func assignInstances(eventsPath, endpointsPath, outputPath string, eventCount uint64, byPair map[pairKey]pool, progressEvery uint64, progressOut io.Writer) error {
	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		return err
	}
	defer er.Close()
	epr, err := openRawEndpoints(endpointsPath)
	if err != nil {
		return err
	}
	defer epr.close()

	tmpPath := outputPath + ".tmp"
	w, err := corpus.CreateDEEQueueIDs(tmpPath, eventCount)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			w.Close()
		}
	}()

	groups := make(map[groupKey]*groupSlots)
	assignments := make(map[spanKey]spanAssignment)
	p := newProgress(2, progressEvery, progressOut)
	var n uint64
	for {
		ev, readErr := er.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("events record %d: %w", n, readErr)
		}
		endpoint, endpointErr := epr.next()
		if endpointErr != nil {
			return fmt.Errorf("event %d: %w", n, endpointErr)
		}
		pair := pairKey{Service: ev.ServiceID, Endpoint: endpoint}
		pool, ok := byPair[pair]
		if !ok {
			return fmt.Errorf("event %d: missing pool for service=%d endpoint=%d", n, ev.ServiceID, endpoint)
		}
		group := groupKey{Trace: ev.TraceID, Parent: ev.ParentID, Pair: pair}
		span := spanKey{Trace: ev.TraceID, Span: ev.SpanID}
		var queueID uint32
		switch ev.Kind {
		case corpus.KindStart:
			if _, exists := assignments[span]; exists {
				return fmt.Errorf("event %d: duplicate active span trace=%016x span=%016x", n, ev.TraceID, ev.SpanID)
			}
			gs := groups[group]
			if gs == nil {
				gs = &groupSlots{}
				groups[group] = gs
			}
			slot := gs.acquire()
			if slot >= pool.Instances {
				return fmt.Errorf("event %d: observed concurrency exceeds analyzed pool for service=%d endpoint=%d (slot=%d instances=%d)",
					n, ev.ServiceID, endpoint, slot, pool.Instances)
			}
			queueID = pool.Base + slot
			assignments[span] = spanAssignment{group: group, slot: slot, queueID: queueID}
		case corpus.KindEnd:
			a, exists := assignments[span]
			if !exists {
				return fmt.Errorf("event %d: end without instance assignment trace=%016x span=%016x", n, ev.TraceID, ev.SpanID)
			}
			if a.group != group {
				return fmt.Errorf("event %d: start/end service, endpoint, or parent mismatch for trace=%016x span=%016x", n, ev.TraceID, ev.SpanID)
			}
			queueID = a.queueID
			gs := groups[group]
			if gs == nil {
				return fmt.Errorf("event %d: missing active instance group", n)
			}
			if err := gs.release(a.slot); err != nil {
				return fmt.Errorf("event %d: %w", n, err)
			}
			delete(assignments, span)
			if gs.active == 0 {
				delete(groups, group)
			}
		default:
			return fmt.Errorf("event %d: invalid event kind %d", n, ev.Kind)
		}
		if err := w.Write(queueID); err != nil {
			return err
		}
		n++
		p.tick(n)
	}
	if n != eventCount {
		return fmt.Errorf("pass 2 saw %d events, pass 1 saw %d", n, eventCount)
	}
	if err := finishEndpointStream(epr); err != nil {
		return err
	}
	if len(assignments) != 0 || len(groups) != 0 {
		return fmt.Errorf("assignment ended with %d active spans in %d groups", len(assignments), len(groups))
	}
	if err := w.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return err
	}
	return nil
}

type poolMetadata struct {
	ServiceID   uint16 `json:"service_id"`
	ServiceName string `json:"service_name"`
	EndpointID  uint32 `json:"endpoint_id"`
	Instances   uint32 `json:"instances"`
	QueueBase   uint32 `json:"queue_base"`
}

type metadata struct {
	Schema               string         `json:"schema"`
	Corpus               string         `json:"corpus"`
	Endpoints            string         `json:"endpoints"`
	QueueIDs             string         `json:"queue_ids"`
	EventCount           uint64         `json:"event_count"`
	SpanCount            uint64         `json:"span_count"`
	PairCount            int            `json:"service_endpoint_pair_count"`
	TotalInstances       uint64         `json:"total_instances"`
	MaxInstancesPerPair  uint32         `json:"max_instances_per_pair"`
	ConcurrencySemantics string         `json:"concurrency_semantics"`
	AssignmentSemantics  string         `json:"assignment_semantics"`
	Pools                []poolMetadata `json:"pools"`
}

func writeMetadata(path string, data metadata) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func run(corpusDir, endpointsPath, outputPath, metadataPath string, progressEvery uint64, progressOut io.Writer) error {
	eventsPath, metaPath := corpus.Paths(corpusDir)
	meta, err := corpus.ReadMeta(metaPath)
	if err != nil {
		return fmt.Errorf("read corpus metadata: %w", err)
	}

	fmt.Fprintf(progressOut, "PASS1 analyzing max concurrent same-parent fanout\n")
	a, err := analyzeCorpus(eventsPath, endpointsPath, progressEvery, progressOut)
	if err != nil {
		return err
	}
	pools, byPair, totalInstances, err := buildPools(a.MaxConcurrent)
	if err != nil {
		return err
	}
	fmt.Fprintf(progressOut, "PASS1 complete events=%d spans=%d pairs=%d total_instances=%d\n",
		a.EventCount, a.SpanCount, len(pools), totalInstances)

	fmt.Fprintf(progressOut, "PASS2 assigning distinct instance slots and writing %s\n", outputPath)
	if err := assignInstances(eventsPath, endpointsPath, outputPath, a.EventCount, byPair, progressEvery, progressOut); err != nil {
		return err
	}

	md := metadata{
		Schema:               "bridges.dee_instance_pools.v1",
		Corpus:               filepath.Clean(corpusDir),
		Endpoints:            filepath.Clean(endpointsPath),
		QueueIDs:             filepath.Clean(outputPath),
		EventCount:           a.EventCount,
		SpanCount:            a.SpanCount,
		PairCount:            len(pools),
		TotalInstances:       totalInstances,
		ConcurrencySemantics: "maximum simultaneously active direct children sharing (trace,parent,service,endpoint); corpus event order defines ties (starts precede ends)",
		AssignmentSemantics:  "smallest free slot within each active (trace,parent,service,endpoint) group; queue_id=queue_base+slot",
		Pools:                make([]poolMetadata, 0, len(pools)),
	}
	for _, p := range pools {
		name := ""
		if int(p.Pair.Service) < len(meta.Services) {
			name = meta.Services[p.Pair.Service]
		}
		md.Pools = append(md.Pools, poolMetadata{
			ServiceID: p.Pair.Service, ServiceName: name, EndpointID: p.Pair.Endpoint,
			Instances: p.Instances, QueueBase: p.Base,
		})
		if p.Instances > md.MaxInstancesPerPair {
			md.MaxInstancesPerPair = p.Instances
		}
	}
	if err := writeMetadata(metadataPath, md); err != nil {
		return err
	}
	fmt.Fprintf(progressOut, "DONE queue_ids=%s metadata=%s\n", outputPath, metadataPath)
	return nil
}

func main() {
	var corpusDir, endpointsPath, outputPath, metadataPath string
	var progressEvery uint64
	flag.StringVar(&corpusDir, "corpus", "", "Corpus directory containing events.bin and meta.bin")
	flag.StringVar(&endpointsPath, "endpoints", "", "Raw little-endian uint32 endpoint ID sidecar (one ID per event)")
	flag.StringVar(&outputPath, "output", "", "Output DQID sidecar (one simulated-instance queue ID per event)")
	flag.StringVar(&metadataPath, "metadata", "", "Output pool metadata JSON (default: <output>.json)")
	flag.Uint64Var(&progressEvery, "progress", 50_000_000, "Print progress every N events (0 disables)")
	flag.Parse()
	if corpusDir == "" || endpointsPath == "" || outputPath == "" {
		fmt.Fprintln(os.Stderr, "usage: dee_instance_prep --corpus DIR --endpoints endpoints.bin --output instances.bin [--metadata pools.json]")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if metadataPath == "" {
		metadataPath = outputPath + ".json"
	}
	if err := run(corpusDir, endpointsPath, outputPath, metadataPath, progressEvery, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "dee_instance_prep: %v\n", err)
		os.Exit(1)
	}
}
