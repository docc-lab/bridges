// Command endpoint_prep builds the per-event endpoint-ID sidecar
// (endpoints.bin) that cmd/dee_instance_prep consumes.
//
// The corpus keeps only (TS, SpanID, ParentID, TraceID, Depth, ServiceID,
// Kind); a span's operation name is dropped when the corpus is built. The
// sidecar restores it, as one little-endian uint32 per corpus event, in corpus
// event order, with no header.
//
// An endpoint ID interns the operation name ALONE, globally. dee_instance_prep
// forms a pool per (service, endpoint) by pairing this ID with the corpus's own
// ServiceID, which is why one endpoint ID legitimately appears under many
// services. The numbering itself is arbitrary: any bijective relabelling yields
// identical pools, so this tool does not try to reproduce a previous run's IDs.
//
// Two phases, so the expensive scan can be reused:
//
//	scan  reads a tar stream of Jaeger trace JSON on stdin and writes fixed
//	      20-byte records (traceID, spanID, endpointID) plus a names file.
//	join  sorts those records by (traceID, spanID), walks the corpus
//	      events.bin, and emits one uint32 per event.
//
// The raw archives are ~10.5x their compressed size, so the scan is always fed
// by a pipe and never staged on disk:
//
//	cat trace2_* | zstd -dc | endpoint_prep -phase scan \
//	    -records /path/day2.records -names /path/day2.names.json
//	endpoint_prep -phase join -records /path/day2.records \
//	    -corpus /path/day2_unfilt_corpus -output /path/day2_unfilt_corpus/endpoints.bin
package main

import (
	"archive/tar"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"bridges/corpus"
)

const recSize = 20 // traceID(8) + spanID(8) + endpointID(4), little-endian

type rawRef struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
}

type rawSpan struct {
	SpanID        string   `json:"spanID"`
	OperationName string   `json:"operationName"`
	ProcessID     string   `json:"processID"`
	References    []rawRef `json:"references"`
}

type rawProcess struct {
	ServiceName string `json:"serviceName"`
}

type rawTrace struct {
	TraceID   string                `json:"traceID"`
	Spans     []rawSpan             `json:"spans"`
	Processes map[string]rawProcess `json:"processes"`
}

type rawWrapper struct {
	Data []rawTrace `json:"data"`
}

// interner assigns a dense uint32 to each distinct operation name.
type interner struct {
	mu   sync.Mutex
	ids  map[string]uint32
	name []string
}

func newInterner() *interner { return &interner{ids: make(map[string]uint32, 1<<15)} }

func (in *interner) id(s string) uint32 {
	in.mu.Lock()
	defer in.mu.Unlock()
	if v, ok := in.ids[s]; ok {
		return v
	}
	v := uint32(len(in.name))
	in.ids[s] = v
	in.name = append(in.name, s)
	return v
}

func parseHex64(s string) (uint64, error) { return strconv.ParseUint(s, 16, 64) }

// decodeTraces handles both the bare {"spans":...} and the wrapped
// {"data":[{...}]} shapes that appear in these archives.
func decodeTraces(b []byte) ([]rawTrace, error) {
	var w rawWrapper
	if err := json.Unmarshal(b, &w); err == nil && len(w.Data) > 0 {
		return w.Data, nil
	}
	var t rawTrace
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	if t.TraceID == "" && len(t.Spans) == 0 {
		return nil, fmt.Errorf("no trace payload")
	}
	return []rawTrace{t}, nil
}

func runScan(recordsPath, namesPath string, workers, progress int) error {
	out, err := os.Create(recordsPath)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(out, 1<<22)

	in := newInterner()
	type job struct {
		name string
		body []byte
	}
	jobs := make(chan job, workers*4)
	recs := make(chan []byte, workers*4)

	var wg sync.WaitGroup
	// decodeErrs and spanErrs are real faults. wideTraceIDs is not: a trace ID
	// wider than 64 bits cannot be represented, and loader.go -- the code that
	// built the corpus -- skips those traces too, so the corpus provably
	// contains no span from them. Counting them as failures would abort a
	// perfectly good scan.
	var decodeErrs, spanErrs, wideTraceIDs int64
	var wideSamples, decodeSamples []string
	var errMu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 0, 1<<16)
			for j := range jobs {
				traces, err := decodeTraces(j.body)
				if err != nil {
					errMu.Lock()
					decodeErrs++
					if len(decodeSamples) < 5 {
						decodeSamples = append(decodeSamples, j.name)
					}
					errMu.Unlock()
					continue
				}
				buf = buf[:0]
				for _, t := range traces {
					tid, err := parseHex64(t.TraceID)
					if err != nil {
						errMu.Lock()
						wideTraceIDs++
						if len(wideSamples) < 5 {
							wideSamples = append(wideSamples, t.TraceID)
						}
						errMu.Unlock()
						continue
					}
					for _, s := range t.Spans {
						sid, err := parseHex64(s.SpanID)
						if err != nil {
							errMu.Lock()
							spanErrs++
							errMu.Unlock()
							continue
						}
						ep := in.id(s.OperationName)
						var r [recSize]byte
						binary.LittleEndian.PutUint64(r[0:8], tid)
						binary.LittleEndian.PutUint64(r[8:16], sid)
						binary.LittleEndian.PutUint32(r[16:20], ep)
						buf = append(buf, r[:]...)
					}
				}
				if len(buf) > 0 {
					cp := make([]byte, len(buf))
					copy(cp, buf)
					recs <- cp
				}
			}
		}()
	}

	done := make(chan struct{})
	var written int64
	go func() {
		defer close(done)
		for b := range recs {
			if _, err := bw.Write(b); err != nil {
				fmt.Fprintf(os.Stderr, "write records: %v\n", err)
				os.Exit(1)
			}
			written += int64(len(b)) / recSize
		}
	}()

	tr := tar.NewReader(bufio.NewReaderSize(os.Stdin, 1<<22))
	start := time.Now()
	var members int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		body := make([]byte, h.Size)
		if _, err := io.ReadFull(tr, body); err != nil {
			return fmt.Errorf("read %s: %w", h.Name, err)
		}
		jobs <- job{name: h.Name, body: body}
		members++
		if progress > 0 && members%int64(progress) == 0 {
			el := time.Since(start).Seconds()
			fmt.Fprintf(os.Stderr, "SCAN traces=%d spans=%d ops=%d elapsed=%.0fs rate=%.0f traces/s\n",
				members, written, len(in.name), el, float64(members)/el)
		}
	}
	close(jobs)
	wg.Wait()
	close(recs)
	<-done
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}

	nf, err := os.Create(namesPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(nf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(map[string]any{
		"schema":                 "bridges.endpoint_names.v1",
		"operation_count":        len(in.name),
		"span_records":           written,
		"traces":                 members,
		"decode_errors":          decodeErrs,
		"span_errors":            spanErrs,
		"skipped_wide_trace_ids": wideTraceIDs,
		"skipped_wide_samples":   wideSamples,
		"note":                   "index = endpoint id; an endpoint id interns the operation name alone, globally",
		"operations":             in.name,
	}); err != nil {
		return err
	}
	if err := nf.Close(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "SCAN DONE traces=%d span_records=%d distinct_operations=%d decode_errors=%d span_errors=%d skipped_wide_trace_ids=%d elapsed=%.0fs\n",
		members, written, len(in.name), decodeErrs, spanErrs, wideTraceIDs, time.Since(start).Seconds())
	if wideTraceIDs > 0 {
		fmt.Fprintf(os.Stderr, "  (wide trace ids are expected; the corpus builder skips them too. samples: %v)\n", wideSamples)
	}
	if decodeErrs > 0 || spanErrs > 0 {
		return fmt.Errorf("scan hit %d JSON decode and %d span-id errors (samples: %v); refusing to declare success",
			decodeErrs, spanErrs, decodeSamples)
	}
	return nil
}

type rec struct {
	tid, sid uint64
	ep       uint32
}

func runJoin(recordsPath, corpusDir, outPath string, progress int) error {
	rf, err := os.Open(recordsPath)
	if err != nil {
		return err
	}
	defer rf.Close()
	st, err := rf.Stat()
	if err != nil {
		return err
	}
	if st.Size()%recSize != 0 {
		return fmt.Errorf("records file %d bytes is not a multiple of %d", st.Size(), recSize)
	}
	n := st.Size() / recSize
	fmt.Fprintf(os.Stderr, "JOIN loading %d span records (%.1f GB in memory)\n", n, float64(n)*24/1e9)

	table := make([]rec, 0, n)
	br := bufio.NewReaderSize(rf, 1<<22)
	buf := make([]byte, recSize*4096)
	for {
		m, err := io.ReadFull(br, buf)
		if m == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			break
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		for off := 0; off+recSize <= m; off += recSize {
			table = append(table, rec{
				tid: binary.LittleEndian.Uint64(buf[off : off+8]),
				sid: binary.LittleEndian.Uint64(buf[off+8 : off+16]),
				ep:  binary.LittleEndian.Uint32(buf[off+16 : off+20]),
			})
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	fmt.Fprintf(os.Stderr, "JOIN sorting %d records\n", len(table))
	t0 := time.Now()
	slices.SortFunc(table, func(a, b rec) int {
		if a.tid != b.tid {
			if a.tid < b.tid {
				return -1
			}
			return 1
		}
		if a.sid != b.sid {
			if a.sid < b.sid {
				return -1
			}
			return 1
		}
		return 0
	})
	fmt.Fprintf(os.Stderr, "JOIN sorted in %.0fs\n", time.Since(t0).Seconds())

	lookup := func(tid, sid uint64) (uint32, bool) {
		lo, hi := 0, len(table)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			t := table[mid]
			if t.tid < tid || (t.tid == tid && t.sid < sid) {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < len(table) && table[lo].tid == tid && table[lo].sid == sid {
			return table[lo].ep, true
		}
		return 0, false
	}

	eventsPath, _ := corpus.Paths(corpusDir)
	er, err := corpus.OpenEvents(eventsPath)
	if err != nil {
		return err
	}
	defer er.Close()

	tmp := outPath + ".tmp"
	of, err := os.Create(tmp)
	if err != nil {
		return err
	}
	ow := bufio.NewWriterSize(of, 1<<22)

	var events, missing int64
	var out [4]byte
	start := time.Now()
	for {
		e, err := er.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("events: %w", err)
		}
		ep, ok := lookup(e.TraceID, e.SpanID)
		if !ok {
			missing++
			if missing <= 5 {
				fmt.Fprintf(os.Stderr, "  MISSING trace=%016x span=%016x\n", e.TraceID, e.SpanID)
			}
		}
		binary.LittleEndian.PutUint32(out[:], ep)
		if _, err := ow.Write(out[:]); err != nil {
			return err
		}
		events++
		if progress > 0 && events%int64(progress) == 0 {
			el := time.Since(start).Seconds()
			fmt.Fprintf(os.Stderr, "JOIN events=%d missing=%d elapsed=%.0fs rate=%.0f/s\n",
				events, missing, el, float64(events)/el)
		}
	}
	if err := ow.Flush(); err != nil {
		return err
	}
	if err := of.Close(); err != nil {
		return err
	}
	if missing > 0 {
		os.Remove(tmp)
		return fmt.Errorf("%d of %d corpus events had no matching raw span; refusing to write a sidecar with invented ids", missing, events)
	}
	if err := os.Rename(tmp, outPath); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "JOIN DONE events=%d bytes=%d elapsed=%.0fs\n", events, events*4, time.Since(start).Seconds())
	return nil
}

func main() {
	var (
		phase    = flag.String("phase", "", "scan (stdin tar -> records) or join (records + corpus -> endpoints.bin)")
		records  = flag.String("records", "", "intermediate span-record file")
		names    = flag.String("names", "", "scan: operation-name table (JSON)")
		corpusD  = flag.String("corpus", "", "join: corpus dir (events.bin + meta.bin)")
		output   = flag.String("output", "", "join: endpoints.bin to write")
		workers  = flag.Int("workers", runtime.NumCPU(), "scan: JSON parse workers")
		progress = flag.Int("progress", 0, "print progress every N traces (scan) or events (join); 0 = silent")
	)
	flag.Parse()

	var err error
	switch *phase {
	case "scan":
		if *records == "" || *names == "" {
			err = fmt.Errorf("-records and -names are required for scan")
			break
		}
		err = runScan(*records, *names, *workers, *progress)
	case "join":
		if *records == "" || *corpusD == "" || *output == "" {
			err = fmt.Errorf("-records, -corpus and -output are required for join")
			break
		}
		err = runJoin(*records, *corpusD, *output, *progress)
	default:
		err = fmt.Errorf("-phase must be scan or join")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "endpoint_prep:", err)
		os.Exit(1)
	}
}
