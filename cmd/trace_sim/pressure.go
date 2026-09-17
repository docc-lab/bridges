package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"bridges/bridge"
	"bridges/corpus"
)

// Pressure accounting observes completed span owners. It never changes a
// handler's checkpoint, baggage, reverse-routing, or DEE decisions.
type pressureCounters struct {
	NumSpans                       uint64 `json:"num_spans"`
	NumLeafSpans                   uint64 `json:"num_leaf_spans"`
	NumNonleafSpans                uint64 `json:"num_nonleaf_spans"`
	NumRootSpans                   uint64 `json:"num_root_spans"`
	NumCheckpointSpans             uint64 `json:"num_checkpoint_spans"`
	NumLeafCheckpointSpans         uint64 `json:"num_leaf_checkpoint_spans"`
	NumNonleafCheckpointSpans      uint64 `json:"num_nonleaf_checkpoint_spans"`
	NumRootCheckpointSpans         uint64 `json:"num_root_checkpoint_spans"`
	NumOriginalCheckpointSpans     uint64 `json:"num_original_checkpoint_spans"`
	NumForcedLeafCheckpointSpans   uint64 `json:"num_forced_leaf_checkpoint_spans"`
	NumPromotedCheckpointSpans     uint64 `json:"num_promoted_checkpoint_spans"`
	OwnBRBytes                     uint64 `json:"own_br_bytes"`
	RawCheckpointContentBytes      uint64 `json:"raw_checkpoint_content_bytes"`
	CombinedCheckpointPayloadBytes uint64 `json:"combined_checkpoint_payload_bytes"`
	NumBaggageCalls                uint64 `json:"num_baggage_calls"`
	ForwardBaggageBytes            uint64 `json:"forward_baggage_bytes"`
	NumReverseReturnEdges          uint64 `json:"num_reverse_return_edges"`
	ReverseRawBaggageBytes         uint64 `json:"reverse_raw_baggage_bytes"`
	ReverseEncodedBaggageBytes     uint64 `json:"reverse_encoded_baggage_bytes"`
}

func (a *pressureCounters) add(b pressureCounters) {
	a.NumSpans += b.NumSpans
	a.NumLeafSpans += b.NumLeafSpans
	a.NumNonleafSpans += b.NumNonleafSpans
	a.NumRootSpans += b.NumRootSpans
	a.NumCheckpointSpans += b.NumCheckpointSpans
	a.NumLeafCheckpointSpans += b.NumLeafCheckpointSpans
	a.NumNonleafCheckpointSpans += b.NumNonleafCheckpointSpans
	a.NumRootCheckpointSpans += b.NumRootCheckpointSpans
	a.NumOriginalCheckpointSpans += b.NumOriginalCheckpointSpans
	a.NumForcedLeafCheckpointSpans += b.NumForcedLeafCheckpointSpans
	a.NumPromotedCheckpointSpans += b.NumPromotedCheckpointSpans
	a.OwnBRBytes += b.OwnBRBytes
	a.RawCheckpointContentBytes += b.RawCheckpointContentBytes
	a.CombinedCheckpointPayloadBytes += b.CombinedCheckpointPayloadBytes
	a.NumBaggageCalls += b.NumBaggageCalls
	a.ForwardBaggageBytes += b.ForwardBaggageBytes
	a.NumReverseReturnEdges += b.NumReverseReturnEdges
	a.ReverseRawBaggageBytes += b.ReverseRawBaggageBytes
	a.ReverseEncodedBaggageBytes += b.ReverseEncodedBaggageBytes
}

type pressureInstanceKey struct {
	serviceID  uint16
	instanceID uint32
}

type pressureSpan struct {
	serviceID                                               uint16
	instanceID                                              uint32
	parentID                                                uint64
	depth                                                   int
	hasChildren, ended                                      bool
	original, forced, promoted, baggageFound                bool
	ownBRBytes, rawCheckpointBytes, combinedCheckpointBytes uint64
	forwardBytes, reverseRawBytes, reverseEncodedBytes      uint64
}

type checkpointPressure struct {
	serviceNames  []string
	withInstances bool
	active        map[uint64]map[uint64]*pressureSpan
	totals        pressureCounters
	numTraces     uint64
	services      map[uint16]*pressureCounters
	instances     map[pressureInstanceKey]*pressureCounters
	depths        map[int]*pressureCounters
}

func newCheckpointPressure(services []string, withInstances bool) *checkpointPressure {
	return &checkpointPressure{serviceNames: append([]string(nil), services...), withInstances: withInstances,
		active: make(map[uint64]map[uint64]*pressureSpan), services: make(map[uint16]*pressureCounters),
		instances: make(map[pressureInstanceKey]*pressureCounters), depths: make(map[int]*pressureCounters)}
}

func validatePressureConfig(c config) error {
	if c.checkpointPressure == "" {
		if c.pressureInstanceIDs != "" {
			return fmt.Errorf("--pressure-instance-ids requires --checkpoint-pressure")
		}
		return nil
	}
	if c.corpusDir == "" {
		return fmt.Errorf("--checkpoint-pressure requires corpus mode (--corpus)")
	}
	if c.workers > 1 {
		return fmt.Errorf("--checkpoint-pressure requires --workers 1; pressure output is not shard-merged")
	}
	if c.mode != "pcrb" && c.mode != "cgprb" && c.mode != "sb3" {
		return fmt.Errorf("--checkpoint-pressure supports pcrb, cgprb and sb3")
	}
	return nil
}

func openPressureInstanceIDs(path, eventsPath string) (*corpus.DEEQueueReader, error) {
	r, err := corpus.OpenDEEQueueIDs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(eventsPath)
	if err != nil {
		r.Close()
		return nil, err
	}
	if info.Size() < corpus.HeaderSize || (info.Size()-corpus.HeaderSize)%corpus.EventRecordSize != 0 {
		r.Close()
		return nil, fmt.Errorf("invalid events.bin size %d", info.Size())
	}
	n := uint64((info.Size() - corpus.HeaderSize) / corpus.EventRecordSize)
	if r.Expected() != n {
		r.Close()
		return nil, fmt.Errorf("pressure instance-ID count %d does not match corpus event count %d", r.Expected(), n)
	}
	return r, nil
}

func (p *checkpointPressure) onStart(e streamEvent, r bridge.StartResult) {
	if p == nil {
		return
	}
	if int(e.serviceID) >= len(p.serviceNames) || e.depth < 0 {
		panic(fmt.Sprintf("checkpoint pressure: invalid service/depth for span %x", e.spanID))
	}
	t := p.active[e.traceID]
	if t == nil {
		t = make(map[uint64]*pressureSpan)
		p.active[e.traceID] = t
	}
	if t[e.spanID] != nil {
		panic(fmt.Sprintf("checkpoint pressure: duplicate start for span %x", e.spanID))
	}
	s := &pressureSpan{serviceID: e.serviceID, instanceID: e.pressureInstanceID, parentID: e.parentID, depth: e.depth}
	t[e.spanID] = s
	if r.EmitBytes > 0 {
		s.ownBRBytes = uint64(r.EmitBytes)
		s.original = true
	}
	if r.BaggageFound {
		s.baggageFound = true
		s.forwardBytes = uint64(r.BaggageBytes)
	}
}

func (p *checkpointPressure) onEnd(e streamEvent, r bridge.EndResult) {
	if p == nil {
		return
	}
	s := p.active[e.traceID][e.spanID]
	if s == nil || s.ended {
		panic(fmt.Sprintf("checkpoint pressure: missing start or duplicate end for span %x", e.spanID))
	}
	if s.serviceID != e.serviceID || s.parentID != e.parentID || s.depth != e.depth || (p.withInstances && s.instanceID != e.pressureInstanceID) {
		panic(fmt.Sprintf("checkpoint pressure: start/end identity, depth or instance mismatch for span %x", e.spanID))
	}
	s.ended = true
	if s.ownBRBytes == 0 && r.EmitBytes > 0 {
		s.ownBRBytes = uint64(r.EmitBytes)
	}
	s.rawCheckpointBytes = s.ownBRBytes
	s.combinedCheckpointBytes = s.ownBRBytes
	if rr := r.Reverse; rr != nil {
		s.original = rr.OriginalCheckpoint
		s.forced = rr.ForcedLeafCheckpoint
		s.promoted = rr.PromotedCheckpoint
		s.combinedCheckpointBytes = uint64(rr.CheckpointBytes)
		for _, truss := range rr.Accepted {
			s.rawCheckpointBytes += uint64(truss.RawBytes())
		}
		if rr.ReturnEdge != (e.parentID != 0) {
			panic(fmt.Sprintf("checkpoint pressure: return-edge mismatch for span %x", e.spanID))
		}
		s.reverseRawBytes = uint64(rr.ReverseRawBytes)
		s.reverseEncodedBytes = uint64(rr.ReverseEncodedBytes)
	}
}

func (p *checkpointPressure) finishTrace(tid uint64, expectedSpans int) {
	if p == nil {
		return
	}
	t := p.active[tid]
	if len(t) != expectedSpans {
		panic(fmt.Sprintf("checkpoint pressure: trace %x has %d spans, expected %d", tid, len(t), expectedSpans))
	}
	// Classify leaves from the complete trace topology, not service identity,
	// checkpoint type, end order, or the post-loss reconstruction input.
	for sid, s := range t {
		if !s.ended {
			panic(fmt.Sprintf("checkpoint pressure: span %x has not ended", sid))
		}
		if s.parentID != 0 {
			parent := t[s.parentID]
			if parent == nil || s.depth != parent.depth+1 {
				panic(fmt.Sprintf("checkpoint pressure: invalid full-trace parent/depth for span %x", sid))
			}
			parent.hasChildren = true
		} else if s.depth != 0 {
			panic(fmt.Sprintf("checkpoint pressure: root span %x has nonzero depth", sid))
		}
	}
	for _, s := range t {
		c := pressureCounters{
			NumSpans:                     1,
			NumOriginalCheckpointSpans:   uint64(btoi(s.original)),
			NumForcedLeafCheckpointSpans: uint64(btoi(s.forced)),
			NumPromotedCheckpointSpans:   uint64(btoi(s.promoted)),
			OwnBRBytes:                   s.ownBRBytes, RawCheckpointContentBytes: s.rawCheckpointBytes,
			CombinedCheckpointPayloadBytes: s.combinedCheckpointBytes,
			NumBaggageCalls:                uint64(btoi(s.baggageFound)), ForwardBaggageBytes: s.forwardBytes,
			// Same denominator in every arm, including reverse-disabled zero-byte returns.
			NumReverseReturnEdges:  uint64(btoi(s.parentID != 0)),
			ReverseRawBaggageBytes: s.reverseRawBytes, ReverseEncodedBaggageBytes: s.reverseEncodedBytes,
		}
		c.NumRootSpans = uint64(btoi(s.parentID == 0))
		c.NumLeafSpans = uint64(btoi(!s.hasChildren))
		c.NumNonleafSpans = uint64(btoi(s.hasChildren))
		if c.OwnBRBytes > 0 {
			c.NumCheckpointSpans = 1
			c.NumLeafCheckpointSpans = c.NumLeafSpans
			c.NumNonleafCheckpointSpans = c.NumNonleafSpans
			c.NumRootCheckpointSpans = c.NumRootSpans
			// In ordinary forward mode, an end-only leaf emission is forced.
			if c.NumOriginalCheckpointSpans+c.NumPromotedCheckpointSpans == 0 && !s.hasChildren {
				c.NumForcedLeafCheckpointSpans = 1
			}
		}
		if c.NumOriginalCheckpointSpans+c.NumForcedLeafCheckpointSpans+c.NumPromotedCheckpointSpans != c.NumCheckpointSpans {
			panic(fmt.Sprintf("checkpoint pressure: checkpoint category mismatch in trace %x", tid))
		}
		p.totals.add(c)
		if p.services[s.serviceID] == nil {
			p.services[s.serviceID] = &pressureCounters{}
		}
		p.services[s.serviceID].add(c)
		if p.withInstances {
			key := pressureInstanceKey{s.serviceID, s.instanceID}
			if p.instances[key] == nil {
				p.instances[key] = &pressureCounters{}
			}
			p.instances[key].add(c)
		}
		if p.depths[s.depth] == nil {
			p.depths[s.depth] = &pressureCounters{}
		}
		p.depths[s.depth].add(c)
	}
	p.numTraces++
	delete(p.active, tid)
}

type pressureServiceRow struct {
	ServiceID   uint16 `json:"service_id"`
	ServiceName string `json:"service_name"`
	pressureCounters
}

type pressureInstanceRow struct {
	ServiceID   uint16 `json:"service_id"`
	ServiceName string `json:"service_name"`
	InstanceID  uint32 `json:"instance_id"`
	pressureCounters
}

type pressureDepthRow struct {
	Depth int `json:"depth"`
	pressureCounters
}

type checkpointPressureFile struct {
	Schema                  string                  `json:"schema"`
	Mode                    string                  `json:"mode"`
	CheckpointDistance      int                     `json:"checkpoint_distance"`
	CheckpointRandomization *bridge.CheckpointRange `json:"checkpoint_randomization,omitempty"`
	ReverseConfig           *bridge.ReverseConfig   `json:"reverse_config,omitempty"`
	LehmerEE                bool                    `json:"lehmer_ee"`
	DEEInstanceQueues       bool                    `json:"dee_instance_queues"`
	DEEDequeueOne           bool                    `json:"dee_dequeue_one"`
	InstanceModel           string                  `json:"instance_model"`
	Accounting              map[string]string       `json:"accounting"`
	NumTraces               uint64                  `json:"num_traces"`
	Totals                  pressureCounters        `json:"totals"`
	Services                []pressureServiceRow    `json:"services"`
	Instances               []pressureInstanceRow   `json:"instances"`
	Depths                  []pressureDepthRow      `json:"depths"`
}

func (p *checkpointPressure) write(path string, c config) error {
	if len(p.active) != 0 {
		return fmt.Errorf("%d traces are incomplete", len(p.active))
	}
	out := checkpointPressureFile{
		Schema: "bridges.checkpoint_pressure.v2", Mode: c.mode, CheckpointDistance: c.checkpointDistance,
		CheckpointRandomization: c.checkpointPolicy, ReverseConfig: c.reverse,
		LehmerEE: c.lehmerEE, DEEInstanceQueues: c.deeQueueIDs != "", DEEDequeueOne: c.deeDequeueOne,
		InstanceModel: "service-only; no supplied instance sidecar", NumTraces: p.numTraces, Totals: p.totals,
		Services: make([]pressureServiceRow, 0, len(p.services)), Instances: make([]pressureInstanceRow, 0, len(p.instances)), Depths: make([]pressureDepthRow, 0, len(p.depths)),
		Accounting: map[string]string{
			"population":                        "all completed selected spans before collection loss; includes roots and zero-checkpoint services/instances",
			"leaf":                              "zero children in the complete selected trace topology; a single-span root is also a leaf",
			"checkpoint_owner":                  "one distinct emitting span; original start emissions attributed to that owner's completion; multiple accepted trusses count once",
			"checkpoint_categories":             "original + forced_leaf + promoted = total; original is a forward reset, promoted is only an export checkpoint",
			"instance_identity":                 "(service_id, instance_id); supplied modeled queue/endpoint-instance slots, not observed physical server instances",
			"own_br_bytes":                      "receiver's own _br key and value; includes 3-byte key; excludes _d/_oc and returned bundle",
			"raw_checkpoint_content_bytes":      "own_br_bytes + accepted truss raw bytes including origin ID/depth and any remaining TTL; excludes returned-bundle attribute key and binary framing",
			"combined_checkpoint_payload_bytes": "own_br_bytes + bridges.checkpoint key and emitted binary bundle value; excludes _d/_oc and external OTLP encoding",
			"encoding":                          bridge.ReverseEncoding,
			"forward_baggage_bytes":             "existing baggage-call byte accounting, attributed to receiving span/service",
			"num_reverse_return_edges":          "every non-root span-return edge, including zero-byte returns and reverse-disabled baseline",
			"reverse_raw_baggage_bytes":         "return bytes attributed to returning child owner; origin ID/depth, exact truss and optional TTL; excludes binary framing",
			"reverse_encoded_baggage_bytes":     "actual binary return-bundle value, including version and record framing; excludes external RPC/OTLP encoding",
			"transport_scope":                   "logical recorded span edges, not verified physical RPC response hops",
		},
	}
	if p.withInstances {
		out.InstanceModel = "supplied modeled queue/endpoint-instance slots (per-event sidecar); not observed physical instances"
	}
	for id, counters := range p.services {
		out.Services = append(out.Services, pressureServiceRow{id, p.serviceNames[id], *counters})
	}
	for key, counters := range p.instances {
		out.Instances = append(out.Instances, pressureInstanceRow{key.serviceID, p.serviceNames[key.serviceID], key.instanceID, *counters})
	}
	for depth, counters := range p.depths {
		out.Depths = append(out.Depths, pressureDepthRow{depth, *counters})
	}
	sort.Slice(out.Services, func(i, j int) bool { return out.Services[i].ServiceID < out.Services[j].ServiceID })
	sort.Slice(out.Instances, func(i, j int) bool {
		a, b := out.Instances[i], out.Instances[j]
		if a.ServiceID != b.ServiceID {
			return a.ServiceID < b.ServiceID
		}
		return a.InstanceID < b.InstanceID
	})
	sort.Slice(out.Depths, func(i, j int) bool { return out.Depths[i].Depth < out.Depths[j].Depth })
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
