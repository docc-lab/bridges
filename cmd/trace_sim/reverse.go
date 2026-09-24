package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"bridges/bridge"
)

// optionalProbability distinguishes an explicit zero from an absent argument.
type optionalProbability struct {
	value float64
	set   bool
}

func (p *optionalProbability) String() string { return strconv.FormatFloat(p.value, 'g', -1, 64) }
func (p *optionalProbability) Set(s string) error {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1 {
		return fmt.Errorf("probability must be finite and in [0,1]")
	}
	p.value, p.set = v, true
	return nil
}

func parseReverseConfig(c config, policy string, q float64, p optionalProbability, ttl string, seed uint64, exponent float64, passCheckpoints bool) (*bridge.ReverseConfig, error) {
	if policy == "" {
		if p.set || ttl != "" || passCheckpoints {
			return nil, fmt.Errorf("reverse options require --reverse-policy")
		}
		return nil, nil
	}
	if c.mode != "pcrb" && c.mode != "cgprb" && c.mode != "sb3" {
		return nil, fmt.Errorf("--reverse-policy supports pcrb, cgprb and sb3")
	}
	switch policy {
	case "ttl", "probability", "inverse_depth", "depth_linear", "depth_quadratic", "depth_cubic", "depth_quartic", "depth_ratio", "upstream_pressure":
	default:
		return nil, fmt.Errorf("unknown reverse policy %q", policy)
	}
	if math.IsNaN(q) || math.IsInf(q, 0) || q < 0 || q > 1 {
		return nil, fmt.Errorf("--leaf-reject must be finite and in [0,1]")
	}
	if (policy == "probability") != p.set {
		return nil, fmt.Errorf("--reverse-probability is required exactly for --reverse-policy probability")
	}
	if (policy == "depth_ratio") != (exponent > 0) {
		return nil, fmt.Errorf("--reverse-exponent is required exactly for --reverse-policy depth_ratio")
	}
	if ttl != "" && policy != "ttl" {
		return nil, fmt.Errorf("--reverse-ttl-range is valid only for ttl policy")
	}
	lo, hi := c.checkpointDistance, c.checkpointDistance
	if c.checkpointPolicy != nil {
		lo, hi = c.checkpointPolicy.Min, c.checkpointPolicy.Max
	}
	if ttl != "" {
		parts := strings.Split(ttl, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("reverse TTL range must be MIN:MAX")
		}
		var e1, e2 error
		lo, e1 = strconv.Atoi(parts[0])
		hi, e2 = strconv.Atoi(parts[1])
		if e1 != nil || e2 != nil {
			return nil, fmt.Errorf("reverse TTL range must contain integer distances")
		}
	}
	if policy != "ttl" {
		lo, hi = 0, 0
	}
	out := &bridge.ReverseConfig{Policy: policy, LeafRejectProbability: q, Probability: p.value, Exponent: exponent, TTLMin: lo, TTLMax: hi, Seed: seed, PassCheckpoints: passCheckpoints}
	if err := out.Validate(); err != nil {
		return nil, err
	}
	return out, nil
}

// This is a span-tree transport model. Uber does not label client/server pairs,
// so encoded bytes must not be described as measured RPC response traffic.
var reverseAccounting = map[string]string{
	"encoding":                          bridge.ReverseEncoding,
	"transport_execution":               "serialized child return values are decoded by the parent; emitted checkpoint bundle values are retained in ReverseEndResult",
	"depth_unit":                        "one input span; root depth zero",
	"reverse_baggage_sampling":          "every non-root span-return edge, including zero-byte returns",
	"reverse_transport_scope":           "logical span edges; not verified physical RPC response hops",
	"reverse_raw_bytes":                 "sum of origin ID, origin-depth varint, exact truss bytes, and optional TTL byte; excludes binary bundle framing",
	"reverse_encoded_bytes":             "actual binary return-bundle value, including version and record framing; excludes external RPC/OTLP encoding",
	"bridge_payload_bytes":              "receiver's own _br key and value only; excludes bridges.checkpoint and _d/_oc",
	"combined_checkpoint_payload_bytes": "own _br plus bridges.checkpoint key and emitted binary bundle value; excludes _d/_oc and external OTLP encoding",
	"raw_truss_bytes":                   "exact originating bridge value, sampled once per rejected leaf",
	"checkpoint_count":                  "distinct emitting spans; multiple accepted trusses count once",
	"checkpoint_count_categories":       "original + retained forced leaf + promoted = total; receiving checkpoints overlap original and promoted",
}

// Scalar totals are exported per trace and appended to streaming CSVs only in
// reverse mode. Existing twelve-column CSVs retain their original format.
type reverseTraceMetrics struct {
	OriginalCheckpointSpans, ForcedLeafCheckpointSpans, RejectedLeaves int
	ReceivingCheckpointSpans, PromotedCheckpointSpans                  int
	ReturnedTrusses, AcceptedTrusses, MandatoryAbsorptions             int
	MaxAcceptedTrusses, MaxReceivedTrusses                             int
	RawTrussSum, OriginMetadataSum                                     int
	CombinedCheckpointSum, CombinedCheckpointMax                       int
	NumReverseReturnEdges, NumNonemptyReverseReturnEdges               int
	ReverseRawSum, ReverseRawMax, ReverseEncodedSum, ReverseEncodedMax int
	ReverseDistanceSum, ReverseDistanceMax                             int
}

type reverseMetricColumn struct {
	name string
	ptr  func(*reverseTraceMetrics) *int
}

var reverseMetricColumns = []reverseMetricColumn{
	{"num_original_checkpoint_spans", func(m *reverseTraceMetrics) *int { return &m.OriginalCheckpointSpans }},
	{"num_forced_leaf_checkpoint_spans", func(m *reverseTraceMetrics) *int { return &m.ForcedLeafCheckpointSpans }},
	{"num_rejected_leaves", func(m *reverseTraceMetrics) *int { return &m.RejectedLeaves }},
	{"num_reverse_receiving_checkpoint_spans", func(m *reverseTraceMetrics) *int { return &m.ReceivingCheckpointSpans }},
	{"num_reverse_promoted_checkpoint_spans", func(m *reverseTraceMetrics) *int { return &m.PromotedCheckpointSpans }},
	{"num_returned_trusses", func(m *reverseTraceMetrics) *int { return &m.ReturnedTrusses }},
	{"num_accepted_trusses", func(m *reverseTraceMetrics) *int { return &m.AcceptedTrusses }},
	{"num_mandatory_absorptions", func(m *reverseTraceMetrics) *int { return &m.MandatoryAbsorptions }},
	{"max_accepted_trusses", func(m *reverseTraceMetrics) *int { return &m.MaxAcceptedTrusses }},
	{"max_received_trusses", func(m *reverseTraceMetrics) *int { return &m.MaxReceivedTrusses }},
	{"raw_truss_sum", func(m *reverseTraceMetrics) *int { return &m.RawTrussSum }},
	{"origin_metadata_sum", func(m *reverseTraceMetrics) *int { return &m.OriginMetadataSum }},
	{"combined_checkpoint_payload_sum", func(m *reverseTraceMetrics) *int { return &m.CombinedCheckpointSum }},
	{"max_combined_checkpoint_payload", func(m *reverseTraceMetrics) *int { return &m.CombinedCheckpointMax }},
	{"num_reverse_return_edges", func(m *reverseTraceMetrics) *int { return &m.NumReverseReturnEdges }},
	{"num_nonempty_reverse_return_edges", func(m *reverseTraceMetrics) *int { return &m.NumNonemptyReverseReturnEdges }},
	{"reverse_raw_baggage_sum", func(m *reverseTraceMetrics) *int { return &m.ReverseRawSum }},
	{"max_reverse_raw_baggage", func(m *reverseTraceMetrics) *int { return &m.ReverseRawMax }},
	{"reverse_encoded_baggage_sum", func(m *reverseTraceMetrics) *int { return &m.ReverseEncodedSum }},
	{"max_reverse_encoded_baggage", func(m *reverseTraceMetrics) *int { return &m.ReverseEncodedMax }},
	{"reverse_distance_sum", func(m *reverseTraceMetrics) *int { return &m.ReverseDistanceSum }},
	{"max_reverse_distance", func(m *reverseTraceMetrics) *int { return &m.ReverseDistanceMax }},
}

func (s *simState) recordReverse(m *TraceMetrics, r *bridge.ReverseEndResult) {
	if r == nil || m == nil {
		return
	}
	if m.Reverse == nil {
		m.Reverse = &reverseTraceMetrics{}
	}
	a := m.Reverse
	a.OriginalCheckpointSpans += btoi(r.OriginalCheckpoint)
	a.ForcedLeafCheckpointSpans += btoi(r.ForcedLeafCheckpoint)
	a.RejectedLeaves += btoi(r.RejectedLeaf)
	a.ReceivingCheckpointSpans += btoi(r.ReceivingCheckpoint)
	a.PromotedCheckpointSpans += btoi(r.PromotedCheckpoint)
	a.AcceptedTrusses += len(r.Accepted)
	a.MaxAcceptedTrusses = max(a.MaxAcceptedTrusses, len(r.Accepted))
	a.MaxReceivedTrusses = max(a.MaxReceivedTrusses, r.PendingReceived)
	if r.ReceivingCheckpoint {
		s.hist.recordReverse("accepted_trusses_per_receiving_checkpoint", len(r.Accepted))
	}
	if r.PendingReceived > 0 {
		s.hist.recordReverse("received_trusses_per_receiver", r.PendingReceived)
	}
	if r.RejectedLeaf {
		// A rejected leaf has no child fan-in; exactly its own intact truss returns.
		for _, truss := range r.Returned {
			a.ReturnedTrusses++
			a.RawTrussSum += truss.RawTrussBytes()
			a.OriginMetadataSum += truss.OriginMetadataBytes()
			s.hist.recordReverse("raw_truss_bytes", truss.RawTrussBytes())
			s.hist.recordReverse("origin_metadata_bytes", truss.OriginMetadataBytes())
		}
	}
	if r.CheckpointBytes > 0 {
		a.CombinedCheckpointSum += r.CheckpointBytes
		a.CombinedCheckpointMax = max(a.CombinedCheckpointMax, r.CheckpointBytes)
		s.hist.recordReverse("combined_checkpoint_payload_bytes", r.CheckpointBytes)
	}
	if r.ReturnEdge {
		a.NumReverseReturnEdges++
		a.ReverseRawSum += r.ReverseRawBytes
		a.ReverseEncodedSum += r.ReverseEncodedBytes
		a.ReverseRawMax = max(a.ReverseRawMax, r.ReverseRawBytes)
		a.ReverseEncodedMax = max(a.ReverseEncodedMax, r.ReverseEncodedBytes)
		s.hist.recordReverse("reverse_raw_baggage_bytes", r.ReverseRawBytes)
		s.hist.recordReverse("reverse_encoded_baggage_bytes", r.ReverseEncodedBytes)
		if len(r.Returned) > 0 {
			a.NumNonemptyReverseReturnEdges++
			s.hist.recordReverse("returned_bundle_raw_bytes", r.ReverseRawBytes)
		}
	}
	for _, route := range r.Routes {
		a.MandatoryAbsorptions += btoi(route.Mandatory)
		a.ReverseDistanceSum += route.Distance
		a.ReverseDistanceMax = max(a.ReverseDistanceMax, route.Distance)
		s.hist.recordReverse("reverse_distance_span_hops", route.Distance)
		s.hist.recordReverseRoute(route)
	}
}

func reverseConfigJSON(c *bridge.ReverseConfig) string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return string(b)
}
