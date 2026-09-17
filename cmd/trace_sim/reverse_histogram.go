package main

import (
	"bridges/bridge"
	"sort"
)

// Generic value bins keep checkpoint/truss counts distinct from byte sizes.
type distributionBin struct {
	Value int    `json:"value"`
	Count uint64 `json:"count"`
}
type distributionOutput struct {
	Unit  string            `json:"unit"`
	Count uint64            `json:"count"`
	Sum   uint64            `json:"sum"`
	Min   int               `json:"min"`
	Max   int               `json:"max"`
	Bins  []distributionBin `json:"bins"`
}

var reverseHistogramUnits = map[string]string{
	"raw_truss_bytes": "bytes", "origin_metadata_bytes": "bytes",
	"combined_checkpoint_payload_bytes": "bytes", "returned_bundle_raw_bytes": "bytes",
	"reverse_raw_baggage_bytes": "bytes", "reverse_encoded_baggage_bytes": "bytes",
	"checkpoint_spans_per_trace": "spans", "accepted_trusses_per_receiving_checkpoint": "trusses",
	"received_trusses_per_receiver": "trusses", "reverse_distance_span_hops": "span_hops",
}

func (h *sizeHistograms) recordReverse(metric string, n int) {
	if h == nil || n < 0 {
		return
	}
	h.mu.Lock()
	if h.reverse[metric] == nil {
		h.reverse[metric] = make(map[int]uint64)
	}
	h.reverse[metric][n]++
	h.mu.Unlock()
}
func snapshotReverseHistograms(m map[string]map[int]uint64) map[string]distributionOutput {
	out := make(map[string]distributionOutput, len(reverseHistogramUnits))
	for name, unit := range reverseHistogramUnits {
		v := snapshotHistogram(m[name])
		d := distributionOutput{Unit: unit, Count: v.Count, Sum: v.SumBytes, Min: v.MinBytes, Max: v.MaxBytes, Bins: make([]distributionBin, 0, len(v.Bins))}
		for _, b := range v.Bins {
			d.Bins = append(d.Bins, distributionBin{Value: b.Bytes, Count: b.Count})
		}
		out[name] = d
	}
	return out
}

type reverseRouteKey struct {
	OriginDepth             int  `json:"origin_depth"`
	OriginalCheckpointDepth int  `json:"original_checkpoint_depth"`
	ReceiverDepth           int  `json:"receiver_depth"`
	Mandatory               bool `json:"mandatory"`
}
type reverseRouteCount struct {
	reverseRouteKey
	Count uint64 `json:"count"`
}

func (h *sizeHistograms) recordReverseRoute(r bridge.ReverseRoute) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.routes[reverseRouteKey{r.OriginDepth, r.OriginalCheckpointDepth, r.ReceiverDepth, r.Mandatory}]++
	h.mu.Unlock()
}
func snapshotReverseRoutes(m map[reverseRouteKey]uint64) []reverseRouteCount {
	out := make([]reverseRouteCount, 0, len(m))
	for k, c := range m {
		out = append(out, reverseRouteCount{k, c})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.OriginDepth != b.OriginDepth {
			return a.OriginDepth < b.OriginDepth
		}
		if a.OriginalCheckpointDepth != b.OriginalCheckpointDepth {
			return a.OriginalCheckpointDepth < b.OriginalCheckpointDepth
		}
		if a.ReceiverDepth != b.ReceiverDepth {
			return a.ReceiverDepth < b.ReceiverDepth
		}
		return !a.Mandatory && b.Mandatory
	})
	return out
}
