package recon

import (
	"fmt"

	"bridges/bridge"
)

// ReverseEvidence is one intact origin truss collected on another span. It is
// deliberately separate from Span: the origin's ParentID is not present in the
// reverse wire format, and its ordinary span record may have been dropped.
// CarrierSpanID identifies the exported record that retained this evidence; it
// is neither the origin's parent nor a replacement checkpoint-window root.
// The existing Reconstruct* entry points do not yet accept this evidence type.
type ReverseEvidence struct {
	CarrierSpanID uint64
	OriginSpanID  uint64
	OriginDepth   int
	Kind          string
	RawPayload    []byte

	WindowCPD      int
	BloomM, BloomK uint32
	CkptPrefix     []byte
	BloomBits      []byte
	LeafCarrier    bool // property of the origin payload, never of its receiver
	HA             []HAEntry
	SparseOrdinals []bridge.SB3Branch
	DEE            []bridge.DEEQuad
}

// DecodeReverseEvidence extracts the checkpoint trusses in a collected
// bridges.checkpoint value encoded by bridge.EncodeReverseContext. An error
// rejects the whole bundle; no partially decoded bundle is returned. The
// caller supplies the surviving export owner's ID, not any ground-truth data.
func DecodeReverseEvidence(carrierSpanID uint64, encoded []byte, cfg Config) ([]ReverseEvidence, error) {
	segments, err := bridge.DecodeReverseContext(encoded)
	if err != nil {
		return nil, err
	}
	out := make([]ReverseEvidence, 0, len(segments))
	for i, segment := range segments {
		evidence, err := DecodeReverseSegmentEvidence(carrierSpanID, segment, cfg)
		if err != nil {
			return nil, fmt.Errorf("reverse segment %d: %w", i, err)
		}
		out = append(out, evidence)
	}
	return out, nil
}

// DecodeReverseSegmentEvidence validates origin metadata against the intact
// payload and decodes only facts carried on the wire. Routing TTL and the
// simulator-only OriginCheckpointDepth are not reconstruction evidence.
func DecodeReverseSegmentEvidence(carrierSpanID uint64, segment bridge.ReverseSegment, cfg Config) (ReverseEvidence, error) {
	var out ReverseEvidence
	if carrierSpanID == 0 || segment.OriginSpanID == 0 {
		return out, fmt.Errorf("reverse evidence needs nonzero carrier and origin IDs")
	}
	if segment.OriginDepth < 0 {
		return out, fmt.Errorf("reverse evidence has negative origin depth")
	}
	if cfg.PrefixLen < 1 || cfg.PrefixLen > 8 {
		return out, fmt.Errorf("reverse evidence requires checkpoint prefix length 1..8")
	}
	if len(segment.Payload) == 0 {
		return out, fmt.Errorf("reverse evidence has no bridge payload")
	}
	out = ReverseEvidence{
		CarrierSpanID: carrierSpanID,
		OriginSpanID:  segment.OriginSpanID,
		OriginDepth:   segment.OriginDepth,
		Kind:          segment.Kind,
		RawPayload:    append([]byte(nil), segment.Payload...),
	}
	var err error
	out.WindowCPD, out.BloomM, out.BloomK, err = DecodeBloomGeometry(out.RawPayload, cfg)
	if err != nil {
		return ReverseEvidence{}, err
	}
	if out.BloomM == 0 || out.BloomK == 0 {
		return ReverseEvidence{}, fmt.Errorf("reverse evidence has no configured Bloom geometry")
	}
	var depth int
	switch segment.Kind {
	case "checkpoint.pb":
		depth, out.CkptPrefix, out.BloomBits, err = DecodePCRBPayload(out.RawPayload, cfg)
	case "checkpoint.cgpb":
		depth, out.CkptPrefix, out.BloomBits, out.HA, err = DecodeCGPRBPayload(out.RawPayload, cfg)
	case "checkpoint.sb":
		depth, out.CkptPrefix, out.BloomBits, out.HA, out.SparseOrdinals, out.DEE, err = DecodeSB3SpanPayloadFull(out.RawPayload, cfg)
	default:
		err = fmt.Errorf("unsupported reverse checkpoint kind %q", segment.Kind)
	}
	if err != nil {
		return ReverseEvidence{}, err
	}
	if depth != segment.OriginDepth {
		return ReverseEvidence{}, fmt.Errorf("reverse origin depth %d disagrees with payload depth %d", segment.OriginDepth, depth)
	}
	if cfg.RandomizedCheckpoints {
		out.LeafCarrier = bridge.IsLeafPayload(out.RawPayload)
	} else {
		out.LeafCarrier = depth%max(1, cfg.CPD) != 0
	}
	return out, nil
}
