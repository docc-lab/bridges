package recon

import (
	"bridges/bloom"
	"bridges/bridge"
	"fmt"
)

// payloadBodyOffset is where a payload's body begins: after the type byte, and
// after the assigned-distance byte when checkpoints are randomized.
func payloadBodyOffset(cfg Config) int {
	if cfg.RandomizedCheckpoints {
		return 1 + bridge.PayloadDistanceBytes
	}
	return 1
}

// DecodeBloomGeometry reads the assigned window distance from the byte that
// follows the type byte. Deployment bounds and FP policy determine its exact M
// and K; neither the random seed nor original trace topology is used.
func DecodeBloomGeometry(payload []byte, cfg Config) (distance int, m, k uint32, err error) {
	if !cfg.RandomizedCheckpoints {
		return cfg.CPD, cfg.BloomM, cfg.BloomK, nil
	}
	if len(payload) < payloadBodyOffset(cfg) {
		return 0, 0, 0, fmt.Errorf("missing bridge payload")
	}
	distance, err = bridge.PayloadDistance(payload[1], cfg.CheckpointMin, cfg.CPD)
	if err != nil {
		return 0, 0, 0, err
	}
	m, k = bloom.EstimateParameters(bridge.PCRBBloomCapacity(distance), cfg.BloomFP)
	return distance, m, k, nil
}

func cgpSpanBloom(s *Span, cfg Config) *bloom.Filter {
	if s.BloomM != 0 && s.BloomK != 0 {
		cfg.BloomM, cfg.BloomK = s.BloomM, s.BloomK
	}
	return cgpBloom(s.BloomBits, cfg)
}
