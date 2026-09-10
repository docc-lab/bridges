package recon

import (
	"bridges/bloom"
	"bridges/bridge"
	"fmt"
)

// DecodeBloomGeometry reads the assigned window distance from the existing
// type-byte bits. Deployment bounds and FP policy determine its exact M and K;
// neither the random seed nor original trace topology is used.
func DecodeBloomGeometry(payload []byte, cfg Config) (distance int, m, k uint32, err error) {
	if !cfg.RandomizedCheckpoints {
		return cfg.CPD, cfg.BloomM, cfg.BloomK, nil
	}
	if len(payload) == 0 {
		return 0, 0, 0, fmt.Errorf("missing bridge payload")
	}
	distance, err = bridge.PayloadDistance(payload[0], cfg.CheckpointMin, cfg.CPD)
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
