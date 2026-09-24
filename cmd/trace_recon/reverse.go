package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"bridges/bridge"
	"bridges/recon"
)

// parseReverseConfig mirrors the simulator's reverse options. The reverse TTL
// range defaults to the forward checkpoint range or fixed distance.
func parseReverseConfig(c config, policy string, p float64, ttl string, seed uint64, q float64, exponent float64, passCheckpoints bool) (*bridge.ReverseConfig, error) {
	if c.mode != "pb0" && c.mode != "cgp0" && c.mode != "sb3" {
		return nil, fmt.Errorf("--reverse-policy supports pb0, cgp0 and sb3")
	}
	if c.pb0Legacy || c.cgp0Legacy || c.verifyInline {
		return nil, fmt.Errorf("reverse trusses require the default greedy engine; legacy reconstructors and --verify do not support them")
	}
	lo, hi := c.checkpointDistance, c.checkpointDistance
	if c.checkpointPolicy != nil {
		lo, hi = c.checkpointPolicy.Min, c.checkpointPolicy.Max
	}
	if ttl != "" {
		if policy != "ttl" {
			return nil, fmt.Errorf("--reverse-ttl-range is valid only for the ttl policy")
		}
		parts := strings.Split(ttl, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("reverse TTL range must be MIN:MAX")
		}
		var err error
		if lo, err = strconv.Atoi(parts[0]); err != nil {
			return nil, fmt.Errorf("reverse TTL range must contain integer distances")
		}
		if hi, err = strconv.Atoi(parts[1]); err != nil {
			return nil, fmt.Errorf("reverse TTL range must contain integer distances")
		}
	}
	rc := &bridge.ReverseConfig{Policy: policy, LeafRejectProbability: q, Exponent: exponent, TTLMin: lo, TTLMax: hi, Seed: seed, PassCheckpoints: passCheckpoints}
	if (policy == "depth_ratio") != (exponent > 0) {
		return nil, fmt.Errorf("--reverse-exponent is required exactly for --reverse-policy depth_ratio")
	}
	if policy == "probability" {
		if p < 0 {
			return nil, fmt.Errorf("--reverse-probability is required exactly for --reverse-policy probability")
		}
		rc.Probability = p
	} else if p >= 0 {
		return nil, fmt.Errorf("--reverse-probability is valid only for the probability policy")
	}
	if err := rc.Validate(); err != nil {
		return nil, err
	}
	return rc, nil
}

// wrapReverse routes unscheduled leaf trusses through the collected span
// stream when reverse trusses are configured. Sinks on the base handler must
// be wired before wrapping; the wrapper leaves forward scheduling unchanged.
func wrapReverse(h bridge.Handler, c config) bridge.Handler {
	if c.reverse == nil {
		return h
	}
	rh, err := bridge.NewReverseHandler(h, *c.reverse)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	return rh
}

// mergeReverseEvidence decodes every returned-truss bundle and restores the
// intended checkpoint set: each truss identifies a checkpoint leaf, bound to
// its surviving record or reconstructed from the truss alone. Trusses are
// checkpoint payloads and are retained regardless of whether the ordinary
// record of the span that carried them survived collection. This is
// collection-side decoding and runs before reconstruction timing starts.
func (ha *harness) mergeReverseEvidence(tid uint64, spans []collSpan, dropped map[uint64]struct{}, survivors []recon.Span) []recon.Span {
	_ = dropped
	var evidence []recon.ReverseEvidence
	for _, s := range spans {
		if s.ckpt == nil {
			continue
		}
		decoded, err := recon.DecodeReverseEvidence(s.spanID, s.ckpt, ha.cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "trace %016x span %016x: reverse bundle: %v\n", tid, s.spanID, err)
			os.Exit(1)
		}
		evidence = append(evidence, decoded...)
	}
	if len(evidence) == 0 {
		return survivors
	}
	merged, err := recon.MergeReverseEvidence(survivors, evidence)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trace %016x: reverse evidence: %v\n", tid, err)
		os.Exit(1)
	}
	return merged
}
