package bridge

import (
	"math"
	"testing"
)

// The depth-weighted policies are normalized distributions over the origin's
// ancestors: the per-hop acceptance probabilities must sum to 1 across depths
// 0..n-1. Their first-hop value at the immediate parent fixes how quickly a
// truss is absorbed, and is (k+1)/n asymptotically for weights (d+1)^k.
func TestDepthWeightedPoliciesAreNormalized(t *testing.T) {
	for _, policy := range []string{"depth_linear", "depth_quadratic"} {
		for _, n := range []int{1, 2, 3, 5, 8, 15, 40, 90} {
			total := 0.0
			for d := 0; d < n; d++ {
				p := ReverseAcceptanceProbability(policy, 0, d, n)
				if p < 0 || p > 1 {
					t.Fatalf("%s n=%d d=%d: probability %g out of range", policy, n, d, p)
				}
				total += p
			}
			if math.Abs(total-1) > 1e-9 {
				t.Fatalf("%s n=%d: weights sum to %g, want 1", policy, n, total)
			}
		}
	}
}

func TestDepthQuadraticIsSteeperThanLinear(t *testing.T) {
	for _, n := range []int{3, 5, 15, 40} {
		// Deeper receivers accept more often under quadratic; shallow ones less.
		first := ReverseAcceptanceProbability("depth_quadratic", 0, n-1, n)
		linearFirst := ReverseAcceptanceProbability("depth_linear", 0, n-1, n)
		if first <= linearFirst {
			t.Fatalf("n=%d: quadratic first-hop %g must exceed linear %g", n, first, linearFirst)
		}
		if root := ReverseAcceptanceProbability("depth_quadratic", 0, 0, n); n > 2 &&
			root >= ReverseAcceptanceProbability("depth_linear", 0, 0, n) {
			t.Fatalf("n=%d: quadratic at the root %g must be below linear", n, root)
		}
		// Monotone increasing in receiver depth.
		for d := 1; d < n; d++ {
			if ReverseAcceptanceProbability("depth_quadratic", 0, d, n) <=
				ReverseAcceptanceProbability("depth_quadratic", 0, d-1, n) {
				t.Fatalf("n=%d d=%d: quadratic must increase with receiver depth", n, d)
			}
		}
		// Out-of-range receivers never accept.
		for _, d := range []int{n, n + 1} {
			if p := ReverseAcceptanceProbability("depth_quadratic", 0, d, n); p != 0 {
				t.Fatalf("n=%d d=%d: expected 0, got %g", n, d, p)
			}
		}
	}
}

// depth_ratio is NOT normalized: it is a per-hop rule whose first-hop value is
// (n/(n+1))^m. Unlike the normalized weights, that RISES with origin depth, so
// deep origins are absorbed earlier than shallow ones. The exponent tunes it
// from near-certain absorption at the parent (m small) down toward zero.
func TestDepthRatioFirstHopRisesWithOriginDepth(t *testing.T) {
	for _, m := range []float64{1, 2, 4, 8, 16, 24} {
		prev := -1.0
		for _, n := range []int{3, 5, 8, 13, 15, 20, 40} {
			first := reverseAcceptance("depth_ratio", 0, m, n-1, n)
			want := math.Pow(float64(n)/float64(n+1), m)
			if math.Abs(first-want) > 1e-12 {
				t.Fatalf("m=%g n=%d: first hop %g, want %g", m, n, first, want)
			}
			if first <= 0 || first >= 1 {
				t.Fatalf("m=%g n=%d: probability %g must lie strictly in (0,1)", m, n, first)
			}
			if first <= prev {
				t.Fatalf("m=%g n=%d: first hop %g must exceed the shallower origin's %g", m, n, first, prev)
			}
			prev = first
		}
	}
	// Larger exponents absorb less at any fixed receiver, and acceptance still
	// increases with receiver depth within one origin.
	for _, n := range []int{5, 15, 40} {
		for d := 1; d < n; d++ {
			if reverseAcceptance("depth_ratio", 0, 4, d, n) <= reverseAcceptance("depth_ratio", 0, 4, d-1, n) {
				t.Fatalf("n=%d d=%d: must increase with receiver depth", n, d)
			}
			if reverseAcceptance("depth_ratio", 0, 8, d, n) >= reverseAcceptance("depth_ratio", 0, 4, d, n) {
				t.Fatalf("n=%d d=%d: larger exponent must lower acceptance", n, d)
			}
		}
		for _, d := range []int{n, n + 1} {
			if p := reverseAcceptance("depth_ratio", 0, 4, d, n); p != 0 {
				t.Fatalf("n=%d d=%d: expected 0, got %g", n, d, p)
			}
		}
	}
}

func TestDepthRatioConfigValidation(t *testing.T) {
	ok := ReverseConfig{Policy: "depth_ratio", LeafRejectProbability: 1, Exponent: 4}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ReverseConfig{
		{Policy: "depth_ratio", LeafRejectProbability: 1},
		{Policy: "depth_ratio", LeafRejectProbability: 1, Exponent: -1},
		{Policy: "inverse_depth", LeafRejectProbability: 1, Exponent: 4},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("expected rejection for %+v", bad)
		}
	}
}
