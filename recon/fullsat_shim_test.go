//go:build cpsat

package recon

import "testing"

// Smoke test: the general CP-SAT model shim solves a trivial model end-to-end
// over the wire protocol. Exercises MODEL/EO/OBJ + the DIMACS literal mapping
// and the CGo round trip.
func TestFullsatShimSmoke(t *testing.T) {
	if fullsatSolveFn == nil {
		t.Fatal("fullsatSolveFn nil (built without -tags cpsat?)")
	}

	// ExactlyOne(v0,v1,v2); maximize 3*v0 + 1*v1 + 1*v2 -> v0=1, obj=3.
	model := "MODEL 3\nEO 3 1 2 3\nOBJ 3 3 0 1 1 1 2\n"
	assign, obj, status := fullsatSolveFn(model, 3, 0, 42)
	if status != 1 {
		t.Fatalf("status = %d, want 1 (OPTIMAL)", status)
	}
	if obj != 3 {
		t.Errorf("obj = %d, want 3", obj)
	}
	if !assign[0] || assign[1] || assign[2] {
		t.Errorf("assign = %v, want [true false false]", assign)
	}

	// AtMostOne + an implication + a >=1 linear constraint, negative-literal use.
	//   vars: 0,1,2,3
	//   ALO(v1,v2)        -> at least one of v1,v2
	//   IMP v1 -> v0      (lit 2 -> lit 1)
	//   AMO(v1,v2)        -> not both
	//   LIN >= 1 over v3  -> v3 must be 1
	//   maximize 5*v2 + 1*v0  (prefer v2; v2 forces nothing, v1 would force v0)
	m2 := "MODEL 4\nALO 2 2 3\nIMP 2 1\nAMO 2 2 3\nLIN 1 1 1 1 3\nOBJ 2 5 2 1 0\n"
	a2, o2, s2 := fullsatSolveFn(m2, 4, 0, 42)
	if s2 == 0 {
		t.Fatalf("m2 unsolved (status 0)")
	}
	// optimum: v2=1 (AMO forces v1=0), v3=1 (LIN), and v0 is free so it goes to 1
	// for the +1; obj = 5*v2 + 1*v0 = 6.
	if !a2[0] || a2[1] || !a2[2] || !a2[3] {
		t.Errorf("m2 assign = %v, want v0=1 v1=0 v2=1 v3=1", a2)
	}
	if o2 != 6 {
		t.Errorf("m2 obj = %d, want 6", o2)
	}
}
