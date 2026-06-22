//go:build cpsat

package recon

import "testing"

// TestCpsatBoolToy proves the general 0/1 shim builds, links OR-Tools, and
// solves a model with a linear objective AND both >= and <= constraints —
// exactly the primitives Phase 4 needs (the ">=" is the HA >=2-children
// constraint the old narrow shim could not express).
func TestCpsatBoolToy(t *testing.T) {
	if cpsatBoolSolveFn == nil {
		t.Fatal("cpsatBoolSolveFn nil under -tags cpsat")
	}
	// maximize x0+x1+x2  s.t.  x0+x1+x2 <= 2  and  x0+x1 >= 1
	model := "VARS 3\n" +
		"OBJ 3 1 0 1 1 1 2\n" +
		"CON 1 2 3 1 0 1 1 1 2\n" + // <= 2
		"CON 0 1 2 1 0 1 1\n" // >= 1
	vals, ok := cpsatBoolSolveFn(model, 3, 5.0)
	if !ok {
		t.Fatal("solver returned no solution")
	}
	sum := vals[0] + vals[1] + vals[2]
	if sum != 2 {
		t.Fatalf("expected optimum sum=2 under <=2, got vals=%v (sum=%d)", vals, sum)
	}
	if vals[0]+vals[1] < 1 {
		t.Fatalf(">=1 constraint on x0+x1 violated: vals=%v", vals)
	}
	t.Logf("toy solved: vals=%v sum=%d", vals, sum)
}
