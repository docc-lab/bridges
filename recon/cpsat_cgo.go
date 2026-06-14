//go:build cpsat

// Wires the CP-SAT cgo shim in as the per-cluster MAP solver. Built only with
// `-tags cpsat` (which pulls in OR-Tools via cgo); the default engine build is
// pure Go and leaves cpsatSolveFn nil, so nothing here affects normal runs.
//
// When active, solveCluster builds the same "C/I/O" cluster block it emits for
// the independent validator, hands it to CP-SAT, and adopts CP-SAT's proven-
// optimal assignment in place of bt()'s best-found one. Enabled per-run by the
// TRACE_RECON_CPSAT env var (see cpsatEnabled in pcrs.go).
package recon

/*
#cgo CXXFLAGS: -std=gnu++17 -DOR_PROTO_DLL= -DPROTOBUF_USE_DLLS -DEIGEN_MPL2_ONLY -DHAVE_CONFIG_H -DUSE_BOP -DUSE_CBC -DUSE_CLP -DUSE_GLOP -DUSE_HIGHS -DUSE_MATH_OPT -DUSE_PDLP -DUSE_SCIP -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/coin -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/highs -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/eigen3
#cgo LDFLAGS: -L/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib -lortools -Wl,-rpath,/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib -ldl
#include <stdlib.h>
#include "cpsat_shim.h"
*/
import "C"

import (
	"os"
	"strconv"
	"unsafe"
)

// per-cluster CP-SAT time limit (seconds). CP-SAT returns as soon as it PROVES
// optimality (sub-second on almost every cluster), so this caps only the few
// certify-hard clusters. Since the shim now accepts the best FEASIBLE
// assignment at the limit (not just a proven-OPTIMAL one), the cap trades a
// little optimality-proving for bounded latency instead of triggering the
// pure-Go bt() grind. Override with TRACE_RECON_CPSAT_TLIM for tuning.
var cpsatTimeLimitS = func() float64 {
	if v := os.Getenv("TRACE_RECON_CPSAT_TLIM"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return 5.0
}()

func init() {
	cpsatSolveFn = cgoCpsatSolve
}

// cgoCpsatSolve solves one cluster block to MAP optimality and returns, per
// item, the chosen option index within that item's emitted feasible-option
// list (-1 = skip). ok is false if CP-SAT did not prove optimality.
func cgoCpsatSolve(block string, nItems int) (assign []int, ok bool) {
	cb := C.CString(block)
	defer C.free(unsafe.Pointer(cb))
	out := make([]C.int, nItems)
	var solved C.int
	var pout *C.int
	if nItems > 0 {
		pout = &out[0]
	}
	C.cpsat_solve_assign(cb, C.double(cpsatTimeLimitS), &solved, pout, C.int(nItems))
	if solved == 0 {
		return nil, false
	}
	assign = make([]int, nItems)
	for i := 0; i < nItems; i++ {
		assign[i] = int(out[i])
	}
	return assign, true
}
