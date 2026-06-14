//go:build cpsat

// CGo binding for the general CP-SAT model shim (fullsat_shim.cpp). Built only
// with `-tags cpsat`. Exposes fullsatSolve, used by the --fullsat-pb /
// --fullsat-cgpb model builders to solve one per-cluster model.
package recon

/*
#cgo CXXFLAGS: -std=gnu++17 -DOR_PROTO_DLL= -DPROTOBUF_USE_DLLS -DEIGEN_MPL2_ONLY -DHAVE_CONFIG_H -DUSE_BOP -DUSE_CBC -DUSE_CLP -DUSE_GLOP -DUSE_HIGHS -DUSE_MATH_OPT -DUSE_PDLP -DUSE_SCIP -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/coin -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/highs -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/eigen3
#cgo LDFLAGS: -L/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib -lortools -Wl,-rpath,/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib -ldl
#include <stdlib.h>
#include "fullsat_shim.h"
*/
import "C"

import "unsafe"

func init() {
	fullsatSolveFn = cgoFullsatSolve
}

// cgoFullsatSolve builds and solves the declarative model `text` (see
// docs/fullsat_shim.md) over numVars boolean variables. Returns the assignment
// (assign[i] = true iff var i is 1), the objective, and status: 1=OPTIMAL,
// 2=FEASIBLE, 0=no solution.
func cgoFullsatSolve(text string, numVars int, maxDetTime float64, seed int64) (assign []bool, obj int64, status int) {
	cm := C.CString(text)
	defer C.free(unsafe.Pointer(cm))
	out := make([]C.int, numVars)
	var pout *C.int
	if numVars > 0 {
		pout = &out[0]
	}
	var st C.int
	o := C.fullsat_solve(cm, C.double(maxDetTime), C.longlong(seed), &st, pout, C.int(numVars))
	status = int(st)
	if status == 0 {
		return nil, 0, 0
	}
	assign = make([]bool, numVars)
	for i := 0; i < numVars; i++ {
		assign[i] = out[i] != 0
	}
	return assign, int64(o), status
}
