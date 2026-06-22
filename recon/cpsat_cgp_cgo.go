//go:build cpsat

package recon

/*
#include <stdlib.h>
#include "cpsat_cgp_shim.h"
*/
import "C"

import "unsafe"

func init() { cpsatBoolSolveFn = cgoCpsatSolveBool }

// cgoCpsatSolveBool hands a general 0/1 model (VARS/OBJ/CON text) to CP-SAT and
// returns the per-var assignment. ok is false if no usable solution.
func cgoCpsatSolveBool(model string, nvars int, tlim float64) ([]int, bool) {
	cm := C.CString(model)
	defer C.free(unsafe.Pointer(cm))
	out := make([]C.int, nvars)
	var pout *C.int
	if nvars > 0 {
		pout = &out[0]
	}
	if C.cpsat_solve_bool(cm, C.double(tlim), pout, C.int(nvars)) == 0 {
		return nil, false
	}
	vals := make([]int, nvars)
	for i := 0; i < nvars; i++ {
		vals[i] = int(out[i])
	}
	return vals, true
}
