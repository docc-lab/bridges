//go:build cpsat

// cpsat_poc: proof-of-concept that OR-Tools CP-SAT is callable from Go via
// cgo. Reads a cluster dump (TRACE_RECON_CLUSTERDUMP output), solves each
// cluster through the C++ shim, and reports the same OVER/MISS/CAP audit
// the standalone validator does. Build: go build -tags cpsat ./tools/cpsat
package main

/*
#cgo CXXFLAGS: -std=gnu++17 -DOR_PROTO_DLL= -DPROTOBUF_USE_DLLS -DEIGEN_MPL2_ONLY -DHAVE_CONFIG_H -DUSE_BOP -DUSE_CBC -DUSE_CLP -DUSE_GLOP -DUSE_HIGHS -DUSE_MATH_OPT -DUSE_PDLP -DUSE_SCIP -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/coin -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/highs -isystem /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/include/eigen3
#cgo LDFLAGS: -L/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib -lortools -Wl,-rpath,/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib -ldl
#include <stdlib.h>
#include "shim.h"
*/
import "C"

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

func solve(block string, tlim float64) (int64, bool) {
	c := C.CString(block)
	defer C.free(unsafe.Pointer(c))
	var s C.int
	v := C.cpsat_solve(c, C.double(tlim), &s)
	return int64(v), s != 0
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cpsat_poc <clusterdump> [tlim=10]")
		os.Exit(2)
	}
	tlim := 10.0
	if len(os.Args) >= 3 {
		tlim, _ = strconv.ParseFloat(os.Args[2], 64)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<28)

	var total, ok, over, miss, skip, capped int64
	var block strings.Builder
	var ours int64
	var cap bool
	flush := func() {
		if block.Len() == 0 {
			return
		}
		total++
		if cap {
			capped++
		}
		opt, solved := solve(block.String(), tlim)
		if !solved {
			skip++
		} else if ours > opt {
			over++
			if over <= 10 {
				fmt.Printf("OVER #%d ours=%d > opt=%d\n", total, ours, opt)
			}
		} else if ours < opt && !cap {
			miss++
			if miss <= 10 {
				fmt.Printf("MISS #%d ours=%d < opt=%d\n", total, ours, opt)
			}
		} else {
			ok++
		}
	}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "C ") {
			flush()
			block.Reset()
			fields := strings.Fields(line)
			ours, _ = strconv.ParseInt(fields[1], 10, 64)
			cap = fields[2] == "1"
		}
		block.WriteString(line)
		block.WriteByte('\n')
	}
	flush()
	fmt.Printf("\n=== cgo CP-SAT audit ===\nclusters=%d OK=%d OVER=%d MISS=%d SKIP=%d capped=%d\n",
		total, ok, over, miss, skip, capped)
	if over > 0 || miss > 0 {
		os.Exit(1)
	}
}
