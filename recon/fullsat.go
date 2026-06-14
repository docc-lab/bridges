package recon

// fullsatSolveFn is the general CP-SAT model solver, wired by fullsat_cgo.go
// under `-tags cpsat`; nil in the pure-Go build. Builders for --fullsat-pb /
// --fullsat-cgpb emit a declarative model (docs/fullsat_shim.md) and call this.
//
//	text       declarative model text
//	numVars    number of boolean vars declared (MODEL <numVars>)
//	maxDetTime deterministic time budget (machine-independent), 0 = none
//	seed       CP-SAT random seed
//
// returns assign[i]=true iff var i is 1, the objective, and status
// (1=OPTIMAL, 2=FEASIBLE, 0=no solution / solver not linked).
var fullsatSolveFn func(text string, numVars int, maxDetTime float64, seed int64) (assign []bool, obj int64, status int)
