#ifndef CPSAT_SHIM_H
#define CPSAT_SHIM_H
#ifdef __cplusplus
extern "C" {
#endif
/* Solve one cluster block (the "C/I/O" text the Go engine dumps) to MAP
   optimality with CP-SAT. Returns the optimum; *solved=1 iff proven. */
long long cpsat_solve(const char* clusterBlock, double time_limit_s, int* solved);
#ifdef __cplusplus
}
#endif
#endif
