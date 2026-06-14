//go:build cpsat

#ifndef RECON_CPSAT_SHIM_H
#define RECON_CPSAT_SHIM_H
#ifdef __cplusplus
extern "C" {
#endif
/* Solve one PCRS cluster block (the "C/I/O" text the engine emits for the
   independent validator) to MAP optimality with CP-SAT, and read back the
   chosen option per item.

   block      : the cluster text (one "C" line, then nItems "I" lines each
                followed by its feasible "O" lines).
   tlim       : per-cluster time limit (seconds).
   solved     : set to 1 iff CP-SAT proved OPTIMAL.
   assign     : caller-provided int[maxItems]; on success assign[i] is the
                0-based index of the chosen option WITHIN ITEM i's emitted
                feasible-option list, or -1 if item i is skipped.
   maxItems   : capacity of assign.

   Returns the optimum objective (matching the engine's bestScore scale). */
long long cpsat_solve_assign(const char* block, double tlim, int* solved,
                             int* assign, int maxItems);
#ifdef __cplusplus
}
#endif
#endif
