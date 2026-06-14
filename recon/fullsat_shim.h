//go:build cpsat

#ifndef RECON_FULLSAT_SHIM_H
#define RECON_FULLSAT_SHIM_H
#ifdef __cplusplus
extern "C" {
#endif
/* General declarative CP-SAT model server (see docs/fullsat_shim.md).

   model     : the model text. Boolean vars indexed 0..V-1. Literals are
               DIMACS-style signed ints (var i: +(i+1) true, -(i+1) false).
               Lines:
                 MODEL <V>
                 FIX  <var> <0|1>
                 AMO  <k> <lit...>          AtMostOne
                 ALO  <k> <lit...>          AtLeastOne (BoolOr)
                 EO   <k> <lit...>          ExactlyOne
                 IMP  <litA> <litB>         litA -> litB
                 LIN  <0|1|2> <rhs> <k> <c1> <v1> ...   op: 0=LE 1=GE 2=EQ
                 OBJ  <k> <w1> <v1> ...      MAXIMIZE sum(wi*vi); wi may be <0
   maxDetTime: deterministic time budget (NOT wall-clock; machine-independent).
   seed      : CP-SAT random seed.
   status    : set to 0=none(INFEASIBLE/UNKNOWN), 1=OPTIMAL, 2=FEASIBLE.
   assign    : caller-provided int[numVars]; on a solution assign[i] is 0/1.
   numVars   : capacity of assign (must equal the model's V).

   Returns the objective value (rounded), or 0 when no solution. */
long long fullsat_solve(const char* model, double maxDetTime, long long seed,
                        int* status, int* assign, int numVars);
#ifdef __cplusplus
}
#endif
#endif
