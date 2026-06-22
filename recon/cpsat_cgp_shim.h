//go:build cpsat

#ifndef RECON_CPSAT_CGP_SHIM_H
#define RECON_CPSAT_CGP_SHIM_H
#ifdef __cplusplus
extern "C" {
#endif
/* General 0/1 CP-SAT model solver for the from-scratch CGP reconstructor.

   model : a text model with line types (whitespace-separated tokens):
             VARS <n>
                 declares n boolean decision vars, indices 0..n-1.
             OBJ <k> <c_0> <v_0> ... <c_{k-1}> <v_{k-1}>
                 maximize sum_i c_i * x[v_i].   (at most one OBJ line)
             CON <op> <rhs> <k> <c_0> <v_0> ...
                 linear constraint sum_i c_i*x[v_i] <op> rhs,
                 op: 0 => >=, 1 => <=, 2 => ==.   (any number of CON lines)
   tlim  : time limit (seconds).
   vals  : caller int[nvars]; on success vals[i] = 0/1 for var i.
   nvars : capacity of vals (must equal the model's VARS n).

   Returns 1 if CP-SAT returned a usable (OPTIMAL or FEASIBLE) assignment,
   else 0. */
int cpsat_solve_bool(const char* model, double tlim, int* vals, int nvars);
#ifdef __cplusplus
}
#endif
#endif
