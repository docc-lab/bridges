//go:build cpsat

// General 0/1 CP-SAT model solver (C-ABI), for the from-scratch CGP
// reconstructor's Phase-4 feasibility-constrained optimization. The Go side
// encodes the whole model (one-hot per fragment, >=2 per HA fan-out,
// all-downstream exclusions, depth-bloom objective) as boolean vars + linear
// constraints; this just builds and solves it. Built only with -tags cpsat.
#include "cpsat_cgp_shim.h"

#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "ortools/sat/cp_model.h"

using namespace operations_research;
using namespace operations_research::sat;

extern "C" int cpsat_solve_bool(const char* model, double tlim, int* vals,
                                int nvars) {
  std::istringstream in(model);
  std::string tok;
  CpModelBuilder m;
  std::vector<BoolVar> x;
  int n = 0;
  bool haveObj = false;

  while (in >> tok) {
    if (tok == "VARS") {
      in >> n;
      x.clear();
      x.reserve(n);
      for (int i = 0; i < n; ++i) x.push_back(m.NewBoolVar());
    } else if (tok == "OBJ") {
      int k;
      in >> k;
      LinearExpr obj;
      for (int j = 0; j < k; ++j) {
        long long c;
        int v;
        in >> c >> v;
        if (v >= 0 && v < n) obj += c * x[v];
      }
      m.Maximize(obj);
      haveObj = true;
    } else if (tok == "CON") {
      int op;
      long long rhs;
      int k;
      in >> op >> rhs >> k;
      LinearExpr e;
      for (int j = 0; j < k; ++j) {
        long long c;
        int v;
        in >> c >> v;
        if (v >= 0 && v < n) e += c * x[v];
      }
      if (op == 0)
        m.AddGreaterOrEqual(e, rhs);
      else if (op == 1)
        m.AddLessOrEqual(e, rhs);
      else
        m.AddEquality(e, rhs);
    }
    // unknown tokens (e.g. a trailing END) are ignored
  }
  (void)haveObj;

  Model model_;
  SatParameters p;
  p.set_num_search_workers(1);
  p.set_max_time_in_seconds(tlim);
  model_.Add(NewSatParameters(p));
  CpSolverResponse r = SolveCpModel(m.Build(), &model_);
  if (getenv("TRACE_RECON_CGP2SOLVE"))
    fprintf(stderr, "CGP2SOLVE vars=%d status=%d (2=OPT 4=FEAS 3=INFEAS 0=UNKNOWN)\n",
            n, (int)r.status());
  if (r.status() != CpSolverStatus::OPTIMAL &&
      r.status() != CpSolverStatus::FEASIBLE)
    return 0;
  for (int i = 0; i < n && i < nvars; ++i)
    vals[i] = SolutionBooleanValue(r, x[i]) ? 1 : 0;
  return 1;
}
