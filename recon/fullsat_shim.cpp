//go:build cpsat

// C-ABI shim: build a general CP-SAT model from the declarative wire protocol
// (docs/fullsat_shim.md) and solve it single-worker / deterministic. This is the
// model server for --fullsat-pb and --fullsat-cgpb: Go emits the model, this
// builds it with CpModelBuilder and returns the boolean assignment. It is
// independent of the legacy "C/I/O" cluster shim (cpsat_shim.cpp), which stays
// for the per-cluster contested-merge path.
#include "fullsat_shim.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "ortools/sat/cp_model.h"

using namespace operations_research;
using namespace operations_research::sat;

extern "C" long long fullsat_solve(const char* model, double maxDetTime,
                                   long long seed, int* status, int* assign,
                                   int numVars) {
  *status = 0;
  std::istringstream in(model);
  std::string kw;

  CpModelBuilder m;
  std::vector<BoolVar> v;
  bool haveObj = false;
  LinearExpr obj;

  auto lit = [&](long long L) -> BoolVar {
    int idx = (int)std::llabs(L) - 1;
    BoolVar b = v[idx];
    return (L < 0) ? b.Not() : b;
  };

  while (in >> kw) {
    if (kw == "MODEL") {
      int V;
      in >> V;
      v.clear();
      v.reserve(V);
      for (int i = 0; i < V; ++i) v.push_back(m.NewBoolVar());
    } else if (kw == "FIX") {
      int var, val;
      in >> var >> val;
      m.FixVariable(v[var], val != 0);
    } else if (kw == "AMO" || kw == "ALO" || kw == "EO") {
      int k;
      in >> k;
      std::vector<BoolVar> lits(k);
      for (int i = 0; i < k; ++i) {
        long long L;
        in >> L;
        lits[i] = lit(L);
      }
      if (kw == "AMO")
        m.AddAtMostOne(lits);
      else if (kw == "ALO")
        m.AddBoolOr(lits);
      else
        m.AddExactlyOne(lits);
    } else if (kw == "IMP") {
      long long a, b;
      in >> a >> b;
      m.AddImplication(lit(a), lit(b));
    } else if (kw == "LIN") {
      int op, k;
      long long rhs;
      in >> op >> rhs >> k;
      LinearExpr e;
      for (int i = 0; i < k; ++i) {
        long long c;
        int var;
        in >> c >> var;
        e += c * v[var];
      }
      const int64_t kInf = std::numeric_limits<int64_t>::max() / 4;
      if (op == 0)
        m.AddLinearConstraint(e, Domain(-kInf, rhs));   // <= rhs
      else if (op == 1)
        m.AddLinearConstraint(e, Domain(rhs, kInf));    // >= rhs
      else
        m.AddLinearConstraint(e, Domain(rhs, rhs));     // == rhs
    } else if (kw == "OBJ") {
      int k;
      in >> k;
      for (int i = 0; i < k; ++i) {
        long long w;
        int var;
        in >> w >> var;
        obj += w * v[var];
      }
      haveObj = true;
    } else {
      // Unknown keyword: consume the rest of the line and ignore.
      std::string rest;
      std::getline(in, rest);
    }
  }

  if ((int)v.size() != numVars) return 0;  // model/caller var-count mismatch
  if (haveObj) m.Maximize(obj);

  Model model_;
  SatParameters p;
  p.set_num_search_workers(1);
  p.set_random_seed((int)seed);
  if (maxDetTime > 0) p.set_max_deterministic_time(maxDetTime);
  model_.Add(NewSatParameters(p));
  CpSolverResponse r = SolveCpModel(m.Build(), &model_);

  if (r.status() == CpSolverStatus::OPTIMAL)
    *status = 1;
  else if (r.status() == CpSolverStatus::FEASIBLE)
    *status = 2;
  else
    return 0;  // INFEASIBLE / UNKNOWN / MODEL_INVALID

  for (int i = 0; i < numVars && i < (int)v.size(); ++i)
    assign[i] = SolutionBooleanValue(r, v[i]) ? 1 : 0;
  return haveObj ? (long long)llround(r.objective_value()) : 0;
}
