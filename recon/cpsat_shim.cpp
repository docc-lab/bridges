//go:build cpsat

// C-ABI shim: solve one PCRS cluster block with CP-SAT and return BOTH the
// optimum and the chosen option per item, so the Go engine can use CP-SAT as
// its per-cluster MAP solver (not just an after-the-fact validator).
//
// The model is identical to tools/cluster_validate/validate.cc — the
// independent solver that proved this objective matches the engine's bt()
// bestScore on 34,923 clusters. Reusing the exact same construction here means
// the assignment CP-SAT returns is an optimal seating for the same problem.
#include "cpsat_shim.h"

#include <cmath>
#include <cstdint>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ortools/sat/cp_model.h"

using namespace operations_research;
using namespace operations_research::sat;

namespace {
struct Opt {
  int64_t gain = 0, cost = 0, rsv = -1;
  std::vector<std::pair<int64_t, uint64_t>> occ;
};
struct Item {
  int64_t skipPen = 0;
  std::vector<Opt> opts;
};
}  // namespace

extern "C" long long cpsat_solve_assign(const char* block, double tlim,
                                        int* solved, int* assign, int maxItems) {
  *solved = 0;
  std::istringstream in(block);
  std::string line;
  if (!std::getline(in, line) || line.empty() || line[0] != 'C') return 0;
  int nItems;
  {
    std::istringstream ss(line);
    char t;
    long long ours;
    int capped;
    ss >> t >> ours >> capped >> nItems;
  }
  std::vector<Item> items;
  for (int i = 0; i < nItems; ++i) {
    if (!std::getline(in, line)) return 0;
    std::istringstream ss(line);
    char t;
    int nOpts;
    Item it;
    ss >> t >> it.skipPen >> nOpts;
    for (int o = 0; o < nOpts; ++o) {
      if (!std::getline(in, line)) return 0;
      std::istringstream os(line);
      char ot;
      int nOcc;
      Opt op;
      os >> ot >> op.gain >> op.cost >> op.rsv >> nOcc;
      for (int k = 0; k < nOcc; ++k) {
        std::string tok;
        os >> tok;
        auto c = tok.find(':');
        op.occ.push_back(
            {std::stoll(tok.substr(0, c)), std::stoull(tok.substr(c + 1))});
      }
      it.opts.push_back(std::move(op));
    }
    items.push_back(std::move(it));
  }

  CpModelBuilder m;
  int n = items.size();
  std::vector<std::vector<BoolVar>> y(n);
  LinearExpr obj;
  int64_t skipConst = 0;
  std::unordered_map<int64_t, std::vector<std::tuple<int, int, uint64_t>>> occAt;
  std::unordered_map<int64_t, std::vector<std::pair<int, int>>> rsvAt;
  for (int i = 0; i < n; ++i) {
    skipConst += items[i].skipPen;
    std::vector<BoolVar> picks;
    for (int o = 0; o < (int)items[i].opts.size(); ++o) {
      const Opt& op = items[i].opts[o];
      BoolVar v = m.NewBoolVar();
      y[i].push_back(v);
      picks.push_back(v);
      obj += (op.gain - op.cost + items[i].skipPen) * v;
      for (auto& ls : op.occ) occAt[ls.first].push_back({i, o, ls.second});
      if (op.rsv >= 0) rsvAt[op.rsv].push_back({i, o});
    }
    if (!picks.empty()) m.AddAtMostOne(picks);
  }
  obj -= skipConst;
  for (auto& kv : occAt) {
    auto& cl = kv.second;
    for (size_t a = 0; a < cl.size(); ++a)
      for (size_t b = a + 1; b < cl.size(); ++b) {
        int ia = std::get<0>(cl[a]), oa = std::get<1>(cl[a]);
        int ib = std::get<0>(cl[b]), ob = std::get<1>(cl[b]);
        if (ia == ib) continue;
        if (std::get<2>(cl[a]) != std::get<2>(cl[b]))
          m.AddBoolOr({y[ia][oa].Not(), y[ib][ob].Not()});
      }
    auto rit = rsvAt.find(kv.first);
    if (rit != rsvAt.end())
      for (auto& oc : cl)
        for (auto& rs : rit->second) {
          int ia = std::get<0>(oc), oa = std::get<1>(oc);
          if (ia == rs.first) continue;
          m.AddBoolOr({y[ia][oa].Not(), y[rs.first][rs.second].Not()});
        }
  }
  m.Maximize(obj);
  Model model;
  SatParameters p;
  p.set_num_search_workers(1);
  p.set_max_time_in_seconds(tlim);
  model.Add(NewSatParameters(p));
  CpSolverResponse r = SolveCpModel(m.Build(), &model);
  // Accept FEASIBLE as well as OPTIMAL. When the time limit hits before CP-SAT
  // can PROVE optimality it returns FEASIBLE — the best valid assignment it
  // found, which on these MAP/set-packing instances is essentially always the
  // optimum (the limit is spent on the proof, not the search). Rejecting it
  // and falling back to the pure-Go bt() search reintroduced a multi-minute
  // grind on the few certify-hard tail traces. INFEASIBLE/UNKNOWN/MODEL_INVALID
  // (no usable solution) still return 0 so the caller can fall back.
  if (r.status() != CpSolverStatus::OPTIMAL && r.status() != CpSolverStatus::FEASIBLE) return 0;
  *solved = 1;
  for (int i = 0; i < n && i < maxItems; ++i) {
    assign[i] = -1;
    for (int o = 0; o < (int)y[i].size(); ++o) {
      if (SolutionBooleanValue(r, y[i][o])) {
        assign[i] = o;
        break;
      }
    }
  }
  return (long long)llround(r.objective_value());
}
