// Independent validator for the PCRS per-cluster branch-and-bound.
//
// It re-solves each exported cluster's MAP optimization with CP-SAT
// (Google OR-Tools) — a completely separate, well-known exact solver —
// to PROVEN optimality, then checks our Go engine's reported bestScore
// against the true optimum. Shares no code with the engine.
//
// Cluster objective (must match recon/pcrs.go): choose at most one option
// per item; an unchosen item is "skipped" at its skip penalty. Maximize
//   sum_chosen (gain - displacementCost)  -  sum_skipped skipPen.
// Feasibility between two chosen options of DIFFERENT items:
//   - they may not occupy the same level with DIFFERENT spans
//     (same span = alter-ego, allowed);
//   - an occupied (span) level may not coincide with a reservation level;
//   - two reservation levels may coincide (sibling synthetics, allowed).
//
// Verdicts per cluster:
//   OVER  : our bestScore > CP-SAT optimum   -> BUG (over-claim)
//   MISS  : our bestScore < optimum AND not capped -> BUG (search missed it)
//   OK    : equal (or capped with ours <= optimum)
//   SKIP  : CP-SAT could not prove optimality in the time limit
//
// Build: see build.sh in this directory.

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "ortools/sat/cp_model.h"

using namespace operations_research;
using namespace operations_research::sat;

struct Opt {
  int64_t gain = 0;
  int64_t cost = 0;
  int64_t rsv = -1;                          // reservation level id (-1 none)
  std::vector<std::pair<int64_t, uint64_t>> occ;  // (levelID, spanID)
};
struct Item {
  int64_t skipPen = 0;
  std::vector<Opt> opts;
};
struct Cluster {
  int64_t ourScore = 0;
  int capped = 0;
  std::vector<Item> items;
};

// Solve one cluster to optimality; returns {status_ok, optimum}.
static std::pair<bool, int64_t> SolveCluster(const Cluster& c, double tlim) {
  CpModelBuilder m;
  int n = c.items.size();
  std::vector<std::vector<BoolVar>> y(n);
  LinearExpr obj;
  int64_t skipConst = 0;  // sum of all skip penalties

  // Per level: occ claims (item,opt,span) and rsv claims (item,opt).
  std::unordered_map<int64_t, std::vector<std::tuple<int, int, uint64_t>>> occAt;
  std::unordered_map<int64_t, std::vector<std::pair<int, int>>> rsvAt;

  for (int i = 0; i < n; ++i) {
    const Item& it = c.items[i];
    skipConst += it.skipPen;
    std::vector<BoolVar> picks;
    for (int o = 0; o < (int)it.opts.size(); ++o) {
      const Opt& op = it.opts[o];
      BoolVar v = m.NewBoolVar();
      y[i].push_back(v);
      picks.push_back(v);
      // value vs skipping this item: (gain-cost) - (-skipPen) = gain-cost+skipPen
      obj += (op.gain - op.cost + it.skipPen) * v;
      for (auto& ls : op.occ) occAt[ls.first].push_back({i, o, ls.second});
      if (op.rsv >= 0) rsvAt[op.rsv].push_back({i, o});
    }
    if (!picks.empty()) m.AddAtMostOne(picks);  // <=1 option (0 = skip)
  }
  obj -= skipConst;  // constant: everyone skipped baseline

  // Conflict clauses.
  for (auto& kv : occAt) {
    auto& claims = kv.second;
    // occ-occ: different spans on the same level conflict (same span OK)
    for (size_t a = 0; a < claims.size(); ++a)
      for (size_t b = a + 1; b < claims.size(); ++b) {
        int ia = std::get<0>(claims[a]), oa = std::get<1>(claims[a]);
        int ib = std::get<0>(claims[b]), ob = std::get<1>(claims[b]);
        if (ia == ib) continue;  // same item: handled by AtMostOne
        if (std::get<2>(claims[a]) != std::get<2>(claims[b]))
          m.AddBoolOr({y[ia][oa].Not(), y[ib][ob].Not()});
      }
    // occ-rsv on the same level always conflict
    auto rit = rsvAt.find(kv.first);
    if (rit != rsvAt.end())
      for (auto& oc : claims)
        for (auto& rs : rit->second) {
          int ia = std::get<0>(oc), oa = std::get<1>(oc);
          if (ia == rs.first) continue;
          m.AddBoolOr({y[ia][oa].Not(), y[rs.first][rs.second].Not()});
        }
  }

  m.Maximize(obj);
  Model model;
  SatParameters p;
  p.set_num_search_workers(1);          // deterministic
  p.set_max_time_in_seconds(tlim);
  model.Add(NewSatParameters(p));
  CpSolverResponse r = SolveCpModel(m.Build(), &model);
  if (r.status() == CpSolverStatus::OPTIMAL)
    return {true, (int64_t)llround(r.objective_value())};
  return {false, 0};
}

int main(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: validate <clusterdump> [time_limit_s=10]\n");
    return 2;
  }
  double tlim = argc >= 3 ? atof(argv[2]) : 10.0;
  std::ifstream in(argv[1]);
  if (!in) { fprintf(stderr, "cannot open %s\n", argv[1]); return 2; }

  long total = 0, ok = 0, over = 0, miss = 0, skipv = 0, cappedN = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] != 'C') continue;
    Cluster c;
    int nItems;
    { std::istringstream ss(line); char t; ss >> t >> c.ourScore >> c.capped >> nItems; }
    for (int i = 0; i < nItems; ++i) {
      std::getline(in, line);
      std::istringstream ss(line);
      char t; int nOpts; Item it;
      ss >> t >> it.skipPen >> nOpts;
      for (int o = 0; o < nOpts; ++o) {
        std::getline(in, line);
        std::istringstream os(line);
        char ot; int nOcc; Opt op;
        os >> ot >> op.gain >> op.cost >> op.rsv >> nOcc;
        for (int k = 0; k < nOcc; ++k) {
          std::string tok; os >> tok;
          auto colon = tok.find(':');
          op.occ.push_back({std::stoll(tok.substr(0, colon)),
                            std::stoull(tok.substr(colon + 1))});
        }
        it.opts.push_back(std::move(op));
      }
      c.items.push_back(std::move(it));
    }
    ++total;
    if (c.capped) ++cappedN;
    auto [solved, opt] = SolveCluster(c, tlim);
    if (!solved) { ++skipv; continue; }
    if (c.ourScore > opt) {
      ++over;
      if (over <= 10)
        printf("OVER  cluster #%ld: ours=%lld > optimum=%lld (items=%zu)\n",
               total, (long long)c.ourScore, (long long)opt, c.items.size());
    } else if (c.ourScore < opt && !c.capped) {
      ++miss;
      if (miss <= 10)
        printf("MISS  cluster #%ld: ours=%lld < optimum=%lld (items=%zu)\n",
               total, (long long)c.ourScore, (long long)opt, c.items.size());
    } else {
      ++ok;
      if (c.capped)
        printf("CAP   cluster #%ld: ours=%lld optimum=%lld %s (items=%zu)\n",
               total, (long long)c.ourScore, (long long)opt,
               c.ourScore == opt ? "[best-found WAS optimal]" : "[truncated below optimum]",
               c.items.size());
    }
  }
  printf("\n=== validation summary ===\n");
  printf("clusters=%ld  OK=%ld  OVER(bug)=%ld  MISS(bug)=%ld  SKIP(timeout)=%ld  capped=%ld\n",
         total, ok, over, miss, skipv, cappedN);
  return (over > 0 || miss > 0) ? 1 : 0;
}
