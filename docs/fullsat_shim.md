# Full-SAT shim protocol

Status: design spec (pre-implementation). Companion to `cgp_cpsat_model.md`.

The shim becomes a **general declarative CP-SAT model server**: Go emits one model
per cluster over a line-oriented wire format, the shim builds it with
`CpModelBuilder`, solves single-worker/deterministic, and returns the assignment.
The shim is **mode-agnostic** — `--fullsat-pb` and `--fullsat-cgpb` differ only in
which constraints Go emits, not in the shim. This replaces the legacy `C/I/O`
items×options protocol (which stays only for the legacy cgprb contested-merge call
until that path is retired).

## Two modes (Go-side model builders)

- **`--fullsat-pb`** — full CP-SAT model of the current (HA-free) reconstruction:
  `place` / `thread` / `cover` variables + the existing per-option gains as the
  objective. No `name` variables. This is the **parity target**: shadow-A/B it
  against current PCRS and require exact/wrong/oe/lost (+ audit) parity before
  trusting the CP-SAT path.
- **`--fullsat-cgpb`** — the same model PLUS naming: `name[pos,id]` variables,
  `AtMostOne` per position (exclusivity), channeling implications
  (`place ⟹ name(parent)`, `place ⟹ named ancestors on route`), and HA names
  hard-fixed. This is the CGP topology + connectivity feature.

(`--cgrp` is superseded by these two and its broken hard-`'B'` block is removed.)

## Wire grammar (Go → shim)

Line-oriented ASCII, one statement per line, tokens space-separated. Variables are
booleans indexed `0..V-1`. **Literals are DIMACS-style signed ints**: for variable
`i`, positive literal = `i+1`, negative = `-(i+1)` (0 is unused).

```
MODEL <V>                               # declare V boolean vars
FIX  <var> <0|1>                        # hard-assign a var
AMO  <k> <lit1> ... <litk>              # AtMostOne(literals)
ALO  <k> <lit1> ... <litk>              # AtLeastOne  (= BoolOr / clause)
EO   <k> <lit1> ... <litk>              # ExactlyOne
IMP  <litA> <litB>                      # litA -> litB
LIN  <LE|GE|EQ> <rhs> <k> <c1> <var1> ... <ck> <vark>   # sum(ci*vari) op rhs
OBJ  <k> <w1> <var1> ... <wk> <vark>    # MAXIMIZE sum(wi*vari); wi may be negative
SOLVE <maxDetTime> <seed>               # run; deterministic time budget + rng seed
```

Result (shim → Go):

```
S <OPTIMAL|FEASIBLE|INFEASIBLE|UNKNOWN>
T <k> <var1> ... <vark>                 # vars assigned true (omitted if not SAT)
```

Notes:
- Objective weights are int64. The lexicographic tiers (unplaced ≫ coverage ≫
  likelihood) are encoded as large magnitudes; Go must size tiers so the summed
  objective over the largest cluster stays < 2^63 (tier sizing = max-term-count ×
  tier-weight per band). If headroom is tight, fall back to staged lexicographic
  solves rather than risk overflow.
- `FEASIBLE` (capped by `maxDetTime`) is accepted, exactly as today — the cluster
  ships its best-found complete assignment.

## CpModelBuilder mapping (shim side)

| line | OR-Tools call |
|---|---|
| `MODEL V` | `V` × `model.NewBoolVar()` into a vector |
| `FIX v b` | `model.FixVariable(var[v], b)` |
| `AMO …` | `model.AddAtMostOne(lits)` |
| `ALO …` | `model.AddBoolOr(lits)` |
| `EO …` | `model.AddExactlyOne(lits)` |
| `IMP a b` | `model.AddImplication(litA, litB)` |
| `LIN op rhs …` | `model.AddLinearConstraint(WeightedSum(vars,coeffs), Domain)` (`LE`→[min,rhs], `GE`→[rhs,max], `EQ`→[rhs,rhs]) |
| `OBJ …` | `model.Maximize(WeightedSum(vars,weights))` |
| `SOLVE dt seed` | params: `num_search_workers=1`, `max_deterministic_time=dt`, `random_seed=seed`, then `Solve` |

A signed literal `L` maps to `var[abs(L)-1]` (positive) or `Not(var[abs(L)-1])`
(negative).

## What each Go builder emits

Common (both modes), per cluster, after Go enumerates items/options:
- `place[o,p]` per feasible (orphan **fragment**, position) — `o` is a rigid whole
  fragment, the var commits its entire subtree. `EO` over each fragment's options
  **plus a not-placed literal** carrying the dominant unplaced penalty (keeps the
  placement guarantee soft-but-dominant). A **common-spine coalesced** placement is
  just another position option with combined gain, generated only for >1-path reach
  **within one window**.
- `thread[fs,d,c]` per interior candidate; `AMO` per `(fs,d)`.
- coverage: per **every** open end `e`, `LIN GE 1` over the literals that cover `e`,
  OR a `cover[e]` reified var with a dominant-tier penalty term.
- `OBJ`: per-option **gains** (explained positives) as weights — **counted once**
  (no separate credit for a spine-implied end's coverage); penalties (unplaced,
  coverage-violation) as negative weights in their dominant tiers.

`--fullsat-cgpb` adds:
- `name[pos,id]` vars; `AMO(name[pos,·])` per position (exclusivity — the thing the
  hard-`'B'` wall botched).
- **Pre-coalescing (hard).** Go groups every fragment root + orphan by `ParentID`
  (incl. checkpoint roots, which name into the parent window). Each distinct
  `ParentID` → one shared `name` var, `FIX`ed =1, and each child's parent slot
  hard-bound to it (`FIX`/`IMP`) so all coalesced children route through the one
  node. ≥2 children = a recovered fan-out. This is committed before any scored
  option.
- `FIX name[pos,id]=1` for HA-witnessed branch points (carrier-certain).
- channeling: `IMP place[o,p] → name[above(p), parentid(o)]` and
  `IMP place[o,p] → name[posₖ,idₖ]` for each named ancestor on o's route.

The objective is unchanged in *form* between modes — naming changes which
assignments are feasible/scored, not the objective shape.

## Determinism

`num_search_workers=1`, fixed `random_seed`, `max_deterministic_time` (NOT
wall-clock). Trace-level parallelism (one goroutine per trace) is unchanged, so
single-worker per cluster costs no throughput. See `cgp_cpsat_model.md`.

## Index allocation (Go)

Per cluster, Go maintains dense maps: `(orphan,pos)→var`, `(pos,id)→var`,
`(fs,d,cand)→var`, `end→cover var`, plus penalty vars. It emits the model, reads
back the `T` true-var set, and inverts the maps to apply the assignment to the
ledger/result. The maps are per-cluster and discarded after.

## Validation

1. Build `--fullsat-pb`; current PCRS untouched.
2. Shadow-A/B on the sweep cells: parity-or-better on exact/wrong/oe/lost + the
   fault-injection audit. Only then does `--fullsat-pb` supersede the Go B&B.
3. `--fullsat-cgpb` builds on the validated `--fullsat-pb` model.
