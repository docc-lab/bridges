# Reproducing the bridges evaluation (cgp2 accuracy/timing + bagsize figures)

End-to-end recipe for the full-corpus results: reconstruction correctness (FP
rate), per-trace reconstruction time, baggage/payload overhead, and the paper
figures. Paths below are for the eval machine — adjust to your layout.

## 0. Prerequisites & build

The from-scratch call-graph-preserving reconstructor (`cgp2`) solves a CP-SAT
model via OR-Tools (cgo), gated behind the `cpsat` build tag.

```bash
export ORT=/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755   # OR-Tools C++ 9.15
export LD_LIBRARY_PATH=$ORT/lib:$LD_LIBRARY_PATH
go build -tags cpsat -o trace_recon_cgp2 ./cmd/trace_recon
```

`LD_LIBRARY_PATH` must be set for every run of the binary (the solver is a
shared lib). Without `-tags cpsat` the reconstructor returns empty results.

## 1. Corpora

Full unfiltered Uber corpora, built in **lenient** mode (`cmd/trace_archive`,
`loader requireClean=false`: drops dangling-parent subtrees, collapses identical
duplicate spans, rejects only cyclic / duplicate-conflict / unrooted traces;
CRISP timestamp normalization applied to all):

- `/mydata/uber/bignode_state/day1_unfilt_corpus` — 521,305 traces
- `/mydata/uber/bignode_state/day2_unfilt_corpus` — 843,274 traces

## 2. Reconstruction sweep (accuracy + per-trace timing)

`cgp2` over cgprb payloads, full 8-byte checkpoint roots (`--prefix-len 8`),
deterministic (sorted candidate/option/constraint order + fixed CP-SAT seed).

Single config:

```bash
trace_recon_cgp2 --corpus <day_corpus> --mode cgp2 \
  --checkpoint-distance <CPD> --prefix-len 8 \
  --drop-rate <R> --per-trace-drop-seed <PRIME_FLAGS> \
  --workers 8 --timing <out.csv> -o /dev/null
```

- **Prime (bloom-sizing) modes** (`<PRIME_FLAGS>`):
  - prime-up  : `--prime-m`                       (round m up to next prime)
  - prime-down: `--prime-m --prime-m-bytecap`     (round down if up adds a byte)
  - no-prime  : *(omit both)*                      (no prime rounding)
- Swept axes: **drop rates** 0.05 0.25 0.5 0.75 0.95 1.0; **CPD** 3–8; **days** 1,2.
- Full sweep driver (216 configs, 6-way waves): `scripts/repro/cgp2_timing_sweep.sh`.

Each run prints to stderr and (with `--timing`) writes a per-trace CSV:

- `CGP2 ... correctExclEmpty=X%` — reconstruction **correctness on the feasible
  (non-empty) subset**. Reconstruction **FP rate = 100 − exclEmpty** (fraction of
  traces with ≥1 incorrectly-reconnected fragment). `empty` traces are no-drop
  (nothing to reconstruct); `credit`/`exclEmpty` include/exclude them.
- `TIMING[all]` and `TIMING[exclEmpty]` — per-trace wall-time (ms): mean, p50,
  p90, p99, max. Use **exclEmpty** (times only the traces that did work).
- CSV columns: `tid, survivors, spans, dropped, feasible, recon_ns`.

Notes: recon time depends on **CPD and drop rate** (peaks at mid-drop ~0.5–0.75,
falls toward the extremes), but is **independent of prime mode**. `PROGRESS`
lines (every 10k traces) let you track a long run.

## 3. Consolidate results

`scripts/repro/gen_timing_results.py` scans the per-config stderr dumps and emits
a consolidated markdown (pooled day1+day2 exclEmpty accuracy by mode + exclEmpty
recon-time by day). Output: `/mydata/uber/bignode_state/cgp2_timing_results.md`.
Per-trace timing CSVs: `/mydata/uber/bignode_state/cgp2_timing/`.

## 4. Bagsize (baggage / payload overhead)

Per-trace bagsize JSONs (one per scheme × cpd, both days) live in
`/mydata/uber/bignode_state/bagsize_full_day{1,2}/`
(`pcrb_cpdN.json`, `cgprb_cpdN.json`, `sbridge_fw_cpdN.json`; produced by
`cmd/trace_sim --bagsize`, sbridge with `--prefix-len 8 --fp-bits 64`).

Pool day1+day2, filter single-span traces, and map to the plotter's scheme keys
(`pcrb→pb`, `cgprb→cgpb`, `sbridge_fw→structural`):

```bash
python3 scripts/repro/pool_bagsize_plotdata.py
# -> /mydata/uber/bignode_state/bagsize_pooled_plotdata/pooled_<key>_cpd<N>.json
```

Column semantics (per-trace aggregates, not per-emit): `min/med/p99` are over
per-trace means; `avg` is the exact corpus-weighted mean; `max` is the true
global single-emit max. Single-span traces (`num_spans==1`) are excluded.

## 5. Figures

### Bagsize scatter figures (`plot_bagsize_field.py`)

Median + IQR error bars over a jittered per-trace point cloud, per cpd. The
midpoints are **not** a trend, so `--no-line`. Final style:

```bash
DD=/mydata/uber/bignode_state/bagsize_pooled_plotdata
FLAGS="--bridges pb cgpb structural --cpds 2 3 4 5 6 7 8 \
  --data-dir $DD --prefix pooled_ --file-template {prefix}{mode}_cpd{cpd}.json \
  --ymax-iqr-pct 50 --no-line --short-labels \
  --label-fs 60 --tick-fs 52 --legend-fs 50 \
  --marker-size 20 --cloud-size 55 --scatter-alpha 0.28 \
  --err-lw 4 --capsize 16 --x-offset-step 0.19 --jitter 0.05 \
  --figw 16 --figh 8.5 --ylabel bytes"

python3 plot_bagsize_field.py --field avg_baggage_call        $FLAGS --out baggage_bytes.pdf
python3 plot_bagsize_field.py --field amortized_by_checkpoint $FLAGS --out payload_bytes.pdf
python3 plot_bagsize_field.py --field max_baggage_call        $FLAGS --out baggage_bytes_worstcase.pdf
python3 plot_bagsize_field.py --field max_checkpoint_payload  $FLAGS --out payload_bytes_worstcase.pdf
```

Legend: `--short-labels` renders P (Path/pcrb), CG (Call-Graph-Preserving/cgprb),
S (Structural/sbridge). Any `--out` extension works (`.pdf` vector for the paper).

### Reconstruction-error bar charts (grouped by drop rate)

Error % = `100 − exclEmpty`, day1+day2 pooled feasible-weighted. Regenerate the
`cgp2_err_prime{up,down}_bydrop.{pdf,png}` figures with
`scripts/repro/plot_recon_error_bydrop.py` — it reads `clean`/`feasible` from each
config's stderr dump and computes the pooled FP rate (bars grouped by drop rate,
one per cpd).

## Provenance summary

| artifact | location |
|---|---|
| binary | `trace_recon_cgp2` (built `-tags cpsat`) |
| timing CSVs | `/mydata/uber/bignode_state/cgp2_timing/` |
| consolidated results | `/mydata/uber/bignode_state/cgp2_timing_results.md` |
| bagsize JSONs | `/mydata/uber/bignode_state/bagsize_full_day{1,2}/` |
| pooled plot data | `/mydata/uber/bignode_state/bagsize_pooled_plotdata/` |
| figures | `/users/tomislav/*_bytes*_unfiltered.{pdf,png}` |
