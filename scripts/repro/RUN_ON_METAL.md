# Running the corrected recon sweep on a big box (e.g. c6a.metal, 192 cores)

The multi-drop sweep (`multidrop_sweep.sh`) reconstructs **six modes** over both
Uber days, all 3 prime modes, cpd 3-8, and all 6 drop rates. The modes form a
ladder per bridge type:

| mode | what | scored on |
|---|---|---|
| pb0 / cgp0 | lean greedy (deepest bloom-match, early-stop) | path / strict |
| pb1 / cgp1 | greedy over the solver's full candidate set | path / strict |
| pb2 / cgp2 | CP-SAT solver (pooled join-blooms) | path / strict |

(pb* = path scoring: connectivity to the true ancestor. cgp* = strict: connectivity
AND topology.) With `--drop-rates`, each pass decodes a trace once and
reconstructs it under every rate, so 6 modes x 2 days x 3 primes x 6 cpd = 216
passes cover what would otherwise be 1296 single-drop configs.

## 1. Data to copy (store path — no `events.bin` needed)

The `--trace-store` path reads only each corpus's `meta.bin` (trace order), not
the large `events.bin`. Copy:

| source (this eval box) | size | dest on target |
|---|---|---|
| `/mydata/uber/day1.store` | 24 GB | `data/day1.store` |
| `/mydata/uber/day2.store` | 41 GB | `data/day2.store` |
| `/mydata/uber/bignode_state/day1_unfilt_corpus/meta.bin` | 6 MB | `meta/day1_unfilt_corpus/meta.bin` |
| `/mydata/uber/bignode_state/day2_unfilt_corpus/meta.bin` | 10 MB | `meta/day2_unfilt_corpus/meta.bin` |
| `or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/` | 267 MB | `./or-tools_.../` (cgo build + runtime) |

Put the `.store` files on instance-store **NVMe** (not EBS) — the store path
does heavy random reads. ~65 GB total; `events.bin` (~100 GB) is not needed.

```bash
mkdir -p data meta/day1_unfilt_corpus meta/day2_unfilt_corpus
rsync -aP HOST:/mydata/uber/day{1,2}.store data/
rsync -aP HOST:/mydata/uber/bignode_state/day1_unfilt_corpus/meta.bin meta/day1_unfilt_corpus/
rsync -aP HOST:/mydata/uber/bignode_state/day2_unfilt_corpus/meta.bin meta/day2_unfilt_corpus/
rsync -aP HOST:/path/to/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755 .
```

## 2. Build

```bash
export ORT=$PWD/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755
export LD_LIBRARY_PATH=$ORT/lib:$LD_LIBRARY_PATH
cd bridges && go build -tags cpsat -o ../trace_recon ./cmd/trace_recon && cd ..
```

## 3. Run

```bash
export LD_LIBRARY_PATH=$PWD/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib:$LD_LIBRARY_PATH
WORKERS=28 bash bridges/scripts/repro/multidrop_sweep.sh          # full accuracy + timing
```

`WORKERS` x 6 concurrent cpds = total recon threads. Keep it **<= cores - a few**
(e.g. 28x6=168 on 192) so every in-flight reconstruction owns a core and
`recon_ns` is uncontended — **that's what makes the timing clean**; no separate
low-contention pass needed. Watch progress with `tail -f out/sweep_status.txt`
and the `PROGRESS` lines in `out/*.log` (every 10k traces, all 6 rates' exclEmpty).

Drops are per-trace-seeded, so results are bit-identical to separate
`--per-trace-drop-seed --drop-rate` runs (the drop sets are even nested).

**Quick clean-timing pass** (don't need full-corpus accuracy, just the recon-time
distributions fast): set `SAMPLE` to a uniform random subset — same seed => same
subset across modes, so cross-mode timing stays comparable:

```bash
SAMPLE=200000 WORKERS=28 bash bridges/scripts/repro/multidrop_sweep.sh
```

The greedy modes (pb0/cgp0, pb1/cgp1) run much faster than the solver (pb2/cgp2),
so this finishes quickly; keep threads <= cores for the timing to stay honest.

## 4. Outputs to bring back

- **Accuracy** (FP-rate bars): `out/<recon>_<day>_<prime>_c<cpd>.log` — each has 6
  `<MODE>[dc] ... correctExclEmpty=..%` lines (MODE = CGP0/CGP1/CGP2/PB0/PB1/PB2).
  Tiny; copy all.
- **Timing** (recon-time violins): `out/<recon>_timing/timing_<day>_<prime>_c<cpd>_<dc>.csv`
  (per-trace `recon_ns`). Larger; `tar czf timing.tgz out/*_timing` and copy that,
  or build the violin `.npz` cache on the box and copy just that.

Back home: `explode_multidrop_logs.py <out_dir>` turns the logs into the per-drop
files the bar plotter reads (defaults to all six modes); the `plot_recon_*` /
`plot_sbridge_time_violin.py` plotters then read the timing CSVs directly (point
`VIOLIN_TD` / `ERR_SC` at the copied `out/`).
