# Running the corrected recon sweep on a big box (e.g. c6a.metal, 192 cores)

The multi-drop sweep (`multidrop_sweep.sh`) reconstructs cgp2 + pb2 over both
Uber days, all 3 prime modes, cpd 3-8, and all 6 drop rates. With
`--drop-rates`, each corpus pass decodes a trace once and reconstructs it under
every rate, so 72 passes cover what used to be 432 single-drop configs.

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
WORKERS=28 bash bridges/scripts/repro/multidrop_sweep.sh
```

`WORKERS` x 6 concurrent cpds = total recon threads. Keep it **<= cores - a few**
(e.g. 28x6=168 on 192) so every in-flight reconstruction owns a core and
`recon_ns` is uncontended — clean timing, no separate low-contention pass needed.
Watch progress with `tail -f out/sweep_status.txt` and the `PROGRESS` lines in
`out/*.log` (every 10k traces, all 6 rates' exclEmpty per line).

Drops are per-trace-seeded, so results are bit-identical to separate
`--per-trace-drop-seed --drop-rate` runs (the drop sets are even nested).

## 4. Outputs to bring back

- **Accuracy** (FP-rate bars): `out/<recon>_<day>_<prime>_c<cpd>.log` — each has 6
  `CGP2[dc]/PB2[dc] ... correctExclEmpty=..%` lines. Tiny; copy all.
- **Timing** (recon-time violins): `out/<recon>_timing/timing_<day>_<prime>_c<cpd>_<dc>.csv`
  (per-trace `recon_ns`). Large (~10-15 GB); `tar czf timing.tgz out/*_timing` and
  copy that, or build the violin `.npz` cache on the box and copy just that.

The `scripts/repro/plot_recon_*` and `plot_sbridge_time_violin.py` plotters read
these directly (point their timing dir / dump glob at `out/`).
