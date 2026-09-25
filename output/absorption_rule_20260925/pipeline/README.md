# Pipeline — how the data under /mydata/uber was produced

`scripts/` reads files; this directory made them. Nothing here was tracked
before, so the analyses were reproducible only from data already on disk.

Paths inside these files point at the machine each ran on and are kept verbatim
rather than rewritten — `driver.py` referring to `/home/ubuntu/...` and
`/dev/shm/...` is the big-node layout, not this one.

## probes/

Nine 50k-trace day-1 probes, all `--mode pcrb --workers 1 --first 50000
--checkpoint-seed 42 --reverse-seed 42 --prefix-len 8 --bloom-fp 0.0001`, one
job per physical core with SMT siblings left idle.

| file | what it isolates |
|---|---|
| `policy_probe.sh` | six acceptance policies × {stop, pass}; the mandatory-absorption masking result |
| `passckpt_probe.sh` | forward / reverse-stop / reverse-pass, one policy |
| `svcdepth_probe.sh` | adds `--pressure-service-depths` (joint service × depth cells) |
| `instdepth_probe.sh` | adds the span-depth moments |
| `relpos_probe.sh` | adds the relative-position moments |
| `relbin_probe.sh` | adds the span-binned call-path marginal; the CPD 1:9 reference |
| `cpd21_probe.sh` | same at `--checkpoint-range 1:21`, which needs the two-byte context |
| `cpdsparse_probe.sh` | `1:16` and `14:21`; superseded by `cpd21_probe.sh` and never run |
| `ctxregress.sh` | byte-identity check that the context change left 1:9 untouched |

The probes were run in that order and each adds an accumulator to the one
before, so the later outputs are supersets. `relbin_probe` is the one the
call-path figures read.

## sim/

Full-corpus bagsize and pressure runs.

- `sim_day2_cpd19_depth_cubic.run_sim.sh` — day-2 forward + reverse, stop arm.
- `sim_day2_passthrough.run_sim.sh` — the staged day-2 passthrough run. Superseded:
  the run that produced `sim_passthrough_day2/` was executed on the big node from
  an equivalent script that was not preserved.
- `passthrough_compare.py` — regenerates the stop-vs-pass table in that run's README.
- `PROVENANCE_passthrough_day2.md`, `PROVENANCE_day1_cpd19.md` — engine commit,
  binary sha256, corpus, flags and hygiene gates for the two passthrough sims.
- `CORRECTION_bytes.md` — the export / in-band / return-wire scoping correction
  those READMEs refer to.

**`sim_day1_cpd19` has no run script.** It was produced on another machine and
only its README survived; `PROVENANCE_day1_cpd19.md` records the full flag set,
so it is reconstructible but not re-runnable as-is. That run supplies half the
pooled truss-size table.

## recon/

The reconstruction sweeps behind the error and violin figures, and the
`aggregate_cells.json` that `scripts/recon_error_time.py` reads.

- `<policy>_<day>.driver.py` — shards the corpus, runs `trace_recon` per
  (mode, arm, drop rate), pins one job per core. The four differ only in
  paths and shard counts (30 for day 1, 50 for day 2).
- `<policy>_<day>.aggregate.py` — per-cell counters into `aggregate_cells.json`.
  All five variants differ; none is a copy of `aggregate_template.py`.
- `compare_pooled.py` — checks a re-run against a previous sweep cell by cell.
- `reverse_vs_forward.py` — the arm comparison.

These runs are engine `b53c0b0` and the stop arm; they predate
`--reverse-pass-checkpoints`.
