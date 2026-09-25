# Scripts

Run with the repo venv: `/users/tomislav/bridges/.venv/bin/python <script>`.
All read from `/mydata/uber/...` directly and write figures next to themselves;
the figure scripts also print the tables behind each plot.

Pin to a free core (`taskset -c 20 ...`) if an evaluation run is in flight —
never share a physical core, and never a hyperthread sibling, with a live sim.

## Result tables

| script | produces |
|---|---|
| `bagsize_table.py` | pooled day-1 + day-2 truss sizes and the LaTeX rows for the bagsize table |
| `recon_times.py` | reconstruction time per bridge, per drop rate and pooled over all six |
| `density_sweep.py` | six policies × {stop, pass} at CPD 1:9 and 1:21 |
| `headtohead.py` | shipped (`cubic`+stop) against candidate (`upstream`+pass), both densities |
| `absorption_decomposition.py` | trusses per new emitter, the count x weight conservation identity, and the byte transfer onto root-hosting instances |
| `position_correlations.py` | which structural coordinate predicts where passing hurts, and how homogeneous instances are in it |
| `goloc.py` | non-blank non-comment Go line counter (comments, raw strings, rune literals) |
| `reach.py` | attributes recon lines per bridge by transitive call closure from each entry point |

## Figures

| script | figure |
|---|---|
| `recon_error_time.py` | `recon_error_*` (grouped bars) and `recon_time_*` (violins), both policies, both arms |
| `bagsize_row.py` | `bagsize_row_cpd19`, `bagsize_row_cpd5` |
| `checkpoint_rate.py` | per-instance and per-service checkpoint rate distributions |
| `checkpoint_weight.py` | companion: bytes per emission |
| `relpos_profile.py` | rate and weight against call-path position |
| `hotspot_map.py` | load by depth × leaf share, stop vs pass vs change |
| `hotspot_map_bytes.py` | same map, coloured by bytes per emission |
| `depth_checkpoint_share.py` | checkpointing share by depth, both policies, forward vs reverse |
| `depth_checkpoint_share_passckpt.py` | same, for the 50k stop/pass probe |
| `policy_tradeoff_passthrough.py` | acceptance-policy trade-off frontier, stop vs pass |

`recon_error_time.py` and `bagsize_row.py` label bridges **PB / CGPB / SB** via
their `BR` list; the other scripts carry their own labels.

## Data dependencies

Everything lives under `/mydata/uber`. Nothing here writes to it.

- `sim_day1_cpd19/`, `sim_passthrough_day2/` — full-corpus passthrough sims
- `sim_day2_cpd19_depth_cubic/`, `recon_sim_cpd5_fixed/` — stop-arm baselines
- `recon_final_cpd19_depth_cubic/day{1,2}` — reconstruction, both days
- `relbin_probe_20260924/`, `cpd21_probe_20260924/` — 50k density probes
- `policy_probe_20260924/`, `instdepth_probe_20260924/`, `relpos_probe_20260924/` — 50k policy and position probes
