# Where returning trusses are absorbed, and what it costs

Produced 2026-09-25. Covers the absorption-rule evaluation
(`--reverse-pass-checkpoints` on vs off), the checkpoint-density sweep that
motivated the two-byte context, and the pooled day-1 + day-2 numbers used in the
paper's truss-size table and reconstruction-time figures.

Bridge names follow the paper: **PB**, **CGPB**, **SB** (sim modes `pcrb`,
`cgprb`, `sb3`; recon modes `pb0`, `cgp0`, `sb3`).

## Provenance

| dataset | corpus | traces | engine | arm |
|---|---|---|---|---|
| `sim_day1_cpd19/cpd19_reverse_passthrough` | day 1 | 521,305 | `8cc104f` | pass |
| `sim_passthrough_day2/cpd19_reverse_passthrough` | day 2 | 843,274 | `8cc104f` | pass |
| `sim_day2_cpd19_depth_cubic` | day 2 | 843,274 | pre-`8cc104f` | stop |
| `recon_final_cpd19_depth_cubic/day{1,2}` | both | 1,364,579 | `b53c0b0` | stop |
| `relbin_probe_20260924` | day 1, first 50k | 50,000 | `4a6f428` | both |
| `cpd21_probe_20260924` | day 1, first 50k | 50,000 | `4a6f428` | both |

All CPD 1:9 seed 42 unless stated, `depth_cubic`, `--leaf-reject 1` on reverse
arms, `--prefix-len 8 --bloom-fp 0.0001`, `--reverse-seed 42`.

**Two configurations are mixed in the paper tables.** Sizes come from the
passthrough arm at `8cc104f`; reconstruction times come from the stop arm at
`b53c0b0`, which predates the flag. If they appear together as one result that
needs stating, or the recon needs rerunning under passthrough.

## Truss sizes — pooled day 1 + day 2, 1,364,579 traces, CPD 1:9, passthrough

Exact from per-value histogram bins; pooling is an exact bin merge and bin counts
were verified against the declared totals. Bytes.

| bridge | field | n | median | mean | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| PB | checkpoint payload | 345,728,844 | 30 | 74.4 | 342 | 5,852 | 640,621 |
| PB | forward baggage | 1,275,014,678 | 28 | 26.1 | 33 | 33 | 33 |
| PB | reverse baggage | 1,275,014,678 | 33 | 70.7 | 815 | 6,160 | 1,062,254 |
| CGPB | checkpoint payload | 345,728,844 | 30 | 76.9 | 349 | 5,873 | 646,093 |
| CGPB | forward baggage | 1,275,014,678 | 28 | 27.0 | 42 | 42 | 42 |
| CGPB | reverse baggage | 1,275,014,678 | 33 | 73.2 | 843 | 6,370 | 1,063,586 |
| SB | checkpoint payload | 345,728,844 | 45 | 97.7 | 445 | 7,071 | 824,586 |
| SB | forward baggage | 1,275,014,678 | 32 | 34.6 | 86 | 116 | 15,212 |
| SB | reverse baggage | 1,275,014,678 | 37 | 88.0 | 1,013 | 7,660 | 1,432,461 |

Forward baggage for PB and CGPB is fixed-width (prefix + fixed-geometry Bloom):
p99, p99.9 and max are all the same value, so it has no tail at all. Only SB
varies, because of its sparse ordinals and DEE quads. It is also the one field
whose median exceeds its mean.

`scripts/bagsize_table.py` regenerates this and prints the LaTeX rows.

## Reconstruction time — pooled day 1 + day 2, CPD 1:9, stop arm

Every per-trace row, not the sampled subset the violin figure draws. Feasible
traces with `recon_ns > 0`; `recon_ns` excludes I/O by construction. Each trace
contributes up to one reconstruction per drop rate, so n counts reconstructions
and the pooled mean is n-weighted across drop rates.

| bridge | arm | n | mean ms | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|
| PB | reverse | 7,968,832 | 6.671 | 0.957 | 8.175 | 97.63 | 684.70 | 32,395.6 |
| CGPB | reverse | 7,968,832 | 7.670 | 1.094 | 9.517 | 113.85 | 804.01 | 30,657.9 |
| SB | reverse | 7,968,832 | 36.702 | 3.161 | 22.467 | 343.45 | 3,816.97 | 148,759.3 |
| PB | forward | 7,822,626 | 5.534 | 0.790 | 7.258 | 83.19 | 499.64 | 20,268.7 |
| CGPB | forward | 7,822,626 | 6.130 | 0.900 | 8.388 | 95.92 | 518.52 | 17,878.7 |
| SB | forward | 7,822,626 | 16.217 | 2.222 | 19.832 | 258.99 | 1,451.13 | 55,017.3 |

Times are non-monotonic in drop rate, peaking at 0.5–0.75. Per-drop-rate figures
are in `scripts/recon_times.py` output.

## Reconstruction lines of Go

Non-blank, non-comment, excluding tests, the simulator and plotting/analysis.
`scripts/goloc.py` counts; `scripts/reach.py` attributes by transitive call
closure from each mode's entry point.

**All three bridges share one engine.** `ReconstructPB0` and `ReconstructCGP0`
are `reconstructFullEvidenceGreedyTopology` with flags (`NoFanout`,
`SB3IgnoreOrdinals`); SB adds the ordinal and structure layers. There is no
disjoint per-bridge count.

| | lines | funcs |
|---|---|---|
| PB reachable | 3,273 | 89 |
| CGPB reachable | 3,272 | 89 |
| SB reachable | 3,891 | 102 |
| **union** | **3,908** | **104** |

Partition of the union: exclusive to PB 9, to CGPB 8, to SB 627; shared by all
three 3,264. Excludes the `--pb0-legacy`/`--cgp0-legacy` paths (unused in every
paper run); including them gives 4,129.

Not counted: payload decoders invoked by the driver (`DecodePCRBPayload` 40,
`DecodeCGPRBPayload` 56, `DecodeSB3SpanPayloadFull` 40 — 4,044 with them);
`cmd/trace_recon/` driver 4,080; the whole `recon/` package 11,925, most of which
serves eight other modes not in the paper. The closure over-approximates and
cannot follow interface dispatch, so per-mode figures are upper bounds.

## The absorption rule

Default: a returning truss is absorbed at the first scheduled checkpoint above
its origin. `--reverse-pass-checkpoints` leaves the trace root as the only forced
absorber.

**Mandatory absorption is load-bearing.** 67.2% of absorptions under
`depth_cubic` and 82.1% under `upstream_pressure`; passing drops these to 42.1%
and 27.6%. Policies whose acceptance falls with height were barely running — the
cap, not the policy, placed most checkpoints.

**Absorbing at a scheduled checkpoint is free in count terms.** That span was
already emitting, so the truss adds bytes to an existing emission rather than
creating one. 5.93 trusses pile onto each new emitter under the default, 4.14
when passing (12.54 vs 6.00 for `upstream_pressure`).

**Total checkpoint bytes are conserved** (+0.5% for cubic, −0.9% for 1/n): the
same trusses absorbed elsewhere. What changes is the factorization — emissions
rise 9.9% and mean weight falls 8.5%. The cost is a ~3× rise in reverse WIRE
bytes (24.35 → 72.54 B per return edge), which is transport, not emission.

**Passing raises the per-instance checkpoint rate tail.** p99 rate goes
0.51–0.69 → 0.86–0.95 across five policies; the p10 is pinned at 0.169 in all
ten arms, so the whole change is the upper tail. Measured per service the effect
has the same sign and about half the magnitude, and no service ever approaches
saturation — per-service reporting would have missed it.

**The interior is relieved and the root pays.** Emission weight falls 26% for the
leaf-free instances carrying 73.7% of checkpoint bytes and is unchanged for
leaf-dominated ones, while the 0.5% of instances hosting root spans go from 1,807
to 3,917 B per emission. Net, 121.9 MB — 13% of the corpus total — relocates onto
484 instances, almost exactly cancelled by savings over the other 59,488. The
trace root absorbs mandatorily under both rules, so it is the one position where
passing raises count and weight at once.

**Cost is graded by call-path position**, `depth/(depth+height)`. Against the
per-instance rate change this correlates −0.650, versus −0.365 for leaf share,
+0.019 for absolute depth and +0.007 for depth normalized by trace depth. Under
the default the rate is flat at 0.23–0.31 along the whole path and rises only at
the root; passing lifts it everywhere by a margin growing monotonically toward
the root (+0.010 at position 0.9, +0.227 at 0.2 for cubic; +0.006 and +0.498 for
`upstream_pressure`, which reaches 0.997 at the root).

## Checkpoint density sweep

50k day-1 traces, PB, six policies × {stop, pass}, at CPD 1:9 (E[D]=5) and 1:21
(E[D]=11). 1:21 requires the two-byte context and payload distance byte.

pass/stop ratio of p99 worst checkpoint payload:

| policy | 1:9 | 1:21 |
|---|---|---|
| inverse_depth | 2.31 | 1.51 |
| depth_linear | 2.55 | 1.69 |
| depth_quadratic | 2.82 | 1.82 |
| depth_cubic | 3.13 | 1.85 |
| depth_quartic | 3.42 | 1.89 |
| **upstream_pressure** | **0.64** | **0.49** |

Every deep-biased policy moves toward 1.0 as checkpoints get sparser but stays
above it. `upstream_pressure` is the only policy whose acceptance RISES as a
truss climbs, and the only one where passing reduces the worst checkpoint — p99
payload 43,064 → 21,315 B and p99 pile-up 778 → 415 at 1:21, both roughly halved,
holding at p50, p90 and p99 alike.

Sparse checkpoints are expensive regardless of arm: at 1:21 forward baggage rises
1,205 MB → 2,098 MB (+74%) and checkpoint bytes 906 MB → 1,359 MB (+50%), because
each window Bloom holds ~11 entries instead of ~5. That dwarfs the two bytes the
format change cost.

Head-to-head, shipped (`cubic`+stop) vs candidate (`upstream`+pass) at 1:21: 11%
fewer emitting spans, 7% fewer checkpoint bytes, worst checkpoint roughly halved
at p50 and p99 — paid for with 2.10× reverse wire and a worse instance
saturation tail (p99 rate 0.792 → 0.958). At 1:9 the candidate is a wash.

## Caveats

- Instances are the supplied modeled queue/endpoint-instance slots from the
  per-event sidecar, not observed physical servers. Effect directions reproduce
  at service granularity, which does not depend on that model; absolute
  per-instance rates do.
- The absorption-rule, density and hotspot results are 50k day-1 probes. Root
  concentration in particular scales with trace count differently from interior
  effects and should be confirmed on the full corpus.
- Reconstruction at 1:21 was never run. Sparser checkpoints mean longer bridges
  and larger Bloom windows, so accuracy and time could move independently of
  these byte numbers.
- Three metrics were tried and discarded before the per-instance rate: over-share
  (a ratio of within-service shares, unstable for small instances and it
  reintroduces the service), self-ranked top-1% share (selects the top on the
  same noisy quantity it measures, inflating the sparser arm), and traffic-ranked
  top-1% share (unbiased but dominated by the few huge instances, so a saturating
  small instance is invisible). Do not reuse them.
