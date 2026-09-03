# CGP0 fanout-evidence correlation at drop rate 1

Population: first 100,000 unfiltered Day-1 traces; prime-up Bloom sizing; maximal-evidence CGP0; CPD 3–8; drop rate 1; seed 42.

A *carrier window* is the full or partial window terminated by any `_br` emitter. This includes leaf checkpoints; leaves contribute evidence but are excluded from reconnection candidates. A *known fanout* is established by an HA witness or by at least two surviving fragments naming the same missing parent. Local path correctness uses truth only for evaluation.

## Aggregate results

| CPD | clean traces | local carrier paths | true fanouts/window | HA/window | HA path coverage | applicable fanout groups/route | multi-Bloom groups/route | fanout tests/route | route topology | anchor sanity |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 89.8880% | 99.68632% | 0.9648 | 0.1050 | 10.88% | 0.2297 | 0.1139 | 0.0168 | 99.44503% | 100.00000% |
| 4 | 88.9640% | 99.70060% | 1.1908 | 0.1166 | 9.79% | 0.2399 | 0.1075 | 0.0399 | 99.55126% | 100.00000% |
| 5 | 90.5160% | 99.70849% | 1.4109 | 0.1213 | 8.60% | 0.2628 | 0.1228 | 0.0888 | 99.58266% | 100.00000% |
| 6 | 90.7160% | 99.60964% | 1.5405 | 0.1286 | 8.35% | 0.2726 | 0.1238 | 0.1327 | 99.38297% | 100.00000% |
| 7 | 94.8710% | 99.79821% | 1.6965 | 0.1306 | 7.70% | 0.2856 | 0.1328 | 0.1805 | 99.74645% | 100.00000% |
| 8 | 92.5650% | 99.72118% | 1.8844 | 0.1360 | 7.22% | 0.2808 | 0.1164 | 0.2240 | 99.56323% | 100.00000% |

## Within-CPD point-biserial correlations

Positive values mean the evidence measure is associated with a correct local path or route topology. Raw fanout count is also a complexity measure, so its sign must not be interpreted as an isolated treatment effect.

| CPD | true fanouts vs local path | carrier HA vs local path | HA coverage vs local path | required HA vs route topology | fanout groups vs route topology | multi-Bloom groups vs route topology | fanout tests vs route topology |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 0.0027 | 0.0036 | 0.0057 | 0.0213 | 0.0202 | 0.0074 | -0.0431 |
| 4 | -0.0016 | -0.0027 | -0.0012 | 0.0120 | 0.0079 | -0.0007 | -0.0333 |
| 5 | -0.0106 | -0.0037 | -0.0001 | 0.0106 | 0.0071 | 0.0001 | -0.0258 |
| 6 | -0.0215 | -0.0091 | -0.0022 | 0.0020 | 0.0075 | 0.0111 | -0.0302 |
| 7 | -0.0265 | -0.0043 | 0.0017 | 0.0013 | 0.0016 | 0.0028 | -0.0172 |
| 8 | -0.0324 | -0.0089 | 0.0014 | 0.0006 | 0.0048 | 0.0083 | -0.0198 |

## Conditional route-topology error rates

The zero-versus-positive comparisons are descriptive. They condition on CPD but not on trace size or route ambiguity, and route units from the same trace are not statistically independent.

| CPD | no required HA | ≥1 required HA | no applicable fanout group | ≥1 applicable fanout group | no fanout test triggered | ≥1 fanout test triggered |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 0.6143% | 0.1315% | 0.6353% | 0.2722% | 0.5222% | 2.5325% |
| 4 | 0.4812% | 0.2507% | 0.4766% | 0.3556% | 0.4051% | 1.7304% |
| 5 | 0.4464% | 0.2553% | 0.4422% | 0.3426% | 0.3824% | 0.8574% |
| 6 | 0.6240% | 0.5814% | 0.6492% | 0.5252% | 0.5683% | 1.0521% |
| 7 | 0.2564% | 0.2394% | 0.2586% | 0.2400% | 0.2295% | 0.4238% |
| 8 | 0.4386% | 0.4286% | 0.4554% | 0.3861% | 0.4074% | 0.6097% |

## Across-CPD ecological correlations with clean-trace rate

These correlations have only six CPD-level observations. They test whether the proposed mechanism moves with the CPD trend, but cannot establish that the mechanism causes the trend.

| aggregate measure | Pearson r |
|---|---:|
| truth fanouts per window | 0.7546 |
| ha entries per window | 0.6869 |
| ha path coverage | -0.7427 |
| required ha per route | 0.6934 |
| applicable fanout groups per route | 0.8229 |
| multi bloom fanout groups per route | 0.7894 |
| fanout candidate tests per route | 0.8099 |

## Interpretation

The data support the fanout-evidence hypothesis as a **partial mechanism**, not as a complete causal explanation:

- Mean applicable fanout groups per route increase from 0.2297 at CPD 3 to 0.2856 at CPD 7, then fall to 0.2808 at CPD 8. Their six-cell ecological correlation with clean-trace rate is 0.8229. The CPD 7 peak and CPD 8 decline move in the same direction as reconstruction accuracy.
- Within every CPD, route units with at least one applicable fanout group have a lower topology-error rate than units with none. Required HA evidence has the same direction in every cell, although its advantage narrows at the larger CPDs.
- Merely placing more true fanouts on an individual carrier path does not make that path easier: the within-CPD correlations are essentially zero at CPD 3–4 and increasingly negative thereafter. Fanout count is also route complexity; the helpful variable is usable hard/grouped evidence, not raw branching alone.
- A triggered fanout candidate test is associated with more errors. This does not imply that the test harms reconstruction: such tests occur only when an otherwise-admissible candidate reaches a fanout constraint, so the counter preferentially identifies ambiguous routes (endogenous selection).
- Global known-fanout path coverage is 100% in every cell. At drop rate 1 this follows the design: a fanout's second-child lineage carries its HA to a protected periodic or leaf checkpoint. Carrier-local HA coverage falls as windows contain more fanouts, so the benefit comes from pooling evidence across route members rather than from every carrier naming every fanout.

Thus the results are consistent with longer windows improving topology in part because they expose more HA/exact-parent fanout constraints to a shared route decision. They do not show that this is the only CPD-dependent effect; a controlled fanout-evidence ablation or matched-stratum analysis would be needed for a causal estimate.

## Instrumentation invariant

Off-window HA records: **0**. The expected value is zero.

Detailed conditional bins are in `fanout_correlation_route_bins.csv` and `fanout_correlation_window_bins.csv`.
