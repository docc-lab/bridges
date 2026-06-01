# Arrival rate vs data loss

We set out to measure whether **trace arrival rate correlates with
dangling-parent rate** in the Uber Zenodo dataset — i.e., does higher
incoming load coincide with more orphaned spans? The original framing
turned out to be unanswerable from this dataset (see *Limits of the
corpus* below), so we pivoted to three structural / per-service proxies
that this corpus *can* answer. The most informative result is a strong
U-shape between per-trace span-emission rate and dangling-parent
probability.

## What we mean by "data loss"

A **dangling span** is a span with at least one `CHILD_OF` reference
whose `spanID` is not present in the trace JSON. After deduping
identical refs (an instrumentation artifact — see
`docs/cleanliness_filter.md`), `5.72%` of the 531,085 sanitized traces
contain at least one dangling-parent span. This is the C2 condition of
the cleanliness filter, counted at per-span granularity rather than as
a trace-level boolean.

The OpenTelemetry community calls these *orphan spans*; documented
causes are broken context propagation, mismatched propagation formats,
inconsistent sampling, uninstrumented libraries, lost async context,
fragmented backends, and unflushed buffers (Checkly,
*Troubleshooting missing or orphan spans*). Several of those —
especially unflushed buffers and sampling-under-load — are
**load-correlated**, which motivates this analysis.

## Limits of the corpus

The Zenodo artifact (`https://zenodo.org/records/13947828`,
*Tale of Errors in Microservices*) is described as "trace1 (first
day)" of the underlying production stream. But the timestamps tell a
different story.

For 531,085 sanitized traces:

| Quantile | days since earliest trace |
|---:|---:|
| q=0.001 | 1.6 |
| q=0.05 | 58.4 |
| q=0.50 | 578.5 |
| q=0.95 | 1100.9 |
| q=1.00 | 1158.3 |

Start times are spread **uniformly across ~3.2 years**, with median
inter-arrival gap 265 s and even peak 1-minute buckets containing
only 5 traces. That is the signature of **per-trace random time
offsets applied during sanitization for privacy** — the timestamps no
longer preserve real production arrival rates. Wall-clock arrival
rate against this corpus measures the sampling distribution, not load.

Consequently, we abandoned the direct arrival-rate analysis and used
three proxies that survive sanitization:

1. **Per-trace structural complexity** — does a "heavier-looking"
   trace lose more spans?
2. **Per-trace span emission rate** — does intra-trace span density
   in time (the closest in-corpus analogue of "instantaneous load")
   correlate with loss?
3. **Per-service activity** — do busier services have higher per-span
   dangling rates?

## Methodology

### Extraction

`cmd/arrival_loss_extract` walks every JSON file in
`/mydata/uber/traces/traces-sanitized/` (531,085 files, ~200 GB raw)
and emits two CSVs:

`traces.csv` (one row per trace):

| column | meaning |
|---|---|
| `start_us` | min span `startTime` in the trace (µs since epoch, sanitized) |
| `end_us` | max `startTime + duration` |
| `trace_duration_us` | `end_us − start_us` |
| `num_spans` | span count in the trace |
| `num_dangling_spans` | spans with ≥1 dangling `CHILD_OF` ref (after intra-span dedup) |
| `max_concurrent_spans` | peak overlap of `[start, end]` intervals via event sweep |
| `max_depth` | max root-to-leaf distance on the in-trace inferred forest |
| `max_fanout` | max direct-child count on that forest |

`services.csv` (one row per service, aggregated across the dataset):

| column | meaning |
|---|---|
| `service_name` | service name from the `processes` table |
| `total_spans` | spans emitted by this service across all traces |
| `total_dangling_spans` | spans where this service was the child side of a dangling `CHILD_OF` |
| `total_traces` | distinct traces this service appeared in |

Construction notes:

- The cleanliness filter is **not** applied. The dependent variable is
  precisely what the filter would remove.
- `max_depth` / `max_fanout` are computed on the in-trace forest:
  each span's parent edge is its first `CHILD_OF` *that resolves to a
  span present in the trace*; dangling refs are dropped, so a dangling
  span becomes an extra root.
- Service-level dangling counts are **child-side only**. We can't
  attribute a missing parent to its (absent) service.
- 32 parallel workers, full corpus in ~29 minutes.

### Analysis

`scripts/arrival_loss_analyze.py` reads both CSVs and computes:

- For each per-trace proxy: Spearman + point-biserial correlation
  with `has_dangling = (num_dangling_spans > 0)`, plus a 20-bin
  decile-lift plot of `P(has_dangling)` vs proxy value.
- For per-service: Spearman correlation of `dangling_rate =
  total_dangling_spans / total_spans` against `total_spans` and
  `total_traces`, restricted to services with ≥ 100 spans (noise
  floor).

Reproducing:

```bash
go build -o /tmp/arrival_loss_extract ./cmd/arrival_loss_extract/
/tmp/arrival_loss_extract \
    --src /mydata/uber/traces/traces-sanitized/ \
    --out-dir /mydata/uber/results_full/arrival_loss/

python scripts/arrival_loss_analyze.py \
    --traces-csv /mydata/uber/results_full/arrival_loss/traces.csv \
    --services-csv /mydata/uber/results_full/arrival_loss/services.csv \
    --out-dir /users/tomislav/bridges/uber_results/plots/arrival_loss/
```

## Results

### (1) Per-trace structural complexity

531,085 traces; baseline `P(has_dangling)` = **5.72 %**. All p-values
are effectively zero given the sample size.

| Proxy | Spearman | Point-biserial |
|---|---:|---:|
| `num_spans` | **+0.314** | **+0.447** |
| `max_concurrent_spans` | +0.306 | +0.408 |
| `trace_duration_us` | +0.288 | +0.334 |
| `max_depth` | +0.275 | +0.300 |
| `max_fanout` | +0.236 | +0.244 |
| `span_emission_rate` | −0.026 | +0.195 |

All five "size-shaped" proxies (everything except emission rate) point
the same way: **bigger / wider / deeper / longer traces are more
likely to contain at least one dangling-parent span**. The natural
mechanism is mechanical: each additional span is another opportunity
for a context-propagation gap, and complex traces touch more service
boundaries (more sampling and propagation decisions).

Caveat: these five variables are highly intercorrelated — they are
all rising-with-trace-size — so their individual coefficients aren't
separable causes. Treat the headline as "trace complexity broadly
predicts loss".

Plots: `decile_num_spans.png`, `decile_max_concurrent_spans.png`,
`decile_trace_duration_us.png`, `decile_max_depth.png`,
`decile_max_fanout.png`.

### (2) The U-shape — span_emission_rate

`span_emission_rate = num_spans / trace_duration_s` is the closest
in-corpus analogue of "spans per second of instantaneous load". Its
Spearman with `has_dangling` looks weak (−0.026), but **the
relationship is non-monotone**, so Spearman can't see it. The 20-bin
decile lift exposes a clean U-shape:

```
bin   span_emission_rate (spans/s)        n      P(dangling)
  0       0.00 –     0.25              26,555     24.21 %
  1       0.25 –     7.66              26,554     22.47 %
  2       7.66 –    27.53              26,554      6.09 %
  3      27.53 –    56.97              26,554      1.89 %
  4      56.97 –    97.46              26,554      1.37 %
  5      97.46 –   126.19              26,555      0.78 %
  6     126.19 –   150.12              26,554      0.27 %   ← minimum
  7     150.12 –   180.81              26,554      0.40 %
  8     180.81 –   210.69              26,554      0.76 %
 …
 15     486.12 –   607.07              26,555      7.49 %
 …
 19   1483.86 – 53683.63               26,555     28.62 %
```

Both extremes lose ~24–29 % of traces; the middle band (100–400
spans/s) loses **< 1 %**. Two distinct mechanisms appear to be
collapsed onto one axis:

- **Left tail (low emission rate).** Many spans spread over a very
  long wall-clock window. These are predominantly *phantom-span* /
  long-stale-traceID traces (C1 multi-root pattern documented in
  `docs/cleanliness_filter.md`): a `traceID` is reused over hours
  or days, accumulating structurally independent sub-flows. They are
  also more likely to contain dangling refs because the "trace" was
  never one coherent flow to begin with.
- **Right tail (high emission rate).** Many spans packed into a very
  short window. These look like real burst-driven loss — instrumentation
  buffer pressure, sampling collisions, hot-path async-context loss.
  This tail is the closest signal in the dataset to the original
  "high load → lost spans" hypothesis.

The U-shape is the strongest qualitative finding of this analysis.

Plot: `decile_span_emission_rate.png`.

### (3) Per-service activity

1,150 distinct services; 1,035 with ≥ 100 spans.

- Pooled dangling rate over these services: **0.06 %** (per-span,
  not per-trace).
- **`dangling_rate` vs `total_spans`** (Spearman) = **+0.574**,
  p ≈ 8e-92.
- **`dangling_rate` vs `total_traces`** (Spearman) = **+0.538**,
  p ≈ 1e-78.
- `Pearson(log total_spans, dangling_rate)` ≈ 0 (not significant) —
  the relationship is monotone but not linear in log space.

Headline: **busier services have higher per-span dangling rates**.
The effect is substantial in rank but small in absolute terms — the
worst-rate offenders are *not* the busiest services. Top-10 by
`dangling_rate` (services with ≥ 100 spans):

| service | total spans | dangling | rate |
|---|---:|---:|---:|
| Service452 | 2,391 | 906 | **37.9 %** |
| Service454 | 3,268 | 587 | 17.96 % |
| Service1087 | 7,809 | 1,034 | 13.24 % |
| Service1095 | 6,008 | 751 | 12.50 % |
| Service955 | 54,189 | 5,976 | 11.03 % |
| Service453 | 2,108,574 | 77,913 | 3.70 % |
| Service1205 | 376 | 12 | 3.19 % |
| Service274 | 34,943 | 488 | 1.40 % |
| Service388 | 3,912,302 | 47,059 | 1.20 % |
| Service1285 | 175 | 2 | 1.14 % |

For comparison, the single biggest service `Service766` (49.6 M
spans) has dangling rate **0.25 %**. The worst-offender list mixes
low-volume outliers (`Service452`, `Service1205`, `Service1285`) with
moderately busy ones (`Service453`, `Service955`); volume alone
doesn't predict bad behaviour.

Plots: `services_total_spans.png`, `services_total_traces.png`.

## Caveats and what this analysis does *not* say

- **No wall-clock load measurement.** Per-trace timestamps are
  sanitized; nothing here measures real production span-emission
  rates over wall-clock time. For that we need a corpus that
  preserves timestamps — internal Uber data, DeathStarBench
  workload replays, or Alibaba's MicroTrace.
- **Multicollinearity in (1).** The five size-shaped proxies all
  move together; their individual coefficients aren't separable
  causes.
- **Child-side attribution only for (3).** Missing-parent loss can't
  be attributed to the parent's service because the parent (and its
  service) is absent from the JSON.
- **Mixture of populations in (2).** The U-shape combines at least
  two distinct phenomena — phantom-span aggregation (left tail) and
  in-trace burst loss (right tail). Separating them requires
  applying C1 filtering before recomputing the curve; that is a
  natural follow-on.

## File index

| Artifact | Path |
|---|---|
| Extractor source | `cmd/arrival_loss_extract/main.go` |
| Analyzer source | `scripts/arrival_loss_analyze.py` |
| Per-trace CSV (~30 MB) | `/mydata/uber/results_full/arrival_loss/traces.csv` |
| Per-service CSV (~27 KB) | `/mydata/uber/results_full/arrival_loss/services.csv` |
| Plots & correlation summary | `uber_results/plots/arrival_loss/` |
