# Trace cleanliness filter

The bridges simulator and our reference Python implementation filter out
"unclean" traces before running any handler. Both are intended for paper
methodology; this document records exactly what gets filtered and why,
because the filter is non-trivially selective on real tracing data.

## Definition

`_trace_is_clean` (`bridges/trace_simulator.py`, lines 1075–1125) accepts a
trace iff **all five** of the following hold:

1. **Exactly one root span.** A root is a span whose normalized
   `parent_span_id` is `None` (i.e., no `parentSpanID` field and no
   `CHILD_OF` reference resolvable to a span in this trace).

2. **No dangling `CHILD_OF` references.** Every span ID referenced by a
   `CHILD_OF` must exist among the trace's own span IDs. Pointers to
   parents not present in the JSON are rejected.

3. **At most one *distinct* `CHILD_OF` reference per span.** A span with
   two or more `CHILD_OF` references pointing to **different** parent
   spanIDs — i.e. asserting multiple causal parents — makes the whole
   trace fail. Duplicate `CHILD_OF` refs pointing at the same parent
   spanID are deduplicated before this check (they're an instrumentation
   artifact in some Jaeger-era trace producers, not real DAG topology).

4. **Reference / parent agreement.** If a span has a `CHILD_OF` reference
   *and* a `parentSpanID` field, the `CHILD_OF` reference must point at the
   same parent as `parentSpanID`. (For Uber data, where `parentSpanID` is
   absent and `parent_span_id` is *derived* from the first `CHILD_OF`,
   condition 4 is trivially satisfied.)

5. **Span IDs are unique within the trace.** OpenTracing/Jaeger semantics
   require span ID uniqueness. Some traces in the wild duplicate spans
   (likely from retry/sampling mechanics emitting the same span twice).
   Treating duplicates as untrustworthy, we reject the entire trace.

A single bad span anywhere in the trace dirties the whole trace.

## When it is applied

In the Python sim, `_trace_is_clean` runs only when `--random-sample` (or
`--seed`) is passed (see `load_traces_from_dir`, lines 1128–1221). The Go
port mirrors this via the `--require-clean` flag in `cmd/trace_sim`.

We use it consistently when building reproducible random samples for
end-to-end validation of the Go port against the Python reference. Without
it, the simulator processes every trace as-is — including those with
multi-parent fan-in DAGs, dangling pointers, or split roots, all of which
would otherwise need handler-level workarounds.

## Empirical impact (Uber traces)

On a 10K random sample drawn uniformly from
`/mydata/uber/traces/traces-sanitized` (531,085 trace files, seed=42),
measured by `cmd/trace_clean_report`:

| Metric | Value |
|---|---|
| Pass rate | **65.74%** |
| Fail rate (≥1 violation) | 34.26% |

Per-condition violation rates (a single trace can violate multiple):

| Cond | % of all traces | Description |
|---|---|---|
| C1 | 30.09% | root count != 1 |
| C2 | 5.57% | dangling `CHILD_OF` reference |
| C3 | 12.49% | span has > 1 *distinct* `CHILD_OF` reference |
| C4 | 0.00% | (trivially satisfied for Uber data) |
| C5 | 2.21% | duplicate span ID within trace |

The dominant rejection causes are **C1 (multi-root)** and **C3 (genuine
multi-parent)**. C2 and C5 contribute small minorities. The earlier
framing (when C3 was reported at ~64% before dedup) was misleading: **the
vast majority of pre-dedup C3 violations were instrumentation-emitted
duplicate references to the same parent**, not real multi-parent topology.
After deduplicating identical `CHILD_OF` refs, C3 drops by roughly 5×
(63.93% → 12.49%) and the corpus pass rate roughly doubles
(33.40% → 65.74%).

## Why traces have these patterns

The patterns the filter rejects correspond to known issues in distributed
tracing systems, well-documented in the OpenTelemetry / Jaeger community:

### C3 (multi-`CHILD_OF`): mostly instrumentation duplicate refs

Direct inspection of the 10K sample's multi-`CHILD_OF` spans shows a clear
breakdown of the *pre-dedup* population (866K multi-parent spans across
6,393 traces):

- **96.56%** of multi-`CHILD_OF` cases have **both refs pointing to the
  literal same `spanID`** — verified by reading the raw JSON. These are
  duplicate references, almost certainly emitted twice by the Jaeger
  client / instrumentation framework (e.g., once at span construction and
  once during context propagation, or by a wrapping decorator). Our `C3`
  check now deduplicates these before counting.
- **~3.4%** are *lineage-redundant*: refs to two distinct spans where one
  is an ancestor of the other (e.g., parent + grandparent of the same
  chain), again indicating overzealous instrumentation, not DAG topology.
- **~0.01%** are *genuine* DAG fan-in (true multi-parent topology). On the
  filtered cohort (traces passing C1, C2, C5) the residual cross-service
  fan-ins are dominated by a single specific edge (`Service12 →
  Service775`, ~0.13% of fan-ins), suggesting one specific shared-backend
  pattern in the dataset.

### C2 (dangling `CHILD_OF`): typically context-propagation breakage

Dangling refs occur when a span's `CHILD_OF` points at a `spanID` not
present in the trace JSON. The OpenTelemetry community calls these
[orphan spans](https://www.checklyhq.com/docs/traces-open-telemetry/importing-traces/troubleshooting-missing-spans/):
*"segments of a trace that are disconnected from either their parent
spans or from the root span. Missing spans in distributed traces usually
come down to one of a few predictable problems: broken context
propagation, mismatched propagation formats, inconsistent sampling,
uninstrumented libraries, lost async context, fragmented backends, or
unflushed buffers."* Sampling-induced orphans (the parent was sampled
away while the child was kept) and async-boundary orphans
(`OpenTelemetry stores context in thread-local storage, and when
execution moves to a different thread, that storage is empty`) are the
two most common causes documented.

### C1 (multi-root): "phantom span" / fragmented-trace pattern

Empirical: on the 3,009 multi-root traces in the 10K sample, **100% of
them have every root in its own connected component** — i.e. the trace is
a set of fully disconnected subgraphs that share only the `traceID`
field, with **0% per-span `traceID` mismatches** (the spans all *think*
they belong to the same trace). Distribution: p50=5 roots/components,
p99=550, max=9,685 disconnected subgraphs in a single `traceID`. Median
component size is 1 (lone stranded singletons); p99 component size is
2,286 spans.

This pattern matches Honeycomb's documented
[phantom-span case study](https://www.honeycomb.io/blog/opentelemetry-gotchas-phantom-spans),
where unrelated requests merged into one trace of 34,278 spans because
external clients sent their own `traceparent`/`tracestate` headers and
the receiving service's W3C Trace Context propagator naively adopted
them as parent context. Our 9,685-roots-in-one-trace outlier is the same
phenomenon at a comparable scale.

Two complementary mechanisms apply:

- **Header-trust phantom spans** (Honeycomb): inbound `traceparent`
  headers from untrusted callers cause the receiving service to merge
  unrelated requests under one externally-supplied `traceID`. Recommended
  fix is to strip these headers at infrastructure boundaries.
- **Async/background context loss**: when async work is scheduled (timer,
  callback, thread pool, queue handler) without explicitly propagating
  the OpenTelemetry context, the resulting span carries the originating
  `traceID` but has no `parent_span_id`, appearing as a synthetic
  additional root. The OTel community's recommended fix is to use **span
  Links** rather than reusing the `traceID` for cross-context async
  work — see
  [OpenTelemetry context propagation](https://opentelemetry.io/docs/concepts/context-propagation/).

### C5 (duplicate spanIDs): retry/sampling re-emission

Rare (~2.2%) but real. Plausibly the same span being emitted twice via
retry mechanisms or by overlapping sampling decisions. The OpenTracing /
Jaeger spec requires span ID uniqueness within a trace, so we treat
duplicate-ID traces as untrustworthy and reject them.

## Methodological consequence

Both the simulator and CRISP-style critical-path analyses assume traces
are **strict trees**. The cleanliness filter enforces that assumption.

After deduplicating identical `CHILD_OF` references, **about one-third of
the dataset is still excluded** (34.26% on the 10K sample). The
exclusions are dominated by two well-documented data-quality issues
rather than legitimate trace topology:

- **C1 (~30%)**: traces whose `traceID` aggregates structurally
  independent sub-flows — the *phantom-span* / async-context-loss
  pattern described in the OpenTelemetry community literature. These
  could be partially recovered by treating each connected component as a
  separate trace, but that's a non-trivial change to analysis semantics
  and orthogonal to the bridges evaluation.
- **C3 (~12%)**: residual genuine multi-`CHILD_OF` cases after dedup.
  Sub-analysis suggests these are mostly lineage-redundant references
  (parent + ancestor) rather than true DAG topology; less than 0.01% of
  spans appear to be true multi-parent fan-ins.

The framing for paper methodology should therefore be:

> "We exclude traces that fail any of five structural conditions
> (single root, no dangling references, deduplicated single `CHILD_OF`
> per span, reference/parent agreement, unique span IDs). After
> deduplicating identical `CHILD_OF` references emitted by some
> Jaeger-era instrumentation, 65.7% of randomly sampled traces pass.
> The dominant remaining rejection (~30%, C1) corresponds to the
> phantom-span / fragmented-trace pattern documented by Honeycomb and
> others, in which a single `traceID` aggregates structurally
> independent sub-flows due to header-trust propagation issues or
> async-boundary context loss. These are data-quality exclusions, not
> evidence of valid multi-entry causal graphs."

When discussing scope of evaluation in the paper, the relevant numbers
are:

- Full corpus: 531,085 trace files in the sanitized directory
- 10K random sample: 65.74% pass rate after dedup (34.26% excluded)
- Selection criterion: passes `_trace_is_clean`, equivalent to "is a
  strict tree with exactly one root, no dangling parent references, no
  duplicate span IDs, and at most one *distinct* `CHILD_OF` ref per span"

## References

The patterns the filter rejects are recognized in the distributed-tracing
community as known instrumentation / propagation issues, not novel
phenomena specific to Uber's workload:

- **Honeycomb, *OpenTelemetry Gotchas: Phantom Spans*.** Documented case
  study of unrelated requests merging into one trace (34,278 spans) due
  to W3C Trace Context propagator naively adopting external
  `traceparent` headers. <https://www.honeycomb.io/blog/opentelemetry-gotchas-phantom-spans>
- **Checkly, *Troubleshooting missing or orphan spans*.** Catalog of
  causes for orphan spans (broken context propagation, mismatched
  formats, inconsistent sampling, uninstrumented libraries, async
  context loss, fragmented backends, unflushed buffers).
  <https://www.checklyhq.com/docs/traces-open-telemetry/importing-traces/troubleshooting-missing-spans/>
- **OpenTelemetry, *Context Propagation* and *Traces* concept docs.**
  Explicit guidance to use **span Links** (not `traceID` reuse) for
  async/background work whose timing can't be predicted at the producer
  side. <https://opentelemetry.io/docs/concepts/context-propagation/>,
  <https://opentelemetry.io/docs/concepts/signals/traces/>
- **OpenTelemetry specification, issue #1188 — *Support restarting the
  trace with a different trace ID*.** Discussion motivated by the
  observation that *"providers are often reluctant to respect
  customer-supplied trace ID because it can be abused by the caller
  (e.g. sending the same trace ID for all requests)"* — the same
  phenomenon as the phantom-span case study, framed at the spec level.
  <https://github.com/open-telemetry/opentelemetry-specification/issues/1188>
- **OpenTelemetry-JS, *Missing parent spans* discussion #3440.**
  Community Q&A on debugging context-propagation gaps that produce
  orphan spans.
  <https://github.com/open-telemetry/opentelemetry-js/discussions/3440>
- **Jaeger issue #2177 — *trace-without-root-span streaming strategy*.**
  Backend-side observation of fragmented traces where the root span
  arrives last (or not at all) under certain deployment topologies.
  <https://github.com/jaegertracing/jaeger/issues/2177>
- **OneUptime, *How to Debug Missing Spans in OpenTelemetry Distributed
  Traces*.** Engineering write-up of common orphan-span causes and
  remediation patterns.
  <https://oneuptime.com/blog/post/2026-02-06-debug-missing-spans-opentelemetry-distributed-traces/view>
