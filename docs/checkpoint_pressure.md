# Checkpoint export pressure

`trace_sim --checkpoint-pressure FILE` adds per-service and optional modeled
instance totals without changing checkpoint scheduling, reverse routing, or DEE
transport. It supports PB0 (`pcrb`), CGP0 (`cgprb`), and SB3 in corpus mode with
one worker. The output can be used alone or with the existing per-trace CSV and
size histograms.

```bash
trace_sim --corpus /path/to/corpus --mode pcrb --bagsize --workers 1 \
  --checkpoint-range 2:8 --checkpoint-seed 42 \
  --prefix-len 8 --bloom-fp 0.0001 \
  --reverse-policy inverse_depth --leaf-reject 1 --reverse-seed 42 \
  --pressure-instance-ids /path/to/dee_queue_ids.bin \
  --checkpoint-pressure pressure.json
```

For the fixed-distance control use `--checkpoint-distance 5` in place of
`--checkpoint-range 2:8`. For the reverse-disabled control use `--leaf-reject 0`.
Using the same reverse wrapper in all arms keeps checkpoint exports associated
with completion consistently. For SB3 add `--lehmer-ee --fp-bits 64
--dee-dequeue-one --dee-queue-ids /path/to/dee_queue_ids.bin`.

The pressure sidecar is independent of DEE transport. It uses the existing
one-uint32-per-event queue-ID format and validates its length and each span's
start/end assignment. When no separate pressure sidecar is provided, SB3 can
reuse its DEE queue-ID sidecar. Supplying pressure attribution to PB0 or CGP0
does not enable DEE transport.

## Counters and populations

The `bridges.checkpoint_pressure.v2` JSON contains configuration and accounting
definitions, global totals, and rows for active services, modeled instances
when supplied, and depths. Entities with spans but zero checkpoints are
retained. A service/instance processes each span once, not once per event.

Leaf status comes from the complete input topology. A span is a leaf iff it
has no recorded children. Roots with children are non-leaves; a singleton
trace's root is a leaf. A service may host both roles on different calls.
Root counters overlap the leaf/non-leaf populations.

Each final owner counts as at most one checkpoint, even when it accepts many
returned trusses. Leaf plus non-leaf checkpoint counts equal total checkpoint
counts. Original, forced-leaf, and reverse-promoted categories also partition
the checkpoint population. Returned trusses are charged to their accepting
owner for export-byte accounting.

The byte counters keep three quantities separate:

- `own_br_bytes`: the owner's own bridge attribute key and value.
- `raw_checkpoint_content_bytes`: own `_br` plus accepted truss bytes, origin
  metadata, and any explicit reverse TTL bytes. Returned attribute names,
  the bundle version, and record length framing are excluded.
- `combined_checkpoint_payload_bytes`: own `_br` plus the actual binary
  returned-truss attribute, including its attribute name.

Forward baggage is attributed to the span at call start. Reverse baggage is
attributed to the completing span's outgoing logical return edge. Baggage
counters are separate from collector-bound checkpoint payloads. These counters
measure serialized bytes and checkpoint frequency; they are not measured CPU
costs, physical network framing, or real-time burst rates.
Checkpoint-byte counters exclude separately emitted end-event attributes such
as `_d` and `_oc`; they measure checkpoint content, not the complete telemetry
export of SB3.

The Uber sidecar currently assigns globally disjoint queue slots within each
service/endpoint pool. Different parents and traces can reuse the same slot
concurrently. These are modeled endpoint-instance slots, not observed physical
service instances. Per-service measurements use the recorded service labels.

## Distribution definitions

For a service or modeled instance:

```text
checkpoint_rate = num_checkpoint_spans / num_spans
overall_rate = total_checkpoint_spans / total_spans
relative_checkpoint_rate = checkpoint_rate / overall_rate
```

A traffic-weighted CDF gives each entity weight equal to its processed span
count. Its vertical axis is the fraction of span traffic, not the fraction of
services. An entity-weighted CDF gives each active entity equal weight. Both
include zero-checkpoint entities. The traffic-weighted mean relative rate is
one; the equal-entity mean need not be one.

The traffic-weighted coefficient of variation is:

```text
CV = sqrt(sum(num_spans_i * (relative_checkpoint_rate_i - 1)^2)
          / sum(num_spans_i))
```

It measures concentration after accounting for overall checkpoint volume.
Report it alongside total checkpoint frequency; a proportional decrease in all
entities' checkpoint counts leaves this concentration measure unchanged.
Cost figures use absolute bytes per processed span on shared axes across bridge
types. Main checkpoint distributions use absolute checkpoint percentages.
Relative variation appears in separate figures, with absolute standard deviation
beside unitless CV. Every CV divides by its own run's mean: a lower CV can coexist
with a higher byte cost or greater absolute spread. It must not be presented as a
byte-cost reduction.
Analogous rates and concentration measures can be computed for raw and encoded
checkpoint bytes per processed span. Keep leaf checkpoint probability
(`leaf checkpoints / leaf spans`) distinct from the fraction of all checkpoints
that happen to be leaves.

## Four-way ablation

The first pressure experiment uses the same chronological 100,000-trace prefix
as the reverse-size comparison: 91,945,996 spans, with every selected trace
completed. The four arms are fixed CPD 5 and uniform CPD 2–8, each with reverse
propagation disabled or enabled. Reverse-enabled arms use inverse-depth and
leaf rejection probability one. All arms use no-prime and the same input and
modeled instance assignments.

PB0 supplies five paired checkpoint/reverse seeds, 42 through 46, for the
checkpoint-count distributions. Its deterministic fixed-distance, reverse-off
control is run once and reused in paired contrasts. The same four arms at seed
42 are also measured for CGP0 and SB3 to validate identical checkpoint placement
and compare byte pressure. SB3 uses Lehmer coding, 64-bit fingerprints, and
service-instance DEE queues with single-pop.

Seed bands describe the minimum and maximum over the five seeds, not confidence
intervals or independent workload samples. Fixed CPD 5 and uniform CPD 2–8 have
the same mean assigned distance; realized checkpoint volumes can differ because
of finite paths, branching, and mandatory leaves. Spatial concentration is the
subject of this experiment; temporal burstiness requires a separate timing
study.
