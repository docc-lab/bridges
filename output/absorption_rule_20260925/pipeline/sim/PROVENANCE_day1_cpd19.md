# Day-1 bagsize/pressure simulation — random CPD 1:9, depth_cubic

First day-1 simulation in this series; everything prior was day 2 only.
Produced 2026-09-25 over the full day-1 corpus (521,305 traces /
475,488,643 spans), no sampling.

| item | value |
|---|---|
| engine | commit `8cc104f` |
| binary sha256 | `ffe3c71d6c1fa22b8eac2564733fbc58d0345add22aca9f446d2615763873441` |
| corpus | `/mydata/uber/day1_unfilt_corpus` |
| instance sidecar | `/mydata/uber/endpoint_instance_state/corpus/dee_queue_ids.bin` (950,977,286 records = 2 x 475,488,643 spans) |
| checkpoints | `--checkpoint-range 1:9 --checkpoint-seed 42` |
| reverse | `--reverse-policy depth_cubic --reverse-seed 42`; forward arm `--leaf-reject 0`, reverse `--leaf-reject 1` |
| other | `--bagsize --prefix-len 8 --bloom-fp 0.0001 --pressure-instance-ids`; SB3 adds `--fp-bits 64 --lehmer-ee --dee-dequeue-one --dee-queue-ids` |
| prime | none — `trace_sim` has no prime flags; all sim geometry is NON-PRIME |

Contents: `cpd19_forward_and_reverse/` (forward + reverse, 3 bridges each) and
`cpd19_reverse_passthrough/` (reverse with `--reverse-pass-checkpoints`, 3 bridges).

Gates: 9/9 jobs exit 0; `dee_instance_queues=true` for SB3 only (correct — the
flag is SB3-only by design); `instance_model` = "supplied modeled
queue/endpoint-instance slots (per-event sidecar)" on all nine, i.e. NOT the
service-only fallback.

**Caveat on two jobs:** `pcrb_forward` (cpu_ratio 0.9459) and `cgprb_forward`
(0.9694) fell below the 0.99 CPU gate from I/O wait on cold reads of the shared
38 GB `events.bin`. Byte and count outputs are deterministic and unaffected;
only those two wall times are soft.

## Results (GB) — scopes are like-for-like

EXPORT = `combined_checkpoint_payload_bytes`;
TOTAL = export + `baggage_call_bytes` (forward in-band) + `reverse_encoded_baggage_bytes` (return wire).

```
bridge config           |    emit spans   export  fwd bag  ret wire    TOTAL |  hops  max
PB0    forward          |   249,764,917     6.27    12.41      0.00    18.68 |     -    0
PB0    reverse          |   117,540,835     9.32    12.41     11.54    33.28 |  2.05    8
PB0    reverse+passthru |   129,205,225     9.38    12.41     34.44    56.23 |  6.31  110
CGP0   forward          |   249,764,917     6.54    12.86      0.00    19.39 |     -    0
CGP0   reverse          |   117,540,835     9.62    12.86     11.93    34.42 |  2.05    8
CGP0   reverse+passthru |   129,205,225     9.69    12.86     35.64    58.19 |  6.31  110
SB3    forward          |   249,764,917     8.62    16.41      0.00    25.03 |     -    0
SB3    reverse          |   117,540,835    12.05    16.41     14.60    43.07 |  2.05    8
SB3    reverse+passthru |   129,205,225    12.29    16.41     42.72    71.43 |  6.31  110
```

Reverse vs forward:      export +49 / +47 / +40 %,  total +78 / +77 / +72 %
Passthrough vs reverse:  export +0.6 / +0.7 / +2.0 %, return wire +198 / +199 / +193 %,
                         total +69 / +69 / +66 %, emitting spans +9.9 %

Day 1 replicates day 2 closely (day 2: export +48/+47/+39, total +79/+78/+73,
passthrough return wire +181/+181/+176, emit +9.4%). Reverse cuts emitting spans
249.8M -> 117.5M (-52.9%); passthrough pushes that back up to 129.2M. Hops
2.05 -> 6.31, max 8 -> 110 (the old cap was the CPD ceiling).

Day-2 counterparts: `sim_day2_cpd19_depth_cubic/` (forward+reverse, and see its
`CORRECTION_bytes.md` for the byte-scope rules) and `sim_passthrough_day2/`.
