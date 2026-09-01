# Day-1 S-Bridge Lehmer, instance, and dequeue comparison

This experiment uses the complete endpoint-enhanced Uber day-1 corpus and
crosses three independent S-Bridge choices: service versus endpoint-instance
DEE queues, legacy versus Lehmer EE/DEE group encoding, and drain-all versus
single-record DEE pickup.

## Notation

- `B`: baseline S-Bridge (service queues, drain all, legacy EE/DEE encoding)
- `I`: endpoint-instance DEE queues
- `L`: per-group Lehmer EE/DEE coding
- `D`: dequeue at most one FIFO DEE record per call

`D` is intentionally distinct from the reconstruction-safe trace-ID
coalescing (`C`) and 4-byte owner IDs (`O`) in the earlier comparison table.
Neither `C` nor `O` is varied by this experiment.

## Inputs and fixed configuration

- Corpus: `/mydata/uber/endpoint_instance_state/corpus`
- Instance sidecar: `/mydata/uber/endpoint_instance_state/corpus/dee_queue_ids.bin`
- Traces: 521,305
- Spans: 475,488,643
- Events: 950,977,286
- Mode: `sbridge`
- Checkpoint distance: 4
- Checkpoint prefix: 8 bytes
- Fingerprints: 64 bits
- Histogram bins in raw JSON: exact byte sizes

The eight variants all process the same corpus in the same event order. A
baggage histogram has 474,967,338 observations, exactly one per non-root span
(`spans - traces`).

## Aggregate size results

All variants contain 474,967,338 baggage calls and 262,104,331 emitted
payloads. Their minima are also identical: 13 bytes for baggage and 16 bytes
for payload.

| Variant | Baggage total | Baggage mean | Baggage max | Payload total | Payload mean | Payload max | Combined total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `B` | 11,578,303,522 B | 24.377052 B | 126,464 B | 10,086,595,402 B | 38.483131 B | 126,477 B | 21,664,898,924 B |
| `B+L` | 11,497,642,934 B | 24.207229 B | 128,571 B | 10,017,976,680 B | 38.221332 B | 128,584 B | 21,515,619,614 B |
| `I` | 11,424,705,832 B | 24.053666 B | 46,590 B | 9,932,910,790 B | 37.896782 B | 57,668 B | 21,357,616,622 B |
| `I+L` | 11,356,737,024 B | 23.910564 B | 39,929 B | 9,873,549,895 B | 37.670304 B | 57,756 B | 21,230,286,919 B |
| `B+D` | 11,209,161,104 B | 23.599857 B | 10,457 B | 9,859,113,153 B | 37.615224 B | 11,232 B | 21,068,274,257 B |
| `B+L+D` | 11,152,667,061 B | 23.480914 B | 7,247 B | 9,804,756,490 B | 37.407839 B | 7,685 B | 20,957,423,551 B |
| `I+D` | 11,356,308,367 B | 23.909662 B | 5,024 B | 9,929,143,283 B | 37.882408 B | 10,832 B | 21,285,451,650 B |
| `I+L+D` | 11,291,583,202 B | 23.773389 B | 3,223 B | 9,869,941,461 B | 37.656537 B | 7,540 B | 21,161,524,663 B |

Lehmer's effect relative to the same queue and pickup model is:

| Base variant | Baggage reduction | Payload reduction | Combined reduction | Baggage-max change | Payload-max change |
| --- | ---: | ---: | ---: | ---: | ---: |
| `B` | 80,660,588 B (0.696653%) | 68,618,722 B (0.680296%) | 149,279,310 B (0.689038%) | +2,107 B | +2,107 B |
| `I` | 67,968,808 B (0.594928%) | 59,360,895 B (0.597618%) | 127,329,703 B (0.596179%) | -6,661 B | +88 B |
| `B+D` | 56,494,043 B (0.503999%) | 54,356,663 B (0.551334%) | 110,850,706 B (0.526150%) | -3,210 B | -3,547 B |
| `I+D` | 64,725,165 B (0.569949%) | 59,201,822 B (0.596243%) | 123,926,987 B (0.582215%) | -1,801 B | -3,292 B |

Lehmer reduces total baggage, payload, and combined bytes in all four matched
comparisons. It does not guarantee a smaller maximum observation: the service
drain-all maximum increases by 2,107 bytes, and the instance drain-all payload
maximum increases by 88 bytes. The complete distributions therefore matter
more than the maxima alone.

Single-record pickup reduces the service-queue combined total by 596,624,667
bytes (2.753877%) without Lehmer and 558,196,063 bytes (2.594376%) with Lehmer.
For instance queues, the reductions are 72,164,972 bytes (0.337889%) and
68,762,256 bytes (0.323888%), respectively.

Instance queues reduce drain-all combined bytes by 307,282,302 bytes
(1.418342%) without Lehmer and 285,332,695 bytes (1.326165%) with Lehmer. With
single pickup they instead cost 217,177,393 bytes (1.030827%) and 204,101,112
bytes (0.973885%) more than service queues, while materially shrinking the
worst baggage call.

## No-Lehmer DEE queue lifecycle

| Statistic | `B` | `I` | `B+D` | `I+D` |
| --- | ---: | ---: | ---: | ---: |
| Pickup attempts | 475,488,643 | 475,488,643 | 475,488,643 | 475,488,643 |
| Empty pickups | 468,704,539 | 462,135,929 | 445,820,123 | 445,858,888 |
| Non-empty pickup calls | 6,784,104 | 13,352,714 | 29,668,520 | 29,629,755 |
| Enqueued records | 29,671,136 | 29,671,136 | 29,671,136 | 29,671,136 |
| Enqueued bytes | 926,232,754 | 926,232,754 | 926,232,754 | 926,232,754 |
| Dequeued records | 29,669,548 | 29,661,663 | 29,668,520 | 29,629,755 |
| Dequeued bytes | 926,180,692 | 925,895,748 | 926,149,788 | 924,906,628 |
| Final backlog queues | 407 | 6,824 | 422 | 6,870 |
| Final backlog records | 1,588 | 9,473 | 2,616 | 41,381 |
| Final backlog bytes | 52,062 | 337,006 | 82,966 | 1,326,126 |
| Maximum queue records | 4,590 | 2,061 | 4,590 | 2,061 |
| Maximum queue bytes | 126,429 | 57,620 | 126,429 | 78,430 |

For every variant, `enqueued - dequeued = final backlog` exactly for both
records and bytes. Drain-all can also finish with a backlog when a queue has no
later pickup; `D` intentionally leaves more queued records at the end.

The four no-Lehmer simulations ran concurrently. Individual elapsed times were
1h08m09.787s (`B`), 1h07m59.161s (`I`), 1h07m51.692s (`B+D`), and
1h07m25.516s (`I+D`). Both instance runs consumed the complete DQID sidecar
without early EOF, extra records, trailing data, or simulator-state errors.

## Distribution outputs

The CSV files contain numeric percentages suitable for plotting; the Markdown
files contain the same values with percent signs. Rows use the shared ranges
from `0-15` through `98304-131071`, plus an explicit `131072+` overflow row so
that every column accounts for the complete distribution.

The primary tables are split by DEE pickup paradigm:

| Pickup paradigm | Baggage | Payload |
| --- | --- | --- |
| Base (drain all) | `baggage_distribution_base.csv` / `.md` | `payload_distribution_base.csv` / `.md` |
| Single-pop | `baggage_distribution_single_pop.csv` / `.md` | `payload_distribution_single_pop.csv` / `.md` |

Every split table uses these descriptive columns:

1. `Service queues / ordinal-list EE/DEE`
2. `Service queues / Lehmer-coded EE/DEE`
3. `Endpoint-instance queues / ordinal-list EE/DEE`
4. `Endpoint-instance queues / Lehmer-coded EE/DEE`

`baggage_distribution.csv` / `.md` and `payload_distribution.csv` / `.md`
retain all eight configurations as secondary cross-check tables. Their labels
include the `Base:` or `Single-pop:` paradigm prefix rather than abbreviations.

## Distribution plots

- `size_distribution_base.svg`: base/drain-all pickup, linear scale
- `size_distribution_single_pop.svg`: single-pop pickup, linear scale
- `size_distribution_base_log.svg`: base/drain-all pickup, log scale
- `size_distribution_single_pop_log.svg`: single-pop pickup, log scale

Each vector figure has separate baggage-call and emitted-payload panels. The
linear figures treat byte ranges categorically and use a linear percentage
y-axis. The log figures use logarithmic byte position and percentage mass to
show the extreme tail. Queue model is encoded by color; EE/DEE encoding is
encoded by solid versus dashed lines and circle versus square markers.

Regenerate all four plots without third-party Python packages using:

```bash
python3 scripts/plot_sbridge_size_distributions.py
```

## Raw exact histograms

| Variant | Queue model | Pickup | Lehmer | File |
| --- | --- | --- | --- | --- |
| `B` | Service | Drain all | No | `sbridge_raw_fw_cpd4_day1_service_drain_all.json` |
| `B+L` | Service | Drain all | Yes | `../dee_instance_histograms/sbridge_lehmer_fw_cpd4_day1_service_queues.json` |
| `I` | Instance | Drain all | No | `sbridge_raw_fw_cpd4_day1_instance_drain_all.json` |
| `I+L` | Instance | Drain all | Yes | `../dee_instance_histograms/sbridge_lehmer_fw_cpd4_day1_instance_queues.json` |
| `B+D` | Service | Dequeue one | No | `sbridge_raw_fw_cpd4_day1_service_dequeue_one.json` |
| `B+L+D` | Service | Dequeue one | Yes | `../dee_single_dequeue_histograms/sbridge_lehmer_fw_cpd4_day1_service_queues_dequeue_one.json` |
| `I+D` | Instance | Dequeue one | No | `sbridge_raw_fw_cpd4_day1_instance_dequeue_one.json` |
| `I+L+D` | Instance | Dequeue one | Yes | `../dee_single_dequeue_histograms/sbridge_lehmer_fw_cpd4_day1_instance_queues_dequeue_one.json` |

## No-Lehmer run commands

All commands below were run from the repository root. None includes
`--lehmer-ee`.

```bash
trace_sim --corpus /mydata/uber/endpoint_instance_state/corpus \
  --bagsize --mode sbridge --checkpoint-distance 4 --prefix-len 8 --fp-bits 64 \
  --progress 50000 \
  --size-histograms output/dee_full_comparison/sbridge_raw_fw_cpd4_day1_service_drain_all.json

trace_sim --corpus /mydata/uber/endpoint_instance_state/corpus \
  --bagsize --mode sbridge --checkpoint-distance 4 --prefix-len 8 --fp-bits 64 \
  --progress 50000 \
  --dee-queue-ids /mydata/uber/endpoint_instance_state/corpus/dee_queue_ids.bin \
  --size-histograms output/dee_full_comparison/sbridge_raw_fw_cpd4_day1_instance_drain_all.json

trace_sim --corpus /mydata/uber/endpoint_instance_state/corpus \
  --bagsize --mode sbridge --checkpoint-distance 4 --prefix-len 8 --fp-bits 64 \
  --progress 50000 --dee-dequeue-one \
  --size-histograms output/dee_full_comparison/sbridge_raw_fw_cpd4_day1_service_dequeue_one.json

trace_sim --corpus /mydata/uber/endpoint_instance_state/corpus \
  --bagsize --mode sbridge --checkpoint-distance 4 --prefix-len 8 --fp-bits 64 \
  --progress 50000 --dee-dequeue-one \
  --dee-queue-ids /mydata/uber/endpoint_instance_state/corpus/dee_queue_ids.bin \
  --size-histograms output/dee_full_comparison/sbridge_raw_fw_cpd4_day1_instance_dequeue_one.json
```

The distribution files were generated with `trace_sim histtable`. For example,
the base baggage CSV uses:

```bash
trace_sim histtable --metric baggage --format csv --precision 9 \
  --output output/dee_full_comparison/baggage_distribution_base.csv \
  'Service queues / ordinal-list EE/DEE=output/dee_full_comparison/sbridge_raw_fw_cpd4_day1_service_drain_all.json' \
  'Service queues / Lehmer-coded EE/DEE=output/dee_instance_histograms/sbridge_lehmer_fw_cpd4_day1_service_queues.json' \
  'Endpoint-instance queues / ordinal-list EE/DEE=output/dee_full_comparison/sbridge_raw_fw_cpd4_day1_instance_drain_all.json' \
  'Endpoint-instance queues / Lehmer-coded EE/DEE=output/dee_instance_histograms/sbridge_lehmer_fw_cpd4_day1_instance_queues.json'
```
