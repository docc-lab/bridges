# Day-1 S-Bridge single-DEE-dequeue experiment

These exact histograms test `--dee-dequeue-one` on the complete
endpoint-enhanced Uber day-1 corpus. The experiment covers both the legacy
per-service queues and the simulated per-instance queues, and compares them to
the previously collected drain-all baselines.

## Inputs and fixed configuration

- Corpus: `/mydata/uber/endpoint_instance_state/corpus`
- Traces: 521,305
- Spans: 475,488,643
- Events / DEE pickup attempts: 950,977,286 / 475,488,643
- Mode: `sbridge`
- Checkpoint distance: 4
- Lehmer EE/DEE coding: enabled
- Checkpoint prefix: 8 bytes
- Fingerprints: 64 bits
- Histogram bins: exact byte sizes

The single-dequeue runs remove at most one FIFO DEE record on each span start.
The final backlog is reported as-is and is not force-drained.

## Size results

All four variants contain exactly 474,967,338 baggage calls and 262,104,331
emitted payloads.

| Queue model | Pickup policy | Baggage total | Baggage mean | Baggage max | Payload total | Payload mean | Payload max | Combined total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Service | Drain all | 11,497,642,934 B | 24.207229 B | 128,571 B | 10,017,976,680 B | 38.221332 B | 128,584 B | 21,515,619,614 B |
| Instance | Drain all | 11,356,737,024 B | 23.910564 B | 39,929 B | 9,873,549,895 B | 37.670304 B | 57,756 B | 21,230,286,919 B |
| Service | Dequeue one | 11,152,667,061 B | 23.480914 B | 7,247 B | 9,804,756,490 B | 37.407839 B | 7,685 B | 20,957,423,551 B |
| Instance | Dequeue one | 11,291,583,202 B | 23.773389 B | 3,223 B | 9,869,941,461 B | 37.656537 B | 7,540 B | 21,161,524,663 B |

Relative to drain-all with the same queue model:

| Queue model | Baggage reduction | Payload reduction | Combined reduction |
| --- | ---: | ---: | ---: |
| Service | 344,975,873 B (3.000405%) | 213,220,190 B (2.128376%) | 558,196,063 B / 532.337 MiB (2.594376%) |
| Instance | 65,153,822 B (0.573702%) | 3,608,434 B (0.036546%) | 68,762,256 B / 65.577 MiB (0.323888%) |

With single dequeue, instance queues use 204,101,112 more combined bytes
(0.973885%) than service queues, but reduce the worst baggage call from 7,247
to 3,223 bytes and the worst emitted payload from 7,685 to 7,540 bytes.

## Distribution tails

| Queue model | Pickup | Metric | p50 | p90 | p95 | p99 | p99.9 | p99.99 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Service | Drain all | Baggage | 16 | 35 | 35 | 71 | 445 | 2,247 |
| Instance | Drain all | Baggage | 16 | 35 | 35 | 71 | 305 | 1,845 |
| Service | Dequeue one | Baggage | 16 | 35 | 36 | 81 | 119 | 166 |
| Instance | Dequeue one | Baggage | 16 | 35 | 53 | 63 | 92 | 136 |
| Service | Drain all | Payload | 38 | 48 | 49 | 104 | 525 | 2,745 |
| Instance | Drain all | Payload | 38 | 48 | 49 | 104 | 374 | 1,991 |
| Service | Dequeue one | Payload | 38 | 48 | 56 | 105 | 160 | 235 |
| Instance | Dequeue one | Payload | 38 | 49 | 76 | 82 | 114 | 201 |

Single-record pickup spreads small DEE payloads across more calls, which can
raise mid/high percentiles, while sharply reducing the extreme tail.

## DEE queue lifecycle

| Statistic | Service queues | Instance queues |
| --- | ---: | ---: |
| Pickup attempts | 475,488,643 | 475,488,643 |
| Empty pickups | 445,820,123 | 445,858,888 |
| Non-empty pickups / dequeued records | 29,668,520 | 29,629,755 |
| Enqueued records | 29,671,136 | 29,671,136 |
| Enqueued bytes | 883,210,771 | 883,210,771 |
| Dequeued bytes | 883,131,080 | 881,952,254 |
| Final backlog queues | 422 | 6,870 |
| Final backlog records | 2,616 (0.008817%) | 41,381 (0.139466%) |
| Final backlog bytes | 79,691 (0.009023%) | 1,258,517 (0.142493%) |
| Maximum queue records | 4,590 | 2,061 |
| Maximum queue bytes | 128,536 | 68,995 |

For both variants, `enqueued - dequeued = backlog` exactly for records and
bytes. Service queues dequeued 99.991183% of records; instance queues dequeued
99.860534%.

## Outputs

- `sbridge_lehmer_fw_cpd4_day1_service_queues_dequeue_one.json`
- `sbridge_lehmer_fw_cpd4_day1_instance_queues_dequeue_one.json`

Both runs completed in 1h09m23s while running concurrently. The instance run
consumed the complete DQID sidecar without early EOF, extra records, trailing
data, queue-stat underflow, or simulator-state errors.
