# Day-1 S-Bridge per-instance DEE queue comparison

These exact histograms compare S-Bridge's legacy per-service DEE queues with
the optional per-simulated-instance queues. Both runs used the complete
endpoint-enhanced Uber day-1 corpus, not the smaller cleaned corpus.

## Inputs

- Corpus: `/mydata/uber/endpoint_instance_state/corpus`
- Traces: 521,305
- Spans: 475,488,643
- Events: 950,977,286
- Service+endpoint pools: 21,715
- Simulated instances / DEE queues: 84,617
- Maximum instances in one pool: 5,443 (`Service560`, endpoint ID 7)

The queue-ID sidecar was produced by `cmd/dee_instance_prep`. Concurrent calls
sharing one `(trace, parent, service, endpoint)` group are assigned distinct
instance slots; slots are reused after calls end.

## Fixed simulation configuration

- Mode: `sbridge`
- Checkpoint distance: 4
- Lehmer EE/DEE coding: enabled
- Checkpoint prefix: 8 bytes
- Fingerprints: 64 bits
- Histogram bins: exact byte sizes

The only A/B difference is the presence of `--dee-queue-ids` in the
per-instance run.

## Results

| Metric | Legacy service queues | Per-instance queues | Reduction | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Baggage-call total | 11,497,642,934 B | 11,356,737,024 B | 140,905,910 B | 1.225520% |
| Baggage-call mean | 24.207229 B | 23.910564 B | 0.296664 B | 1.225520% |
| Baggage-call maximum | 128,571 B | 39,929 B | 88,642 B | 68.944008% |
| Emitted-payload total | 10,017,976,680 B | 9,873,549,895 B | 144,426,785 B | 1.441676% |
| Emitted-payload mean | 38.221332 B | 37.670304 B | 0.551028 B | 1.441676% |
| Emitted-payload maximum | 128,584 B | 57,756 B | 70,828 B | 55.083059% |
| Combined total | 21,515,619,614 B | 21,230,286,919 B | 285,332,695 B (272.114 MiB) | 1.326165% |

Both variants contain exactly 474,967,338 baggage calls and 262,104,331
emitted payloads. Their minima are also unchanged: 13 bytes for baggage and
16 bytes for payload. The legacy result matches the previously committed
complete day-1 full-width Lehmer histogram exactly, apart from the newly added
`dee_instance_queues` configuration field.

Runtime on this machine was 1h02m59.858s for legacy service queues and
1h05m55.924s for per-instance queues, a 4.658% increase. The per-instance run
consumed the complete queue-ID sidecar without count, early-EOF, trailing-data,
or simulator-state errors.

## Commands

```bash
trace_sim --corpus /mydata/uber/endpoint_instance_state/corpus \
  --mode sbridge --bagsize --checkpoint-distance 4 --lehmer-ee \
  --prefix-len 8 --fp-bits 64 \
  --size-histograms sbridge_lehmer_fw_cpd4_day1_service_queues.json

trace_sim --corpus /mydata/uber/endpoint_instance_state/corpus \
  --mode sbridge --bagsize --checkpoint-distance 4 --lehmer-ee \
  --prefix-len 8 --fp-bits 64 \
  --dee-queue-ids /mydata/uber/endpoint_instance_state/corpus/dee_queue_ids.bin \
  --size-histograms sbridge_lehmer_fw_cpd4_day1_instance_queues.json
```
