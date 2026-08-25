# Per-instance S-Bridge DEE queues

S-Bridge historically modeled one delayed-end-event (DEE) queue per service.
That makes a wide fanout of identical calls behave as if every call reached one
physical service instance. The optional instance sidecar instead models a pool
for each `(service ID, endpoint ID)` and gives every simulated instance its own
cross-trace DEE queue.

## Pool sizing and assignment

`cmd/dee_instance_prep` makes two streaming passes over `events.bin` and the
endpoint sidecar:

1. For each service+endpoint pair, measure the largest number of simultaneously
   active direct children sharing one `(trace ID, parent span ID)`. This maximum
   is the pair's instance-pool size. Overlap is evaluated in corpus event order,
   where starts precede ends at equal timestamps.
2. Replay the corpus and assign the smallest free slot within every active
   `(trace, parent, service, endpoint)` group. Concurrent calls in a group
   therefore always have distinct slots. A slot can be reused once its call
   ends. The same queue ID is written for a span's start and end events.

Queue-ID ranges are disjoint between service+endpoint pairs. The output is a
versioned `DQID` sidecar containing a record count followed by one little-endian
`uint32` queue ID per corpus event. The command refuses truncated or extra
endpoint records, unbalanced calls, pool overflows, and start/end mismatches.
It writes the large sidecar through a temporary path and renames it only after
the complete second pass succeeds.

```bash
go run ./cmd/dee_instance_prep \
  --corpus /path/to/corpus \
  --endpoints /path/to/endpoints.bin \
  --output /path/to/dee_queue_ids.bin \
  --metadata /path/to/dee_instance_pools.json
```

## Simulation

Pass the generated sidecar to S-Bridge:

```bash
go run ./cmd/trace_sim \
  --corpus /path/to/corpus \
  --mode sbridge --bagsize \
  --dee-queue-ids /path/to/dee_queue_ids.bin \
  --size-histograms /path/to/histograms.json
```

This option affects only the key used for S-Bridge's DEE queues. It composes
with `--lehmer-ee` and all existing S-Bridge size controls. Without
`--dee-queue-ids`, behavior remains the original one-queue-per-service model.
Histogram metadata records `"dee_instance_queues": true` when the sidecar was
used, and histogram merging rejects a mixture of per-service and per-instance
runs.
