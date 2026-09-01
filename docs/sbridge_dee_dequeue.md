# Optional single-record DEE pickup

S-Bridge normally drains the complete selected delayed-end-event (DEE) queue
at every span start. `trace_sim --dee-dequeue-one` changes pickup so that each
call removes at most one queued DEE record instead.

The queue remains FIFO: a call receives the oldest pending DEE quad, and every
newer quad stays queued for a later call. If there are more generated records
than later calls, the simulation intentionally ends with a backlog. Those
records are not force-drained or charged to a synthetic final request.

This setting is independent of queue granularity:

- Without `--dee-queue-ids`, it applies to the legacy per-service queues.
- With `--dee-queue-ids`, it applies independently to every simulated
  service+endpoint instance queue.

It does not change the EE/DEE wire format and can be combined with
`--lehmer-ee`. The default remains drain-all. Exact histogram output records
the setting as `"dee_dequeue_one": true`, and `histmerge` rejects inputs that
mix the two pickup policies.

S-Bridge histogram runs also include `dee_queue_stats`, with pickup attempts,
empty/non-empty pickups, records and bytes enqueued/dequeued, final backlog
queues/records/bytes, and maximum observed queue records/bytes. A final backlog
is reported as-is; it is not force-drained. When compatible histogram files
are merged, lifecycle and backlog totals are summed and queue maxima take the
maximum of the inputs.

```bash
trace_sim --corpus /path/to/corpus \
  --mode sbridge --bagsize --dee-dequeue-one \
  --size-histograms /path/to/histograms.json
```
