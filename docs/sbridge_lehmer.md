# Optional Lehmer coding for S-Bridge EE/DEE groups

S-Bridge can encode end-event (EE) and delayed-end-event (DEE) ordinal groups
as Lehmer/factoradic ranks. The legacy varint-per-ordinal format remains the
default; enable the new format with `--lehmer-ee` in `trace_sim`,
`sbridge_recon`, or `stream_eval`.

The option must agree on the emitting and reconstruction sides. It is a wire
format choice, not a self-describing payload bit.

## EE groups

For a chain level whose child has start ordinal `o`, every EE value is a
distinct member of `1..o-1`. For a group of `k` values, the encoded form is:

```text
varint(k) || rank[ceil(log2(P(o-1,k)) / 8)]
```

where `P(n,k) = n!/(n-k)!`. The rank is big-endian and zero-padded to the
derived fixed width. Empty and singleton-alphabet ranks occupy zero bytes.

The per-chain-level group boundary is retained. Consequently, reconstruction
still knows which sibling ends occurred before each later sibling start; the
coding does not collapse the representation to an end-only permutation.

## DEE groups

A DEE has no breadcrumb-chain context. Its total child count is therefore
included as a candidate-independent rank universe:

```text
trace_id[16] || varint(owner_depth) || owner_fp
|| varint(child_count) || varint(k)
|| rank[ceil(log2(P(child_count,k)) / 8)]
```

Decoded ordinals remain available to the existing owner-attribution/content
pruning logic when truncated owner fingerprints collide. As in the legacy
format, the parent's final child end is omitted and recovered as the only
ordinal absent from its EE and DEE groups.

Ranks use arbitrary-precision integers. Ordinal rank/selection uses a Fenwick
tree, making encoding and decoding `O(k log n)` apart from big-integer work.

## Exact size histograms

`trace_sim --size-histograms <file.json>` records exact corpus-wide,
event-level distributions without retaining per-trace metrics. The output has
sorted bins plus count, sum, minimum, and maximum for:

- `baggage_call_bytes`: bytes carried by each baggage-bearing start event.
- `bridge_payload_bytes`: bytes persisted by each emitted `_br` payload.

The histogram mode can be used by itself or alongside `-o`/`--stream-metrics`.
Compatible day or partition outputs can be pooled exactly with:

```bash
trace_sim histmerge overall.json day1.json day2.json
```

For side-by-side distribution tables, `histtable` rebins one or more exact
histograms into the shared byte ranges used by the S-Bridge comparison study.
It writes numeric percentages as CSV for plotting, or a percentage-formatted
Markdown table for reports. Every exact input bin must be accounted for; the
final `131072+` row keeps oversized observations from disappearing.

```bash
trace_sim histtable --metric baggage --format csv --output baggage.csv \
  B=baseline.json B+L=lehmer.json I=instances.json I+L=instances-lehmer.json

trace_sim histtable --metric payload --format markdown --output payload.md \
  B=baseline.json B+L=lehmer.json I=instances.json I+L=instances-lehmer.json
```
