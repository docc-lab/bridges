# Reverse-truss simulation and size measurements

The simulator returns an unscheduled leaf's intact checkpoint truss upstream.
Every receiver that accepts a truss emits its own `_br` and a binary bundle of
accepted returns under `bridges.checkpoint`. Original checkpoints and roots
absorb all arriving trusses. Each other receiver decides independently for each
truss; accepting one does not force acceptance of its siblings. Promotion does
not reset the forward checkpoint window, change Bloom geometry, or reseed CPD.

Routing follows `/users/tomislav/reverse_probability_primer.md`. Encoding follows
this simulator's native binary conventions, as requested by the user; matching
Blueprint's JSON transport format is not a requirement. This implementation
replaces the earlier assumed JSON/base64 envelope. Old encoded-byte results
must not be mixed with these binary measurements.

## Execution and encoding

At a leaf's completion, its actual serialized `_br` value becomes the returned
truss. The handler delivers `ReverseEndResult.ReturnContext` bytes to its parent.
The parent decodes those bytes before routing them. Each accepted bundle is
exposed in `ReverseEndResult.CheckpointContext`, alongside the receiver's own
`EndResult.Payload`. Diagnostic `Returned` structs are not used as transport.

A nonempty binary bundle contains:

```text
version: one byte, value 1
then records until the end of the buffer:
    unsigned-varint(payload_length * 2 + TTL_present)
    origin span ID: 8 bytes, big endian
    origin absolute depth: unsigned varint
    reverse TTL: one byte, only when TTL_present = 1
    exact native bridge payload: payload_length bytes
```

The native payload's type tag distinguishes PB0, CGP0, and SB3; no repeated kind
string is necessary. Probability records carry no TTL byte. Empty return baggage
occupies zero bytes. Original checkpoint depth used for route analysis is not
serialized and does not influence routing. Decoding a malformed bundle fails
atomically. Lengths support large fan-in bundles without truncation.

The simulator measures these exact buffers. It does not add JSON/base64,
fingerprints, legacy SDK metadata, or a hypothetical network envelope. External
RPC/OTLP encoding is outside these counters, as with existing forward bridge
payload measurements.

## Routing policies

| Policy | Conditional acceptance at receiver depth d for origin depth n |
|---|---|
| `ttl` | Independent distance D; initial TTL D-1; accept an incoming zero. |
| `probability` | Configured constant p. |
| `inverse_depth` | 1/n. |
| `depth_linear` | 2*(d+1)/(n*(n+1)). |
| `upstream_pressure` | 1/(d+1). |

Explicit TTL takes precedence in mixed bundles. Leaf rejection, reverse TTL,
and acceptance have independent deterministic streams, separate from forward
CPD. Acceptance is keyed by seed, trace, origin, and receiver. Repeated completion
makes no second decision. Trace eviction checks returned/accepted conservation.
Original checkpoints retain their original own payload even though export is
finalized on completion. Promoted receivers take a non-mutating own snapshot.

The Uber corpus has no client/server span-kind mapping. Every non-root span
contributes one logical return edge, including empty returns; recorded ancestors
make decisions. These are not counts of verified physical RPC response hops.
Children must complete before their parents. Malformed lifecycle order fails
explicitly. SB3 DEE queues retain their native instance and single-pop behavior.

## Running a comparison

```bash
trace_sim --corpus /path/to/corpus --mode sb3 --bagsize \
  --checkpoint-range 2:8 --checkpoint-seed 42 \
  --prefix-len 8 --bloom-fp 0.0001 --fp-bits 64 \
  --lehmer-ee --dee-queue-ids /path/to/dee_queue_ids.bin --dee-dequeue-one \
  --reverse-policy depth_linear --leaf-reject 1 --reverse-seed 42 \
  --workers 1 --stream-metrics reverse.csv --size-histograms reverse.json
```

Use `pcrb` for PB0 or `cgprb` for CGP0, omitting SB-specific options. Omitting
`--reverse-policy` preserves the existing forward-only output. Setting
`--leaf-reject 0` with a policy records checkpoint categories but produces no
returns; its original forward baggage and own `_br` metrics match the baseline.
`--reverse-ttl-range MIN:MAX` defaults to the forward CPD range or fixed distance
and supports independent distances 1 through 256.

## Byte and count definitions

| Metric | Observation and contents |
|---|---|
| `bridge_payload_bytes` | Each checkpoint's own `_br`, including its 3-byte key. |
| `raw_truss_bytes` | Exact originating `_br` value once per rejected leaf; no local key. |
| `origin_metadata_bytes` | Origin ID and absolute-depth varint once per rejected leaf. |
| `combined_checkpoint_payload_bytes` | Emitted own `_br` plus the `bridges.checkpoint` key and its actual binary bundle, when present. |
| `reverse_raw_baggage_bytes` | Origin ID/depth, exact trusses, optional TTL on each non-root return; excludes record/bundle framing. |
| `reverse_encoded_baggage_bytes` | Length of the actual delivered binary return buffer on each non-root edge, including framing and zero-byte returns. |
| `returned_bundle_raw_bytes` | Raw content on nonempty return edges only. |
| `checkpoint_spans_per_trace` | Distinct emitted checkpoint spans per trace. |
| `accepted_trusses_per_receiving_checkpoint` | Trusses emitted together by one receiving checkpoint. |
| `received_trusses_per_receiver` | Pending trusses at each receiver with returns. |
| `reverse_distance_span_hops` | Origin-to-emitter distance for every accepted truss. |

The complete checkpoint metric includes its receiver's own data. Ordinary `_d`
and `_oc` attributes remain separately counted; the checkpoint metric is not
all telemetry bytes. Forward and reverse transferred baggage are separate from
checkpoint export bytes. Reverse raw content is a component of encoded baggage,
not an additional traffic term to add to it.

Checkpoint categories satisfy:

```text
distinct checkpoints = original + retained forced leaf + promoted receiver
```

Receiving checkpoints overlap original and promoted checkpoints. Several accepted
trusses still count as one checkpoint span. Rejected leaves retain ordinary
metadata and have no local `_br` emission.

Native reverse histograms use `bridges.size_histograms.v3`, pressure outputs use
`bridges.checkpoint_pressure.v2`, and CSV headers identify
`bridges.reverse.binary.v1`. Exact bins retain zeros and the entire tail.
Converters refuse to relabel old modeled-envelope results as native binary data.

## Evaluation scope

Size/count measurements occur before collection loss. Reverse evidence is
consumed by the shared PB0/CGP0/SB3 topology engine under the
intended-checkpoint model; see
[reverse_reconstruction.md](reverse_reconstruction.md) for that contract and
for the reconstruction accuracy and timing sweeps. This size evaluation
measures bytes and checkpoint counts only; reconstruction accuracy comes from
the reconstruction harness, never from these size runs.

The matched workload is 100,000 day-1 traces (91,945,996 spans), selected by
first root start in the original chronological stream, with all selected traces
completed. Original service/instance IDs and event ordering are preserved.
DEE queues start empty and retain final backlog. The cohort differs from the
first-100,000-by-metadata-order reconstruction benchmark.
