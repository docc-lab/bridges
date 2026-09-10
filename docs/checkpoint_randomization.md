# Randomized checkpoint distance

`--checkpoint-range MIN:MAX` enables randomized checkpoint spacing in the modern
bridge handlers and their default greedy reconstructors. The bounds are
inclusive **distances**, measured in parent-child edges. CPD 2 means every other
span: checkpoints occur at depths 0, 2, 4, and so on when the range is `2:2`.
Without this flag, fixed `--checkpoint-distance` behavior is unchanged.

## Countdown and emission

Each context carries one additional baggage byte containing both the assigned
distance and the remaining countdown:

1. The trace root checkpoints and samples a distance `D` in the configured range.
2. A checkpoint sets the countdown to `D - 1` for each child.
3. A span receiving a zero countdown checkpoints and samples its next distance.
   A span receiving a positive countdown decrements that field, preserving the
   assigned distance.
4. Every leaf emits its bridge payload, even if it did not reach its checkpoint.

For CPD 2, the checkpoint's countdown is 1, its child's is 0, and its grandchild
checkpoints. All children receive the same outgoing TTL; sibling calls do not
consume each other's countdown. Checkpoints on different branches subsequently
sample their own distances.

For range 2–8, the distance index `D - 2` occupies three bits and the countdown
occupies three bits; two bits remain unused. The assigned distance is available
throughout the window without a second baggage byte. Checkpointing tests the
countdown field, rather than comparing the complete packed byte to zero.

Distances must satisfy `1 <= MIN <= MAX <= 256`. The configured range must also
fit in one byte: the distance index needs enough bits for `MAX - MIN + 1`
choices, and the countdown needs enough bits for `MAX` values. Their combined
width must not exceed eight bits. For example, 2:8, 1:16, and the singleton
256:256 fit; 1:17 and 255:256 do not. Unsupported ranges are rejected.

Sampling is uniform over the inclusive range, with deterministic choices
derived from checkpoint trace ID, span ID, and `--checkpoint-seed` (default 42).
This seed is independent of sampling and span-drop seeds. Checkpoint choices
therefore agree across bridge modes, streaming/replay paths, and worker counts.

## Reconstruction and wire format

PB0, CGP0, and SB3 resolve windows using the emitted checkpoint-root prefix and
surviving checkpoint records, rather than depth modulo a fixed distance. Each
window retains its own Bloom, fanout, and ordinal evidence. Its actual root depth
sets reconstruction bounds and SB3 ordinal alignment; EE/DEE processing then
uses the recovered topology as before. Reconstruction does not use original
trace topology or reproduce the simulator's random choices.

Window membership propagates through connected span records, shared named
parents, and witnessed fanouts. A checkpoint's payload closes its incoming
window; its outgoing context begins the window rooted at that checkpoint.
Candidate indexes are partitioned by checkpoint root. Known membership excludes
anchors and fanouts assigned to another checkpoint root, even when their Bloom
tests are positive. Fragments without a connected checkpoint can borrow
covering evidence, after which their assigned window also
constrains candidate selection. Greedy joins check that an already reconstructed
route still leads toward the same checkpoint root.

Randomized payloads use bits 3–6 of the existing type byte for the assigned
distance index. Bits 0–2 retain the bridge type; bit `0x80` marks early leaves.
This adds no payload byte. Checkpoints emitted on START do not set the leaf
flag, including those later found to be leaves. Their payload describes the
incoming window; the newly sampled distance sizes the outgoing window.
The updated PCRB, CGPRB, and SB3 decoders read these fields; callers populate
`recon.Span.LeafCarrier` using `bridge.IsLeafPayload` in randomized mode.

Each window's Bloom is sized for its **assigned distance**, using the same
`max(1, D - 1)` capacity, false-positive target, and optional prime rounding as
fixed CPD D. Leaves retain that geometry even when they emit early. Reconstruction
uses the payload's geometry for all membership tests, including borrowed and
pooled evidence. Connected records can disambiguate short
checkpoint prefixes; remaining conflicting
matches at different depths leave the window root unresolved.
Use `--prefix-len 8` when full checkpoint identity is needed.

## Commands and results

Simulation mode names are `pcrb`, `cgprb`, and `sb3`; corresponding reconstruction
names are `pb0`, `cgp0`, and `sb3`. For example:

```bash
go build -o bin/ ./cmd/trace_sim ./cmd/trace_recon

bin/trace_sim --corpus /path/to/corpus --mode pcrb \
  --checkpoint-range 2:8 --checkpoint-seed 42 --prefix-len 8 \
  --bagsize -o sizes.json

bin/trace_recon --corpus /path/to/corpus --mode pb0 \
  --checkpoint-range 2:8 --checkpoint-seed 42 --prefix-len 8 \
  --drop-rates 0.5,1 --per-trace-drop-seed -o reconstruction.json
```

The range overrides `--checkpoint-distance`. JSON outputs record the maximum
under `checkpoint_distance` and the full policy under `checkpoint_randomization`:

```json
{"distance_min": 2, "distance_max": 8, "seed": 42}
```

Streamed metrics CSV and size histograms preserve the same policy. `csv2json`
retains it; `histmerge` rejects inputs with different ranges or checkpoint seeds.
Fixed-mode outputs omit this additional metadata.

The range flag is supported by the default PB0/CGP0/SB3 reconstruction engines.
Legacy engines and the independent `--verify` path still assume fixed spacing;
the CLI rejects those combinations.

For Go callers, construct the handler, call `bridge.ConfigureCheckpoints` before
processing events, build `recon.NewPCRBConfig` with the maximum distance and the
same Bloom/prefix parameters, and set `RandomizedCheckpoints = true` and
`CheckpointMin = policy.Min`. Populate each carrier's `WindowCPD`, `BloomM`, and
`BloomK` with `recon.DecodeBloomGeometry(payload, cfg)` alongside its decoded
payload fields. The maximum distance remains a search bound, not a Bloom size.

The original day 1 runs in `.local/variable-cpd/day1/` and `day1_loss75/` used
maximum-distance Bloom sizing and are superseded. Corrected full-corpus results
are kept separately in `.local/variable-cpd/day1_assigned_bloom/`.
