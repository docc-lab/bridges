# Absolute depth emission (`--emit-depth`)

Reconstruction needs every surviving span's **absolute call depth** to
compute hole sizes (`orphan.depth − anchor.depth − 1` = number of
synthetic spans to insert). Today, non-checkpoint spans export no depth
at all, and PB/CGPB checkpoint payloads carry only `depth % cpd`, which
is ambiguous the moment a checkpoint span can be lost or a fragment
loses its anchor.

`--emit-depth` is **opt-in**. Flag-off behavior is byte-identical to the
Python simulator (pinned by `bridge/testdata/golden.json`); all
pre-existing results remain valid. Flag-on is the reconstruction-era
accounting.

Empirical cost note: in the Uber corpus, p50 max trace depth is 20, p99
is 56, and only 11 of 531k traces exceed depth 127 — so
`varint(absolute_depth)` is 1 byte for >99.99% of spans, i.e. the same
size as `varint(depth % cpd)`.

## Wire formats (flag on)

```
_br value = type(1 byte: 1=PB, 2=CGPB, 3=SB) || body

PB    body = varint(depth) || bloom[bloomLen]              # was varint(depthMod)
CGPB  body = varint(depth) || bloom[bloomLen] || ha*       # was varint(depthMod)
        ha entry = parent_span_id[8 BE] || varint(depth)   # was varint(depthMod)
SB    body = varint(depth) || ckpt8[8] || groups || ends || dee   # unchanged

_d  value = varint(depth)      # only on spans that never emit _br
```

- `depth` is absolute call depth: root = 0, +1 per hop, never reset at
  checkpoints (only the bloom resets — the same asymmetry the S-Bridge
  payload already has).
- The decoder derives `depthMod = depth % cpd` and the bloom geometry
  from global config; no information is lost by the swap.
- The CGPB hash-array entry's depth field becomes absolute as well, so
  phase-2 fan-out merging aligns across subtrees without modular
  reasoning.

## Which spans carry what

Every span carries exactly one of `_br` or `_d`:

| span                                          | carrier            | decided  |
|-----------------------------------------------|--------------------|----------|
| depth checkpoint (`depth % cpd == 0`, incl. root) | `_br` (depth inside) | OnStart |
| leaf that is not already a checkpoint          | `_br` (depth inside) | OnEnd   |
| interior non-checkpoint span                   | `_d`               | OnEnd    |

## Accounting

```
EmitBytes(_br)  = 3 + typeID-const + len(body)   # structure unchanged;
                                                 # varint(depthMod) -> varint(depth)
DepthBytes(_d)  = 2 + VarintLen(depth)           # DepthKeyBytes = 2 ("_d")
BaggageBytes    = 3 + len(in-band body)          # depthMod -> depth swap too:
                                                 # the SDK must propagate absolute
                                                 # depth to count it
```

S-Bridge `_br`/baggage accounting is unchanged (its payload already
leads with absolute depth); it only gains `_d` on interior
non-checkpoint spans. Vanilla emits nothing in either mode.

Because all depths in the golden corpus (and almost all in Uber) are
< 128, flag-on `EmitBytes`/`BaggageBytes` equal flag-off values
exactly; the measurable delta is the `_d` overhead, which keeps the
ablation clean.

New per-trace metrics: `num_depth_spans`, `depth_overhead_sum` —
emitted in the bagsize JSON only when the flag is on. Existing keys
keep their exact current semantics (checkpoint-only sums).

## Known divergence (deliberate)

The legacy accounting charges 1/2/3 bytes for the bridge type ID (the
Python quirk where the numeric ID doubles as its byte count). Real
serialized artifacts for reconstruction will use a 1-byte type tag
regardless of mode; the *accounting* keeps the legacy arithmetic so the
flag-on vs flag-off delta is purely the depth change.

The Python reference simulator is not updated; depth mode is Go-only,
covered by Go-native tests on top of the untouched Python goldens.
