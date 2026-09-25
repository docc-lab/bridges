# CORRECTION — byte accounting for this dataset (issued 2026-09-23)

An earlier summary of these results stated reverse costs "~3x the bytes".
**That is wrong.** It compared the forward arm's EXPORT bytes against the
reverse arm's EXPORT + RETURN-WIRE bytes, and omitted the forward arm's own
in-band baggage from both sides. `docs/reverse_trusses.md` states plainly that
forward and reverse transferred baggage are separate from checkpoint export
bytes and must not be added as one term.

Correct fields:
* `baggage_call_bytes.sum_bytes`            — forward in-band baggage (wire)
* `combined_checkpoint_payload_bytes.sum`   — export: own `_br` + returned bundle
* `reverse_encoded_baggage_bytes.sum`       — return traffic (wire)

Day 2, random CPD 1:9, GB:

```
bridge arm       own _br   export  fwd bag  ret wire |  EXPORT   TOTAL
PB0    forward     10.97    10.97    20.88      0.00 |   10.97   31.85
PB0    reverse      4.73    16.27    20.88     19.83 |   16.27   56.98
CGP0   forward     11.42    11.42    21.62      0.00 |   11.42   33.03
CGP0   reverse      5.00    16.77    21.62     20.50 |   16.77   58.89
SB3    forward     15.13    15.13    27.73      0.00 |   15.13   42.86
SB3    reverse      7.29    21.11    27.73     25.19 |   21.11   74.03
```

Reverse vs forward: **export +48% / +47% / +39%** (PB0/CGP0/SB3);
**total +79% / +78% / +73%**. Not 3x.

Reverse export decomposes as expected — the same truss data plus span IDs plus
framing (PB0): raw truss 6.69 + origin metadata 2.58 + receivers' own `_br`
4.73 + framing = 16.27 GB export.

Return-wire (19.83 GB) exceeds emitted bundle content (18.84 GB raw) because a
truss travels a mean 1.96 hops before absorption and is retransmitted at each
hop. Retransmission, not double counting.

Unchanged and still correct: emitting spans -54.8% (437,414,765 -> 197,849,424),
so per-emission bytes concentrate ~7x.
