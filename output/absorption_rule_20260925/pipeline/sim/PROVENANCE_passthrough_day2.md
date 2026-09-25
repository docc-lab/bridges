# Day-2 bagsize/pressure sim with --reverse-pass-checkpoints (passthrough ON)

Reverse arm only (the forward arm carries no trusses, so it is unaffected and
was not rerun). Produced 2026-09-24.

| item | value |
|---|---|
| engine | commit `8cc104f` — "bridge: let a returning truss pass a scheduled checkpoint" |
| binary sha256 | `ffe3c71d6c1fa22b8eac2564733fbc58d0345add22aca9f446d2615763873441` |
| flag | `--reverse-pass-checkpoints` (default off; requires `--reverse-policy`) |
| configs | `cpd19_reverse_passthrough/` = `--checkpoint-range 1:9 --checkpoint-seed 42`; `cpd5_reverse_passthrough/` = `--checkpoint-distance 5` |
| everything else | `--bagsize --prefix-len 8 --bloom-fp 0.0001 --reverse-policy depth_cubic --leaf-reject 1 --reverse-seed 42 --pressure-instance-ids`, SB3 adds `--fp-bits 64 --lehmer-ee --dee-dequeue-one --dee-queue-ids` |
| baselines (passthrough OFF) | `sim_day2_cpd19_depth_cubic/` and `recon_sim_cpd5_fixed/day2_sim/` |
| hygiene | all 6 jobs exit 0, cpu_ratio 0.984-1.000 |

What the flag changes: previously a returning truss was absorbed
unconditionally at the first scheduled checkpoint above its origin. With the
flag, only the acceptance policy decides; the trace root stays the sole forced
absorber.

## Result: passthrough makes reverse substantially MORE expensive

```
                 emit spans   export   return wire    total     mean hops  max
CPD 1:9  PB0       +9.4%      +0.5%      +180.8%     +63.1%    1.96 -> 5.68  8 -> 92
         CGP0      +9.4%      +0.7%      +181.2%     +63.3%
         SB3       +9.4%      +1.9%      +175.7%     +60.3%
CPD 5    PB0      +12.6%      +0.4%      +219.7%     +70.5%    1.69 -> 5.49  4 -> 92
         CGP0     +12.6%      +0.6%      +220.8%     +70.7%
         SB3      +12.6%      +2.1%      +214.4%     +66.7%
```

Scopes are like-for-like: EXPORT = `combined_checkpoint_payload_bytes`;
TOTAL = export + `baggage_call_bytes` (forward in-band) + `reverse_encoded_baggage_bytes`
(return wire). See `CORRECTION_bytes.md` in the CPD 1:9 sim directory.

Reading:
* **Export barely moves (+0.4 to +2.1%).** The same trusses (286,299,822 at
  1:9; 295,788,032 at CPD 5 — identical to the baselines) with the same
  content, absorbed elsewhere. Collector-side cost is essentially unchanged.
* **Return wire triples.** A truss is retransmitted at every hop and mean
  travel roughly tripled. The entire cost increase lives here.
* **Emitting spans go UP, not down** (+9.4% / +12.6%): trusses spread across
  more distinct absorbers instead of piling into the first scheduled checkpoint.
* Max hops 8 -> 92 and 4 -> 92 confirms the old cap was exactly the CPD ceiling.

Per the commit message, `depth_cubic` weights acceptance toward DEEPER
receivers (p proportional to (d+1)^3) while a truss travels upward, so lifting
the cap permits longer journeys while the policy still favours absorbing early.
Pushing load toward the root would need a shallow-biased policy as well.

`compare.py` regenerates the table from the histogram JSONs.
