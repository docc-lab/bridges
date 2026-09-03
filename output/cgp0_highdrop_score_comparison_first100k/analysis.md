# CGP0 high-drop scorer/reconstructor decomposition

Controlled corpus: first 100,000 Day 1 traces; per-trace-seeded drops (seed 42); drop rates 0.75, 0.95, and 1.0 only; CPD 3–8; Bloom target 0.0001; prime-up and no-prime modes.

`C/input` is the canonical evidence-bounded scorer on obligations determined from surviving records. `H/input` substitutes the historical permissive node-plus-anchor scorer without changing that denominator. `H/emitted` reproduces the complete historical evaluation contract: historical scorer and the output-dependent `Reconnected > 0` denominator. All entries are trace error rates.

## Principal findings

- Denominator selection has no effect in these high-drop cells: there were 0 input obligations without an emitted reconstruction and 0 emissions without an input obligation.
- On the same maximal reconstruction, changing only the scorer increases trace error by 0.0040–1.3701 percentage points. The scorer change alone therefore does not explain the full visual gap.
- Legacy CGP0 has 100.0000–100.0000% canonical error: it systematically emits anonymous nodes where surviving `ParentID` records make the identities nameable.
- Historical scores for maximal and legacy output are not a fixed-domain algorithm comparison. The historical scorer grades non-anonymous reconstructed nodes, so maximal exact-parent materialization exposes upstream edges that legacy output hides. An anchor-only breakdown or a hybrid using legacy anchor choices with canonical materialization is required to isolate route-selection effects.

## prime-up, drop 0.75

| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 7.9261% | 7.9221% | 7.9221% | 100.0000% | 5.9797% | 5.9797% |
| 4 | 8.3514% | 8.3151% | 8.3151% | 100.0000% | 6.2371% | 6.2371% |
| 5 | 7.0142% | 6.9516% | 6.9516% | 100.0000% | 5.3217% | 5.3217% |
| 6 | 6.8810% | 6.7508% | 6.7508% | 100.0000% | 5.7819% | 5.7819% |
| 7 | 4.2277% | 4.1399% | 4.1399% | 100.0000% | 3.6514% | 3.6514% |
| 8 | 5.5447% | 5.4085% | 5.4085% | 100.0000% | 4.8221% | 4.8221% |

## prime-up, drop 0.95

| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 9.7473% | 9.7383% | 9.7383% | 100.0000% | 6.0725% | 6.0725% |
| 4 | 10.5853% | 10.4941% | 10.4941% | 100.0000% | 5.2475% | 5.2475% |
| 5 | 8.9905% | 8.8301% | 8.8301% | 100.0000% | 4.4496% | 4.4496% |
| 6 | 8.7710% | 8.4101% | 8.4101% | 100.0000% | 4.7022% | 4.7022% |
| 7 | 5.0691% | 4.7343% | 4.7343% | 100.0000% | 2.8408% | 2.8408% |
| 8 | 7.1010% | 6.5877% | 6.5877% | 100.0000% | 3.7008% | 3.7008% |

## prime-up, drop 1

| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 10.1202% | 10.1112% | 10.1112% | 100.0000% | 5.8778% | 5.8778% |
| 4 | 11.0449% | 10.9429% | 10.9429% | 100.0000% | 4.4686% | 4.4686% |
| 5 | 9.4917% | 9.2915% | 9.2915% | 100.0000% | 3.8561% | 3.8561% |
| 6 | 9.2915% | 8.6910% | 8.6910% | 100.0000% | 3.8321% | 3.8321% |
| 7 | 5.1332% | 4.6428% | 4.6428% | 100.0000% | 2.2418% | 2.2418% |
| 8 | 7.4410% | 6.7114% | 6.7114% | 100.0000% | 2.7873% | 2.7873% |

## no-prime, drop 0.75

| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 9.2073% | 9.2012% | 9.2012% | 100.0000% | 7.0175% | 7.0175% |
| 4 | 17.9311% | 17.8473% | 17.8473% | 100.0000% | 13.9648% | 13.9648% |
| 5 | 8.1385% | 8.0799% | 8.0799% | 100.0000% | 6.1715% | 6.1715% |
| 6 | 18.3156% | 18.0320% | 18.0320% | 100.0000% | 15.8934% | 15.8934% |
| 7 | 13.4612% | 13.1554% | 13.1554% | 100.0000% | 11.8706% | 11.8706% |
| 8 | 5.9545% | 5.8203% | 5.8203% | 100.0000% | 5.3409% | 5.3409% |

## no-prime, drop 0.95

| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 11.3912% | 11.3842% | 11.3842% | 100.0000% | 7.2273% | 7.2273% |
| 4 | 21.5174% | 21.3460% | 21.3460% | 100.0000% | 12.0789% | 12.0789% |
| 5 | 10.3157% | 10.1452% | 10.1452% | 100.0000% | 5.1874% | 5.1874% |
| 6 | 21.7390% | 21.0323% | 21.0323% | 100.0000% | 12.9840% | 12.9840% |
| 7 | 15.6654% | 14.7232% | 14.7232% | 100.0000% | 9.2912% | 9.2912% |
| 8 | 7.5631% | 7.0027% | 7.0027% | 100.0000% | 3.9945% | 3.9945% |

## no-prime, drop 1

| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 11.7725% | 11.7645% | 11.7645% | 100.0000% | 6.9226% | 6.9226% |
| 4 | 22.0168% | 21.8067% | 21.8067% | 100.0000% | 10.4294% | 10.4294% |
| 5 | 10.8668% | 10.6606% | 10.6606% | 100.0000% | 4.3926% | 4.3926% |
| 6 | 22.4952% | 21.5805% | 21.5805% | 100.0000% | 10.9429% | 10.9429% |
| 7 | 15.9299% | 14.5598% | 14.5598% | 100.0000% | 7.1608% | 7.1608% |
| 8 | 7.8894% | 7.0847% | 7.0847% | 100.0000% | 3.0114% | 3.0114% |

## Exact interpretation

For each fixed prime mode, CPD, and drop rate:

- `maximal C/input − legacy C/input` isolates the reconstructor change under the canonical contract.
- `C/input − H/input` isolates the scorer change on exactly the same traces.
- `H/input − H/emitted` isolates the output-dependent denominator change while holding the historical scorer fixed.
- The raw counts and scorer-disagreement contingency table are in `comparison_cells.csv`; no pooled averages are used in the tables above.

This experiment does not claim bit-for-bit reproduction of the old paper figure, which pooled full Day 1 and Day 2. It is a controlled causal decomposition on the same first-100k Day 1 population used by the current figures.
