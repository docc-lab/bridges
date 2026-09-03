# CGP0 matched-chain evidence, first 100k Day-1 traces

Prime-rounded Bloom geometry; drop rates 0.75, 0.95, and 1.0; CPD 3–8. A matched level is one non-checkpoint ID on the accepted anchor-to-checkpoint chain that passed all applicable carrier Blooms.

| Drop | CPD | Clean traces | Mean matched levels / route | Routes with >0 levels | Initial candidates rejected by chain | Anchor-unit accuracy | r(mean levels, clean) | r(min levels, clean) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.75 | 3 | 92.0739% | 0.1660 | 16.6037% | 288455/5528933 (5.2172%) | 99.9583% | -0.0185 | +0.0138 |
| 0.75 | 4 | 91.6486% | 0.3993 | 27.6672% | 424023/10088501 (4.2030%) | 99.9491% | -0.0279 | +0.0157 |
| 0.75 | 5 | 92.9858% | 0.5895 | 35.3139% | 389492/14988248 (2.5986%) | 99.9644% | -0.0315 | +0.0173 |
| 0.75 | 6 | 93.1190% | 0.8007 | 44.2749% | 441747/18442150 (2.3953%) | 99.9409% | -0.0447 | +0.0198 |
| 0.75 | 7 | 95.7723% | 0.9363 | 49.0551% | 224776/22392667 (1.0038%) | 99.9549% | -0.0607 | +0.0069 |
| 0.75 | 8 | 94.4553% | 1.0801 | 54.7972% | 380382/26588802 (1.4306%) | 99.9570% | -0.0884 | +0.0134 |
| 0.95 | 3 | 90.2527% | 0.0363 | 3.6333% | 73934/1401111 (5.2768%) | 99.9900% | -0.0121 | +0.0055 |
| 0.95 | 4 | 89.4147% | 0.0939 | 6.5658% | 102194/2416229 (4.2295%) | 99.9870% | -0.0153 | +0.0063 |
| 0.95 | 5 | 91.0095% | 0.1422 | 8.9181% | 89725/3486080 (2.5738%) | 99.9905% | -0.0169 | +0.0062 |
| 0.95 | 6 | 91.2290% | 0.2003 | 11.8828% | 95412/4120664 (2.3155%) | 99.9820% | -0.0254 | +0.0068 |
| 0.95 | 7 | 94.9309% | 0.2423 | 13.8433% | 48580/4924264 (0.9865%) | 99.9874% | -0.0271 | +0.0055 |
| 0.95 | 8 | 92.8990% | 0.2912 | 16.2837% | 80488/5734429 (1.4036%) | 99.9815% | -0.0495 | +0.0070 |
| 1.00 | 3 | 89.8798% | 0.0000 | 0.0000% | 0/0 (NA) | 100.0000% | NA | NA |
| 1.00 | 4 | 88.9551% | 0.0000 | 0.0000% | 0/0 (NA) | 100.0000% | NA | NA |
| 1.00 | 5 | 90.5083% | 0.0000 | 0.0000% | 0/0 (NA) | 100.0000% | NA | NA |
| 1.00 | 6 | 90.7085% | 0.0000 | 0.0000% | 0/0 (NA) | 100.0000% | NA | NA |
| 1.00 | 7 | 94.8668% | 0.0000 | 0.0000% | 0/0 (NA) | 100.0000% | NA | NA |
| 1.00 | 8 | 92.5590% | 0.0000 | 0.0000% | 0/0 (NA) | 100.0000% | NA | NA |

## Across-CPD cell correlations

These six-point correlations are descriptive, not causal; CPD changes other evidence and Bloom geometry too.

| Drop | r(mean matched levels, clean rate) | r(nonzero-chain share, clean rate) |
|---:|---:|---:|
| 0.75 | +0.8345 | +0.8200 |
| 0.95 | +0.7915 | +0.7870 |
| 1.00 | NA | NA |

## Interpretation

- At drop 0.75, the full chain predicate rejected 2,148,875 initial hits across the six CPD runs. 2,147,270 (99.9253%) failed after exactly one complete level; only 84 (0.0039%) survived two or more levels before rejection.
- At drop 0.95, the full chain predicate rejected 490,333 initial hits across the six CPD runs. 490,326 (99.9986%) failed after exactly one complete level; only 2 (0.0004%) survived two or more levels before rejection.
- Accepted-route anchor accuracy is already 99.9409%–99.9905% at drop 0.75/0.95, while canonical whole-trace cleanliness is materially lower. Most remaining errors are therefore inside the recovered named topology rather than checkpoint/path anchoring.
- The positive across-CPD correlations do not establish that longer chains cause the accuracy gain. Within each fixed CPD, the mean-length correlation is weakly negative, and almost no candidate is rejected only after matching two or more levels. CPD simultaneously changes fanout/HA availability, checkpoint density, candidate population, and Bloom geometry.

At drop rate 1.0, all non-checkpoint records are dropped, so selected survivor-anchor chains necessarily have zero probabilistic levels. Any CPD-dependent accuracy difference there must come from another mechanism (for example Bloom geometry or HA/fanout routing), not surviving-anchor chain corroboration.
