# SB3 full Day-1 topology + structure reconstruction

## Configuration

- Corpus: full unfiltered Day 1 (`521,305` traces)
- Checkpoint distance: `4`
- Drop rate: `0.5`
- Drop seed: `42`, per-trace seeded
- Bloom false-positive target: `0.0001`, prime-up sizing
- Checkpoint anchor: `8` bytes
- DEE owner fingerprint: `64` bits
- EE/DEE encoding: Lehmer
- Reconstruction evidence profile: maximal
- Workers: `14`

## Topology

- Nontrivial clean: `489,620 / 512,334` (`95.566564%`)
- All-trace clean: `498,591 / 521,305` (`95.642858%`)
- Exact reconstructed edges: `437,938,201 / 455,101,746` (`96.229%`)
- Surviving edges exact: `368,249,023 / 368,249,023` (`100%`)
- Ordinal-compatible traces: `499,866 / 521,305` (`95.887436%`)
- Hard surviving-record conflicts: `0`
- Explicit sparse ordinal placements: `192,966,261`
- Inferred first/unary-child ordinal-1 labels: `281,668,115`

These topology totals are identical to the preceding canonical prime-up SB3
run. Adding the structure phase did not alter topology selection or scoring.

## Structure, conditional on clean topology

- Checked: `498,591`
- Structurally complete: `498,591 / 498,591` (`100%`)
- Incomplete: `0`
- DEE records placed: `19,001,541`
- DEE owner ambiguity: `0`
- DEE with no valid owner: `0`
- Multi-child parent end orders exact: `19,002,521 / 19,002,521` (`100%`)
- Full trace event order exact: `498,591 / 498,591` (`100%`)
- Critical path exact: `498,591 / 498,591` (`100%`)

The structure percentages are conditional on the topology being clean. With
topology failures counted as full-pipeline failures, the end-to-end clean rate
is the all-trace topology rate, `95.642858%`.

## Runtime

- Wall time: `23m12.46s`
- Peak RSS: `1,390,992 KiB`
- Exit status: `0`

Raw counters are in `sb3.json`; progress and `/usr/bin/time -v` output are in
`sb3.log`.
