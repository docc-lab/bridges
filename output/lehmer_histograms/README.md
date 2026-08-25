# S-Bridge Lehmer EE/DEE size histograms

These files were produced from both unfiltered Uber corpus days, not from
`/mydata/uber/corpus_full` (the cleaned day-1 corpus).

## Inputs

| Input corpus | Traces | Spans |
| --- | ---: | ---: |
| `/mydata/uber/bignode_state/day1_unfilt_corpus` | 521,305 | 475,488,643 |
| `/mydata/uber/bignode_state/day2_unfilt_corpus` | 843,274 | 800,890,614 |
| Combined | 1,364,579 | 1,276,379,257 |

## Configuration

- Mode: `sbridge`
- Checkpoint distance: 4
- Lehmer EE/DEE encoding: enabled (`--lehmer-ee`)
- Trace prefix: 8 bytes
- Fingerprint: 64 bits
- Histogram bins: exact byte sizes (no sampling or bin coarsening)

Each run used `--bagsize --mode sbridge --checkpoint-distance 4 --lehmer-ee
--prefix-len 8 --fp-bits 64 --size-histograms <output>`.

## Outputs

| File | Metric | Count | Sum bytes | Min | Max | Mean |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `sbridge_lehmer_fw_cpd4_overall.json` | Baggage at calls | 1,275,014,678 | 31,158,284,581 | 13 | 128,571 | 24.437589 |
| `sbridge_lehmer_fw_cpd4_overall.json` | Emitted `_br` payload | 718,174,757 | 27,349,411,910 | 16 | 128,584 | 38.081834 |

The `day1` and `day2` JSON files contain the corresponding per-day exact
histograms. The `overall` file was created with `trace_sim histmerge`, which
validates the schema and run configuration and sums counts for matching exact
byte-size bins.

## Single-group Lehmer savings

`sbridge_lehmer_group_savings_overall.json` compares every non-empty EE and
DEE group once, when the handler forms it, across both unfiltered days. It is
an intrinsic per-group comparison, not weighted by how often an EE group later
propagates in breadcrumb baggage.

For EE, the comparison includes the common group-count varint plus either the
legacy ordinal varints or the Lehmer rank. For DEE it additionally includes
the Lehmer format's child-count varint. Fixed DEE trace, depth, and owner fields
are unchanged and therefore cancel out.

| Group | Groups | Legacy mean | Lehmer mean | Mean saving | Aggregate reduction | Maximum byte saving | Maximum percentage saving |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| EE | 14,440,874 | 13.689151 | 9.737303 | 3.951848 bytes | 28.8685% | 611 bytes (1,492 to 881) | 66.6667% (6 to 2) |
| DEE | 79,730,293 | 6.270052 | 4.808210 | 1.461842 bytes | 23.3147% | 5,116 bytes (20,258 to 15,142) | 51.4523% (241 to 117) |
| Combined | 94,171,167 | 7.407749 | 5.564072 | 1.843677 bytes | 24.8885% | 5,116 bytes | 66.6667% |

Savings are signed averages. A minority of sparse EE groups and many one-item
DEE groups grow: the worst EE case was +6 bytes (50 to 56), and the worst DEE
case was +1 byte (2 to 3).
