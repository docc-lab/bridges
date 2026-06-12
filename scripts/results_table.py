#!/usr/bin/env python3
"""Print the canonical results table from trace_recon result JSONs.

Usage: results_table.py LABEL=path.json [LABEL=path.json ...]

Field semantics (verified against the v8e reference numbers):
  num_anchor_correct  C  bridges whose anchor is the NEAREST surviving true
                         ancestor (best possible against the reduced set)
  num_anchor_ancestor A  bridges whose anchor is ANY true ancestor (C plus
                         the too-shallow ones)
  num_misattached     W  bridges whose anchor is not the nearest surviving
                         ancestor (includes the too-shallow true ancestors)
  num_reconnected     R  total scored bridges; R == C + W

  exact%  = C / R           lineage and depth both right
  wrong%  = W / R           anchor != nearest surviving ancestor. ANY
                            non-exact attachment is wrong (per project
                            ruling: attaching to B when C was the true
                            attachment point is an error, full stop).
  benign% = (A - C) / R     sub-class of wrong: anchor still a true
                            ancestor (no false ancestry, depth lost) —
                            shown for diagnosis, already inside wrong%
  tr%     = % of traces with at least one wrong bridge
"""
import json
import os
import sys


def tolist(v, n):
    return v if isinstance(v, list) else [v] + [0] * (n - 1)


def row(path):
    d = json.load(open(os.path.expanduser(path)))
    n = d["num_traces"]
    C = tolist(d["num_anchor_correct"], n)
    A = tolist(d["num_anchor_ancestor"], n)
    W = tolist(d["num_misattached"], n)
    sC, sA, sW = sum(C), sum(A), sum(W)
    R = sC + sW

    def s(k):
        v = d.get(k, 0)
        return sum(v) if isinstance(v, list) else v

    spans = s("num_spans")
    oe, oem = s("num_open_ends"), s("num_open_ends_matched")
    return {
        "exact": 100 * sC / R,
        "benign": 100 * (sA - sC) / R,
        "wrong": 100 * sW / R,
        "tr": 100 * sum(1 for w in W if w > 0) / n,
        "oem": 100 * oem / oe if oe else 100.0,
        "lost": 100 * s("num_spans_lost") / spans,
        "placed": s("num_orphans_placed"),
        "ooe": s("num_orphan_open_ends"),
        "forced": s("num_forced_matches"),
    }


def main():
    print(f"{'config':>24} | {'exact%':>7} | {'benign%':>7} | "
          f"{'wrong% (tr%)':>16} | {'oe match':>10} | {'lost%':>7} | "
          f"{'placed':>7} | {'o-oe':>5} | {'forced':>6}")
    for arg in sys.argv[1:]:
        if arg == "--":
            print()
            continue
        label, path = arg.split("=", 1)
        m = row(path)
        print(f"{label:>24} | {m['exact']:6.2f}% | {m['benign']:6.2f}% | "
              f"{m['wrong']:7.4f}% ({m['tr']:5.2f}) | {m['oem']:9.4f}% | "
              f"{m['lost']:6.3f}% | {m['placed']:7d} | {m['ooe']:5d} | "
              f"{m['forced']:6d}")


if __name__ == "__main__":
    main()
