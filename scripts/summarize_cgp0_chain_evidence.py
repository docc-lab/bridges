#!/usr/bin/env python3
"""Summarize the CGP0 high-drop chain-evidence JSON outputs."""

from __future__ import annotations

import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path


def ratio(num: int | float, den: int | float) -> float:
    return float(num) / float(den) if den else math.nan


def weighted_mean(bins: list[dict], key: str, weight: str) -> float:
    total = sum(int(row[weight]) for row in bins)
    return ratio(sum(float(row[key]) * int(row[weight]) for row in bins), total)


def fmt_pct(value: float) -> str:
    return "NA" if math.isnan(value) else f"{100 * value:.4f}%"


def fmt_corr(value: float | None) -> str:
    return "NA" if value is None else f"{value:+.4f}"


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    dx = sum((x - mx) ** 2 for x in xs)
    dy = sum((y - my) ** 2 for y in ys)
    if dx == 0 or dy == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(dx * dy)


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "output/cgp0_chain_evidence_first100k_prime_up")
    result_dir = root / "results"
    rows: list[dict] = []
    route_bins: list[dict] = []
    candidate_bins: list[dict] = []
    for path in sorted(result_dir.glob("cgp0_prime_up_cpd*.json")):
        match = re.search(r"cpd(\d+)$", path.stem)
        if not match:
            continue
        cpd = int(match.group(1))
        data = json.loads(path.read_text())
        for rate in data["rate_summaries"]:
            topo = rate["topology_summary"]
            chain = rate["chain_evidence_summary"]
            bins = chain["route_performance_by_matched_levels"]
            routed = int(chain["routed_units"])
            nonzero = sum(int(b["route_units"]) for b in bins if int(b["matched_levels"]) > 0)
            row = {
                "cpd": cpd,
                "drop_rate": float(rate["drop_rate"]),
                "feasible_traces": int(topo["feasible"]),
                "clean_traces": int(topo["clean"]),
                "trace_clean_rate": ratio(topo["clean"], topo["feasible"]),
                "candidate_initial_hits": int(chain["candidate_initial_bloom_hits"]),
                "candidate_chain_rejected": int(chain["candidate_chain_rejected"]),
                "candidate_rejection_rate": ratio(chain["candidate_chain_rejected"], chain["candidate_initial_bloom_hits"]),
                "routed_units": routed,
                "mean_matched_levels": weighted_mean(bins, "matched_levels", "route_units"),
                "nonzero_chain_route_share": ratio(nonzero, routed),
                "anchor_unit_accuracy": ratio(chain["anchor_correct_units"], routed),
                "pearson_trace_mean": chain["pearson_mean_matched_levels_vs_canonical_clean"],
                "pearson_trace_min": chain["pearson_minimum_matched_levels_vs_canonical_clean"],
                "pearson_trace_checks": chain["pearson_mean_positive_bloom_checks_vs_canonical_clean"],
            }
            rows.append(row)
            for b in bins:
                route_bins.append({"cpd": cpd, "drop_rate": row["drop_rate"], **b})
            for outcome, field in (
                ("accepted", "accepted_candidates_by_matched_levels"),
                ("rejected", "rejected_candidates_after_matched_levels"),
            ):
                for b in chain[field]:
                    candidate_bins.append({"cpd": cpd, "drop_rate": row["drop_rate"], "outcome": outcome, **b})

    if not rows:
        print(f"no completed result JSON files under {result_dir}", file=sys.stderr)
        return 1

    rows.sort(key=lambda r: (r["drop_rate"], r["cpd"]))
    root.mkdir(parents=True, exist_ok=True)
    with (root / "chain_evidence_cells.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with (root / "chain_evidence_route_bins.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(route_bins[0]))
        writer.writeheader()
        writer.writerows(route_bins)
    with (root / "chain_evidence_candidate_bins.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(candidate_bins[0]))
        writer.writeheader()
        writer.writerows(candidate_bins)

    by_rate: dict[float, list[dict]] = defaultdict(list)
    for row in rows:
        by_rate[row["drop_rate"]].append(row)

    lines = [
        "# CGP0 matched-chain evidence, first 100k Day-1 traces",
        "",
        "Prime-rounded Bloom geometry; drop rates 0.75, 0.95, and 1.0; CPD 3–8. "
        "A matched level is one non-checkpoint ID on the accepted anchor-to-checkpoint chain "
        "that passed all applicable carrier Blooms.",
        "",
        "| Drop | CPD | Clean traces | Mean matched levels / route | Routes with >0 levels | Initial candidates rejected by chain | Anchor-unit accuracy | r(mean levels, clean) | r(min levels, clean) |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['drop_rate']:.2f} | {row['cpd']} | {fmt_pct(row['trace_clean_rate'])} | "
            f"{row['mean_matched_levels']:.4f} | {fmt_pct(row['nonzero_chain_route_share'])} | "
            f"{row['candidate_chain_rejected']}/{row['candidate_initial_hits']} "
            f"({fmt_pct(row['candidate_rejection_rate'])}) | {fmt_pct(row['anchor_unit_accuracy'])} | "
            f"{fmt_corr(row['pearson_trace_mean'])} | {fmt_corr(row['pearson_trace_min'])} |"
        )

    lines.extend(["", "## Across-CPD cell correlations", ""])
    lines.append("These six-point correlations are descriptive, not causal; CPD changes other evidence and Bloom geometry too.")
    lines.append("")
    lines.append("| Drop | r(mean matched levels, clean rate) | r(nonzero-chain share, clean rate) |")
    lines.append("|---:|---:|---:|")
    for rate, group in sorted(by_rate.items()):
        clean = [r["trace_clean_rate"] for r in group]
        r_mean = pearson([r["mean_matched_levels"] for r in group], clean)
        r_nonzero = pearson([r["nonzero_chain_route_share"] for r in group], clean)
        lines.append(f"| {rate:.2f} | {fmt_corr(r_mean)} | {fmt_corr(r_nonzero)} |")

    lines.extend(["", "## Interpretation", ""])
    for rate in (0.75, 0.95):
        rejected = [
            b for b in candidate_bins
            if b["drop_rate"] == rate and b["outcome"] == "rejected"
        ]
        total = sum(int(b["count"]) for b in rejected)
        after_one = sum(int(b["count"]) for b in rejected if int(b["matched_levels"]) == 1)
        after_two_plus = sum(int(b["count"]) for b in rejected if int(b["matched_levels"]) >= 2)
        lines.append(
            f"- At drop {rate:.2f}, the full chain predicate rejected {total:,} initial hits across the six CPD runs. "
            f"{after_one:,} ({fmt_pct(ratio(after_one, total))}) failed after exactly one complete level; "
            f"only {after_two_plus:,} ({fmt_pct(ratio(after_two_plus, total))}) survived two or more levels before rejection."
        )
    non_drop_one = [r for r in rows if r["drop_rate"] < 1]
    min_anchor = min(r["anchor_unit_accuracy"] for r in non_drop_one)
    max_anchor = max(r["anchor_unit_accuracy"] for r in non_drop_one)
    lines.extend([
        f"- Accepted-route anchor accuracy is already {fmt_pct(min_anchor)}–{fmt_pct(max_anchor)} at drop 0.75/0.95, "
        "while canonical whole-trace cleanliness is materially lower. Most remaining errors are therefore inside "
        "the recovered named topology rather than checkpoint/path anchoring.",
        "- The positive across-CPD correlations do not establish that longer chains cause the accuracy gain. "
        "Within each fixed CPD, the mean-length correlation is weakly negative, and almost no candidate is rejected "
        "only after matching two or more levels. CPD simultaneously changes fanout/HA availability, checkpoint density, "
        "candidate population, and Bloom geometry.",
    ])

    lines.extend([
        "",
        "At drop rate 1.0, all non-checkpoint records are dropped, so selected survivor-anchor "
        "chains necessarily have zero probabilistic levels. Any CPD-dependent accuracy difference "
        "there must come from another mechanism (for example Bloom geometry or HA/fanout routing), "
        "not surviving-anchor chain corroboration.",
        "",
    ])
    (root / "analysis.md").write_text("\n".join(lines))
    print(root / "analysis.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
