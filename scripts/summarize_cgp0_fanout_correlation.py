#!/usr/bin/env python3
"""Summarize the prime-up/drop-1 CGP0 fanout-correlation sweep."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "cgp0_fanout_correlation_drop1_first100k_prime_up"
RESULTS = OUT / "results"


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    den = math.sqrt(sum(x * x for x in dx) * sum(y * y for y in dy))
    if den == 0:
        return None
    return sum(x * y for x, y in zip(dx, dy)) / den


def pct(n: int, d: int) -> float:
    return 100.0 * n / d if d else 100.0


def fmt(value: float | None, digits: int = 4) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def zero_positive_split(bins: list[dict[str, object]]) -> tuple[int, int, int, int]:
    zero_n = zero_correct = positive_n = positive_correct = 0
    for item in bins:
        n = int(item["route_units"])
        correct = int(item["topology_correct"])
        if item["bucket"] == "0":
            zero_n += n
            zero_correct += correct
        else:
            positive_n += n
            positive_correct += correct
    return zero_n, zero_n - zero_correct, positive_n, positive_n - positive_correct


rows: list[dict[str, object]] = []
route_bins: list[dict[str, object]] = []
window_bins: list[dict[str, object]] = []
for cpd in range(3, 9):
    path = RESULTS / f"cgp0_prime_up_drop1_cpd{cpd}.json"
    with path.open() as handle:
        doc = json.load(handle)
    topo = doc["topology_summary"]
    fan = doc["greedy_summary"]["fanout_evidence"]
    required_split = zero_positive_split(
        fan["route_performance_by_required_ha_fanouts"]
    )
    groups_split = zero_positive_split(
        fan["route_performance_by_applicable_fanout_groups"]
    )
    tests_split = zero_positive_split(
        fan["route_performance_by_fanout_candidate_tests"]
    )
    row = {
        "cpd": cpd,
        "traces": topo["traces"],
        "clean_traces": topo["clean_all"],
        "clean_rate_pct": pct(topo["clean_all"], topo["traces"]),
        "edge_wrong": topo["edge_wrong"],
        "carrier_windows": fan["carrier_windows"],
        "local_window_accuracy_pct": pct(
            fan["locally_correct_carrier_windows"], fan["carrier_windows"]
        ),
        "truth_fanouts_per_window": (
            fan["truth_fanout_occurrences_on_window_paths"] / fan["carrier_windows"]
        ),
        "ha_entries_per_window": fan["mean_ha_entries_per_carrier_window"],
        "ha_path_coverage": fan["weighted_ha_path_coverage"],
        "known_fanout_path_coverage": fan["weighted_known_fanout_path_coverage"],
        "route_units": fan["routed_units_measured"],
        "route_anchor_accuracy_pct": pct(
            fan["correct_anchor_routes"], fan["routed_units_measured"]
        ),
        "route_topology_accuracy_pct": pct(
            fan["correct_topology_routes"], fan["routed_units_measured"]
        ),
        "required_ha_per_route": fan["mean_required_ha_fanouts_per_route"],
        "applicable_fanout_groups_per_route": fan[
            "mean_applicable_fanout_groups_per_route"
        ],
        "multi_bloom_fanout_groups_per_route": fan[
            "mean_multi_bloom_fanout_groups_per_route"
        ],
        "fanout_candidate_tests_per_route": fan[
            "mean_fanout_candidate_tests_per_route"
        ],
        "r_window_truth_fanouts_vs_local_correct": fan[
            "pearson_window_truth_fanout_count_vs_local_path_correct"
        ],
        "r_window_ha_vs_local_correct": fan[
            "pearson_window_ha_count_vs_local_path_correct"
        ],
        "r_window_ha_coverage_vs_local_correct": fan[
            "pearson_window_ha_coverage_vs_local_path_correct"
        ],
        "r_route_required_ha_vs_topology_correct": fan[
            "pearson_required_ha_fanouts_vs_route_topology_correct"
        ],
        "r_route_fanout_groups_vs_topology_correct": fan[
            "pearson_applicable_fanout_groups_vs_route_topology_correct"
        ],
        "r_route_multi_bloom_vs_topology_correct": fan[
            "pearson_multi_bloom_fanout_groups_vs_route_topology_correct"
        ],
        "r_route_fanout_tests_vs_topology_correct": fan[
            "pearson_fanout_candidate_tests_vs_route_topology_correct"
        ],
        "ha_entries_off_window_paths": fan["ha_entries_off_window_paths"],
        "required_ha_zero_routes": required_split[0],
        "required_ha_zero_errors": required_split[1],
        "required_ha_positive_routes": required_split[2],
        "required_ha_positive_errors": required_split[3],
        "fanout_groups_zero_routes": groups_split[0],
        "fanout_groups_zero_errors": groups_split[1],
        "fanout_groups_positive_routes": groups_split[2],
        "fanout_groups_positive_errors": groups_split[3],
        "fanout_tests_zero_routes": tests_split[0],
        "fanout_tests_zero_errors": tests_split[1],
        "fanout_tests_positive_routes": tests_split[2],
        "fanout_tests_positive_errors": tests_split[3],
    }
    rows.append(row)

    for metric, key in (
        ("required_ha_fanouts", "route_performance_by_required_ha_fanouts"),
        ("applicable_fanout_groups", "route_performance_by_applicable_fanout_groups"),
        ("multi_bloom_fanout_groups", "route_performance_by_multi_bloom_fanout_groups"),
        ("fanout_candidate_tests", "route_performance_by_fanout_candidate_tests"),
    ):
        for item in fan[key]:
            route_bins.append({"cpd": cpd, "metric": metric, **item})
    for metric, key in (
        ("carrier_ha_count", "window_performance_by_ha_count"),
        ("known_fanout_count", "window_performance_by_known_fanout_count"),
        ("truth_fanout_count", "window_performance_by_truth_fanout_count"),
        ("carrier_ha_coverage", "window_performance_by_ha_coverage"),
    ):
        for item in fan[key]:
            window_bins.append({"cpd": cpd, "metric": metric, **item})


def write_csv(path: Path, values: list[dict[str, object]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(values[0]))
        writer.writeheader()
        writer.writerows(values)


write_csv(OUT / "fanout_correlation_cells.csv", rows)
write_csv(OUT / "fanout_correlation_route_bins.csv", route_bins)
write_csv(OUT / "fanout_correlation_window_bins.csv", window_bins)

clean_rates = [float(row["clean_rate_pct"]) for row in rows]
across = {
    key: pearson([float(row[key]) for row in rows], clean_rates)
    for key in (
        "truth_fanouts_per_window",
        "ha_entries_per_window",
        "ha_path_coverage",
        "required_ha_per_route",
        "applicable_fanout_groups_per_route",
        "multi_bloom_fanout_groups_per_route",
        "fanout_candidate_tests_per_route",
    )
}

lines = [
    "# CGP0 fanout-evidence correlation at drop rate 1",
    "",
    "Population: first 100,000 unfiltered Day-1 traces; prime-up Bloom sizing; "
    "maximal-evidence CGP0; CPD 3–8; drop rate 1; seed 42.",
    "",
    "A *carrier window* is the full or partial window terminated by any `_br` "
    "emitter. This includes leaf checkpoints; leaves contribute evidence but are "
    "excluded from reconnection candidates. A *known fanout* is established by "
    "an HA witness or by at least two surviving fragments naming the same missing "
    "parent. Local path correctness uses truth only for evaluation.",
    "",
    "## Aggregate results",
    "",
    "| CPD | clean traces | local carrier paths | true fanouts/window | HA/window | HA path coverage | applicable fanout groups/route | multi-Bloom groups/route | fanout tests/route | route topology | anchor sanity |",
    "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
]
for row in rows:
    lines.append(
        f"| {row['cpd']} | {row['clean_rate_pct']:.4f}% | "
        f"{row['local_window_accuracy_pct']:.5f}% | "
        f"{row['truth_fanouts_per_window']:.4f} | "
        f"{row['ha_entries_per_window']:.4f} | "
        f"{100 * row['ha_path_coverage']:.2f}% | "
        f"{row['applicable_fanout_groups_per_route']:.4f} | "
        f"{row['multi_bloom_fanout_groups_per_route']:.4f} | "
        f"{row['fanout_candidate_tests_per_route']:.4f} | "
        f"{row['route_topology_accuracy_pct']:.5f}% | "
        f"{row['route_anchor_accuracy_pct']:.5f}% |"
    )

lines.extend(
    [
        "",
        "## Within-CPD point-biserial correlations",
        "",
        "Positive values mean the evidence measure is associated with a correct "
        "local path or route topology. Raw fanout count is also a complexity measure, "
        "so its sign must not be interpreted as an isolated treatment effect.",
        "",
        "| CPD | true fanouts vs local path | carrier HA vs local path | HA coverage vs local path | required HA vs route topology | fanout groups vs route topology | multi-Bloom groups vs route topology | fanout tests vs route topology |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
)
for row in rows:
    lines.append(
        f"| {row['cpd']} | {fmt(row['r_window_truth_fanouts_vs_local_correct'])} | "
        f"{fmt(row['r_window_ha_vs_local_correct'])} | "
        f"{fmt(row['r_window_ha_coverage_vs_local_correct'])} | "
        f"{fmt(row['r_route_required_ha_vs_topology_correct'])} | "
        f"{fmt(row['r_route_fanout_groups_vs_topology_correct'])} | "
        f"{fmt(row['r_route_multi_bloom_vs_topology_correct'])} | "
        f"{fmt(row['r_route_fanout_tests_vs_topology_correct'])} |"
    )

lines.extend(
    [
        "",
        "## Conditional route-topology error rates",
        "",
        "The zero-versus-positive comparisons are descriptive. They condition on "
        "CPD but not on trace size or route ambiguity, and route units from the same "
        "trace are not statistically independent.",
        "",
        "| CPD | no required HA | ≥1 required HA | no applicable fanout group | ≥1 applicable fanout group | no fanout test triggered | ≥1 fanout test triggered |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
)
for row in rows:
    rate = lambda errors, total: 100.0 * errors / total if total else 0.0
    lines.append(
        f"| {row['cpd']} | "
        f"{rate(row['required_ha_zero_errors'], row['required_ha_zero_routes']):.4f}% | "
        f"{rate(row['required_ha_positive_errors'], row['required_ha_positive_routes']):.4f}% | "
        f"{rate(row['fanout_groups_zero_errors'], row['fanout_groups_zero_routes']):.4f}% | "
        f"{rate(row['fanout_groups_positive_errors'], row['fanout_groups_positive_routes']):.4f}% | "
        f"{rate(row['fanout_tests_zero_errors'], row['fanout_tests_zero_routes']):.4f}% | "
        f"{rate(row['fanout_tests_positive_errors'], row['fanout_tests_positive_routes']):.4f}% |"
    )

lines.extend(
    [
        "",
        "## Across-CPD ecological correlations with clean-trace rate",
        "",
        "These correlations have only six CPD-level observations. They test whether "
        "the proposed mechanism moves with the CPD trend, but cannot establish that "
        "the mechanism causes the trend.",
        "",
        "| aggregate measure | Pearson r |",
        "|---|---:|",
    ]
)
for key, value in across.items():
    lines.append(f"| {key.replace('_', ' ')} | {fmt(value)} |")

lines.extend(
    [
        "",
        "## Interpretation",
        "",
        "The data support the fanout-evidence hypothesis as a **partial mechanism**, "
        "not as a complete causal explanation:",
        "",
        f"- Mean applicable fanout groups per route increase from "
        f"{rows[0]['applicable_fanout_groups_per_route']:.4f} at CPD 3 to "
        f"{rows[4]['applicable_fanout_groups_per_route']:.4f} at CPD 7, then fall "
        f"to {rows[5]['applicable_fanout_groups_per_route']:.4f} at CPD 8. Their "
        f"six-cell ecological correlation with clean-trace rate is "
        f"{fmt(across['applicable_fanout_groups_per_route'])}. The CPD 7 peak and "
        "CPD 8 decline move in the same direction as reconstruction accuracy.",
        "- Within every CPD, route units with at least one applicable fanout group "
        "have a lower topology-error rate than units with none. Required HA evidence "
        "has the same direction in every cell, although its advantage narrows at the "
        "larger CPDs.",
        "- Merely placing more true fanouts on an individual carrier path does not "
        "make that path easier: the within-CPD correlations are essentially zero at "
        "CPD 3–4 and increasingly negative thereafter. Fanout count is also route "
        "complexity; the helpful variable is usable hard/grouped evidence, not raw "
        "branching alone.",
        "- A triggered fanout candidate test is associated with more errors. This "
        "does not imply that the test harms reconstruction: such tests occur only "
        "when an otherwise-admissible candidate reaches a fanout constraint, so the "
        "counter preferentially identifies ambiguous routes (endogenous selection).",
        "- Global known-fanout path coverage is 100% in every cell. At drop rate 1 "
        "this follows the design: a fanout's second-child lineage carries its HA to "
        "a protected periodic or leaf checkpoint. Carrier-local HA coverage falls as "
        "windows contain more fanouts, so the benefit comes from pooling evidence "
        "across route members rather than from every carrier naming every fanout.",
        "",
        "Thus the results are consistent with longer windows improving topology in "
        "part because they expose more HA/exact-parent fanout constraints to a shared "
        "route decision. They do not show that this is the only CPD-dependent effect; "
        "a controlled fanout-evidence ablation or matched-stratum analysis would be "
        "needed for a causal estimate.",
    ]
)

off_path = sum(int(row["ha_entries_off_window_paths"]) for row in rows)
lines.extend(
    [
        "",
        "## Instrumentation invariant",
        "",
        f"Off-window HA records: **{off_path}**. The expected value is zero.",
        "",
        "Detailed conditional bins are in `fanout_correlation_route_bins.csv` and "
        "`fanout_correlation_window_bins.csv`.",
    ]
)
(OUT / "analysis.md").write_text("\n".join(lines) + "\n")
print(OUT / "analysis.md")
