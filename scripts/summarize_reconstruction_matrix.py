#!/usr/bin/env python3
"""Validate and summarize the canonical first-100k reconstruction matrix."""

import json
import re
import sys
from pathlib import Path


MODELS = ("pb0", "cgp0", "sb3")
CPDS = range(3, 9)
RATES = (0.05, 0.25, 0.5, 0.75, 0.95, 1.0)


def pct(numerator, denominator):
    return 100.0 * numerator / denominator if denominator else 100.0


def elapsed_seconds(log_text):
    match = re.search(
        r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([0-9:.]+)",
        log_text,
    )
    if not match:
        return None
    parts = [float(part) for part in match.group(1).split(":")]
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60 + part
    return seconds


def fmt_duration(seconds):
    if seconds is None:
        return "n/a"
    rounded = int(round(seconds))
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "output/reconstruction_matrix_first100k_prime_up_ha_safe")
    rows = {}
    errors = []

    for mode in MODELS:
        for cpd in CPDS:
            stem = root / f"{mode}_cpd{cpd}"
            json_path = stem.with_suffix(".json")
            exit_path = stem.with_suffix(".exit")
            log_path = stem.with_suffix(".log")
            if not json_path.exists() or not exit_path.exists() or not log_path.exists():
                errors.append(f"missing artifact(s): {stem}")
                continue
            if exit_path.read_text().strip() != "0":
                errors.append(f"nonzero exit: {exit_path}")
                continue

            data = json.loads(json_path.read_text())
            if data.get("mode") != mode or data.get("checkpoint_distance") != cpd:
                errors.append(f"configuration mismatch: {json_path}")
            summaries = data.get("rate_summaries", [])
            if len(summaries) != len(RATES):
                errors.append(f"expected six rates: {json_path}")
                continue

            by_rate = {}
            for expected_rate, item in zip(RATES, summaries):
                rate = float(item["drop_rate"])
                if abs(rate - expected_rate) > 1e-12:
                    errors.append(f"rate ordering mismatch: {json_path}")
                topology = item["topology_summary"]
                if topology.get("traces") != 100000:
                    errors.append(f"trace count is not 100000 at {mode}/cpd{cpd}/r{rate}")
                if topology.get("evidence_profile") != "maximal":
                    errors.append(f"non-maximal evidence at {mode}/cpd{cpd}/r{rate}")
                expected_score_policy = (
                    "path-evidence-v1" if mode == "pb0" else "evidence-bounded-v1"
                )
                if topology.get("score_policy") != expected_score_policy:
                    errors.append(
                        f"unexpected score policy at {mode}/cpd{cpd}/r{rate}: "
                        f"expected {expected_score_policy}, got {topology.get('score_policy')}"
                    )
                if mode == "sb3":
                    structure = item.get("sb3_summary", {})
                    if structure.get("checked") != 100000:
                        errors.append(f"SB3 checked count mismatch at cpd{cpd}/r{rate}")
                    for field in ("hard_conflicts", "parent_conflicts", "ha_conflicts"):
                        if structure.get(field) != 0:
                            errors.append(f"SB3 {field} is nonzero at cpd{cpd}/r{rate}")
                    if structure.get("structure_complete") != structure.get("structure_checked"):
                        errors.append(f"SB3 structure is incomplete at cpd{cpd}/r{rate}")
                else:
                    greedy = item.get("greedy_summary", {})
                    if greedy.get("checked") != 100000:
                        errors.append(f"{mode} checked count mismatch at cpd{cpd}/r{rate}")
                    for field in ("hard_conflicts", "parent_conflicts", "ha_conflicts"):
                        if greedy.get(field) != 0:
                            errors.append(f"{mode} {field} is nonzero at cpd{cpd}/r{rate}")
                by_rate[rate] = item

            log_text = log_path.read_text(errors="replace")
            rows[(mode, cpd)] = {
                "rates": by_rate,
                "elapsed": elapsed_seconds(log_text),
            }

    if errors:
        print("Validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    lines = [
        "# First-100k Day-1 canonical reconstruction matrix",
        "",
        "Prime-up, maximal evidence, prefix length 8, per-trace drop seed 42. "
        "SB3 additionally uses 64-bit owner fingerprints and Lehmer EE/DEE coding.",
        "",
        "PB0 is scored with the canonical path-evidence policy; CGP0 and SB3 are "
        "scored with the canonical evidence-bounded topology policy. Consequently, "
        "PB0's 100% result at drop 1 means that, with no surviving records, there is "
        "no observable path evidence that can make an anonymous reconstruction wrong.",
        "",
    ]

    for metric, title in (
        ("nontrivial", "Model-policy-clean among nonempty/feasible traces (%)"),
        ("all", "Model-policy-clean across all traces, including clean empty traces (%)"),
    ):
        lines.extend((f"## {title}", ""))
        header = "| Model | CPD | " + " | ".join(f"drop {rate:g}" for rate in RATES) + " |"
        lines.extend((header, "|---|---:|" + "---:|" * len(RATES)))
        for mode in MODELS:
            for cpd in CPDS:
                cells = []
                for rate in RATES:
                    topology = rows[(mode, cpd)]["rates"][rate]["topology_summary"]
                    if metric == "nontrivial":
                        value = pct(topology["clean"], topology["feasible"])
                    else:
                        value = pct(topology["clean_all"], topology["traces"])
                    cells.append(f"{value:.4f}")
                lines.append(f"| {mode.upper()} | {cpd} | " + " | ".join(cells) + " |")
        lines.append("")

    lines.extend(("## SB3 structure-complete among topology-clean traces (%)", ""))
    header = "| CPD | " + " | ".join(f"drop {rate:g}" for rate in RATES) + " |"
    lines.extend((header, "|---:|" + "---:|" * len(RATES)))
    for cpd in CPDS:
        cells = []
        for rate in RATES:
            structure = rows[("sb3", cpd)]["rates"][rate]["sb3_summary"]
            cells.append(f"{pct(structure['structure_complete'], structure['structure_checked']):.4f}")
        lines.append(f"| {cpd} | " + " | ".join(cells) + " |")
    lines.append("")

    lines.extend(("## Wall-clock runtime per six-rate sweep", "", "| Model | CPD | Runtime |", "|---|---:|---:|"))
    for mode in MODELS:
        for cpd in CPDS:
            lines.append(f"| {mode.upper()} | {cpd} | {fmt_duration(rows[(mode, cpd)]['elapsed'])} |")
    lines.append("")
    total = sum(row["elapsed"] or 0.0 for row in rows.values())
    lines.append(f"Aggregate process wall time: {fmt_duration(total)}.")
    lines.append("")

    report = root / "analysis.md"
    report.write_text("\n".join(lines))
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
