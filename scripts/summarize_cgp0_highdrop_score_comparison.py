#!/usr/bin/env python3
"""Validate and summarize the controlled high-drop CGP0 score experiment."""

import csv
import json
import math
import pathlib
import sys


PRIMES = ("up", "none")
RECONSTRUCTORS = ("maximal", "legacy")
CPDS = tuple(range(3, 9))
RATES = (0.75, 0.95, 1.0)


def error_pct(clean: int, denominator: int) -> float:
    return math.nan if denominator == 0 else 100.0 * (1.0 - clean / denominator)


def fmt(value: float) -> str:
    return "n/a" if math.isnan(value) else f"{value:.4f}%"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load_cells(result_dir: pathlib.Path):
    cells = {}
    for prime in PRIMES:
        for reconstructor in RECONSTRUCTORS:
            for cpd in CPDS:
                stem = result_dir / f"{reconstructor}_{prime}_cpd{cpd}"
                require(stem.with_suffix(".exit").read_text().strip() == "0", f"nonzero exit: {stem}")
                doc = json.loads(stem.with_suffix(".json").read_text())
                require(doc["mode"] == "cgp0", f"wrong mode: {stem}")
                require(doc["num_traces"] == 100000, f"wrong trace count: {stem}")
                require(doc["checkpoint_distance"] == cpd, f"wrong CPD: {stem}")
                require(bool(doc.get("prime_m", False)) == (prime == "up"), f"wrong prime mode: {stem}")
                require(bool(doc.get("cgp0_legacy", False)) == (reconstructor == "legacy"), f"wrong reconstructor: {stem}")
                require(doc.get("compare_scorers") is True, f"comparison absent: {stem}")
                require(doc.get("per_trace_drop_seed") is True, f"drop seeding mismatch: {stem}")
                summaries = doc["rate_summaries"]
                require(len(summaries) == len(RATES), f"wrong rate count: {stem}")
                for rate, summary in zip(RATES, summaries):
                    require(abs(summary["drop_rate"] - rate) < 1e-12, f"wrong rate/order: {stem}")
                    comp = summary["scorer_comparison"]
                    topo = summary["topology_summary"]
                    require(comp["traces"] == 100000, f"comparison trace count mismatch: {stem}")
                    require(comp["input_obligations"] == topo["feasible"], f"obligation mismatch: {stem}")
                    require(comp["canonical_clean_on_input_obligations"] == topo["clean"], f"canonical tally mismatch: {stem}")
                    partition = (
                        comp["both_clean_on_input_obligations"]
                        + comp["canonical_only_clean_on_input_obligations"]
                        + comp["historical_only_clean_on_input_obligations"]
                        + comp["both_wrong_on_input_obligations"]
                    )
                    require(partition == comp["input_obligations"], f"disagreement partition mismatch: {stem}")
                    require(
                        comp["obligation_and_emitted"] + comp["obligation_no_emission"]
                        == comp["input_obligations"],
                        f"obligation/emission partition mismatch: {stem}",
                    )
                    require(
                        comp["obligation_and_emitted"] + comp["emission_no_obligation"]
                        == comp["emitted_reconstructions"],
                        f"emission/obligation partition mismatch: {stem}",
                    )
                    cells[(prime, reconstructor, cpd, rate)] = comp
    return cells


def write_csv(path: pathlib.Path, cells) -> None:
    fields = [
        "prime_mode", "reconstructor", "cpd", "drop_rate", "traces",
        "input_obligations", "emitted_reconstructions", "obligation_and_emitted",
        "obligation_no_emission", "emission_no_obligation",
        "canonical_clean_input", "historical_clean_input",
        "canonical_clean_emitted", "historical_clean_emitted",
        "canonical_error_input_pct", "historical_error_input_pct",
        "canonical_error_emitted_pct", "historical_error_emitted_pct",
        "both_clean_input", "canonical_only_clean_input",
        "historical_only_clean_input", "both_wrong_input",
        "canonical_wrong_segments_input", "historical_wrong_units_input",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for prime in PRIMES:
            for reconstructor in RECONSTRUCTORS:
                for cpd in CPDS:
                    for rate in RATES:
                        c = cells[(prime, reconstructor, cpd, rate)]
                        writer.writerow({
                            "prime_mode": prime,
                            "reconstructor": reconstructor,
                            "cpd": cpd,
                            "drop_rate": rate,
                            "traces": c["traces"],
                            "input_obligations": c["input_obligations"],
                            "emitted_reconstructions": c["emitted_reconstructions"],
                            "obligation_and_emitted": c["obligation_and_emitted"],
                            "obligation_no_emission": c["obligation_no_emission"],
                            "emission_no_obligation": c["emission_no_obligation"],
                            "canonical_clean_input": c["canonical_clean_on_input_obligations"],
                            "historical_clean_input": c["historical_clean_on_input_obligations"],
                            "canonical_clean_emitted": c["canonical_clean_on_emitted_reconstructions"],
                            "historical_clean_emitted": c["historical_clean_on_emitted_reconstructions"],
                            "canonical_error_input_pct": f'{error_pct(c["canonical_clean_on_input_obligations"], c["input_obligations"]):.8f}',
                            "historical_error_input_pct": f'{error_pct(c["historical_clean_on_input_obligations"], c["input_obligations"]):.8f}',
                            "canonical_error_emitted_pct": f'{error_pct(c["canonical_clean_on_emitted_reconstructions"], c["emitted_reconstructions"]):.8f}',
                            "historical_error_emitted_pct": f'{error_pct(c["historical_clean_on_emitted_reconstructions"], c["emitted_reconstructions"]):.8f}',
                            "both_clean_input": c["both_clean_on_input_obligations"],
                            "canonical_only_clean_input": c["canonical_only_clean_on_input_obligations"],
                            "historical_only_clean_input": c["historical_only_clean_on_input_obligations"],
                            "both_wrong_input": c["both_wrong_on_input_obligations"],
                            "canonical_wrong_segments_input": c["canonical_wrong_segments_on_input_obligations"],
                            "historical_wrong_units_input": c["historical_wrong_units_on_input_obligations"],
                        })


def write_markdown(path: pathlib.Path, cells) -> None:
    maximal = [
        cells[(prime, "maximal", cpd, rate)]
        for prime in PRIMES for cpd in CPDS for rate in RATES
    ]
    scorer_delta = [
        error_pct(c["canonical_clean_on_input_obligations"], c["input_obligations"])
        - error_pct(c["historical_clean_on_input_obligations"], c["input_obligations"])
        for c in maximal
    ]
    missing_emissions = sum(
        c["obligation_no_emission"] for c in cells.values()
    )
    spurious_emissions = sum(
        c["emission_no_obligation"] for c in cells.values()
    )
    legacy_canonical = [
        error_pct(cells[(prime, "legacy", cpd, rate)]["canonical_clean_on_input_obligations"],
                  cells[(prime, "legacy", cpd, rate)]["input_obligations"])
        for prime in PRIMES for cpd in CPDS for rate in RATES
    ]
    lines = [
        "# CGP0 high-drop scorer/reconstructor decomposition",
        "",
        "Controlled corpus: first 100,000 Day 1 traces; per-trace-seeded drops (seed 42); "
        "drop rates 0.75, 0.95, and 1.0 only; CPD 3–8; Bloom target 0.0001; "
        "prime-up and no-prime modes.",
        "",
        "`C/input` is the canonical evidence-bounded scorer on obligations determined from "
        "surviving records. `H/input` substitutes the historical permissive node-plus-anchor "
        "scorer without changing that denominator. `H/emitted` reproduces the complete "
        "historical evaluation contract: historical scorer and the output-dependent "
        "`Reconnected > 0` denominator. All entries are trace error rates.",
        "",
        "## Principal findings",
        "",
        f"- Denominator selection has no effect in these high-drop cells: there were {missing_emissions} "
        f"input obligations without an emitted reconstruction and {spurious_emissions} emissions without an input obligation.",
        f"- On the same maximal reconstruction, changing only the scorer increases trace error by "
        f"{min(scorer_delta):.4f}–{max(scorer_delta):.4f} percentage points. The scorer change alone therefore does not explain the full visual gap.",
        f"- Legacy CGP0 has {min(legacy_canonical):.4f}–{max(legacy_canonical):.4f}% canonical error: it systematically emits "
        "anonymous nodes where surviving `ParentID` records make the identities nameable.",
        "- Historical scores for maximal and legacy output are not a fixed-domain algorithm comparison. The historical scorer grades "
        "non-anonymous reconstructed nodes, so maximal exact-parent materialization exposes upstream edges that legacy output hides. "
        "An anchor-only breakdown or a hybrid using legacy anchor choices with canonical materialization is required to isolate route-selection effects.",
        "",
    ]
    for prime in PRIMES:
        prime_title = "prime-up" if prime == "up" else "no-prime"
        for rate in RATES:
            lines += [
                f"## {prime_title}, drop {rate:g}",
                "",
                "| CPD | maximal C/input | maximal H/input | maximal H/emitted | legacy C/input | legacy H/input | legacy H/emitted |",
                "|---:|---:|---:|---:|---:|---:|---:|",
            ]
            for cpd in CPDS:
                m = cells[(prime, "maximal", cpd, rate)]
                l = cells[(prime, "legacy", cpd, rate)]
                vals = []
                for c in (m, l):
                    vals.extend((
                        error_pct(c["canonical_clean_on_input_obligations"], c["input_obligations"]),
                        error_pct(c["historical_clean_on_input_obligations"], c["input_obligations"]),
                        error_pct(c["historical_clean_on_emitted_reconstructions"], c["emitted_reconstructions"]),
                    ))
                lines.append(f"| {cpd} | " + " | ".join(fmt(v) for v in vals) + " |")
            lines.append("")

    lines += [
        "## Exact interpretation",
        "",
        "For each fixed prime mode, CPD, and drop rate:",
        "",
        "- `maximal C/input − legacy C/input` isolates the reconstructor change under the canonical contract.",
        "- `C/input − H/input` isolates the scorer change on exactly the same traces.",
        "- `H/input − H/emitted` isolates the output-dependent denominator change while holding the historical scorer fixed.",
        "- The raw counts and scorer-disagreement contingency table are in `comparison_cells.csv`; no pooled averages are used in the tables above.",
        "",
        "This experiment does not claim bit-for-bit reproduction of the old paper figure, which pooled full Day 1 and Day 2. It is a controlled causal decomposition on the same first-100k Day 1 population used by the current figures.",
        "",
    ]
    path.write_text("\n".join(lines))


def main() -> None:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "output/cgp0_highdrop_score_comparison_first100k").resolve()
    cells = load_cells(root / "results")
    write_csv(root / "comparison_cells.csv", cells)
    write_markdown(root / "analysis.md", cells)
    print(f"validated {len(cells)} cells")
    print(root / "comparison_cells.csv")
    print(root / "analysis.md")


if __name__ == "__main__":
    main()
