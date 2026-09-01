#!/usr/bin/env python3
"""Plot the base and single-pop S-Bridge size-distribution tables as SVG."""

from __future__ import annotations

import argparse
import csv
import html
import math
from pathlib import Path


SERIES = (
    ("Service queues / ordinal-list EE/DEE", "#2563eb", None, "circle"),
    ("Service queues / Lehmer-coded EE/DEE", "#2563eb", "9 6", "square"),
    ("Endpoint-instance queues / ordinal-list EE/DEE", "#d97706", None, "circle"),
    ("Endpoint-instance queues / Lehmer-coded EE/DEE", "#d97706", "9 6", "square"),
)

WIDTH = 1600
HEIGHT = 780
PLOT_TOP = 180
PLOT_BOTTOM = 565
PLOT_HEIGHT = PLOT_BOTTOM - PLOT_TOP
PANELS = ((100, 750), (850, 1500))
LOG_PLOT_BOTTOM = 650
LOG_PLOT_HEIGHT = LOG_PLOT_BOTTOM - PLOT_TOP
LOG_Y_MIN = 1e-7
LOG_Y_MAX = 100.0


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def read_distribution(path: Path) -> dict[str, object]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        expected = [item[0] for item in SERIES]
        if reader.fieldnames is None or reader.fieldnames[:3] != [
            "range",
            "min_bytes",
            "max_bytes",
        ]:
            raise ValueError(f"{path}: unexpected CSV columns")
        if reader.fieldnames[3:] != expected:
            raise ValueError(
                f"{path}: unexpected series labels {reader.fieldnames[3:]!r}"
            )

        rows = []
        totals = {label: 0.0 for label in expected}
        for raw in reader:
            minimum = int(raw["min_bytes"])
            maximum = int(raw["max_bytes"]) if raw["max_bytes"] else None
            if minimum == 0:
                center = 8.0
            elif maximum is not None:
                center = math.sqrt(minimum * (maximum + 1))
            else:
                center = minimum * math.sqrt(2)
            values = {label: float(raw[label]) for label in expected}
            for label, value in values.items():
                if value < 0:
                    raise ValueError(f"{path}: negative percentage for {label}")
                totals[label] += value
            rows.append(
                {
                    "range": raw["range"],
                    "center": center,
                    "values": values,
                }
            )

    if len(rows) != 28:
        raise ValueError(f"{path}: got {len(rows)} byte ranges, want 28")
    for label, total in totals.items():
        if abs(total - 100.0) > 1e-6:
            raise ValueError(f"{path}: {label} totals {total:.12f}%, want 100%")
    return {"path": path, "rows": rows}


def x_position(index: int, count: int, left: float, right: float) -> float:
    return left + index / (count - 1) * (right - left)


def y_position(value: float, maximum: float) -> float:
    return PLOT_BOTTOM - value / maximum * PLOT_HEIGHT


def log_x_position(value: float, left: float, right: float) -> float:
    low = math.log2(8)
    high = math.log2(131072)
    return left + (math.log2(value) - low) / (high - low) * (right - left)


def log_y_position(value: float) -> float:
    high = math.log10(LOG_Y_MAX)
    low = math.log10(LOG_Y_MIN)
    return PLOT_TOP + (high - math.log10(value)) / (high - low) * LOG_PLOT_HEIGHT


def log_y_label(value: float) -> str:
    if value >= 0.001:
        return f"{value:g}%"
    exponent = round(math.log10(value))
    return f"10^{exponent}%"


def marker(parts: list[str], shape: str, x: float, y: float, color: str) -> None:
    if shape == "square":
        parts.append(
            f'<rect x="{x - 2.5:.2f}" y="{y - 2.5:.2f}" width="5" height="5" '
            f'fill="white" stroke="{color}" stroke-width="1.5"/>'
        )
    else:
        parts.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="2.4" fill="white" '
            f'stroke="{color}" stroke-width="1.5"/>'
        )


def add_legend(parts: list[str]) -> None:
    entries = (
        (230, 93, SERIES[0]),
        (800, 93, SERIES[1]),
        (230, 124, SERIES[2]),
        (800, 124, SERIES[3]),
    )
    for x, y, (label, color, dash, shape) in entries:
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        parts.append(
            f'<line x1="{x}" y1="{y}" x2="{x + 42}" y2="{y}" '
            f'stroke="{color}" stroke-width="3"{dash_attr}/>'
        )
        marker(parts, shape, x + 21, y, color)
        parts.append(
            f'<text x="{x + 54}" y="{y + 5}" class="legend">{esc(label)}</text>'
        )


def add_panel(
    parts: list[str],
    data: dict[str, object],
    bounds: tuple[int, int],
    title: str,
    observation_count: str,
) -> None:
    left, right = bounds
    width = right - left
    parts.append(
        f'<rect x="{left}" y="{PLOT_TOP}" width="{width}" height="{PLOT_HEIGHT}" '
        'fill="#ffffff" stroke="#cbd5e1"/>'
    )
    parts.append(
        f'<text x="{left + width / 2:.1f}" y="158" text-anchor="middle" '
        f'class="panel-title">{esc(title)}</text>'
    )
    parts.append(
        f'<text x="{left + width / 2:.1f}" y="174" text-anchor="middle" '
        f'class="panel-subtitle">{esc(observation_count)} observations</text>'
    )

    rows = data["rows"]
    assert isinstance(rows, list)
    largest = max(row["values"][label] for row in rows for label, *_ in SERIES)
    y_maximum = max(10, math.ceil(largest / 10) * 10)
    y_ticks = range(0, y_maximum + 1, 10)
    for tick in y_ticks:
        y = y_position(tick, y_maximum)
        parts.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{right}" y2="{y:.2f}" '
            'stroke="#e2e8f0" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" '
            f'class="tick">{tick}%</text>'
        )

    for index, row in enumerate(rows):
        x = x_position(index, len(rows), left, right)
        parts.append(
            f'<line x1="{x:.2f}" y1="{PLOT_BOTTOM}" x2="{x:.2f}" '
            f'y2="{PLOT_BOTTOM + 5}" stroke="#94a3b8"/>'
        )
        parts.append(
            f'<text x="{x:.2f}" y="{PLOT_BOTTOM + 17}" text-anchor="end" '
            f'transform="rotate(-55 {x:.2f} {PLOT_BOTTOM + 17})" '
            f'class="tick">{esc(row["range"])}</text>'
        )

    for label, color, dash, shape in SERIES:
        points = [
            (
                x_position(index, len(rows), left, right),
                y_position(row["values"][label], y_maximum),
            )
            for index, row in enumerate(rows)
        ]
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        path = " ".join(
            ("M" if index == 0 else "L") + f" {x:.2f} {y:.2f}"
            for index, (x, y) in enumerate(points)
        )
        parts.append(
            f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.5" '
            f'stroke-linejoin="round" stroke-linecap="round"{dash_attr}/>'
        )
        for x, y in points:
            marker(parts, shape, x, y, color)

    parts.append(
        f'<text x="{left + width / 2:.1f}" y="{PLOT_BOTTOM + 142}" '
        'text-anchor="middle" class="axis-label">Byte-size bucket</text>'
    )
    parts.append(
        f'<text x="{left - 73}" y="{(PLOT_TOP + PLOT_BOTTOM) / 2:.1f}" '
        f'transform="rotate(-90 {left - 73} {(PLOT_TOP + PLOT_BOTTOM) / 2:.1f})" '
        'text-anchor="middle" class="axis-label">Observations in bucket (%)</text>'
    )


def add_log_panel(
    parts: list[str],
    data: dict[str, object],
    bounds: tuple[int, int],
    title: str,
    observation_count: str,
) -> None:
    left, right = bounds
    width = right - left
    parts.append(
        f'<rect x="{left}" y="{PLOT_TOP}" width="{width}" height="{LOG_PLOT_HEIGHT}" '
        'fill="#ffffff" stroke="#cbd5e1"/>'
    )
    parts.append(
        f'<text x="{left + width / 2:.1f}" y="158" text-anchor="middle" '
        f'class="panel-title">{esc(title)}</text>'
    )
    parts.append(
        f'<text x="{left + width / 2:.1f}" y="174" text-anchor="middle" '
        f'class="panel-subtitle">{esc(observation_count)} observations</text>'
    )

    y_ticks = (
        100,
        10,
        1,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
    )
    for tick in y_ticks:
        y = log_y_position(tick)
        parts.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{right}" y2="{y:.2f}" '
            'stroke="#e2e8f0" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" '
            f'class="tick">{esc(log_y_label(tick))}</text>'
        )

    x_ticks = (16, 64, 256, 1024, 4096, 16384, 65536)
    for tick in x_ticks:
        x = log_x_position(tick, left, right)
        label = f"{tick // 1024}K" if tick >= 1024 else str(tick)
        parts.append(
            f'<line x1="{x:.2f}" y1="{PLOT_TOP}" x2="{x:.2f}" y2="{LOG_PLOT_BOTTOM}" '
            'stroke="#f1f5f9" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{x:.2f}" y="{LOG_PLOT_BOTTOM + 23}" text-anchor="middle" '
            f'class="tick">{esc(label)}</text>'
        )

    rows = data["rows"]
    assert isinstance(rows, list)
    for label, color, dash, shape in SERIES:
        segments: list[list[tuple[float, float]]] = []
        current: list[tuple[float, float]] = []
        for row in rows:
            value = row["values"][label]
            if value <= 0:
                if current:
                    segments.append(current)
                    current = []
                continue
            current.append(
                (
                    log_x_position(row["center"], left, right),
                    log_y_position(max(value, LOG_Y_MIN)),
                )
            )
        if current:
            segments.append(current)

        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        for segment in segments:
            path = " ".join(
                ("M" if index == 0 else "L") + f" {x:.2f} {y:.2f}"
                for index, (x, y) in enumerate(segment)
            )
            parts.append(
                f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.5" '
                f'stroke-linejoin="round" stroke-linecap="round"{dash_attr}/>'
            )
            for x, y in segment:
                marker(parts, shape, x, y, color)

    parts.append(
        f'<text x="{left + width / 2:.1f}" y="{LOG_PLOT_BOTTOM + 52}" '
        'text-anchor="middle" class="axis-label">Bytes (log₂ scale)</text>'
    )
    parts.append(
        f'<text x="{left - 73}" y="{(PLOT_TOP + LOG_PLOT_BOTTOM) / 2:.1f}" '
        f'transform="rotate(-90 {left - 73} {(PLOT_TOP + LOG_PLOT_BOTTOM) / 2:.1f})" '
        'text-anchor="middle" class="axis-label">Observations in bucket (%)</text>'
    )


def make_figure(
    baggage: dict[str, object],
    payload: dict[str, object],
    title: str,
    subtitle: str,
    output: Path,
) -> None:
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img">',
        f'<title>{esc(title)}</title>',
        """<style>
        text { font-family: Inter, ui-sans-serif, system-ui, -apple-system, sans-serif; fill: #0f172a; }
        .title { font-size: 26px; font-weight: 700; }
        .subtitle { font-size: 15px; fill: #475569; }
        .panel-title { font-size: 17px; font-weight: 650; }
        .panel-subtitle { font-size: 11px; fill: #64748b; }
        .legend { font-size: 13px; }
        .tick { font-size: 11px; fill: #475569; }
        .axis-label { font-size: 13px; font-weight: 600; fill: #334155; }
        .note { font-size: 11px; fill: #64748b; }
        </style>""",
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="#f8fafc"/>',
        f'<text x="{WIDTH / 2}" y="36" text-anchor="middle" class="title">{esc(title)}</text>',
        f'<text x="{WIDTH / 2}" y="62" text-anchor="middle" class="subtitle">{esc(subtitle)}</text>',
    ]
    add_legend(parts)
    add_panel(parts, baggage, PANELS[0], "Baggage-call size", "474,967,338")
    add_panel(parts, payload, PANELS[1], "Emitted-payload size", "262,104,331")
    parts.append(
        f'<text x="{WIDTH / 2}" y="756" text-anchor="middle" class="note">'
        "Bucket probability mass on a linear percentage scale; byte ranges are categorical."
        "</text>"
    )
    parts.append("</svg>")
    output.write_text("\n".join(parts) + "\n", encoding="utf-8")


def make_log_figure(
    baggage: dict[str, object],
    payload: dict[str, object],
    title: str,
    subtitle: str,
    output: Path,
) -> None:
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img">',
        f'<title>{esc(title)}</title>',
        """<style>
        text { font-family: Inter, ui-sans-serif, system-ui, -apple-system, sans-serif; fill: #0f172a; }
        .title { font-size: 26px; font-weight: 700; }
        .subtitle { font-size: 15px; fill: #475569; }
        .panel-title { font-size: 17px; font-weight: 650; }
        .panel-subtitle { font-size: 11px; fill: #64748b; }
        .legend { font-size: 13px; }
        .tick { font-size: 11px; fill: #475569; }
        .axis-label { font-size: 13px; font-weight: 600; fill: #334155; }
        .note { font-size: 11px; fill: #64748b; }
        </style>""",
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="#f8fafc"/>',
        f'<text x="{WIDTH / 2}" y="36" text-anchor="middle" class="title">{esc(title)}</text>',
        f'<text x="{WIDTH / 2}" y="62" text-anchor="middle" class="subtitle">{esc(subtitle)}</text>',
    ]
    add_legend(parts)
    add_log_panel(parts, baggage, PANELS[0], "Baggage-call size", "474,967,338")
    add_log_panel(parts, payload, PANELS[1], "Emitted-payload size", "262,104,331")
    parts.append(
        f'<text x="{WIDTH / 2}" y="756" text-anchor="middle" class="note">'
        "Bucket probability mass; byte position and percentage mass use logarithmic scales. "
        "Zero-mass tail buckets are omitted from lines."
        "</text>"
    )
    parts.append("</svg>")
    output.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--comparison-dir",
        type=Path,
        default=Path("output/dee_full_comparison"),
        help="directory containing the split distribution CSV files",
    )
    args = parser.parse_args()
    directory = args.comparison_dir

    base_baggage = read_distribution(directory / "baggage_distribution_base.csv")
    base_payload = read_distribution(directory / "payload_distribution_base.csv")
    pop_baggage = read_distribution(directory / "baggage_distribution_single_pop.csv")
    pop_payload = read_distribution(directory / "payload_distribution_single_pop.csv")

    make_figure(
        base_baggage,
        base_payload,
        "S-Bridge size distributions — Base pickup",
        "Each call drains the selected delayed-end-event queue",
        directory / "size_distribution_base.svg",
    )
    make_figure(
        pop_baggage,
        pop_payload,
        "S-Bridge size distributions — Single-pop pickup",
        "Each call dequeues at most one FIFO delayed-end-event record",
        directory / "size_distribution_single_pop.svg",
    )
    make_log_figure(
        base_baggage,
        base_payload,
        "S-Bridge size distributions — Base pickup (log scale)",
        "Each call drains the selected delayed-end-event queue",
        directory / "size_distribution_base_log.svg",
    )
    make_log_figure(
        pop_baggage,
        pop_payload,
        "S-Bridge size distributions — Single-pop pickup (log scale)",
        "Each call dequeues at most one FIFO delayed-end-event record",
        directory / "size_distribution_single_pop_log.svg",
    )

    print(directory / "size_distribution_base.svg")
    print(directory / "size_distribution_single_pop.svg")
    print(directory / "size_distribution_base_log.svg")
    print(directory / "size_distribution_single_pop_log.svg")


if __name__ == "__main__":
    main()
