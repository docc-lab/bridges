#!/usr/bin/env python3
"""
Describe a single Jaeger-style trace JSON file (one trace or {"data": [...]}).

Uses the same normalization and "clean trace" rules as trace_simulator.py.

Example:
  python3 trace_analyze.py /path/to/trace.json
  python3 trace_analyze.py trace.json --json
  python3 trace_analyze.py bundle.json --index 2   # third trace in data[]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from trace_simulator import _normalize_span, _trace_is_clean


def _load_raw(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_traces_from_file(path: Path) -> List[Dict[str, Any]]:
    """
    Return a list of trace dicts: {trace_id, spans, processes, _source_file}.
    """
    data = _load_raw(path)
    src = str(path.resolve())
    out: List[Dict[str, Any]] = []

    if isinstance(data, dict) and "data" in data:
        traces = data.get("data") or []
        if not isinstance(traces, list):
            raise ValueError('"data" must be a list')
        for t in traces:
            if not isinstance(t, dict):
                continue
            tid = t.get("traceID") or t.get("traceId") or ""
            spans = t.get("spans") or []
            processes = t.get("processes") or {}
            normalized = [_normalize_span(s, processes, tid) for s in spans]
            out.append(
                {
                    "trace_id": tid,
                    "spans": normalized,
                    "processes": processes,
                    "_source_file": src,
                }
            )
        return out

    if isinstance(data, dict) and ("spans" in data or "traceID" in data or "traceId" in data):
        tid = data.get("traceID") or data.get("traceId") or ""
        spans = data.get("spans") or []
        processes = data.get("processes") or {}
        normalized = [_normalize_span(s, processes, tid) for s in spans]
        out.append(
            {
                "trace_id": tid,
                "spans": normalized,
                "processes": processes,
                "_source_file": src,
            }
        )
        return out

    raise ValueError(
        "Unrecognized JSON shape: expected Jaeger API {data: [...]} or a single trace {traceID, spans, ...}"
    )


def _span_name(s: dict) -> str:
    return str(s.get("operationName") or s.get("name") or s.get("operation_name") or "")


def tree_stats(spans: List[dict]) -> Dict[str, Any]:
    span_id_by_sid: Dict[str, dict] = {}
    for s in spans:
        sid = s.get("spanID") or s.get("spanId") or ""
        if sid:
            span_id_by_sid[sid] = s

    children: Dict[str, List[str]] = defaultdict(list)
    roots: List[str] = []
    for sid, s in span_id_by_sid.items():
        pid = s.get("parent_span_id")
        if pid is None or pid not in span_id_by_sid:
            roots.append(sid)
        else:
            children[pid].append(sid)

    max_fanout = max((len(v) for v in children.values()), default=0)

    depth_by: Dict[str, int] = {}
    for r in roots:
        if r in depth_by:
            continue
        q: deque[Tuple[str, int]] = deque([(r, 0)])
        while q:
            cur, d = q.popleft()
            if cur in depth_by:
                continue
            depth_by[cur] = d
            for ch in children.get(cur, []):
                q.append((ch, d + 1))

    all_ids = set(span_id_by_sid.keys())
    reachable = set(depth_by.keys())
    unreachable = len(all_ids) - len(reachable)
    max_depth = max(depth_by.values()) if depth_by else 0

    return {
        "roots": roots,
        "num_roots": len(roots),
        "max_depth": max_depth,
        "max_fanout": max_fanout,
        "reachable_spans": len(reachable),
        "unreachable_spans": unreachable,
        "depth_by_span_id": depth_by,
    }


def duration_stats(spans: List[dict]) -> Dict[str, Any]:
    durs_ms: List[float] = []
    starts: List[int] = []
    ends: List[int] = []
    zero_runtime = 0
    for s in spans:
        st = int(s.get("start_time_ns") or 0)
        en = int(s.get("end_time_ns") or st)
        starts.append(st)
        ends.append(en)
        dur_ns = en - st
        if dur_ns == 0:
            zero_runtime += 1
        durs_ms.append(dur_ns / 1e6)
    if not spans:
        return {
            "trace_wall_time_ms": 0.0,
            "span_duration_ms_min": None,
            "span_duration_ms_max": None,
            "span_duration_ms_mean": None,
            "spans_zero_runtime_count": 0,
        }
    wall_ns = max(ends) - min(starts) if starts and ends else 0
    return {
        "trace_wall_time_ms": wall_ns / 1e6,
        "span_duration_ms_min": min(durs_ms),
        "span_duration_ms_max": max(durs_ms),
        "span_duration_ms_mean": sum(durs_ms) / len(durs_ms),
        "spans_zero_runtime_count": zero_runtime,
    }


def analyze_trace(trace: Dict[str, Any]) -> Dict[str, Any]:
    spans = trace.get("spans") or []
    tid = trace.get("trace_id") or ""
    clean = _trace_is_clean(spans)
    tstat = tree_stats(spans)
    dstat = duration_stats(spans)
    services = Counter(s.get("_service_name") or "missing_service" for s in spans)
    ops = Counter(_span_name(s) or "(empty)" for s in spans)

    root_ids = tstat["roots"]
    root_id = root_ids[0] if len(root_ids) == 1 else None

    report: Dict[str, Any] = {
        "trace_id": tid,
        "source_file": trace.get("_source_file") or "",
        "span_count": len(spans),
        "clean": clean,
        "num_roots": tstat["num_roots"],
        "root_span_ids": root_ids,
        "primary_root_span_id": root_id,
        "max_depth": tstat["max_depth"],
        "max_fanout": tstat["max_fanout"],
        "reachable_spans": tstat["reachable_spans"],
        "unreachable_spans": tstat["unreachable_spans"],
        "trace_wall_time_ms": dstat["trace_wall_time_ms"],
        "span_duration_ms_min": dstat["span_duration_ms_min"],
        "span_duration_ms_max": dstat["span_duration_ms_max"],
        "span_duration_ms_mean": dstat["span_duration_ms_mean"],
        "spans_zero_runtime_count": dstat["spans_zero_runtime_count"],
        "services": dict(services.most_common()),
        "top_operations": ops.most_common(15),
    }
    return report


def format_text(r: Dict[str, Any]) -> str:
    lines = [
        f"File: {r['source_file']}",
        f"Trace ID: {r['trace_id'] or '(missing)'}",
        f"Spans: {r['span_count']}",
        f"Clean (simulator rules): {'yes' if r['clean'] else 'no'}",
        f"Roots: {r['num_roots']} {r['root_span_ids']}",
        f"Max tree depth (child hops from a root): {r['max_depth']}",
        f"Max fan-out (children of one parent): {r['max_fanout']}",
        f"Reachable from a root / unreachable: {r['reachable_spans']} / {r['unreachable_spans']}",
        f"Trace wall time (max end − min start): {r['trace_wall_time_ms']:.3f} ms",
        f"Span duration (ms): min={r['span_duration_ms_min']}, max={r['span_duration_ms_max']}, mean={r['span_duration_ms_mean']}",
        f"Spans with zero runtime (end_time_ns == start_time_ns): {r['spans_zero_runtime_count']}",
        "",
        "Services (span count):",
    ]
    for svc, n in sorted(r["services"].items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"  {svc}: {n}")
    lines.append("")
    lines.append("Top operations:")
    for name, n in r["top_operations"]:
        lines.append(f"  {n:5d}  {name!r}")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Describe a single trace JSON file.")
    ap.add_argument("trace_file", type=Path, help="Path to Jaeger-style trace JSON")
    ap.add_argument(
        "--index",
        type=int,
        default=None,
        help="If the file contains multiple traces (data[]), analyze trace at this index (0-based).",
    )
    ap.add_argument("--json", action="store_true", help="Print one JSON object per trace (or array if multiple).")
    args = ap.parse_args()

    path = args.trace_file
    if not path.is_file():
        print(f"Not a file: {path}", file=sys.stderr)
        return 1

    try:
        traces = load_traces_from_file(path)
    except (ValueError, OSError, json.JSONDecodeError) as e:
        print(f"Failed to load {path}: {e}", file=sys.stderr)
        return 1

    if not traces:
        print("No traces found in file.", file=sys.stderr)
        return 1

    if args.index is not None:
        if args.index < 0 or args.index >= len(traces):
            print(f"--index {args.index} out of range (file has {len(traces)} trace(s))", file=sys.stderr)
            return 1
        traces = [traces[args.index]]

    reports = [analyze_trace(t) for t in traces]

    if args.json:
        if len(reports) == 1:
            json.dump(reports[0], sys.stdout, indent=2)
            sys.stdout.write("\n")
        else:
            json.dump(reports, sys.stdout, indent=2)
            sys.stdout.write("\n")
    else:
        for i, r in enumerate(reports):
            if len(reports) > 1:
                sys.stdout.write(f"=== Trace {i} / {len(reports)} ===\n")
            sys.stdout.write(format_text(r))

    return 0


if __name__ == "__main__":
    sys.exit(main())
