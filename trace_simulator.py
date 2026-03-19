#!/usr/bin/env python3
"""
Trace simulator: loads trace JSON, turns spans into start/end events,
and runs them through a pluggable bridge (OnStart/OnEnd), mirroring
OpenTelemetry SDK SpanProcessor semantics.

Handlers implement the same conceptual interface as in
blueprint-docc-mod/runtime/plugins/otelcol (e.g. vanilla_processor.go):
  - OnStart(parent_ctx, span): span is mutable (e.g. add baggage/attributes).
  - OnEnd(span): span is read-only for inspection; handler may strip attributes.
"""

import argparse
import json
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Tuple

# Bloom filter for path bridge (and later CGPB)
try:
    from bloom import BloomFilter, estimate_parameters
except ImportError:
    BloomFilter = None  # type: ignore
    estimate_parameters = None  # type: ignore


# -----------------------------------------------------------------------------
# Span representation and tag helpers
# -----------------------------------------------------------------------------

def _tags_list(span: dict) -> list:
    """Return the tags/attributes list (Jaeger-style or OTel-style)."""
    tags = span.get("tags")
    if tags is not None:
        return tags
    attrs = span.get("attributes")
    if isinstance(attrs, dict):
        return [{"key": k, "value": v} for k, v in attrs.items()]
    if isinstance(attrs, list):
        return attrs
    return []


def _set_tags_list(span: dict, tags: list) -> None:
    """Set span tags to a list of {key, value}."""
    if "tags" in span:
        span["tags"] = tags
    else:
        span["attributes"] = {t["key"]: t["value"] for t in tags}
    return


def span_get_tag(span: dict, key: str) -> Optional[Any]:
    """Get the value of a tag/attribute by key."""
    for t in _tags_list(span):
        k = t.get("key") or t.get("Key")
        if k == key:
            return t.get("value") or t.get("Value")
    return None


def span_set_tag(span: dict, key: str, value: Any) -> None:
    """Set a tag/attribute; updates existing or appends."""
    tags = _tags_list(span)
    for t in tags:
        k = t.get("key") or t.get("Key")
        if k == key:
            t["key"] = key
            t["value"] = value
            return
    tags.append({"key": key, "value": value})
    _set_tags_list(span, tags)


def span_remove_tag(span: dict, key: str) -> None:
    """Remove a tag/attribute by key."""
    tags = [t for t in _tags_list(span) if (t.get("key") or t.get("Key")) != key]
    _set_tags_list(span, tags)


def span_has_tag(span: dict, key: str) -> bool:
    """Return True if the span has the given tag."""
    return span_get_tag(span, key) is not None


def baggage_byte_size(span: dict) -> int:
    """
    Total byte size of baggage in transit (UTF-8).
    Sums len(key) + len(value) in bytes for every tag whose key starts with \"__bag\".
    Use this for wire-size estimate, not stringified/JSON size.
    """
    total = 0
    for t in _tags_list(span):
        k = t.get("key") or t.get("Key")
        if not k or not k.startswith("__bag"):
            continue
        v = t.get("value") or t.get("Value")
        if v is None:
            continue
        s = str(v) if not isinstance(v, str) else v
        total += len(k.encode("utf-8")) + len(s.encode("utf-8"))
    return total


# -----------------------------------------------------------------------------
# Parent context (what OnStart receives about the parent)
# -----------------------------------------------------------------------------

class ParentContext:
    """Minimal context for OnStart: identifies the parent so the handler can look up state."""

    __slots__ = ("trace_id", "parent_span_id")

    def __init__(self, trace_id: str, parent_span_id: Optional[str]):
        self.trace_id = trace_id
        self.parent_span_id = parent_span_id  # None for root spans


# -----------------------------------------------------------------------------
# Bridge handler interface (pluggable OnStart / OnEnd)
# -----------------------------------------------------------------------------

class BridgeHandler(ABC):
    """
    Bridge type: custom logic on span start and end.
    Mirrors go.opentelemetry.io/otel/sdk/trace.SpanProcessor:
      - OnStart(parentCtx, span): span is read-write; set baggage/attributes here.
      - OnEnd(span): span is read-only; optionally strip attributes before export.
    """

    @abstractmethod
    def on_start(self, parent_ctx: ParentContext, span: dict) -> None:
        """Called when a span starts. May mutate span (e.g. set tags)."""
        pass

    @abstractmethod
    def on_end(self, span: dict) -> None:
        """Called when a span ends. May not mutate span (read-only); export uses span as-is after this."""
        pass


# -----------------------------------------------------------------------------
# Vanilla handler (no-op; pass-through like vanilla_processor.go)
# -----------------------------------------------------------------------------

class VanillaHandler(BridgeHandler):
    """No-op handler: OnStart does nothing, OnEnd does nothing. For testing the scaffold."""

    def on_start(self, parent_ctx: ParentContext, span: dict) -> None:
        pass

    def on_end(self, span: dict) -> None:
        pass


# -----------------------------------------------------------------------------
# Path bridge constants (match blueprint-docc-mod/runtime/plugins/otelcol/defs.go)
# -----------------------------------------------------------------------------

BAG_BLOOM_FILTER = "__bag.bf"
AncestryKey = "ancestry"
AncestryModeKey = "ancestry_mode"
ANCESTRY_MODE_PB = "pb"


# -----------------------------------------------------------------------------
# Path bridge handler (Bloom-only propagation; pb_processor.go semantics)
# -----------------------------------------------------------------------------

class PathBridgeHandler(BridgeHandler):
    """
    Path bridge: propagate only a Bloom filter in baggage.
    - OnStart: read parent depth and bloom from _span_info; depth_mod = (parent_depth+1) % cpd;
      add current span ID to bloom; set __bag.depth, __bag.bf, __bag.prio, ancestry_mode, ancestry.
      If priority==1 (checkpoint), reset bloom to only this span.
    - OnEnd: server leaves (no children) get priority=1; low-priority spans have ancestry
      and ancestry_mode stripped before export.
    """

    def __init__(self, checkpoint_distance: int = 1, bloom_fp_rate: float = 0.0001):
        if BloomFilter is None or estimate_parameters is None:
            raise RuntimeError("Path bridge requires bloom module (bloom.py)")
        self._cpd = max(1, checkpoint_distance)
        self._bloom_p = bloom_fp_rate
        n = max(1, self._cpd)
        self._bloom_m, self._bloom_k = estimate_parameters(n, self._bloom_p)
        self._span_info: dict = {}  # (trace_id, span_id) -> {depth_mod, bloom, bf_serialized}
        self._has_children: set = set()  # (trace_id, span_id) that have at least one child

    def _empty_bloom(self):
        return BloomFilter(self._bloom_m, self._bloom_k)

    def on_start(self, parent_ctx: ParentContext, span: dict) -> None:
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        parent_id = parent_ctx.parent_span_id

        # Record that parent has a child (for OnEnd leaf detection)
        if parent_id is not None:
            self._has_children.add((trace_id, parent_id))

        parent_info = self._span_info.get((trace_id, parent_id)) if parent_id else None
        if parent_info is not None:
            depth_mod = (parent_info["depth_mod"] + 1) % self._cpd
            bf = BloomFilter.deserialize(
                parent_info["bf_serialized"], self._bloom_m, self._bloom_k
            )
        else:
            depth_mod = 0
            bf = self._empty_bloom()

        bf.add(span_id.encode("utf-8"))
        bf_str = bf.serialize()

        priority = 1 if depth_mod == 0 else 0
        span_set_tag(span, "__bag.depth", depth_mod)
        span_set_tag(span, BAG_BLOOM_FILTER, bf_str)
        span_set_tag(span, "__bag.prio", priority)
        span_set_tag(span, AncestryModeKey, ANCESTRY_MODE_PB)
        span_set_tag(span, AncestryKey, bf_str)
        span_set_tag(span, "depth", depth_mod)

        if priority == 1:
            bf = self._empty_bloom()
            bf.add(span_id.encode("utf-8"))
            bf_str = bf.serialize()
            span_set_tag(span, BAG_BLOOM_FILTER, bf_str)

        self._span_info[(trace_id, span_id)] = {
            "depth_mod": depth_mod,
            "bf_serialized": bf_str,
        }

    def on_end(self, span: dict) -> None:
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        priority = span_get_tag(span, "__bag.prio")
        if priority is None:
            priority = 0
        if isinstance(priority, str):
            priority = int(priority)
        # Leaf (no children) is always checkpoint
        if (trace_id, span_id) not in self._has_children:
            priority = 1
        if priority != 1:
            span_remove_tag(span, AncestryKey)
            span_remove_tag(span, AncestryModeKey)


# -----------------------------------------------------------------------------
# Trace loading
# -----------------------------------------------------------------------------

def _normalize_span(span: dict, processes: Optional[dict], trace_id: str) -> dict:
    """Ensure span has trace_id, span_id, parent_span_id, start_time_ns, end_time_ns, tags list."""
    out = dict(span)
    out.setdefault("traceID", trace_id)
    out.setdefault("traceId", trace_id)
    sid = out.get("spanID") or out.get("spanId") or ""
    out.setdefault("spanID", sid)
    out.setdefault("spanId", sid)

    # Parent: from parentSpanID / parentSpanId or first CHILD_OF reference
    pid = out.get("parentSpanID") or out.get("parentSpanId")
    if not pid:
        for ref in out.get("references") or []:
            if ref and ref.get("refType") == "CHILD_OF":
                pid = ref.get("spanID") or ref.get("spanId")
                break
    out["parent_span_id"] = pid

    # Timestamps: Jaeger uses startTime (microseconds) and duration (microseconds)
    start_us = out.get("startTime") or out.get("startTimeUnixNano", 0)
    if isinstance(start_us, int) and start_us < 1e15:
        start_us = start_us  # assume microseconds
    else:
        start_us = (start_us or 0) // 1000
    duration_us = out.get("duration") or 0
    if isinstance(out.get("duration"), int):
        duration_us = out["duration"]
    out["start_time_ns"] = int(start_us) * 1000
    out["end_time_ns"] = int(start_us) * 1000 + int(duration_us) * 1000

    # Normalize tags to list of {key, value}
    tags = _tags_list(out)
    if tags and isinstance(tags[0].get("key"), str):
        pass
    else:
        out["tags"] = [{"key": k, "value": v} for k, v in (out.get("attributes") or {}).items()]
    return out


def load_traces_from_dir(
    dir_path: str,
    trace_count: Optional[int] = None,
    offset: int = 0,
    random_sample: bool = False,
    seed: Optional[int] = None,
) -> List[dict]:
    """
    Load traces from a directory of JSON files.
    Each file: Jaeger API {"data": [trace, ...]} or single trace {"traceID", "spans", "processes"}.
    Returns list of {"trace_id", "spans", "processes"} (spans normalized).
    """
    path = Path(dir_path)
    if not path.is_dir():
        return []

    files = sorted(path.glob("*.json"))
    if not files:
        return []

    if random_sample:
        import random
        if seed is not None:
            random.seed(seed)
        files = list(files)
        random.shuffle(files)

    if offset > 0:
        files = files[offset:]

    traces = []
    for f in files:
        try:
            with open(f, "r") as fp:
                data = json.load(fp)
        except (json.JSONDecodeError, OSError):
            continue

        # Jaeger API response
        if "data" in data:
            for t in data["data"]:
                tid = t.get("traceID") or t.get("traceId") or ""
                spans = t.get("spans") or []
                processes = t.get("processes") or {}
                normalized = [
                    _normalize_span(s, processes, tid)
                    for s in spans
                ]
                traces.append({"trace_id": tid, "spans": normalized, "processes": processes})
                if trace_count and len(traces) >= trace_count:
                    return traces
            continue

        # Single trace
        tid = data.get("traceID") or data.get("traceId") or ""
        spans = data.get("spans") or []
        processes = data.get("processes") or {}
        normalized = [_normalize_span(s, processes, tid) for s in spans]
        traces.append({"trace_id": tid, "spans": normalized, "processes": processes})
        if trace_count and len(traces) >= trace_count:
            return traces

    return traces


# -----------------------------------------------------------------------------
# Event stream: start/end events sorted like SDK order
# -----------------------------------------------------------------------------

def build_events(trace: dict) -> List[Tuple[int, str, dict]]:
    """Build (timestamp_ns, "start"|"end", span) for a trace. Caller must sort."""
    events = []
    for span in trace["spans"]:
        events.append((span["start_time_ns"], "start", span))
        events.append((span["end_time_ns"], "end", span))
    return events


def sort_events(events: List[Tuple[int, str, dict]]) -> List[Tuple[int, str, dict]]:
    """
    Sort so that at the same timestamp, end events are processed before start events.
    This matches SDK semantics where a span that ends at T and another that starts at T
    should see the end first (so the next span can observe the ended span's state).
    """
    def key(e):
        ts, typ, span = e
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        end_first = 0 if typ == "end" else 1
        return (ts, end_first, trace_id, span_id)

    return sorted(events, key=key)


# -----------------------------------------------------------------------------
# Simulator: run events through a handler and collect output spans
# -----------------------------------------------------------------------------

def run_trace(trace: dict, handler: BridgeHandler) -> List[dict]:
    """
    Run one trace: build events, sort, call handler.on_start / handler.on_end,
    and return the list of spans (one per span, after on_end).
    """
    events = build_events(trace)
    events = sort_events(events)
    output_spans = []

    for ts_ns, typ, span in events:
        if typ == "start":
            parent_id = span.get("parent_span_id")
            trace_id = span.get("traceID") or span.get("traceId") or ""
            parent_ctx = ParentContext(trace_id, parent_id)
            handler.on_start(parent_ctx, span)
        else:
            handler.on_end(span)
            # Export: append a copy so handler can't mutate after the fact
            import copy
            output_spans.append(copy.deepcopy(span))

    return output_spans


def run_traces(traces: List[dict], handler: BridgeHandler) -> List[dict]:
    """Run all traces through the handler; return concatenated output spans."""
    out = []
    for trace in traces:
        out.extend(run_trace(trace, handler))
    return out


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Trace simulator: load trace JSON, run start/end events through a pluggable bridge."
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing trace JSON files",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output JSON file (spans array)",
    )
    parser.add_argument(
        "--mode",
        choices=["vanilla", "pb"],
        default="vanilla",
        help="Bridge mode: vanilla (no-op) or pb (path bridge)",
    )
    parser.add_argument(
        "--checkpoint-distance",
        type=int,
        default=1,
        help="Checkpoint distance for pb mode (default: 1)",
    )
    parser.add_argument(
        "--trace-count",
        type=int,
        default=None,
        help="Max number of traces to load",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip this many JSON files before loading",
    )
    args = parser.parse_args()

    traces = load_traces_from_dir(
        args.input_dir,
        trace_count=args.trace_count,
        offset=args.offset,
    )
    if not traces:
        print("No traces loaded.", file=sys.stderr)
        return 1

    if args.mode == "vanilla":
        handler = VanillaHandler()
    elif args.mode == "pb":
        handler = PathBridgeHandler(
            checkpoint_distance=args.checkpoint_distance,
            bloom_fp_rate=0.0001,
        )
    else:
        handler = VanillaHandler()

    spans = run_traces(traces, handler)

    # Write output: Jaeger-style {"spans": [...]}
    out = {"spans": spans}
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {len(spans)} spans to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
