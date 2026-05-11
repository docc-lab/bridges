#!/usr/bin/env python3
"""
Drive the Python bridge handlers on a synthetic input and dump per-call
outputs as JSON, for golden comparison against the Go port.

Synthetic input: 3 traces with overlapping timestamps and overlapping services
so the S-Bridge DEE queue is exercised across traces.
"""

import json
import os
import sys
from collections import defaultdict, deque

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # bridges/ (parent of bridge/)

import trace_simulator as ts
from trace_simulator import (
    PathBridgeHandler,
    CGPBBridgeHandler,
    SBridgeBridgeHandler,
    VanillaHandler,
    ParentContext,
    BAG_BR,
    EMIT_PAYLOAD_BYTES_TAG,
    baggage_byte_size,
    span_get_tag,
    sort_events,
)

# Build a synthetic trace structured like Python expects after _normalize_span:
# spans have traceID, spanID, parent_span_id, start_time_ns, end_time_ns,
# tags (list), and _service_name.
def span(span_id, parent_id, start_ns, end_ns, service):
    return {
        "traceID": "<set later>",
        "spanID": span_id,
        "spanId": span_id,
        "parent_span_id": parent_id,
        "start_time_ns": start_ns,
        "end_time_ns": end_ns,
        "tags": [],
        "_service_name": service,
    }


def make_traces():
    # Trace A: 5-span tree. R->X (svc1), R->Y (svc2); Y->Z (svc1), Y->W (svc2).
    traceA_id = "aaaaaaaaaaaaaaa1"
    A_spans = [
        span("aaaa000000000001", None,                100, 1000, "svc1"),  # R
        span("aaaa000000000002", "aaaa000000000001",  120,  500, "svc1"),  # X
        span("aaaa000000000003", "aaaa000000000001",  150,  900, "svc2"),  # Y
        span("aaaa000000000004", "aaaa000000000003",  200,  400, "svc1"),  # Z
        span("aaaa000000000005", "aaaa000000000003",  300,  800, "svc2"),  # W
    ]
    # Trace B: smaller, overlaps Trace A in time and shares services.
    traceB_id = "bbbbbbbbbbbbbbb2"
    B_spans = [
        span("bbbb000000000001", None,                250,  950, "svc2"),
        span("bbbb000000000002", "bbbb000000000001",  280,  600, "svc1"),
        span("bbbb000000000003", "bbbb000000000001",  350,  900, "svc2"),
    ]
    # Trace C: starts after A ends, picks up any leftover DEE on svc2.
    traceC_id = "cccccccccccccccc"
    C_spans = [
        span("cccc000000000001", None,               1100, 1500, "svc2"),
        span("cccc000000000002", "cccc000000000001", 1150, 1300, "svc1"),
    ]
    out = []
    for tid, spans_list in [(traceA_id, A_spans), (traceB_id, B_spans), (traceC_id, C_spans)]:
        for s in spans_list:
            s["traceID"] = tid
        out.append({"trace_id": tid, "spans": spans_list, "_source_file": f"{tid}.json"})
    return out


def drive(handler_cls, init_kwargs):
    """Mimics run_traces interleaved mode and records per-event outputs.
    Builds fresh traces inside so handler-induced span-tag mutations from a
    previous run don't leak into this one.
    """
    import copy
    traces = copy.deepcopy(make_traces())
    handler = handler_cls(**init_kwargs)
    all_events = []
    for t in traces:
        all_events.extend(ts.build_events(t))
    all_events = sort_events(all_events)

    next_seq = {}
    out = []
    for ts_ns, kind, depth, span in all_events:
        tid = span.get("traceID") or span.get("traceId") or ""
        sid = span.get("spanID") or span.get("spanId") or ""
        if kind == "start":
            pid = span.get("parent_span_id")
            if pid is None:
                seq_num = 0
            else:
                k = (tid, pid)
                seq_num = next_seq.get(k, 1)
                next_seq[k] = seq_num + 1
            pctx = ParentContext(tid, pid, seq_num=seq_num)
            baggage_found = handler.on_start(pctx, span)
            baggage_bytes = baggage_byte_size(span) if baggage_found else 0
            emit = span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG)
            out.append({
                "kind": "start",
                "trace_id": tid,
                "span_id": sid,
                "service": span.get("_service_name", ""),
                "seq_num": seq_num,
                "baggage_found": bool(baggage_found),
                "baggage_bytes": int(baggage_bytes),
                "emit_bytes": int(emit) if emit is not None else 0,
            })
        else:
            emit_before = span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG)
            handler.on_end(span)
            emit_after = span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG)
            # Bytes set in THIS OnEnd call: the difference between before and after.
            new_emit = 0
            if emit_after is not None and emit_before is None:
                new_emit = int(emit_after)
            out.append({
                "kind": "end",
                "trace_id": tid,
                "span_id": sid,
                "service": span.get("_service_name", ""),
                "emit_bytes": new_emit,
            })
    return out


def main():
    out = {
        "vanilla":    drive(VanillaHandler,         {}),
        "pb_cpd1":    drive(PathBridgeHandler,      {"checkpoint_distance": 1}),
        "pb_cpd3":    drive(PathBridgeHandler,      {"checkpoint_distance": 3}),
        "cgpb_cpd1":  drive(CGPBBridgeHandler,      {"checkpoint_distance": 1}),
        "cgpb_cpd3":  drive(CGPBBridgeHandler,      {"checkpoint_distance": 3}),
        "sb_cpd1":    drive(SBridgeBridgeHandler,   {"checkpoint_distance": 1}),
        "sb_cpd3":    drive(SBridgeBridgeHandler,   {"checkpoint_distance": 3}),
    }
    path = os.path.join(HERE, "golden.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
