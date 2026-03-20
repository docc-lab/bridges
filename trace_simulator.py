#!/usr/bin/env python3
"""
Trace simulator: loads trace JSON, turns spans into start/end events,
and runs them through a pluggable bridge (OnStart/OnEnd), mirroring
OpenTelemetry SDK SpanProcessor semantics.

Handlers implement the same conceptual interface as in
blueprint-docc-mod/runtime/plugins/otelcol (e.g. vanilla_processor.go):
  - OnStart(parent_ctx, span): span is mutable (e.g. add baggage/attributes).
    Returns whether incoming baggage was found for this span (used by --bagsize).
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
            # Use explicit None checks so falsy values like 0 still count.
            v = t.get("value")
            if v is None:
                v = t.get("Value")
            return v
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


def _baggage_value_byte_len(k_stripped: str, v: Any) -> int:
    """
    Byte length of a baggage value for wire-size.
    For "bf", value is raw bytes (or hex string from legacy/output); use decoded bytes length.
    """
    if isinstance(v, bytes):
        return len(v)
    s = str(v) if not isinstance(v, str) else v
    if k_stripped == "bf":
        try:
            return len(bytes.fromhex(s))
        except (ValueError, TypeError):
            pass
    return len(s.encode("utf-8"))


def baggage_byte_size(span: dict) -> int:
    """
    Total byte size of baggage in transit.
    Key length excludes __bag. prefix. Values: raw bytes length; hex-encoded bf is decoded to bytes for count.
    """
    total = 0
    for t in _tags_list(span):
        k = t.get("key") or t.get("Key")
        if not k or not k.startswith("__bag"):
            continue
        v = t.get("value")
        if v is None:
            v = t.get("Value")
        if v is None:
            continue
        if k.startswith("__bag."):
            k_stripped = k[len("__bag."):]
        else:
            k_stripped = k
        total += len(k_stripped.encode("utf-8")) + _baggage_value_byte_len(k_stripped, v)
    return total


def baggage_byte_size_breakdown(span: dict) -> dict:
    """
    Per-baggage-element byte sizes (key without __bag. prefix -> bytes for that key+value).
    bf value counted as raw bytes (hex decoded if string).
    """
    breakdown = {}
    for t in _tags_list(span):
        k = t.get("key") or t.get("Key")
        if not k or not k.startswith("__bag"):
            continue
        v = t.get("value")
        if v is None:
            v = t.get("Value")
        if v is None:
            continue
        if k.startswith("__bag."):
            k_stripped = k[len("__bag."):]
        else:
            k_stripped = k
        size = len(k_stripped.encode("utf-8")) + _baggage_value_byte_len(k_stripped, v)
        breakdown[k_stripped] = size
    return breakdown


# -----------------------------------------------------------------------------
# Parent context (what OnStart receives about the parent)
# -----------------------------------------------------------------------------

class ParentContext:
    """Minimal context for OnStart: identifies the parent so the handler can look up state."""

    __slots__ = ("trace_id", "parent_span_id", "seq_num")

    def __init__(self, trace_id: str, parent_span_id: Optional[str], seq_num: int = 0):
        self.trace_id = trace_id
        self.parent_span_id = parent_span_id  # None for root spans
        self.seq_num = seq_num  # 1-based index of this span among its siblings (as processed)


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
    def on_start(self, parent_ctx: ParentContext, span: dict) -> bool:
        """
        Called when a span starts. May mutate span (e.g. set tags).

        Returns True when the handler found/used incoming baggage from the parent context
        (used for call-recording in --bagsize mode).
        """
        raise NotImplementedError

    @abstractmethod
    def on_end(self, span: dict) -> None:
        """Called when a span ends. May not mutate span (read-only); export uses span as-is after this."""
        pass


# -----------------------------------------------------------------------------
# Vanilla handler (no-op; pass-through like vanilla_processor.go)
# -----------------------------------------------------------------------------

class VanillaHandler(BridgeHandler):
    """No-op handler: OnStart does nothing, OnEnd does nothing. For testing the scaffold."""

    def on_start(self, parent_ctx: ParentContext, span: dict) -> bool:
        return False

    def on_end(self, span: dict) -> None:
        pass


# -----------------------------------------------------------------------------
# Path bridge constants (match blueprint-docc-mod/runtime/plugins/otelcol/defs.go)
# -----------------------------------------------------------------------------

BAG_BLOOM_FILTER = "__bag.bf"
BAG_BR = "__bag._br"
AncestryKey = "ancestry"
AncestryModeKey = "ancestry_mode"
ANCESTRY_MODE_PB = "pb"

# Payload-emission metric (Figure 10): emitted checkpoint payload bytes.
# This is different from baggage/tag wire-size.
EMIT_PAYLOAD_BYTES_TAG = "__emit._br_payload_bytes"
PB_BRIDGE_TYPE_ID = 1  # path bridge type id (fits in 1 byte)
BR_PROPERTY_NAME_OVERHEAD_BYTES = 3  # "_br" property name overhead (as discussed)


# -----------------------------------------------------------------------------
# Packed bridge baggage: _br layout(s)
# - PB:  varint(depth_mod) || bloom_bytes
# - CGPB: varint(depth_mod) || bloom_bytes || hash_array_bytes
# (No explicit priority; checkpoint-ness is derived from depth_mod==0 or leaf status.)
# -----------------------------------------------------------------------------

def _varint_encode(n: int) -> bytes:
    """Encode non-negative int as protobuf-style varint (7 bits per byte, high bit = more)."""
    if n < 0:
        n = 0
    out = []
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n & 0x7F)
    return bytes(out)


def _varint_decode(buf: bytes, start: int) -> tuple:
    """Decode varint from buf[start:]; return (value, new_start)."""
    n = 0
    shift = 0
    i = start
    while i < len(buf):
        b = buf[i]
        n |= (b & 0x7F) << shift
        i += 1
        if (b & 0x80) == 0:
            return (n, i)
        shift += 7
        if shift >= 35:
            break
    return (0, start)


def pack_br(depth_mod: int, bloom_bytes: bytes) -> bytes:
    """Pack path-bridge payload: varint(depth_mod) || bloom_bytes."""
    return _varint_encode(depth_mod) + bloom_bytes


def unpack_br(data: bytes, bloom_len: int) -> Optional[Tuple[int, bytes]]:
    """
    Unpack _br payload. Returns (depth_mod, bloom_bytes) or None if invalid.
    bloom_len must match (m+7)//8 so we know how many bytes to take after the depth varint.
    """
    if len(data) < bloom_len:
        return None
    depth_mod, i = _varint_decode(data, 0)
    if i + bloom_len > len(data):
        return None
    bloom_bytes = data[i : i + bloom_len]
    return (depth_mod, bloom_bytes)


def _span_id_hex_to_8bytes(span_id: str) -> Optional[bytes]:
    """
    Convert a Jaeger spanID hex string into a fixed 8-byte representation.

    Jaeger spanIDs are 64-bit = 16 hex chars. Some synthetic traces may use shorter IDs;
    we left-pad with zeros to 8 bytes for deterministic packing.
    """
    if not span_id:
        return None
    s = span_id.strip().lower()
    if any(c not in "0123456789abcdef" for c in s):
        return None
    try:
        raw = bytes.fromhex(s)
    except ValueError:
        return None
    if len(raw) > 8:
        # Unexpected width; keep the last 8 bytes to avoid negative packing.
        return raw[-8:]
    if len(raw) < 8:
        return b"\x00" * (8 - len(raw)) + raw
    return raw


def pack_cgpb_br(depth_mod: int, bloom_bytes: bytes, ha_bytes: bytes) -> bytes:
    """Pack CGPB bridge baggage: varint(depth_mod) || bloom_bytes || hash_array_bytes."""
    return _varint_encode(depth_mod) + bloom_bytes + ha_bytes


def unpack_cgpb_br(data: bytes, bloom_len: int) -> Optional[Tuple[int, bytes, bytes]]:
    """
    Unpack CGPB _br payload. Returns (depth_mod, bloom_bytes, ha_bytes) or None.
    bloom_len must match the fixed bloom byte length.
    """
    if len(data) < bloom_len:
        return None
    depth_mod, i = _varint_decode(data, 0)
    if i + bloom_len > len(data):
        return None
    bloom_bytes = data[i : i + bloom_len]
    ha_bytes = data[i + bloom_len :]
    return (depth_mod, bloom_bytes, ha_bytes)


# -----------------------------------------------------------------------------
# Path bridge handler (Bloom-only propagation; packed _br baggage)
# -----------------------------------------------------------------------------

class PathBridgeHandler(BridgeHandler):
    """
    Path bridge: single packed baggage field __bag._br = varint(depth_mod) || bloom_bytes.
    - OnStart: read parent _br, unpack to get depth_mod and bloom; compute next depth_mod;
      set __bag._br only; set ancestry/ancestry_mode and _d for export/display.
    - OnEnd: unpack _br to get depth_mod (or use leaf); strip ancestry when not checkpoint.
    """

    def __init__(self, checkpoint_distance: int = 1, bloom_fp_rate: float = 0.0001):
        if BloomFilter is None or estimate_parameters is None:
            raise RuntimeError("Path bridge requires bloom module (bloom.py)")
        self._cpd = max(1, checkpoint_distance)
        self._bloom_p = bloom_fp_rate
        n = max(1, self._cpd)
        self._bloom_m, self._bloom_k = estimate_parameters(n, self._bloom_p)
        self._bloom_len = (self._bloom_m + 7) // 8
        self._span_info: dict = {}  # (trace_id, span_id) -> {"_br": packed_bytes}
        self._has_children: set = set()

    def _empty_bloom(self):
        return BloomFilter(self._bloom_m, self._bloom_k)

    def on_start(self, parent_ctx: ParentContext, span: dict) -> bool:
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        parent_id = parent_ctx.parent_span_id

        if parent_id is not None:
            self._has_children.add((trace_id, parent_id))

        parent_info = self._span_info.get((trace_id, parent_id)) if parent_id else None
        baggage_found = parent_info is not None

        if parent_info is not None:
            packed = parent_info.get("_br")
            if packed is None:
                depth_mod = 0
                bf = self._empty_bloom()
            else:
                if isinstance(packed, str):
                    packed = bytes.fromhex(packed)
                unpacked = unpack_br(packed, self._bloom_len)
                if unpacked is None:
                    depth_mod = 0
                    bf = self._empty_bloom()
                else:
                    parent_depth_mod, parent_bloom_bytes = unpacked
                    depth_mod = (parent_depth_mod + 1) % self._cpd
                    bf = BloomFilter.deserialize(parent_bloom_bytes, self._bloom_m, self._bloom_k)
        else:
            depth_mod = 0
            bf = self._empty_bloom()

        bf.add(span_id.encode("utf-8"))
        bf_bytes = bf.to_bytes()
        is_checkpoint = (depth_mod == 0)

        if is_checkpoint:
            # Figure 10: emitted payload bytes at checkpoint spans (depth-based).
            # Emit data before the reset (bf_bytes currently includes "history up to now" + this span id).
            pre_reset_bf_bytes = bf_bytes
            emitted_bytes = (
                BR_PROPERTY_NAME_OVERHEAD_BYTES
                + PB_BRIDGE_TYPE_ID
                + len(_varint_encode(depth_mod))
                + len(pre_reset_bf_bytes)
            )
            span_set_tag(span, EMIT_PAYLOAD_BYTES_TAG, emitted_bytes)

            bf = self._empty_bloom()
            bf.add(span_id.encode("utf-8"))
            bf_bytes = bf.to_bytes()

        packed = pack_br(depth_mod, bf_bytes)
        span_set_tag(span, BAG_BR, packed)
        span_set_tag(span, AncestryModeKey, ANCESTRY_MODE_PB)
        span_set_tag(span, AncestryKey, bf.serialize())
        span_set_tag(span, "_d", depth_mod)

        self._span_info[(trace_id, span_id)] = {"_br": packed}
        return baggage_found

    def on_end(self, span: dict) -> None:
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        raw = span_get_tag(span, BAG_BR)
        is_leaf = (trace_id, span_id) not in self._has_children

        # Decode depth_mod + bloom_bytes from _br (for leaf-based emission) when possible.
        if raw is None:
            depth_mod = 0
            bloom_bytes = b""
        else:
            if isinstance(raw, str):
                raw = bytes.fromhex(raw)
            unpacked = unpack_br(raw, self._bloom_len)
            if unpacked is None:
                depth_mod = 0
                bloom_bytes = b""
            else:
                depth_mod, bloom_bytes = unpacked

        is_checkpoint = (depth_mod == 0) or is_leaf

        # Leaf-based checkpoint emission (only if we didn't already emit at depth-based checkpoint).
        if is_leaf and span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG) is None:
            emitted_bytes = (
                BR_PROPERTY_NAME_OVERHEAD_BYTES
                + PB_BRIDGE_TYPE_ID
                + len(_varint_encode(depth_mod))
                + len(bloom_bytes)
            )
            span_set_tag(span, EMIT_PAYLOAD_BYTES_TAG, emitted_bytes)

        if not is_checkpoint:
            span_remove_tag(span, AncestryKey)
            span_remove_tag(span, AncestryModeKey)


# -----------------------------------------------------------------------------
# CGPB bridge handler (Bloom + call-graph hash array; packed _br baggage)
# -----------------------------------------------------------------------------

# Payload-emission metric needs to count the bytes of the checkpoint payload we "emit".
# For CGPB that includes bloom bytes plus the packed hash-array bytes.
CGP_BRIDGE_TYPE_ID = 2  # call-graph preserving bridge type id


def _ha_append_entry(ha: bytes, parent_span_id: str, depth_mod: int) -> Optional[bytes]:
    """
    Append one CGPB hash-array entry:
      entry := parent_span_id_bytes(8) || varint(depth_mod)
    """
    pid_bytes = _span_id_hex_to_8bytes(parent_span_id)
    if pid_bytes is None:
        return None
    return ha + pid_bytes + _varint_encode(depth_mod)


class CGPBBridgeHandler(BridgeHandler):
    """
    CGPB: uses a packed baggage field __bag._br containing:
      varint(depth_mod) || bloom_bytes || hash_array_bytes

    - Bloom propagation matches PB: bloom accumulates span IDs, and resets on depth_mod==0.
    - Hash-array propagation: on the 2nd started sibling of a given parent (seq_num == 2),
      append (parent_span_id_bytes, varint(depth_mod)) to the hash-array bytes.
    """

    def __init__(self, checkpoint_distance: int = 1, bloom_fp_rate: float = 0.0001):
        if BloomFilter is None or estimate_parameters is None:
            raise RuntimeError("CGPB bridge requires bloom module (bloom.py)")
        self._cpd = max(1, checkpoint_distance)
        self._bloom_p = bloom_fp_rate
        n = max(1, self._cpd)
        self._bloom_m, self._bloom_k = estimate_parameters(n, self._bloom_p)
        self._bloom_len = (self._bloom_m + 7) // 8
        self._span_info: dict = {}  # (trace_id, span_id) -> {"_br": packed_bytes}
        self._has_children: set = set()

    def _empty_bloom(self) -> BloomFilter:
        return BloomFilter(self._bloom_m, self._bloom_k)

    def on_start(self, parent_ctx: ParentContext, span: dict) -> bool:
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        parent_id = parent_ctx.parent_span_id

        if parent_id is not None:
            self._has_children.add((trace_id, parent_id))

        parent_info = self._span_info.get((trace_id, parent_id)) if parent_id else None
        baggage_found = parent_info is not None

        # Unpack parent bridge state (if present)
        if parent_info is not None:
            packed = parent_info.get("_br")
            if packed is None:
                parent_depth_mod, parent_bf_bytes, parent_ha_bytes = 0, b"", b""
            else:
                if isinstance(packed, str):
                    packed = bytes.fromhex(packed)
                unpacked = unpack_cgpb_br(packed, self._bloom_len)
                if unpacked is None:
                    parent_depth_mod, parent_bf_bytes, parent_ha_bytes = 0, b"", b""
                else:
                    parent_depth_mod, parent_bf_bytes, parent_ha_bytes = unpacked
        else:
            parent_depth_mod, parent_bf_bytes, parent_ha_bytes = 0, b"", b""

        depth_mod = (parent_depth_mod + 1) % self._cpd

        # Bloom state: deserialize parent bloom, add current span, then possibly reset at checkpoint.
        if parent_info is not None and parent_bf_bytes:
            bf = BloomFilter.deserialize(parent_bf_bytes, self._bloom_m, self._bloom_k)
        else:
            bf = self._empty_bloom()

        span_id_bytes = span_id.encode("utf-8")
        # Note: bloom hash inputs must match the rest of the simulator's assumptions.
        # Current PB uses span_id.encode("utf-8") (ASCII hex) as the bloom insertion input.
        bf.add(span_id_bytes)

        # Hash-array propagation: only the 2nd sibling append happens here (seq_num comes from processed order).
        ha_bytes = parent_ha_bytes
        if parent_id is not None and parent_ctx.seq_num == 2:
            updated = _ha_append_entry(ha_bytes, parent_id, depth_mod)
            if updated is not None:
                ha_bytes = updated

        is_checkpoint = (depth_mod == 0)

        if is_checkpoint:
            # Pre-reset bloom bytes are what the checkpoint payload measures.
            pre_reset_bf_bytes = bf.to_bytes()
            emitted_bytes = (
                BR_PROPERTY_NAME_OVERHEAD_BYTES
                + CGP_BRIDGE_TYPE_ID
                + len(_varint_encode(depth_mod))
                + len(pre_reset_bf_bytes)
                + len(ha_bytes)
            )
            span_set_tag(span, EMIT_PAYLOAD_BYTES_TAG, emitted_bytes)

            # Reset bloom state (hash array is not cleared here; it is carried forward).
            bf = self._empty_bloom()
            bf.add(span_id_bytes)

        bf_bytes = bf.to_bytes()
        packed = pack_cgpb_br(depth_mod, bf_bytes, ha_bytes)
        span_set_tag(span, BAG_BR, packed)
        span_set_tag(span, AncestryModeKey, "cgpb")
        span_set_tag(span, AncestryKey, bf.serialize())
        span_set_tag(span, "_d", depth_mod)

        self._span_info[(trace_id, span_id)] = {"_br": packed}
        return baggage_found

    def on_end(self, span: dict) -> None:
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        raw = span_get_tag(span, BAG_BR)

        is_leaf = (trace_id, span_id) not in self._has_children

        if raw is None:
            depth_mod = 0
            bloom_bytes = b""
            ha_bytes = b""
        else:
            if isinstance(raw, str):
                raw = bytes.fromhex(raw)
            unpacked = unpack_cgpb_br(raw, self._bloom_len)
            if unpacked is None:
                depth_mod, bloom_bytes, ha_bytes = 0, b"", b""
            else:
                depth_mod, bloom_bytes, ha_bytes = unpacked

        is_checkpoint = (depth_mod == 0) or is_leaf

        # Leaf-based checkpoint emission (only if we didn't already emit at depth-based checkpoint).
        if is_leaf and span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG) is None:
            emitted_bytes = (
                BR_PROPERTY_NAME_OVERHEAD_BYTES
                + CGP_BRIDGE_TYPE_ID
                + len(_varint_encode(depth_mod))
                + len(bloom_bytes)
                + len(ha_bytes)
            )
            span_set_tag(span, EMIT_PAYLOAD_BYTES_TAG, emitted_bytes)

        if not is_checkpoint:
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


def _trace_is_clean(spans: List[dict]) -> bool:
    """
    Validate that a trace is "clean" for sampling:
    - exactly one root span (a span whose normalized `parent_span_id` is None)
    - no broken CHILD_OF references (referenced span IDs must exist)
    - each span has at most one CHILD_OF reference
    - if a CHILD_OF reference exists, it must match the normalized parent_span_id
    """
    if not spans:
        return False

    span_ids = set()
    for s in spans:
        sid = s.get("spanID") or s.get("spanId") or ""
        if not sid:
            return False
        span_ids.add(sid)

    roots = []
    for s in spans:
        sid = s.get("spanID") or s.get("spanId") or ""
        pid = s.get("parent_span_id")

        refs = s.get("references") or []
        child_of = []
        for r in refs:
            if not r:
                continue
            if r.get("refType") != "CHILD_OF":
                continue
            parent_sid = r.get("spanID") or r.get("spanId") or ""
            if parent_sid:
                child_of.append(parent_sid)

        if len(child_of) > 1:
            return False

        if pid is None:
            # Root: must not have any CHILD_OF references
            if child_of:
                return False
            roots.append(sid)
        else:
            # Non-root: parent must exist
            if pid not in span_ids:
                return False
            # If there is an explicit CHILD_OF reference, it must match normalized parent
            if child_of and child_of[0] != pid:
                return False

    return len(roots) == 1


def load_traces_from_dir(
    dir_path: str,
    trace_count: Optional[int] = None,
    offset: int = 0,
    random_sample: bool = False,
    seed: Optional[int] = None,
    require_clean: bool = False,
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

    traces: List[dict] = []
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
                if require_clean:
                    if not _trace_is_clean(normalized):
                        continue
                traces.append({"trace_id": tid, "spans": normalized, "processes": processes})
                if require_clean and len(traces) % 100 == 0:
                    print(f"Clean-sampling processed traces: {len(traces)}", file=sys.stderr)
                if trace_count and len(traces) >= trace_count:
                    return traces
            continue

        # Single trace
        tid = data.get("traceID") or data.get("traceId") or ""
        spans = data.get("spans") or []
        processes = data.get("processes") or {}
        normalized = [_normalize_span(s, processes, tid) for s in spans]
        if require_clean:
            if not _trace_is_clean(normalized):
                continue
        traces.append({"trace_id": tid, "spans": normalized, "processes": processes})
        if require_clean and len(traces) % 100 == 0:
            print(f"Clean-sampling processed traces: {len(traces)}", file=sys.stderr)
        if trace_count and len(traces) >= trace_count:
            return traces

    return traces


# -----------------------------------------------------------------------------
# Event stream: start/end events sorted like SDK order
# -----------------------------------------------------------------------------

def build_events(trace: dict) -> List[Tuple[int, str, int, dict]]:
    """
    Build (timestamp_ns, "start"|"end", depth, span) for a trace.
    Caller must sort.
    """
    spans = trace.get("spans") or []

    # Compute a stable tree depth so that, when start times tie,
    # parents are processed before children.
    span_id_by_sid: dict = {}
    for s in spans:
        sid = s.get("spanID") or s.get("spanId") or ""
        span_id_by_sid[sid] = s

    children: dict = {}
    roots: list = []
    for sid, s in span_id_by_sid.items():
        pid = s.get("parent_span_id")
        if pid is None or pid not in span_id_by_sid:
            roots.append(sid)
        else:
            children.setdefault(pid, []).append(sid)

    from collections import deque

    depth_by_sid: dict = {}
    q = deque()
    for r in roots:
        depth_by_sid[r] = 0
        q.append(r)

    while q:
        cur = q.popleft()
        cur_d = depth_by_sid.get(cur, 0)
        for ch in children.get(cur, []):
            if ch not in depth_by_sid:
                depth_by_sid[ch] = cur_d + 1
                q.append(ch)

    events: List[Tuple[int, str, int, dict]] = []
    for span in spans:
        sid = span.get("spanID") or span.get("spanId") or ""
        depth = depth_by_sid.get(sid, 0)
        events.append((span["start_time_ns"], "start", depth, span))
        events.append((span["end_time_ns"], "end", depth, span))
    return events


def sort_events(events: List[Tuple[int, str, int, dict]]) -> List[Tuple[int, str, int, dict]]:
    """
    Sort event stream deterministically for bridge state propagation:
    - At the same timestamp: `start` events come before `end` events.
    - For tied timestamps on `end`: deeper (child) ends before shallower (parent).
    This avoids ordering artifacts when timestamps collide.
    """
    def key(e):
        ts, typ, depth, span = e
        trace_id = span.get("traceID") or span.get("traceId") or ""
        span_id = span.get("spanID") or span.get("spanId") or ""
        typ_rank = 0 if typ == "start" else 1  # start first at same ts
        # start tie-break: smaller depth (parents) first
        # end tie-break: larger depth (children) first
        depth_rank = depth if typ == "start" else -depth
        return (ts, typ_rank, depth_rank, trace_id, span_id)

    return sorted(events, key=key)


# -----------------------------------------------------------------------------
# Simulator: run events through a handler and collect output spans
# -----------------------------------------------------------------------------

def run_trace(trace: dict, handler: BridgeHandler, bagsize: bool) -> List[Any]:
    """
    Run one trace: build events, sort, call handler.on_start / handler.on_end,
    and return the list of spans (one per span, after on_end).
    """
    events = build_events(trace)
    events = sort_events(events)
    output_spans: List[dict] = []
    output_calls: List[int] = []
    checkpoint_payload_sum = 0
    checkpoint_payload_count = 0
    checkpoint_payload_max = 0
    emitted_span_ids: set = set()

    trace_id = trace.get("trace_id") or ""
    span_count = len(trace.get("spans") or [])

    # CGPB needs a deterministic sibling ordering signal ("seqNum"): the 1-based index
    # of each child's start among the starts of its siblings. We assign it from the
    # simulator's deterministic event order.
    next_seq_num: dict = {}  # (trace_id, parent_span_id) -> next 1-based seq

    for ts_ns, typ, _depth, span in events:
        if typ == "start":
            parent_id = span.get("parent_span_id")
            if parent_id is None:
                seq_num = 0
            else:
                key = (trace_id, parent_id)
                seq_num = next_seq_num.get(key, 1)
                next_seq_num[key] = seq_num + 1
            parent_ctx = ParentContext(trace_id, parent_id, seq_num=seq_num)
            baggage_found = handler.on_start(parent_ctx, span)
            if bagsize and baggage_found:
                total = baggage_byte_size(span)
                breakdown = baggage_byte_size_breakdown(span)
                output_calls.append(total)
                # Intentionally no per-call logging: output is already compact.

            if bagsize:
                span_id = span.get("spanID") or span.get("spanId") or ""
                emitted = span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG)
                if emitted is not None and span_id not in emitted_span_ids:
                    emitted_span_ids.add(span_id)
                    val = int(emitted)
                    checkpoint_payload_sum += val
                    checkpoint_payload_count += 1
                    checkpoint_payload_max = max(checkpoint_payload_max, val)
        else:
            handler.on_end(span)
            if bagsize:
                span_id = span.get("spanID") or span.get("spanId") or ""
                emitted = span_get_tag(span, EMIT_PAYLOAD_BYTES_TAG)
                if emitted is not None and span_id not in emitted_span_ids:
                    emitted_span_ids.add(span_id)
                    val = int(emitted)
                    checkpoint_payload_sum += val
                    checkpoint_payload_count += 1
                    checkpoint_payload_max = max(checkpoint_payload_max, val)
            # Export: append a copy so handler can't mutate after the fact
            if not bagsize:
                import copy
                output_spans.append(copy.deepcopy(span))

    if not bagsize:
        return output_spans

    total_checkpoint_bytes = checkpoint_payload_sum
    num_checkpoint_spans = checkpoint_payload_count
    amortized_by_total = (total_checkpoint_bytes / span_count) if span_count else 0.0
    amortized_by_checkpoint = (total_checkpoint_bytes / num_checkpoint_spans) if num_checkpoint_spans else 0.0

    # Call baggage metrics (Figure 10 companion): summarize rather than store every call size.
    num_baggage_calls = len(output_calls)
    if num_baggage_calls:
        avg_baggage_call = float(sum(output_calls)) / num_baggage_calls
        max_baggage_call = int(max(output_calls))
    else:
        avg_baggage_call = 0.0
        max_baggage_call = 0

    return {
        "trace_id": trace_id,
        "num_spans": span_count,
        "amortized_by_total": amortized_by_total,
        "amortized_by_checkpoint": amortized_by_checkpoint,
        "num_checkpoint_spans": num_checkpoint_spans,
        "max_checkpoint_payload": checkpoint_payload_max,
        "num_baggage_calls": num_baggage_calls,
        "avg_baggage_call": avg_baggage_call,
        "max_baggage_call": max_baggage_call,
    }


def run_traces(traces: List[dict], handler: BridgeHandler, bagsize: bool) -> List[Any]:
    """Run all traces through the handler; return concatenated output."""
    out: List[Any] = []
    for trace in traces:
        r = run_trace(trace, handler, bagsize=bagsize)
        if bagsize:
            out.append(r)
        else:
            out.extend(r)
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
        choices=["vanilla", "pb", "cgpb"],
        default="vanilla",
        help="Bridge mode: vanilla (no-op), pb (path bridge), or cgpb (call-graph preserving bridge)",
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
    parser.add_argument(
        "--random-sample",
        action="store_true",
        help="Randomly sample trace files from the input directory.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed for --random-sample (predictable selection).",
    )
    parser.add_argument(
        "--bagsize",
        action="store_true",
        help="Output per-trace baggage call sizes and checkpoint payload overhead (Figure 10 metrics).",
    )
    args = parser.parse_args()

    traces = load_traces_from_dir(
        args.input_dir,
        trace_count=args.trace_count,
        offset=args.offset,
        random_sample=args.random_sample or args.seed is not None,
        seed=args.seed,
        require_clean=(args.random_sample or args.seed is not None),
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
    elif args.mode == "cgpb":
        handler = CGPBBridgeHandler(
            checkpoint_distance=args.checkpoint_distance,
            bloom_fp_rate=0.0001,
        )
    else:
        handler = VanillaHandler()

    outputs = run_traces(traces, handler, bagsize=args.bagsize)

    # Write output
    if args.bagsize:
        # Compact "parallel arrays" output.
        # Each index i corresponds to the same trace across all arrays.
        out = {
            "checkpoint_distance": args.checkpoint_distance,
            "num_traces": len(outputs),
            "num_spans": [d["num_spans"] for d in outputs],
            "num_checkpoint_spans": [d["num_checkpoint_spans"] for d in outputs],
            "amortized_by_total": [d["amortized_by_total"] for d in outputs],
            "amortized_by_checkpoint": [d["amortized_by_checkpoint"] for d in outputs],
            "max_checkpoint_payload": [d["max_checkpoint_payload"] for d in outputs],
            "num_baggage_calls": [d["num_baggage_calls"] for d in outputs],
            "avg_baggage_call": [d["avg_baggage_call"] for d in outputs],
            "max_baggage_call": [d["max_baggage_call"] for d in outputs],
        }
    else:
        # Hex-encode any bytes tag values for JSON (visibility only)
        for span in outputs:
            for t in span.get("tags") or []:
                v = t.get("value") or t.get("Value")
                if isinstance(v, bytes):
                    t["value"] = v.hex()
        out = {"spans": outputs}
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)

    written = len(outputs)
    if args.bagsize:
        n_traces = out["num_traces"]
        n_calls = sum(out["num_baggage_calls"])
        print(f"Wrote {n_traces} traces ({n_calls} call baggage samples) to {args.output}", file=sys.stderr)
    else:
        print(f"Wrote {written} spans to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
