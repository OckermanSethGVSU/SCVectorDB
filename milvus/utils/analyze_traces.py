#!/usr/bin/env python3
"""Analyze OTLP JSONL trace exports with hierarchical aggregation.

Input format: OpenTelemetry Collector file exporter JSONL where each line is an
OTLP JSON payload containing resourceSpans -> scopeSpans -> spans.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str
    name: str
    kind: int
    start_ns: int
    end_ns: int
    status_code: Optional[int]
    status_message: Optional[str]
    attributes: Dict[str, str]

    @property
    def duration_ms(self) -> float:
        return max(0.0, (self.end_ns - self.start_ns) / 1_000_000.0)


@dataclass
class TraceTreeNode:
    span: Span
    children: List["TraceTreeNode"] = field(default_factory=list)


@dataclass
class AggNode:
    name: str
    depth: int
    children: Dict[str, "AggNode"] = field(default_factory=dict)
    durations_ms: List[float] = field(default_factory=list)
    traces: set = field(default_factory=set)
    kinds: Counter = field(default_factory=Counter)
    status_codes: Counter = field(default_factory=Counter)

    def add(self, span: Span) -> None:
        self.durations_ms.append(span.duration_ms)
        self.traces.add(span.trace_id)
        self.kinds[span.kind] += 1
        self.status_codes[span.status_code if span.status_code is not None else -1] += 1

    def child(self, name: str) -> "AggNode":
        if name not in self.children:
            self.children[name] = AggNode(name=name, depth=self.depth + 1)
        return self.children[name]


def parse_attributes(raw_attrs: List[dict]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in raw_attrs or []:
        key = item.get("key")
        val = item.get("value", {})
        if not key:
            continue
        if "stringValue" in val:
            out[key] = str(val["stringValue"])
        elif "intValue" in val:
            out[key] = str(val["intValue"])
        elif "doubleValue" in val:
            out[key] = str(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = str(val["boolValue"])
        else:
            out[key] = json.dumps(val, sort_keys=True)
    return out


def iter_spans_from_jsonl(path: Path) -> Iterable[Span]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_no}: {exc}") from exc
            for rspan in payload.get("resourceSpans", []):
                scope_spans = rspan.get("scopeSpans", []) or rspan.get("instrumentationLibrarySpans", [])
                for sscope in scope_spans:
                    for s in sscope.get("spans", []):
                        status = s.get("status") or {}
                        yield Span(
                            trace_id=str(s.get("traceId", "")).lower(),
                            span_id=str(s.get("spanId", "")).lower(),
                            parent_span_id=str(s.get("parentSpanId", "")).lower(),
                            name=str(s.get("name", "")),
                            kind=int(s.get("kind", 0) or 0),
                            start_ns=int(s.get("startTimeUnixNano", 0) or 0),
                            end_ns=int(s.get("endTimeUnixNano", 0) or 0),
                            status_code=int(status["code"]) if "code" in status else None,
                            status_message=status.get("message"),
                            attributes=parse_attributes(s.get("attributes", [])),
                        )


def build_traces(spans: Iterable[Span]) -> Dict[str, Dict[str, Span]]:
    traces: Dict[str, Dict[str, Span]] = defaultdict(dict)
    for sp in spans:
        if not sp.trace_id or not sp.span_id:
            continue
        traces[sp.trace_id][sp.span_id] = sp
    return traces


def build_tree(span_map: Dict[str, Span]) -> List[TraceTreeNode]:
    children: Dict[str, List[Span]] = defaultdict(list)
    roots: List[Span] = []
    for sp in span_map.values():
        if sp.parent_span_id and sp.parent_span_id in span_map:
            children[sp.parent_span_id].append(sp)
        else:
            roots.append(sp)

    def make_node(span: Span) -> TraceTreeNode:
        node = TraceTreeNode(span=span)
        for ch in sorted(children.get(span.span_id, []), key=lambda x: x.start_ns):
            node.children.append(make_node(ch))
        return node

    return [make_node(r) for r in sorted(roots, key=lambda x: x.start_ns)]


def trace_matches(
    roots: List[TraceTreeNode],
    trace_id: str,
    trace_id_filter: Optional[str],
    root_name_filter: Optional[re.Pattern[str]],
    any_span_name_filter: Optional[re.Pattern[str]],
) -> bool:
    if trace_id_filter and trace_id != trace_id_filter:
        return False

    if root_name_filter:
        if not any(root_name_filter.search(r.span.name) for r in roots):
            return False

    if any_span_name_filter:
        stack = list(roots)
        matched = False
        while stack:
            cur = stack.pop()
            if any_span_name_filter.search(cur.span.name):
                matched = True
                break
            stack.extend(cur.children)
        if not matched:
            return False
    return True


def aggregate_trees(root_nodes_per_trace: List[Tuple[str, List[TraceTreeNode]]]) -> AggNode:
    agg_root = AggNode(name="__root__", depth=-1)

    def walk(trace_id: str, tnode: TraceTreeNode, anode: AggNode) -> None:
        child_agg = anode.child(tnode.span.name)
        child_agg.add(tnode.span)
        for c in tnode.children:
            walk(trace_id, c, child_agg)

    for trace_id, roots in root_nodes_per_trace:
        for root in roots:
            walk(trace_id, root, agg_root)
    return agg_root


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    idx = (len(s) - 1) * p
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return s[int(idx)]
    frac = idx - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def stats_str(values: List[float]) -> str:
    if not values:
        return "count=0"
    return (
        f"count={len(values)} min={min(values):.3f}ms max={max(values):.3f}ms "
        f"avg={statistics.fmean(values):.3f}ms p50={percentile(values, 0.50):.3f}ms "
        f"p95={percentile(values, 0.95):.3f}ms"
    )


def print_agg_tree(node: AggNode, min_count: int = 1) -> None:
    def rec(cur: AggNode, prefix: str) -> None:
        visible = [
            c for c in sorted(cur.children.values(), key=lambda n: (-len(n.durations_ms), n.name))
            if len(c.durations_ms) >= min_count
        ]
        for i, child in enumerate(visible):
            is_last = i == len(visible) - 1
            branch = "└─" if is_last else "├─"
            continuation = "   " if is_last else "│  "
            traces_count = len(child.traces)
            kinds = ",".join(f"{k}:{v}" for k, v in sorted(child.kinds.items()))
            print(f"{prefix}{branch} {child.name}")
            print(f"{prefix}{continuation}└─ traces={traces_count} {stats_str(child.durations_ms)} kinds=[{kinds}]")
            rec(child, prefix + continuation)

    rec(node, "")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze OTLP JSONL traces with hierarchical aggregation.")
    p.add_argument("--input", default="traces.jsonl", help="Path to OTLP JSONL trace file")
    p.add_argument("--trace-id", default="", help="Filter to one traceId (hex)")
    p.add_argument("--root-span", default="", help="Regex filter for root span name (example: test_milvus_otel)")
    p.add_argument("--span-name", default="", help="Regex filter: trace must contain a span name matching this")
    p.add_argument("--min-count", type=int, default=1, help="Only print aggregated nodes with at least this count")
    p.add_argument("--max-traces", type=int, default=0, help="Stop after selecting this many traces (0=all)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    trace_id_filter = args.trace_id.strip().lower() or None
    root_name_filter = re.compile(args.root_span) if args.root_span else None
    any_span_name_filter = re.compile(args.span_name) if args.span_name else None

    spans = list(iter_spans_from_jsonl(input_path))
    traces = build_traces(spans)

    selected: List[Tuple[str, List[TraceTreeNode]]] = []
    for tid in sorted(traces.keys()):
        roots = build_tree(traces[tid])
        if not trace_matches(roots, tid, trace_id_filter, root_name_filter, any_span_name_filter):
            continue
        selected.append((tid, roots))
        if args.max_traces and len(selected) >= args.max_traces:
            break

    print(f"Input: {input_path}")
    print(f"Traces total: {len(traces)}")
    print(f"Traces selected: {len(selected)}")
    if not selected:
        return

    agg_root = aggregate_trees(selected)
    print("\nHierarchical aggregation:")
    print_agg_tree(agg_root, min_count=max(1, args.min_count))

    print("\nSelected trace IDs:")
    for tid, _ in selected[:20]:
        print(f"- {tid}")
    if len(selected) > 20:
        print(f"... ({len(selected) - 20} more)")


if __name__ == "__main__":
    main()
