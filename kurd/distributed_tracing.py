"""
Distributed Tracing Context for Kurd

W3C Trace Context propagation across services.

Usage:
    from kurd.distributed_tracing import TracingContext, extract_context, inject_context

    # Extract trace context from incoming request
    trace_context = extract_context(headers)

    # Propagate to upstream service
    upstream_headers = inject_context(trace_context)

    # Create span
    span = trace_context.create_span("tool_execution", {"tool": "add"})
    span.set_attribute("result", 42)
    span.end()

    # Query trace
    trace = trace_context.to_json()
"""

import secrets
import time
from datetime import datetime
from typing import Dict, Optional, List, Any, Tuple
from enum import Enum
from dataclasses import dataclass, field


class SpanKind(Enum):
    """Type of span."""

    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class SpanStatus(Enum):
    """Status of span."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass
class Span:
    """Represents a distributed trace span."""

    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    name: str
    kind: SpanKind
    start_time: float
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    status: SpanStatus = SpanStatus.UNSET
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict] = field(default_factory=list)
    error: Optional[str] = None

    def set_attribute(self, key: str, value: Any) -> None:
        """Set span attribute."""
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Optional[Dict] = None) -> None:
        """Add event to span."""
        self.events.append({
            "name": name,
            "timestamp": datetime.utcnow().isoformat(),
            "attributes": attributes or {},
        })

    def end(self, error: Optional[str] = None) -> None:
        """End span."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000

        if error:
            self.error = error
            self.status = SpanStatus.ERROR
        else:
            self.status = SpanStatus.OK

    def to_dict(self) -> Dict:
        """Export span as dictionary."""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "kind": self.kind.value,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "attributes": self.attributes,
            "events": self.events,
            "error": self.error,
        }


class TracingContext:
    """Manages distributed trace context (W3C Trace Context)."""

    def __init__(
        self,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        trace_flags: str = "01",
    ):
        """
        Initialize tracing context.

        Args:
            trace_id: Trace ID (generated if not provided)
            span_id: Current span ID (generated if not provided)
            parent_span_id: Parent span ID (if any)
            trace_flags: W3C trace flags ("01" for sampled)
        """
        self.trace_id = trace_id or f"{secrets.token_hex(8)}"
        self.span_id = span_id or f"{secrets.token_hex(8)}"
        self.parent_span_id = parent_span_id
        self.trace_flags = trace_flags
        self.spans: List[Span] = []
        self.start_time = time.time()
        self.baggage: Dict[str, str] = {}

    def create_span(
        self,
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[Dict] = None,
    ) -> Span:
        """Create child span."""
        span = Span(
            trace_id=self.trace_id,
            span_id=f"{secrets.token_hex(8)}",
            parent_span_id=self.span_id,
            name=name,
            kind=kind,
            start_time=time.time(),
            attributes=attributes or {},
        )

        self.spans.append(span)
        return span

    def set_baggage(self, key: str, value: str) -> None:
        """Set trace baggage (context propagated to all spans)."""
        self.baggage[key] = value

    def get_baggage(self, key: str) -> Optional[str]:
        """Get trace baggage value."""
        return self.baggage.get(key)

    def to_json(self) -> Dict:
        """Export trace as JSON."""
        return {
            "trace_id": self.trace_id,
            "root_span_id": self.span_id,
            "trace_flags": self.trace_flags,
            "start_time": self.start_time,
            "duration_ms": (time.time() - self.start_time) * 1000,
            "span_count": len(self.spans),
            "spans": [s.to_dict() for s in self.spans],
            "baggage": self.baggage,
        }


def extract_context(headers: Dict[str, str]) -> TracingContext:
    """
    Extract W3C Trace Context from HTTP headers.

    Supports:
    - traceparent: 00-trace_id-span_id-flags
    - tracestate: vendor-specific
    - baggage: key=value pairs

    Args:
        headers: HTTP headers

    Returns:
        TracingContext
    """
    traceparent = headers.get("traceparent", "")
    tracestate = headers.get("tracestate", "")
    baggage_header = headers.get("baggage", "")

    trace_id = None
    span_id = None
    trace_flags = "01"

    if traceparent:
        # Format: version-trace_id-span_id-trace_flags
        parts = traceparent.split("-")
        if len(parts) == 4:
            version, trace_id, span_id, trace_flags = parts

    context = TracingContext(
        trace_id=trace_id,
        span_id=span_id,
        trace_flags=trace_flags,
    )

    # Parse baggage
    if baggage_header:
        for item in baggage_header.split(","):
            if "=" in item:
                key, value = item.split("=", 1)
                context.set_baggage(key.strip(), value.strip())

    return context


def inject_context(context: TracingContext) -> Dict[str, str]:
    """
    Inject W3C Trace Context into HTTP headers.

    Returns:
        Dict of headers to add to upstream request
    """
    # Create new span ID for downstream service
    downstream_span_id = f"{secrets.token_hex(8)}"

    headers = {
        "traceparent": f"00-{context.trace_id}-{downstream_span_id}-{context.trace_flags}",
        "tracestate": f"kurd-span={context.span_id}",
    }

    # Add baggage
    if context.baggage:
        baggage_items = [f"{k}={v}" for k, v in context.baggage.items()]
        headers["baggage"] = ", ".join(baggage_items)

    return headers


def extract_trace_context_from_headers(headers: Dict[str, str]) -> Tuple[str, str]:
    """
    Extract trace ID and span ID from request headers.

    Args:
        headers: HTTP headers (case-insensitive)

    Returns:
        (trace_id, span_id)
    """
    # Normalize header keys to lowercase
    normalized = {k.lower(): v for k, v in headers.items()}

    traceparent = normalized.get("traceparent", "")
    trace_id = None
    span_id = None

    if traceparent:
        parts = traceparent.split("-")
        if len(parts) == 4:
            _, trace_id, span_id, _ = parts

    # Fallback to custom headers
    if not trace_id:
        trace_id = normalized.get("x-trace-id", f"{secrets.token_hex(8)}")
    if not span_id:
        span_id = normalized.get("x-span-id", f"{secrets.token_hex(8)}")

    return trace_id, span_id


class TraceExporter:
    """Exports traces for analysis."""

    def __init__(self):
        """Initialize trace exporter."""
        self.traces: List[Dict] = []

    def export_trace(self, trace_context: TracingContext) -> None:
        """Export trace."""
        self.traces.append(trace_context.to_json())

    def get_traces_by_id(self, trace_id: str) -> List[Dict]:
        """Get all traces with given ID."""
        return [t for t in self.traces if t["trace_id"] == trace_id]

    def get_slow_traces(self, threshold_ms: float = 1000) -> List[Dict]:
        """Get traces exceeding duration threshold."""
        return [
            t for t in self.traces
            if t["duration_ms"] >= threshold_ms
        ]

    def get_error_traces(self) -> List[Dict]:
        """Get traces with errors."""
        results = []
        for trace in self.traces:
            if any(s["status"] == "error" for s in trace.get("spans", [])):
                results.append(trace)
        return results

    def get_statistics(self) -> Dict:
        """Get trace statistics."""
        if not self.traces:
            return {
                "total_traces": 0,
                "average_duration_ms": 0,
                "error_rate": 0,
            }

        total_traces = len(self.traces)
        avg_duration = sum(t["duration_ms"] for t in self.traces) / total_traces
        error_traces = sum(
            1 for t in self.traces
            if any(s["status"] == "error" for s in t.get("spans", []))
        )
        error_rate = (error_traces / total_traces * 100) if total_traces > 0 else 0

        return {
            "total_traces": total_traces,
            "average_duration_ms": avg_duration,
            "error_rate": error_rate,
            "error_count": error_traces,
        }

    def clear(self) -> None:
        """Clear all traces."""
        self.traces.clear()

    def to_json(self) -> Dict:
        """Export as JSON."""
        return {
            "statistics": self.get_statistics(),
            "traces": self.traces[:100],  # Last 100
        }
