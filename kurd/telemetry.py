"""
OpenTelemetry Integration for Kurd

Exports traces, metrics, and logs to OpenTelemetry-compatible backends.

Usage:
    from kurd import Router
    from kurd.telemetry import setup_otel

    otel = setup_otel(
        service_name="mcp-gateway",
        exporter="otlp",
        endpoint="http://localhost:4317"
    )
    router = Router()
    router.set_telemetry(otel)
"""

from typing import Optional, Any, Dict
from dataclasses import dataclass
import json


@dataclass
class OTELConfig:
    """OpenTelemetry configuration."""

    service_name: str
    service_version: str = "0.4.0"
    exporter: str = "otlp"  # otlp, jaeger, datadog, newrelic
    endpoint: Optional[str] = None
    api_key: Optional[str] = None
    environment: str = "production"


class OTELTracer:
    """OpenTelemetry tracer for Kurd."""

    def __init__(self, config: OTELConfig):
        self.config = config
        self.traces: list[dict] = []

    def trace_request(
        self,
        request_id: str,
        tool_name: str,
        latency_ms: float,
        status: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a traced request."""
        span = {
            "trace_id": request_id,
            "span_id": f"{request_id}-span",
            "operation_name": f"tools/{tool_name}",
            "service_name": self.config.service_name,
            "duration_ms": latency_ms,
            "status": status,
            "attributes": attributes or {},
            "resource": {
                "service.name": self.config.service_name,
                "service.version": self.config.service_version,
                "deployment.environment": self.config.environment,
            },
        }
        self.traces.append(span)

    def export_traces(self) -> str:
        """Export traces as JSON."""
        return json.dumps({
            "resourceSpans": [{
                "resource": {
                    "attributes": {
                        "service.name": self.config.service_name,
                        "service.version": self.config.service_version,
                    }
                },
                "scopeSpans": [{
                    "spans": self.traces,
                }],
            }],
        })

    def get_traces(self, limit: int = 100) -> list[dict]:
        """Get recent traces."""
        return self.traces[-limit:]

    def clear_traces(self) -> None:
        """Clear in-memory traces."""
        self.traces.clear()


def setup_otel(
    service_name: str,
    service_version: str = "0.4.0",
    exporter: str = "otlp",
    endpoint: Optional[str] = None,
    api_key: Optional[str] = None,
    environment: str = "production",
) -> OTELTracer:
    """
    Setup OpenTelemetry.

    Args:
        service_name: Name of the service
        service_version: Version of the service
        exporter: 'otlp' (default), 'jaeger', 'datadog', 'newrelic'
        endpoint: Exporter endpoint (e.g., http://localhost:4317)
        api_key: API key for hosted backends
        environment: Deployment environment

    Returns:
        OTELTracer instance

    Example:
        otel = setup_otel(
            service_name="kurd-gateway",
            exporter="otlp",
            endpoint="http://localhost:4317"
        )
    """
    config = OTELConfig(
        service_name=service_name,
        service_version=service_version,
        exporter=exporter,
        endpoint=endpoint,
        api_key=api_key,
        environment=environment,
    )
    return OTELTracer(config)


# Exporter implementations
class OTLPExporter:
    """OTLP (OpenTelemetry Protocol) exporter."""

    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def export(self, traces: list[dict]) -> None:
        """Export traces to OTLP backend."""
        import urllib.request
        import json

        payload = {
            "resourceSpans": [{
                "resource": {},
                "scopeSpans": [{"spans": traces}],
            }],
        }

        request = urllib.request.Request(
            f"{self.endpoint}/v1/traces",
            data=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status == 200
        except Exception as e:
            print(f"OTLP export failed: {e}")
            return False


class DatadogExporter:
    """Datadog trace exporter."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def export(self, traces: list[dict]) -> None:
        """Export traces to Datadog."""
        import urllib.request
        import json

        for trace in traces:
            payload = [
                {
                    "trace_id": trace["trace_id"],
                    "span_id": trace["span_id"],
                    "parent_id": 0,
                    "operation_name": trace["operation_name"],
                    "duration": int(trace["duration_ms"] * 1_000_000),
                }
            ]

            request = urllib.request.Request(
                "https://trace.agent.datadoghq.com/v0.4/traces",
                data=json.dumps(payload).encode(),
                headers={
                    "content-type": "application/json",
                    "DD-API-KEY": self.api_key,
                },
                method="POST",
            )

            try:
                urllib.request.urlopen(request, timeout=5)
            except Exception as e:
                print(f"Datadog export failed: {e}")


class JaegerExporter:
    """Jaeger trace exporter."""

    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def export(self, traces: list[dict]) -> None:
        """Export traces to Jaeger."""
        import urllib.request
        import json

        payload = {
            "batches": [{
                "process": {
                    "serviceName": "kurd",
                },
                "spans": traces,
            }],
        }

        request = urllib.request.Request(
            f"{self.endpoint}/api/traces",
            data=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )

        try:
            urllib.request.urlopen(request, timeout=5)
        except Exception as e:
            print(f"Jaeger export failed: {e}")
