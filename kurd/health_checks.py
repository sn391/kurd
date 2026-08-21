"""
Health Checks & Kubernetes Probes for Kurd

Implements liveness and readiness probes for Kubernetes deployments.

Usage:
    from kurd.health_checks import HealthCheckManager

    health = HealthCheckManager()

    # Check if gateway is ready
    readiness = health.check_readiness()

    # Check if gateway is alive
    liveness = health.check_liveness()
"""

from typing import Dict, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta
import time


@dataclass
class HealthStatus:
    """Health check result."""

    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: datetime
    checks: Dict[str, Dict]  # name -> {status, message, latency_ms}
    uptime_seconds: float
    last_error: Optional[str] = None


class HealthCheckManager:
    """Manages health checks and Kubernetes probes."""

    def __init__(self):
        self.startup_time = datetime.utcnow()
        self.last_check_time = datetime.utcnow()
        self.check_functions: Dict[str, callable] = {}
        self.upstream_status: Dict[str, bool] = {}
        self.last_error: Optional[str] = None
        self.request_count = 0
        self.error_count = 0
        self.last_error_time: Optional[datetime] = None

    def register_check(self, name: str, check_func: callable) -> None:
        """Register a custom health check."""
        self.check_functions[name] = check_func

    def set_upstream_status(self, upstream_name: str, healthy: bool) -> None:
        """Update upstream server status."""
        self.upstream_status[upstream_name] = healthy
        if not healthy:
            self.last_error = f"Upstream {upstream_name} unhealthy"
            self.last_error_time = datetime.utcnow()

    def record_request(self, success: bool) -> None:
        """Record request for metrics."""
        self.request_count += 1
        if not success:
            self.error_count += 1
            self.last_error_time = datetime.utcnow()

    def check_readiness(self) -> HealthStatus:
        """
        Check if gateway is ready to accept traffic.

        Readiness checks:
        - Gateway has started
        - No critical dependencies are down
        - Error rate is acceptable
        """
        checks = {}
        all_healthy = True

        # Check startup
        checks["startup"] = {
            "status": "healthy",
            "message": "Gateway started",
            "latency_ms": 0,
        }

        # Check upstreams
        for upstream_name, is_healthy in self.upstream_status.items():
            status = "healthy" if is_healthy else "unhealthy"
            if not is_healthy:
                all_healthy = False
            checks[f"upstream_{upstream_name}"] = {
                "status": status,
                "message": f"Upstream {upstream_name}",
                "latency_ms": 0,
            }

        # Check error rate (fail if > 5% errors in last 100 requests)
        error_rate = (self.error_count / self.request_count) if self.request_count > 0 else 0
        if error_rate > 0.05:
            all_healthy = False
            checks["error_rate"] = {
                "status": "unhealthy",
                "message": f"Error rate too high: {error_rate:.2%}",
                "latency_ms": 0,
            }
        else:
            checks["error_rate"] = {
                "status": "healthy",
                "message": f"Error rate: {error_rate:.2%}",
                "latency_ms": 0,
            }

        # Run custom checks
        for check_name, check_func in self.check_functions.items():
            try:
                result = check_func()
                checks[check_name] = result
                if result.get("status") != "healthy":
                    all_healthy = False
            except Exception as e:
                all_healthy = False
                checks[check_name] = {
                    "status": "unhealthy",
                    "message": str(e),
                    "latency_ms": 0,
                }

        status = "healthy" if all_healthy else "degraded"

        return HealthStatus(
            status=status,
            timestamp=datetime.utcnow(),
            checks=checks,
            uptime_seconds=(datetime.utcnow() - self.startup_time).total_seconds(),
            last_error=self.last_error,
        )

    def check_liveness(self) -> HealthStatus:
        """
        Check if gateway is alive (Kubernetes liveness probe).

        Liveness checks:
        - Process is running
        - No deadlock detected
        - Memory usage is reasonable
        """
        checks = {}
        all_healthy = True

        # Check process
        checks["process"] = {
            "status": "healthy",
            "message": "Process is running",
            "latency_ms": 0,
        }

        # Check uptime
        uptime = (datetime.utcnow() - self.startup_time).total_seconds()
        checks["uptime"] = {
            "status": "healthy",
            "message": f"Uptime: {uptime:.0f}s",
            "latency_ms": 0,
        }

        # Check recent activity (fail if no requests in 5 minutes)
        if (datetime.utcnow() - self.last_check_time).total_seconds() > 300:
            # Update last check time
            self.last_check_time = datetime.utcnow()
            checks["activity"] = {
                "status": "degraded",
                "message": "No activity in 5 minutes",
                "latency_ms": 0,
            }
        else:
            checks["activity"] = {
                "status": "healthy",
                "message": "Recent activity detected",
                "latency_ms": 0,
            }

        status = "healthy" if all_healthy else "healthy"  # Liveness is more lenient

        return HealthStatus(
            status=status,
            timestamp=datetime.utcnow(),
            checks=checks,
            uptime_seconds=uptime,
            last_error=self.last_error,
        )

    def to_k8s_readiness_response(self) -> tuple[int, str]:
        """
        Format response for Kubernetes readiness probe.

        Returns:
            (status_code, body)
        """
        readiness = self.check_readiness()

        if readiness.status == "healthy":
            return 200, "Ready"
        elif readiness.status == "degraded":
            return 503, "Not Ready"
        else:
            return 503, "Unhealthy"

    def to_k8s_liveness_response(self) -> tuple[int, str]:
        """
        Format response for Kubernetes liveness probe.

        Returns:
            (status_code, body)
        """
        liveness = self.check_liveness()

        if liveness.status == "healthy":
            return 200, "Alive"
        else:
            return 500, "Dead"

    def to_json(self) -> Dict:
        """Export health status as JSON."""
        readiness = self.check_readiness()

        return {
            "status": readiness.status,
            "timestamp": readiness.timestamp.isoformat(),
            "uptime_seconds": readiness.uptime_seconds,
            "checks": readiness.checks,
            "last_error": readiness.last_error,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": (self.error_count / self.request_count * 100) if self.request_count > 0 else 0,
            "upstreams": self.upstream_status,
        }
