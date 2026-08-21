"""
Resource Limits for Kurd

Enforces request/response size limits and resource quotas per tenant.

Usage:
    from kurd.resource_limits import ResourceLimitManager

    limits = ResourceLimitManager()

    # Set global limit
    limits.set_global_request_limit_bytes(10 * 1024 * 1024)  # 10MB

    # Set per-tenant limit
    limits.set_tenant_request_limit_bytes("customer-1", 5 * 1024 * 1024)  # 5MB

    # Check request
    allowed, error = limits.check_request_size("customer-1", request_bytes=1024*1024)
"""

from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class ResourceQuota:
    """Resource quota for tenant."""

    tenant_id: str
    max_request_bytes: int  # Max request size
    max_response_bytes: int  # Max response size
    max_concurrent_requests: int  # Max concurrent requests
    max_requests_per_second: int  # Max RPS


class ResourceLimitManager:
    """Manages resource limits and quotas."""

    def __init__(self):
        # Global limits (defaults)
        self.global_max_request_bytes = 10 * 1024 * 1024  # 10MB
        self.global_max_response_bytes = 50 * 1024 * 1024  # 50MB
        self.global_max_concurrent_requests = 10000
        self.global_max_rps = 100000

        # Per-tenant overrides
        self.tenant_quotas: Dict[str, ResourceQuota] = {}

        # Track usage
        self.tenant_concurrent_requests: Dict[str, int] = {}
        self.tenant_request_counts: Dict[str, dict] = {}  # {tenant_id: {timestamp: count}}

    def set_global_request_limit(self, bytes_limit: int) -> None:
        """Set global request size limit."""
        self.global_max_request_bytes = bytes_limit

    def set_global_response_limit(self, bytes_limit: int) -> None:
        """Set global response size limit."""
        self.global_max_response_bytes = bytes_limit

    def set_tenant_request_limit(self, tenant_id: str, bytes_limit: int) -> None:
        """Set request size limit for tenant."""
        if tenant_id not in self.tenant_quotas:
            self.tenant_quotas[tenant_id] = self._create_quota(tenant_id)

        self.tenant_quotas[tenant_id].max_request_bytes = bytes_limit

    def set_tenant_response_limit(self, tenant_id: str, bytes_limit: int) -> None:
        """Set response size limit for tenant."""
        if tenant_id not in self.tenant_quotas:
            self.tenant_quotas[tenant_id] = self._create_quota(tenant_id)

        self.tenant_quotas[tenant_id].max_response_bytes = bytes_limit

    def set_tenant_concurrent_limit(self, tenant_id: str, limit: int) -> None:
        """Set concurrent request limit for tenant."""
        if tenant_id not in self.tenant_quotas:
            self.tenant_quotas[tenant_id] = self._create_quota(tenant_id)

        self.tenant_quotas[tenant_id].max_concurrent_requests = limit

    def set_tenant_rps_limit(self, tenant_id: str, limit: int) -> None:
        """Set requests-per-second limit for tenant."""
        if tenant_id not in self.tenant_quotas:
            self.tenant_quotas[tenant_id] = self._create_quota(tenant_id)

        self.tenant_quotas[tenant_id].max_requests_per_second = limit

    def check_request_size(
        self,
        tenant_id: str,
        request_bytes: int,
    ) -> tuple[bool, Optional[str]]:
        """Check if request size is within limit."""
        quota = self.tenant_quotas.get(tenant_id)

        if quota:
            max_bytes = quota.max_request_bytes
        else:
            max_bytes = self.global_max_request_bytes

        if request_bytes > max_bytes:
            return False, f"Request size {request_bytes} exceeds limit {max_bytes}"

        return True, None

    def check_response_size(
        self,
        tenant_id: str,
        response_bytes: int,
    ) -> tuple[bool, Optional[str]]:
        """Check if response size is within limit."""
        quota = self.tenant_quotas.get(tenant_id)

        if quota:
            max_bytes = quota.max_response_bytes
        else:
            max_bytes = self.global_max_response_bytes

        if response_bytes > max_bytes:
            return False, f"Response size {response_bytes} exceeds limit {max_bytes}"

        return True, None

    def acquire_concurrent_request(self, tenant_id: str) -> tuple[bool, Optional[str]]:
        """Acquire a concurrent request slot."""
        quota = self.tenant_quotas.get(tenant_id)
        max_concurrent = quota.max_concurrent_requests if quota else self.global_max_concurrent_requests

        current = self.tenant_concurrent_requests.get(tenant_id, 0)

        if current >= max_concurrent:
            return False, f"Concurrent request limit {max_concurrent} exceeded"

        self.tenant_concurrent_requests[tenant_id] = current + 1
        return True, None

    def release_concurrent_request(self, tenant_id: str) -> None:
        """Release a concurrent request slot."""
        current = self.tenant_concurrent_requests.get(tenant_id, 0)
        if current > 0:
            self.tenant_concurrent_requests[tenant_id] = current - 1

    def check_rps(self, tenant_id: str) -> tuple[bool, Optional[str]]:
        """Check if request rate is within limit."""
        import time

        quota = self.tenant_quotas.get(tenant_id)
        max_rps = quota.max_requests_per_second if quota else self.global_max_rps

        current_time = int(time.time())

        if tenant_id not in self.tenant_request_counts:
            self.tenant_request_counts[tenant_id] = {}

        counts = self.tenant_request_counts[tenant_id]

        # Clean up old entries (keep only current second)
        counts = {t: c for t, c in counts.items() if t >= current_time - 1}
        self.tenant_request_counts[tenant_id] = counts

        # Count requests in current second
        current_count = counts.get(current_time, 0)

        if current_count >= max_rps:
            return False, f"Request rate limit {max_rps} req/s exceeded"

        # Increment counter
        counts[current_time] = current_count + 1

        return True, None

    def get_tenant_quota(self, tenant_id: str) -> ResourceQuota:
        """Get quota for tenant (or default)."""
        if tenant_id in self.tenant_quotas:
            return self.tenant_quotas[tenant_id]

        return self._create_quota(tenant_id)

    def _create_quota(self, tenant_id: str) -> ResourceQuota:
        """Create default quota for tenant."""
        return ResourceQuota(
            tenant_id=tenant_id,
            max_request_bytes=self.global_max_request_bytes,
            max_response_bytes=self.global_max_response_bytes,
            max_concurrent_requests=self.global_max_concurrent_requests,
            max_requests_per_second=self.global_max_rps,
        )

    def to_json(self) -> Dict:
        """Export state as JSON."""
        return {
            "global": {
                "max_request_bytes": self.global_max_request_bytes,
                "max_response_bytes": self.global_max_response_bytes,
                "max_concurrent_requests": self.global_max_concurrent_requests,
                "max_rps": self.global_max_rps,
            },
            "tenant_quotas": {
                tid: {
                    "max_request_bytes": q.max_request_bytes,
                    "max_response_bytes": q.max_response_bytes,
                    "max_concurrent_requests": q.max_concurrent_requests,
                    "max_rps": q.max_requests_per_second,
                }
                for tid, q in self.tenant_quotas.items()
            },
            "current_usage": {
                "concurrent_requests": self.tenant_concurrent_requests,
            },
        }
