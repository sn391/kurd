"""
Multi-Tenancy Support for Kurd

Isolate tools, quotas, and authentication per tenant.

Usage:
    from kurd import Router
    from kurd.multitenancy import TenantManager

    manager = TenantManager()
    manager.add_tenant(
        id="acme-corp",
        api_key="sk-acme-...",
        quota_rps=100,
        allowed_tools=["add", "multiply"]
    )

    router = Router()
    router.set_tenant_manager(manager)
"""

from typing import Optional, Dict, List, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import secrets


@dataclass
class Tenant:
    """Represents a single tenant."""

    id: str
    api_key: str
    name: str
    description: Optional[str] = None
    quota_rps: int = 100
    allowed_tools: Set[str] = field(default_factory=lambda: {"*"})  # "*" = all tools
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, str] = field(default_factory=dict)

    # Rate limiting tracking
    request_count: int = field(default=0)
    last_reset: datetime = field(default_factory=datetime.utcnow)

    def can_call_tool(self, tool_name: str) -> bool:
        """Check if tenant is allowed to call this tool."""
        if "*" in self.allowed_tools:
            return True
        return tool_name in self.allowed_tools

    def check_quota(self) -> bool:
        """Check if tenant is within quota."""
        now = datetime.utcnow()
        if (now - self.last_reset).total_seconds() > 1.0:
            self.request_count = 0
            self.last_reset = now

        if self.request_count >= self.quota_rps:
            return False

        self.request_count += 1
        return True

    def get_quota_remaining(self) -> int:
        """Get remaining requests in current window."""
        now = datetime.utcnow()
        if (now - self.last_reset).total_seconds() > 1.0:
            self.request_count = 0
            self.last_reset = now

        return max(0, self.quota_rps - self.request_count)


class TenantManager:
    """Manages multi-tenant configuration and quotas."""

    def __init__(self):
        self.tenants: Dict[str, Tenant] = {}
        self.api_key_to_tenant: Dict[str, str] = {}

    def add_tenant(
        self,
        tenant_id: str,
        name: str,
        quota_rps: int = 100,
        allowed_tools: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Add a new tenant.

        Returns:
            API key for the tenant
        """
        if tenant_id in self.tenants:
            raise ValueError(f"Tenant {tenant_id} already exists")

        if api_key is None:
            api_key = f"sk-{secrets.token_hex(24)}"

        tenant = Tenant(
            id=tenant_id,
            api_key=api_key,
            name=name,
            description=description,
            quota_rps=quota_rps,
            allowed_tools=set(allowed_tools) if allowed_tools else {"*"},
            metadata=metadata or {},
        )

        self.tenants[tenant_id] = tenant
        self.api_key_to_tenant[api_key] = tenant_id

        return api_key

    def remove_tenant(self, tenant_id: str) -> bool:
        """Remove a tenant."""
        if tenant_id not in self.tenants:
            return False

        tenant = self.tenants[tenant_id]
        del self.api_key_to_tenant[tenant.api_key]
        del self.tenants[tenant_id]
        return True

    def get_tenant_by_api_key(self, api_key: str) -> Optional[Tenant]:
        """Get tenant by API key."""
        tenant_id = self.api_key_to_tenant.get(api_key)
        if tenant_id:
            return self.tenants.get(tenant_id)
        return None

    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID."""
        return self.tenants.get(tenant_id)

    def validate_request(
        self,
        api_key: str,
        tool_name: str,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate if request is allowed.

        Returns:
            (allowed, error_message)
        """
        tenant = self.get_tenant_by_api_key(api_key)

        if not tenant:
            return False, "Invalid API key"

        if not tenant.enabled:
            return False, "Tenant is disabled"

        if not tenant.can_call_tool(tool_name):
            return False, f"Tenant not allowed to call {tool_name}"

        if not tenant.check_quota():
            return False, f"Quota exceeded ({tenant.quota_rps} req/s)"

        return True, None

    def get_tenant_status(self, tenant_id: str) -> Optional[Dict]:
        """Get tenant status and quota info."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return None

        return {
            "id": tenant.id,
            "name": tenant.name,
            "enabled": tenant.enabled,
            "quota_rps": tenant.quota_rps,
            "quota_remaining": tenant.get_quota_remaining(),
            "allowed_tools": list(tenant.allowed_tools),
            "created_at": tenant.created_at.isoformat(),
            "metadata": tenant.metadata,
        }

    def list_tenants(self) -> List[Dict]:
        """List all tenants."""
        return [self.get_tenant_status(tid) for tid in self.tenants.keys()]

    def update_tenant_quota(self, tenant_id: str, quota_rps: int) -> bool:
        """Update tenant quota."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        tenant.quota_rps = quota_rps
        return True

    def update_tenant_tools(self, tenant_id: str, allowed_tools: List[str]) -> bool:
        """Update allowed tools for tenant."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        tenant.allowed_tools = set(allowed_tools)
        return True

    def disable_tenant(self, tenant_id: str) -> bool:
        """Disable a tenant."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        tenant.enabled = False
        return True

    def enable_tenant(self, tenant_id: str) -> bool:
        """Enable a tenant."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        tenant.enabled = True
        return True

    def get_metrics(self) -> Dict:
        """Get multi-tenancy metrics."""
        return {
            "total_tenants": len(self.tenants),
            "enabled_tenants": sum(1 for t in self.tenants.values() if t.enabled),
            "total_quota_rps": sum(t.quota_rps for t in self.tenants.values()),
            "tenants": [
                {
                    "id": t.id,
                    "name": t.name,
                    "quota_rps": t.quota_rps,
                    "quota_remaining": t.get_quota_remaining(),
                    "enabled": t.enabled,
                }
                for t in self.tenants.values()
            ],
        }
