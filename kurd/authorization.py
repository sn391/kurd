"""
Authorization & RBAC (Role-Based Access Control) for Kurd

Implements fine-grained access control per tenant with roles and permissions.

Usage:
    from kurd.authorization import AuthorizationManager, Role, Permission

    authz = AuthorizationManager()

    # Create roles
    admin_role = Role("admin", permissions=["tools:*", "config:*"])
    user_role = Role("user", permissions=["tools:read", "tools:call:add"])

    # Assign role to tenant
    authz.assign_role("tenant-1", admin_role)

    # Check permission
    allowed = authz.can_access("tenant-1", "tools/call", "add")
"""

from typing import Set, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class Permission(Enum):
    """Available permissions in Kurd."""

    # Tool management
    TOOLS_LIST = "tools:list"
    TOOLS_READ = "tools:read"
    TOOLS_CALL = "tools:call"
    TOOLS_CREATE = "tools:create"
    TOOLS_UPDATE = "tools:update"
    TOOLS_DELETE = "tools:delete"

    # Configuration
    CONFIG_READ = "config:read"
    CONFIG_UPDATE = "config:update"
    CONFIG_DELETE = "config:delete"

    # Tenant management
    TENANT_READ = "tenant:read"
    TENANT_UPDATE = "tenant:update"
    TENANT_DELETE = "tenant:delete"

    # Audit & monitoring
    AUDIT_READ = "audit:read"
    METRICS_READ = "metrics:read"

    # Admin
    ADMIN_ALL = "admin:*"


@dataclass
class Role:
    """Represents a role with associated permissions."""

    id: str
    name: str
    description: Optional[str] = None
    permissions: Set[str] = field(default_factory=set)

    def has_permission(self, permission: str) -> bool:
        """Check if role has permission (supports wildcards)."""
        if "admin:*" in self.permissions:
            return True

        if permission in self.permissions:
            return True

        # Check wildcard patterns
        parts = permission.split(":")
        for i in range(len(parts)):
            wildcard = ":".join(parts[:i+1]) + ":*"
            if wildcard in self.permissions:
                return True

        return False


@dataclass
class TenantAcl:
    """Access Control List for a tenant."""

    tenant_id: str
    roles: List[str] = field(default_factory=list)  # Role IDs
    direct_permissions: Set[str] = field(default_factory=set)
    tool_whitelist: Optional[Set[str]] = None  # None = all tools allowed
    tool_blacklist: Set[str] = field(default_factory=set)
    resource_quotas: Dict[str, int] = field(default_factory=dict)


class AuthorizationManager:
    """Manages authorization, roles, and permissions."""

    def __init__(self):
        self.roles: Dict[str, Role] = {}
        self.tenant_acls: Dict[str, TenantAcl] = {}
        self._init_default_roles()

    def _init_default_roles(self) -> None:
        """Initialize default roles."""
        self.create_role(
            Role(
                id="admin",
                name="Administrator",
                description="Full access",
                permissions={"admin:*"},
            )
        )

        self.create_role(
            Role(
                id="user",
                name="User",
                description="Read and call tools",
                permissions={"tools:list", "tools:read", "tools:call"},
            )
        )

        self.create_role(
            Role(
                id="viewer",
                name="Viewer",
                description="Read-only access",
                permissions={"tools:list", "tools:read", "metrics:read", "audit:read"},
            )
        )

    def create_role(self, role: Role) -> None:
        """Create a new role."""
        self.roles[role.id] = role

    def get_role(self, role_id: str) -> Optional[Role]:
        """Get a role by ID."""
        return self.roles.get(role_id)

    def assign_role(self, tenant_id: str, role: Role | str) -> None:
        """Assign a role to a tenant."""
        if tenant_id not in self.tenant_acls:
            self.tenant_acls[tenant_id] = TenantAcl(tenant_id=tenant_id)

        role_id = role.id if isinstance(role, Role) else role
        if role_id not in self.tenant_acls[tenant_id].roles:
            self.tenant_acls[tenant_id].roles.append(role_id)

    def revoke_role(self, tenant_id: str, role_id: str) -> bool:
        """Revoke a role from a tenant."""
        if tenant_id in self.tenant_acls:
            try:
                self.tenant_acls[tenant_id].roles.remove(role_id)
                return True
            except ValueError:
                pass
        return False

    def grant_permission(self, tenant_id: str, permission: str) -> None:
        """Grant a direct permission to a tenant."""
        if tenant_id not in self.tenant_acls:
            self.tenant_acls[tenant_id] = TenantAcl(tenant_id=tenant_id)

        self.tenant_acls[tenant_id].direct_permissions.add(permission)

    def revoke_permission(self, tenant_id: str, permission: str) -> None:
        """Revoke a direct permission from a tenant."""
        if tenant_id in self.tenant_acls:
            self.tenant_acls[tenant_id].direct_permissions.discard(permission)

    def can_access(
        self,
        tenant_id: str,
        action: str,
        resource: Optional[str] = None,
    ) -> bool:
        """
        Check if tenant can access a resource.

        Args:
            tenant_id: Tenant ID
            action: Action (e.g., "tools/call", "config/update")
            resource: Resource name (e.g., "add", "multiply")

        Returns:
            True if access allowed
        """
        if tenant_id not in self.tenant_acls:
            return False

        acl = self.tenant_acls[tenant_id]
        permission = f"{action.replace('/', ':')}"

        # Check direct permissions
        if permission in acl.direct_permissions:
            return True

        # Check role permissions
        for role_id in acl.roles:
            role = self.get_role(role_id)
            if role and role.has_permission(permission):
                # Check resource whitelist/blacklist
                if resource:
                    if acl.tool_whitelist and resource not in acl.tool_whitelist:
                        return False
                    if resource in acl.tool_blacklist:
                        return False

                return True

        return False

    def set_tool_whitelist(self, tenant_id: str, tools: Optional[Set[str]]) -> None:
        """Set whitelist of tools tenant can access."""
        if tenant_id not in self.tenant_acls:
            self.tenant_acls[tenant_id] = TenantAcl(tenant_id=tenant_id)

        self.tenant_acls[tenant_id].tool_whitelist = tools

    def set_tool_blacklist(self, tenant_id: str, tools: Set[str]) -> None:
        """Set blacklist of tools tenant cannot access."""
        if tenant_id not in self.tenant_acls:
            self.tenant_acls[tenant_id] = TenantAcl(tenant_id=tenant_id)

        self.tenant_acls[tenant_id].tool_blacklist = tools

    def get_tenant_permissions(self, tenant_id: str) -> Set[str]:
        """Get all permissions for a tenant."""
        if tenant_id not in self.tenant_acls:
            return set()

        acl = self.tenant_acls[tenant_id]
        all_permissions = acl.direct_permissions.copy()

        for role_id in acl.roles:
            role = self.get_role(role_id)
            if role:
                all_permissions.update(role.permissions)

        return all_permissions

    def get_tenant_acl(self, tenant_id: str) -> Optional[TenantAcl]:
        """Get ACL for a tenant."""
        return self.tenant_acls.get(tenant_id)

    def set_resource_quota(
        self,
        tenant_id: str,
        resource: str,
        limit: int,
    ) -> None:
        """Set resource quota for tenant."""
        if tenant_id not in self.tenant_acls:
            self.tenant_acls[tenant_id] = TenantAcl(tenant_id=tenant_id)

        self.tenant_acls[tenant_id].resource_quotas[resource] = limit

    def get_resource_quota(self, tenant_id: str, resource: str) -> Optional[int]:
        """Get resource quota for tenant."""
        if tenant_id in self.tenant_acls:
            return self.tenant_acls[tenant_id].resource_quotas.get(resource)
        return None

    def export_acls(self) -> Dict:
        """Export all ACLs for backup."""
        return {
            "roles": {rid: {
                "id": r.id,
                "name": r.name,
                "description": r.description,
                "permissions": list(r.permissions),
            } for rid, r in self.roles.items()},
            "tenant_acls": {tid: {
                "tenant_id": acl.tenant_id,
                "roles": acl.roles,
                "direct_permissions": list(acl.direct_permissions),
                "tool_whitelist": list(acl.tool_whitelist) if acl.tool_whitelist else None,
                "tool_blacklist": list(acl.tool_blacklist),
                "resource_quotas": acl.resource_quotas,
            } for tid, acl in self.tenant_acls.items()},
        }

    def import_acls(self, data: Dict) -> None:
        """Import ACLs from backup."""
        # Import roles
        for role_data in data.get("roles", {}).values():
            role = Role(
                id=role_data["id"],
                name=role_data["name"],
                description=role_data.get("description"),
                permissions=set(role_data.get("permissions", [])),
            )
            self.create_role(role)

        # Import tenant ACLs
        for acl_data in data.get("tenant_acls", {}).values():
            acl = TenantAcl(
                tenant_id=acl_data["tenant_id"],
                roles=acl_data.get("roles", []),
                direct_permissions=set(acl_data.get("direct_permissions", [])),
                tool_whitelist=set(acl_data["tool_whitelist"]) if acl_data.get("tool_whitelist") else None,
                tool_blacklist=set(acl_data.get("tool_blacklist", [])),
                resource_quotas=acl_data.get("resource_quotas", {}),
            )
            self.tenant_acls[acl_data["tenant_id"]] = acl
