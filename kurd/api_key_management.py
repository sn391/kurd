"""
API Key Management for Kurd

Generates, rotates, and manages API keys with expiration and audit trail.

Usage:
    from kurd.api_key_management import APIKeyManager

    keys = APIKeyManager()

    # Generate key for tenant
    key = keys.generate_key("customer-1", name="Production Key")

    # Validate key
    valid, tenant_id = keys.validate_key(key)

    # Rotate key
    new_key = keys.rotate_key(old_key)

    # Revoke key
    keys.revoke_key(key)
"""

import secrets
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass, field


@dataclass
class APIKey:
    """Represents an API key."""

    key_id: str
    tenant_id: str
    name: str
    created_at: datetime
    last_used: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    key_hash: str = ""  # SHA-256 of actual key
    permissions: list = field(default_factory=lambda: ["tools:call"])
    metadata: Dict = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        """Check if key is valid."""
        if self.revoked_at:
            return False

        now = datetime.utcnow()

        if self.expires_at and now > self.expires_at:
            return False

        return True

    @property
    def is_expired(self) -> bool:
        """Check if key is expired."""
        if not self.expires_at:
            return False

        return datetime.utcnow() > self.expires_at


class APIKeyManager:
    """Manages API keys for authentication."""

    def __init__(self, key_expiry_days: int = 365):
        """
        Initialize API key manager.

        Args:
            key_expiry_days: Default expiration in days (0 = no expiry)
        """
        self.key_expiry_days = key_expiry_days
        self.keys: Dict[str, APIKey] = {}
        self.key_hashes: Dict[str, str] = {}  # hash -> key_id
        self.audit_log: List[Dict] = []

    def generate_key(
        self,
        tenant_id: str,
        name: str = "API Key",
        expiry_days: Optional[int] = None,
        permissions: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
    ) -> str:
        """
        Generate a new API key.

        Args:
            tenant_id: Tenant ID
            name: Friendly name for the key
            expiry_days: Days until expiration (None = use default)
            permissions: List of permissions
            metadata: Custom metadata

        Returns:
            The generated API key (full key, only shown once)
        """
        # Generate random key
        key = f"kurd_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(key.encode()).hexdigest()

        # Create key object
        key_id = f"key_{secrets.token_hex(8)}"
        now = datetime.utcnow()

        if expiry_days is None:
            expiry_days = self.key_expiry_days

        expires_at = now + timedelta(days=expiry_days) if expiry_days > 0 else None

        api_key = APIKey(
            key_id=key_id,
            tenant_id=tenant_id,
            name=name,
            created_at=now,
            expires_at=expires_at,
            key_hash=key_hash,
            permissions=permissions or ["tools:call"],
            metadata=metadata or {},
        )

        self.keys[key_id] = api_key
        self.key_hashes[key_hash] = key_id

        # Audit log
        self._log_event("key_generated", {
            "key_id": key_id,
            "tenant_id": tenant_id,
            "name": name,
        })

        return key

    def validate_key(self, key: str) -> Tuple[bool, Optional[str]]:
        """
        Validate an API key.

        Args:
            key: The API key to validate

        Returns:
            (is_valid, tenant_id or error_message)
        """
        key_hash = hashlib.sha256(key.encode()).hexdigest()

        if key_hash not in self.key_hashes:
            self._log_event("key_validation_failed", {"reason": "not_found"})
            return False, "Invalid API key"

        key_id = self.key_hashes[key_hash]
        api_key = self.keys[key_id]

        if not api_key.is_valid:
            self._log_event("key_validation_failed", {
                "key_id": key_id,
                "reason": "revoked" if api_key.revoked_at else "expired",
            })
            return False, "API key is revoked or expired"

        # Update last used
        api_key.last_used = datetime.utcnow()

        self._log_event("key_validated", {"key_id": key_id})
        return True, api_key.tenant_id

    def rotate_key(self, old_key: str, name: Optional[str] = None) -> Optional[str]:
        """
        Rotate an API key (revoke old, generate new).

        Args:
            old_key: The key to rotate
            name: Name for new key

        Returns:
            New API key, or None if rotation failed
        """
        old_hash = hashlib.sha256(old_key.encode()).hexdigest()

        if old_hash not in self.key_hashes:
            return None

        key_id = self.key_hashes[old_hash]
        old_api_key = self.keys[key_id]

        if not old_api_key.is_valid:
            return None

        # Generate new key
        new_key = self.generate_key(
            tenant_id=old_api_key.tenant_id,
            name=name or f"{old_api_key.name} (rotated)",
            expiry_days=self.key_expiry_days,
            permissions=old_api_key.permissions,
            metadata=old_api_key.metadata,
        )

        # Revoke old key
        self.revoke_key(old_key)

        self._log_event("key_rotated", {
            "old_key_id": key_id,
            "tenant_id": old_api_key.tenant_id,
        })

        return new_key

    def revoke_key(self, key: str) -> bool:
        """
        Revoke an API key.

        Args:
            key: The key to revoke

        Returns:
            True if revoked, False if key not found
        """
        key_hash = hashlib.sha256(key.encode()).hexdigest()

        if key_hash not in self.key_hashes:
            return False

        key_id = self.key_hashes[key_hash]
        api_key = self.keys[key_id]

        api_key.revoked_at = datetime.utcnow()

        self._log_event("key_revoked", {
            "key_id": key_id,
            "tenant_id": api_key.tenant_id,
        })

        return True

    def get_tenant_keys(self, tenant_id: str) -> List[Dict]:
        """Get all keys for a tenant (without secret)."""
        keys = []

        for api_key in self.keys.values():
            if api_key.tenant_id == tenant_id:
                keys.append({
                    "key_id": api_key.key_id,
                    "name": api_key.name,
                    "created_at": api_key.created_at.isoformat(),
                    "last_used": api_key.last_used.isoformat() if api_key.last_used else None,
                    "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None,
                    "valid": api_key.is_valid,
                    "permissions": api_key.permissions,
                })

        return keys

    def get_key_info(self, key: str) -> Optional[Dict]:
        """Get information about a key (without secret)."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()

        if key_hash not in self.key_hashes:
            return None

        key_id = self.key_hashes[key_hash]
        api_key = self.keys[key_id]

        return {
            "key_id": api_key.key_id,
            "tenant_id": api_key.tenant_id,
            "name": api_key.name,
            "created_at": api_key.created_at.isoformat(),
            "last_used": api_key.last_used.isoformat() if api_key.last_used else None,
            "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None,
            "valid": api_key.is_valid,
            "permissions": api_key.permissions,
        }

    def set_key_permissions(self, key: str, permissions: List[str]) -> bool:
        """Update permissions for a key."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()

        if key_hash not in self.key_hashes:
            return False

        key_id = self.key_hashes[key_hash]
        self.keys[key_id].permissions = permissions

        self._log_event("key_permissions_updated", {
            "key_id": key_id,
            "permissions": permissions,
        })

        return True

    def get_audit_log(self, tenant_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get audit log."""
        log = self.audit_log

        if tenant_id:
            log = [e for e in log if e.get("tenant_id") == tenant_id]

        return log[-limit:]

    def _log_event(self, event_type: str, details: Dict) -> None:
        """Log an event."""
        self.audit_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "details": details,
        })

    def cleanup_expired_keys(self) -> int:
        """Remove expired keys. Returns count removed."""
        now = datetime.utcnow()
        count = 0

        keys_to_remove = []
        for key_id, api_key in self.keys.items():
            if api_key.expires_at and now > api_key.expires_at and api_key.revoked_at:
                keys_to_remove.append((key_id, api_key.key_hash))
                count += 1

        for key_id, key_hash in keys_to_remove:
            del self.keys[key_id]
            del self.key_hashes[key_hash]

        return count

    def to_json(self) -> Dict:
        """Export state as JSON."""
        return {
            "key_count": len(self.keys),
            "active_keys": sum(1 for k in self.keys.values() if k.is_valid),
            "expired_keys": sum(1 for k in self.keys.values() if k.is_expired),
            "revoked_keys": sum(1 for k in self.keys.values() if k.revoked_at),
            "audit_log_entries": len(self.audit_log),
        }
