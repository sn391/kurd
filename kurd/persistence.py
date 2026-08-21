"""
Persistent Configuration Storage for Kurd

Stores gateway configuration, tenants, and state in SQLite for crash recovery.

Usage:
    from kurd.persistence import PersistenceManager

    storage = PersistenceManager(db_path="/var/lib/kurd/config.db")

    # Save configuration
    storage.save_gateway_config({
        "global_concurrency": 512,
        "rate_limiting_enabled": True,
    })

    # Recover on restart
    config = storage.load_gateway_config()
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any, List


class PersistenceManager:
    """Manages persistent storage of Kurd configuration and state."""

    def __init__(self, db_path: str = "/var/lib/kurd/config.db"):
        """
        Initialize persistence manager.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gateway_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tenants (
                    tenant_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tools (
                    tool_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    schema TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS upstreams (
                    upstream_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    config TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS authorization (
                    tenant_id TEXT PRIMARY KEY,
                    roles TEXT NOT NULL,
                    permissions TEXT NOT NULL,
                    tool_whitelist TEXT,
                    tool_blacklist TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS backups (
                    backup_id TEXT PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data TEXT NOT NULL,
                    status TEXT DEFAULT 'completed'
                )
            """)

            conn.commit()

    def save_gateway_config(self, config: Dict[str, Any]) -> None:
        """Save gateway configuration."""
        with sqlite3.connect(self.db_path) as conn:
            for key, value in config.items():
                value_str = json.dumps(value) if not isinstance(value, str) else value
                conn.execute(
                    "INSERT OR REPLACE INTO gateway_config (key, value) VALUES (?, ?)",
                    (key, value_str),
                )
            conn.commit()

    def load_gateway_config(self) -> Dict[str, Any]:
        """Load gateway configuration."""
        config = {}
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT key, value FROM gateway_config")
            for key, value in cursor:
                try:
                    config[key] = json.loads(value)
                except json.JSONDecodeError:
                    config[key] = value
        return config

    def save_tenant(self, tenant_id: str, tenant_config: Dict[str, Any]) -> None:
        """Save tenant configuration."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tenants (tenant_id, name, config, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    tenant_id,
                    tenant_config.get("name", tenant_id),
                    json.dumps(tenant_config),
                ),
            )
            conn.commit()

    def load_tenant(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Load tenant configuration."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT config FROM tenants WHERE tenant_id = ?",
                (tenant_id,),
            )
            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
        return None

    def load_all_tenants(self) -> List[Dict[str, Any]]:
        """Load all tenant configurations."""
        tenants = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM tenants")
            for row in cursor:
                tenants.append(json.loads(row[0]))
        return tenants

    def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant configuration."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM tenants WHERE tenant_id = ?",
                (tenant_id,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def save_tool(
        self,
        tool_id: str,
        name: str,
        description: str,
        schema: Dict[str, Any],
    ) -> None:
        """Save tool registration."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tools (tool_id, name, description, schema, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (tool_id, name, description, json.dumps(schema)),
            )
            conn.commit()

    def load_tool(self, tool_id: str) -> Optional[Dict[str, Any]]:
        """Load tool registration."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT name, description, schema FROM tools WHERE tool_id = ?",
                (tool_id,),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "tool_id": tool_id,
                    "name": row[0],
                    "description": row[1],
                    "schema": json.loads(row[2]),
                }
        return None

    def load_all_tools(self) -> List[Dict[str, Any]]:
        """Load all tool registrations."""
        tools = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT tool_id, name, description, schema FROM tools")
            for row in cursor:
                tools.append({
                    "tool_id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "schema": json.loads(row[3]),
                })
        return tools

    def save_upstream(
        self,
        upstream_id: str,
        name: str,
        url: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Save upstream MCP server configuration."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO upstreams (upstream_id, name, url, config, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (upstream_id, name, url, json.dumps(config or {})),
            )
            conn.commit()

    def load_upstream(self, upstream_id: str) -> Optional[Dict[str, Any]]:
        """Load upstream configuration."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT name, url, config FROM upstreams WHERE upstream_id = ?",
                (upstream_id,),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "upstream_id": upstream_id,
                    "name": row[0],
                    "url": row[1],
                    "config": json.loads(row[2]),
                }
        return None

    def load_all_upstreams(self) -> List[Dict[str, Any]]:
        """Load all upstream configurations."""
        upstreams = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT upstream_id, name, url, config FROM upstreams")
            for row in cursor:
                upstreams.append({
                    "upstream_id": row[0],
                    "name": row[1],
                    "url": row[2],
                    "config": json.loads(row[3]),
                })
        return upstreams

    def save_authorization(self, authz_data: Dict[str, Any]) -> None:
        """Save authorization/RBAC configuration."""
        with sqlite3.connect(self.db_path) as conn:
            for tenant_id, perms in authz_data.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO authorization
                    (tenant_id, roles, permissions, tool_whitelist, tool_blacklist, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        tenant_id,
                        json.dumps(perms.get("roles", [])),
                        json.dumps(perms.get("permissions", [])),
                        json.dumps(perms.get("tool_whitelist")) if perms.get("tool_whitelist") else None,
                        json.dumps(perms.get("tool_blacklist", [])),
                    ),
                )
            conn.commit()

    def load_authorization(self) -> Dict[str, Any]:
        """Load all authorization configurations."""
        authz = {}
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT tenant_id, roles, permissions, tool_whitelist, tool_blacklist FROM authorization"
            )
            for row in cursor:
                authz[row[0]] = {
                    "roles": json.loads(row[1]),
                    "permissions": json.loads(row[2]),
                    "tool_whitelist": json.loads(row[3]) if row[3] else None,
                    "tool_blacklist": json.loads(row[4]),
                }
        return authz

    def backup(self, backup_id: str) -> bool:
        """Create a full backup."""
        try:
            backup_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "gateway_config": self.load_gateway_config(),
                "tenants": self.load_all_tenants(),
                "tools": self.load_all_tools(),
                "upstreams": self.load_all_upstreams(),
                "authorization": self.load_authorization(),
            }

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO backups (backup_id, data, status)
                    VALUES (?, ?, 'completed')
                    """,
                    (backup_id, json.dumps(backup_data)),
                )
                conn.commit()
            return True
        except Exception as e:
            print(f"Backup failed: {e}")
            return False

    def restore(self, backup_id: str) -> bool:
        """Restore from a backup."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT data FROM backups WHERE backup_id = ?",
                    (backup_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return False

                backup_data = json.loads(row[0])

                # Restore gateway config
                self.save_gateway_config(backup_data["gateway_config"])

                # Restore tenants
                for tenant in backup_data["tenants"]:
                    self.save_tenant(tenant["tenant_id"], tenant)

                # Restore tools
                for tool in backup_data["tools"]:
                    self.save_tool(
                        tool["tool_id"],
                        tool["name"],
                        tool["description"],
                        tool["schema"],
                    )

                # Restore upstreams
                for upstream in backup_data["upstreams"]:
                    self.save_upstream(
                        upstream["upstream_id"],
                        upstream["name"],
                        upstream["url"],
                        upstream["config"],
                    )

                # Restore authorization
                self.save_authorization(backup_data["authorization"])

            return True
        except Exception as e:
            print(f"Restore failed: {e}")
            return False

    def list_backups(self) -> List[Dict[str, Any]]:
        """List available backups."""
        backups = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT backup_id, timestamp, status FROM backups ORDER BY timestamp DESC")
            for row in cursor:
                backups.append({
                    "backup_id": row[0],
                    "timestamp": row[1],
                    "status": row[2],
                })
        return backups
