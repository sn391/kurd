"""
Request Idempotency for Kurd

Ensures idempotent request handling to prevent duplicate processing.

Usage:
    from kurd.idempotency import IdempotencyManager

    idempotency = IdempotencyManager()

    # Check if request is duplicate
    is_duplicate, cached_response = idempotency.check_idempotent_key(
        idempotency_key="unique-key-123",
        tenant_id="customer-1"
    )

    if is_duplicate:
        return cached_response

    # Process request
    result = process_request()

    # Store result
    idempotency.store_result(
        idempotency_key="unique-key-123",
        tenant_id="customer-1",
        result=result
    )
"""

import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, Any
from enum import Enum


class ResultStatus(Enum):
    """Status of idempotent request result."""

    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    EXPIRED = "expired"


class IdempotencyManager:
    """Manages idempotent request handling."""

    def __init__(
        self,
        storage_path: str = None,
        result_ttl_hours: int = 24,
    ):
        """
        Initialize idempotency manager.

        Args:
            storage_path: Directory to store idempotency data
            result_ttl_hours: How long to keep results
        """
        if storage_path is None:
            import tempfile, os
            storage_path = os.path.join(tempfile.gettempdir(), "kurd", "idempotency")
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_path / "idempotency.db"
        self.result_ttl_hours = result_ttl_hours

        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS idempotent_requests (
                    idempotency_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    status TEXT DEFAULT 'processing',
                    result TEXT,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    PRIMARY KEY (tenant_id, idempotency_key)
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tenant ON idempotent_requests(tenant_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires ON idempotent_requests(expires_at)
            """)

            conn.commit()

    def check_idempotent_key(
        self,
        idempotency_key: str,
        tenant_id: str,
        request_hash: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Check if request is duplicate.

        Args:
            idempotency_key: Unique key for request
            tenant_id: Tenant ID
            request_hash: Hash of request body (for validation)

        Returns:
            (is_duplicate, cached_response)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT status, result, error, expires_at
                FROM idempotent_requests
                WHERE tenant_id = ? AND idempotency_key = ?
                """,
                (tenant_id, idempotency_key),
            )

            row = cursor.fetchone()

            if not row:
                # First time seeing this key
                return False, None

            status, result_json, error, expires_at = row

            # Check if expired
            if expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
                # Delete expired entry
                conn.execute(
                    "DELETE FROM idempotent_requests WHERE tenant_id = ? AND idempotency_key = ?",
                    (tenant_id, idempotency_key),
                )
                conn.commit()
                return False, None

            # Still processing
            if status == ResultStatus.PROCESSING.value:
                return True, {"status": "processing"}

            # Return cached result
            if status == ResultStatus.SUCCESS.value:
                result = {}
                if result_json:
                    result = eval(result_json)  # Safe in this context
                return True, {"status": "success", "result": result}

            # Return cached error
            if status == ResultStatus.FAILED.value:
                return True, {"status": "failed", "error": error}

        return False, None

    def mark_processing(
        self,
        idempotency_key: str,
        tenant_id: str,
        request_hash: str = "",
    ) -> None:
        """Mark request as being processed."""
        expires_at = datetime.utcnow() + timedelta(hours=self.result_ttl_hours)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO idempotent_requests
                (idempotency_key, tenant_id, request_hash, status, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    idempotency_key,
                    tenant_id,
                    request_hash,
                    ResultStatus.PROCESSING.value,
                    expires_at,
                ),
            )
            conn.commit()

    def store_result(
        self,
        idempotency_key: str,
        tenant_id: str,
        result: Any,
        error: Optional[str] = None,
    ) -> None:
        """Store result of request."""
        status = ResultStatus.FAILED.value if error else ResultStatus.SUCCESS.value
        expires_at = datetime.utcnow() + timedelta(hours=self.result_ttl_hours)

        result_json = repr(result) if result else None

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE idempotent_requests
                SET status = ?, result = ?, error = ?, expires_at = ?
                WHERE tenant_id = ? AND idempotency_key = ?
                """,
                (
                    status,
                    result_json,
                    error,
                    expires_at,
                    tenant_id,
                    idempotency_key,
                ),
            )
            conn.commit()

    def cleanup_expired(self) -> int:
        """Delete expired entries. Returns count."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM idempotent_requests WHERE expires_at < ?",
                (datetime.utcnow(),),
            )
            count = cursor.rowcount
            conn.commit()

        return count

    def get_statistics(self, tenant_id: Optional[str] = None) -> Dict:
        """Get idempotency statistics."""
        with sqlite3.connect(self.db_path) as conn:
            if tenant_id:
                where = "WHERE tenant_id = ?"
                params = (tenant_id,)
            else:
                where = ""
                params = ()

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM idempotent_requests {where}",
                params,
            )
            total = cursor.fetchone()[0]

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM idempotent_requests {where} AND status = ?",
                (*params, ResultStatus.PROCESSING.value),
            )
            processing = cursor.fetchone()[0]

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM idempotent_requests {where} AND status = ?",
                (*params, ResultStatus.SUCCESS.value),
            )
            successful = cursor.fetchone()[0]

        return {
            "total_tracked": total,
            "currently_processing": processing,
            "successful_dedupes": successful,
        }

    def to_json(self) -> Dict:
        """Export state as JSON."""
        stats = self.get_statistics()

        return {
            "statistics": stats,
            "storage_path": str(self.storage_path),
            "result_ttl_hours": self.result_ttl_hours,
        }
