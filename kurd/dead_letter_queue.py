"""
Dead Letter Queue (DLQ) for Kurd

Stores failed requests for later replay and analysis.

Usage:
    from kurd.dead_letter_queue import DeadLetterQueue

    dlq = DeadLetterQueue(storage_path="/var/lib/kurd/dlq")

    # Store failed request
    dlq.add_message(
        request_id="req-123",
        tenant_id="customer-1",
        tool_name="add",
        arguments={"a": 1, "b": 2},
        error="Timeout",
        retry_count=3
    )

    # Replay failed request
    dlq.replay_message("req-123")

    # Get failed messages
    failed = dlq.get_failed_messages("customer-1")
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Any, Callable
from enum import Enum


class MessageStatus(Enum):
    """Status of a DLQ message."""

    PENDING = "pending"
    RETRYING = "retrying"
    FAILED = "failed"
    SUCCESSFUL = "successful"
    ARCHIVED = "archived"


class DeadLetterQueue:
    """Manages dead letter queue for failed requests."""

    def __init__(self, storage_path: str = "/var/lib/kurd/dlq", max_retries: int = 5):
        """
        Initialize DLQ.

        Args:
            storage_path: Directory to store DLQ messages
            max_retries: Maximum retry attempts
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.db_path = self.storage_path / "dlq.db"
        self.replay_handlers: Dict[str, Callable] = {}

        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq_messages (
                    message_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    arguments TEXT NOT NULL,
                    error TEXT,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_attempted TIMESTAMP,
                    next_retry_at TIMESTAMP,
                    archived_at TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq_replay_history (
                    replay_id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL,
                    attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN,
                    error TEXT,
                    FOREIGN KEY(message_id) REFERENCES dlq_messages(message_id)
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tenant ON dlq_messages(tenant_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status ON dlq_messages(status)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_next_retry ON dlq_messages(next_retry_at)
            """)

            conn.commit()

    def add_message(
        self,
        request_id: str,
        tenant_id: str,
        tool_name: str,
        arguments: Dict[str, Any],
        error: str,
        retry_count: int = 0,
    ) -> str:
        """
        Add failed request to DLQ.

        Args:
            request_id: Request ID
            tenant_id: Tenant ID
            tool_name: Tool name
            arguments: Tool arguments
            error: Error message
            retry_count: Current retry count

        Returns:
            Message ID
        """
        import secrets

        message_id = f"dlq_{secrets.token_hex(8)}"
        now = datetime.utcnow()

        # Calculate next retry time (exponential backoff)
        retry_delay = min(2 ** retry_count * 60, 3600)  # Max 1 hour
        next_retry = now + timedelta(seconds=retry_delay)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO dlq_messages
                (message_id, request_id, tenant_id, tool_name, arguments, error, retry_count, next_retry_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    request_id,
                    tenant_id,
                    tool_name,
                    json.dumps(arguments),
                    error,
                    retry_count,
                    next_retry,
                ),
            )
            conn.commit()

        return message_id

    def register_replay_handler(self, tool_name: str, handler: Callable) -> None:
        """Register handler to replay tool calls."""
        self.replay_handlers[tool_name] = handler

    def replay_message(self, message_id: str) -> tuple[bool, Optional[str]]:
        """
        Attempt to replay a failed message.

        Args:
            message_id: DLQ message ID

        Returns:
            (success, error_message)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT tool_name, arguments, retry_count FROM dlq_messages WHERE message_id = ?",
                (message_id,),
            )
            row = cursor.fetchone()

            if not row:
                return False, "Message not found"

            tool_name, arguments_json, retry_count = row
            arguments = json.loads(arguments_json)

            # Check if tool has replay handler
            if tool_name not in self.replay_handlers:
                return False, f"No replay handler for tool: {tool_name}"

            # Check retry limit
            if retry_count >= self.max_retries:
                # Mark as failed permanently
                conn.execute(
                    "UPDATE dlq_messages SET status = ? WHERE message_id = ?",
                    (MessageStatus.FAILED.value, message_id),
                )
                conn.commit()
                return False, f"Max retries ({self.max_retries}) exceeded"

            try:
                # Execute replay
                handler = self.replay_handlers[tool_name]
                result = handler(**arguments)

                # Mark as successful
                conn.execute(
                    "UPDATE dlq_messages SET status = ?, last_attempted = ? WHERE message_id = ?",
                    (MessageStatus.SUCCESSFUL.value, datetime.utcnow(), message_id),
                )

                # Record replay
                import secrets
                replay_id = f"replay_{secrets.token_hex(8)}"
                conn.execute(
                    """
                    INSERT INTO dlq_replay_history
                    (replay_id, message_id, success, error)
                    VALUES (?, ?, ?, ?)
                    """,
                    (replay_id, message_id, True, None),
                )

                conn.commit()
                return True, None

            except Exception as e:
                # Increment retry and update next retry time
                new_retry_count = retry_count + 1
                retry_delay = min(2 ** new_retry_count * 60, 3600)
                next_retry = datetime.utcnow() + timedelta(seconds=retry_delay)

                conn.execute(
                    """
                    UPDATE dlq_messages
                    SET retry_count = ?, status = ?, last_attempted = ?, next_retry_at = ?
                    WHERE message_id = ?
                    """,
                    (new_retry_count, MessageStatus.RETRYING.value, datetime.utcnow(), next_retry, message_id),
                )

                # Record replay attempt
                import secrets
                replay_id = f"replay_{secrets.token_hex(8)}"
                conn.execute(
                    """
                    INSERT INTO dlq_replay_history
                    (replay_id, message_id, success, error)
                    VALUES (?, ?, ?, ?)
                    """,
                    (replay_id, message_id, False, str(e)),
                )

                conn.commit()
                return False, str(e)

    def get_failed_messages(
        self,
        tenant_id: str,
        limit: int = 100,
    ) -> List[Dict]:
        """Get failed messages for a tenant."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT message_id, request_id, tool_name, error, retry_count,
                       created_at, next_retry_at, status
                FROM dlq_messages
                WHERE tenant_id = ? AND status IN (?, ?)
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (
                    tenant_id,
                    MessageStatus.PENDING.value,
                    MessageStatus.RETRYING.value,
                    limit,
                ),
            )

            for row in cursor:
                results.append({
                    "message_id": row[0],
                    "request_id": row[1],
                    "tool_name": row[2],
                    "error": row[3],
                    "retry_count": row[4],
                    "created_at": row[5],
                    "next_retry_at": row[6],
                    "status": row[7],
                })

        return results

    def get_pending_replays(self) -> List[Dict]:
        """Get messages ready for replay (next_retry_at < now)."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT message_id, tenant_id, tool_name, retry_count
                FROM dlq_messages
                WHERE status IN (?, ?)
                AND next_retry_at <= datetime('now')
                AND retry_count < ?
                ORDER BY next_retry_at ASC
                """,
                (
                    MessageStatus.PENDING.value,
                    MessageStatus.RETRYING.value,
                    self.max_retries,
                ),
            )

            for row in cursor:
                results.append({
                    "message_id": row[0],
                    "tenant_id": row[1],
                    "tool_name": row[2],
                    "retry_count": row[3],
                })

        return results

    def get_statistics(self, tenant_id: Optional[str] = None) -> Dict:
        """Get DLQ statistics."""
        with sqlite3.connect(self.db_path) as conn:
            if tenant_id:
                where = "WHERE tenant_id = ?"
                status_kw = "AND"
                params = (tenant_id,)
            else:
                where = ""
                status_kw = "WHERE"
                params = ()

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM dlq_messages {where}",
                params,
            )
            total = cursor.fetchone()[0]

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM dlq_messages {where} {status_kw} status = ?",
                (*params, MessageStatus.PENDING.value),
            )
            pending = cursor.fetchone()[0]

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM dlq_messages {where} {status_kw} status = ?",
                (*params, MessageStatus.SUCCESSFUL.value),
            )
            successful = cursor.fetchone()[0]

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM dlq_messages {where} {status_kw} status = ?",
                (*params, MessageStatus.FAILED.value),
            )
            failed = cursor.fetchone()[0]

        return {
            "total_messages": total,
            "pending": pending,
            "successful_replays": successful,
            "permanently_failed": failed,
            "success_rate": (successful / (successful + failed) * 100) if (successful + failed) > 0 else 0,
        }

    def archive_message(self, message_id: str) -> bool:
        """Archive a processed message."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE dlq_messages SET status = ?, archived_at = ? WHERE message_id = ?",
                (MessageStatus.ARCHIVED.value, datetime.utcnow(), message_id),
            )
            conn.commit()

        return True

    def cleanup_archived(self, days: int = 30) -> int:
        """Delete archived messages older than N days. Returns count."""
        cutoff = datetime.utcnow() - timedelta(days=days)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM dlq_messages WHERE status = ? AND archived_at < ?",
                (MessageStatus.ARCHIVED.value, cutoff),
            )
            count = cursor.rowcount
            conn.commit()

        return count

    def to_json(self) -> Dict:
        """Export state as JSON."""
        stats = self.get_statistics()
        pending = self.get_pending_replays()

        return {
            "statistics": stats,
            "pending_replays": len(pending),
            "storage_path": str(self.storage_path),
            "max_retries": self.max_retries,
        }
