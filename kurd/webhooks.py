"""
Webhook Notifications for Kurd

Event-driven webhook system for external integrations.

Usage:
    from kurd.webhooks import WebhookManager, WebhookEvent

    webhooks = WebhookManager()

    # Register webhook
    webhooks.register_webhook(
        url="https://example.com/webhooks",
        events=["error", "dlq_replay", "rate_limit"],
        tenant_id="customer-1"
    )

    # Trigger event
    webhooks.trigger_event(
        event_type="error",
        tenant_id="customer-1",
        data={"tool": "add", "error": "timeout"}
    )

    # List webhook deliveries
    deliveries = webhooks.get_deliveries("webhook-id", limit=100)
"""

import json
import sqlite3
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Any, Callable
from enum import Enum
import threading


class EventType(Enum):
    """Supported webhook events."""

    ERROR = "error"
    DLQ_MESSAGE_ADDED = "dlq_message_added"
    DLQ_REPLAY_SUCCESS = "dlq_replay_success"
    DLQ_REPLAY_FAILED = "dlq_replay_failed"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    HEALTH_CHECK_FAILED = "health_check_failed"
    REQUEST_TIMEOUT = "request_timeout"
    AUTHORIZATION_FAILED = "authorization_failed"
    IDEMPOTENT_DUPLICATE = "idempotent_duplicate"


class WebhookDeliveryStatus(Enum):
    """Status of webhook delivery."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


class WebhookManager:
    """Manages webhook registrations and event delivery."""

    def __init__(
        self,
        storage_path: str = "/var/lib/kurd/webhooks",
        max_retries: int = 5,
        retry_delay_seconds: int = 60,
    ):
        """
        Initialize webhook manager.

        Args:
            storage_path: Directory for webhook storage
            max_retries: Max delivery attempts
            retry_delay_seconds: Initial retry delay
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_path / "webhooks.db"
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.delivery_handlers: Dict[str, Callable] = {}
        self._event_queue: List[Dict] = []
        self._queue_lock = threading.Lock()

        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS webhooks (
                    webhook_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    events TEXT NOT NULL,
                    secret TEXT NOT NULL,
                    active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_triggered TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS webhook_deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    webhook_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT,
                    status TEXT DEFAULT 'pending',
                    attempt INTEGER DEFAULT 1,
                    http_status INTEGER,
                    response_body TEXT,
                    next_retry_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    delivered_at TIMESTAMP,
                    FOREIGN KEY(webhook_id) REFERENCES webhooks(webhook_id)
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tenant ON webhooks(tenant_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_webhook_deliveries ON webhook_deliveries(webhook_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_delivery_status ON webhook_deliveries(status)
            """)

            conn.commit()

    def register_webhook(
        self,
        url: str,
        events: List[str],
        tenant_id: str,
        active: bool = True,
    ) -> str:
        """
        Register a webhook for events.

        Args:
            url: Webhook URL to POST events to
            events: List of event types to subscribe to
            tenant_id: Tenant ID
            active: Whether webhook is enabled

        Returns:
            Webhook ID
        """
        webhook_id = f"wh_{secrets.token_hex(12)}"
        secret = secrets.token_urlsafe(32)
        events_json = json.dumps(events)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO webhooks
                (webhook_id, tenant_id, url, events, secret, active)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (webhook_id, tenant_id, url, events_json, secret, active),
            )
            conn.commit()

        return webhook_id

    def trigger_event(
        self,
        event_type: str,
        tenant_id: str,
        data: Dict[str, Any],
        trace_id: Optional[str] = None,
    ) -> None:
        """
        Trigger a webhook event.

        Args:
            event_type: Type of event (error, dlq_replay, etc.)
            tenant_id: Tenant ID
            data: Event data
            trace_id: Optional trace ID for correlation
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT webhook_id, url, secret FROM webhooks
                WHERE tenant_id = ? AND active = 1 AND json_extract(events, '$') LIKE ?
                """,
                (tenant_id, f"%{event_type}%"),
            )

            webhooks = cursor.fetchall()

        for webhook_id, url, secret in webhooks:
            delivery_id = f"del_{secrets.token_hex(12)}"

            event_payload = {
                "event_type": event_type,
                "tenant_id": tenant_id,
                "timestamp": datetime.utcnow().isoformat(),
                "data": data,
                "trace_id": trace_id,
            }

            # Calculate signature (HMAC-SHA256)
            payload_json = json.dumps(event_payload)
            signature = hmac.new(
                secret.encode(),
                payload_json.encode(),
                hashlib.sha256,
            ).hexdigest()

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO webhook_deliveries
                    (delivery_id, webhook_id, event_type, event_data, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        delivery_id,
                        webhook_id,
                        event_type,
                        payload_json,
                        WebhookDeliveryStatus.PENDING.value,
                    ),
                )

                conn.execute(
                    "UPDATE webhooks SET last_triggered = ? WHERE webhook_id = ?",
                    (datetime.utcnow(), webhook_id),
                )

                conn.commit()

            # Queue for async delivery
            with self._queue_lock:
                self._event_queue.append({
                    "delivery_id": delivery_id,
                    "url": url,
                    "payload": payload_json,
                    "signature": signature,
                    "webhook_id": webhook_id,
                })

    def get_pending_deliveries(self) -> List[Dict]:
        """Get pending webhook deliveries."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT delivery_id, webhook_id, event_type, event_data, attempt
                FROM webhook_deliveries
                WHERE status IN (?, ?)
                AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
                LIMIT 100
                """,
                (
                    WebhookDeliveryStatus.PENDING.value,
                    WebhookDeliveryStatus.RETRYING.value,
                ),
            )

            for row in cursor:
                results.append({
                    "delivery_id": row[0],
                    "webhook_id": row[1],
                    "event_type": row[2],
                    "event_data": json.loads(row[3]) if row[3] else {},
                    "attempt": row[4],
                })

        return results

    def mark_delivery_success(
        self,
        delivery_id: str,
        http_status: int,
        response_body: Optional[str] = None,
    ) -> None:
        """Mark webhook delivery as successful."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = ?, http_status = ?, response_body = ?, delivered_at = ?
                WHERE delivery_id = ?
                """,
                (
                    WebhookDeliveryStatus.DELIVERED.value,
                    http_status,
                    response_body,
                    datetime.utcnow(),
                    delivery_id,
                ),
            )
            conn.commit()

    def mark_delivery_failed(
        self,
        delivery_id: str,
        http_status: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """Mark webhook delivery as failed (may retry)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT attempt FROM webhook_deliveries WHERE delivery_id = ?",
                (delivery_id,),
            )

            row = cursor.fetchone()
            if not row:
                return

            attempt = row[0]

            if attempt >= self.max_retries:
                # Permanently failed
                conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET status = ?, http_status = ?, response_body = ?
                    WHERE delivery_id = ?
                    """,
                    (WebhookDeliveryStatus.FAILED.value, http_status, error, delivery_id),
                )
            else:
                # Schedule retry
                retry_delay = min(
                    self.retry_delay_seconds * (2 ** attempt),
                    3600,  # Max 1 hour
                )
                next_retry = datetime.utcnow() + timedelta(seconds=retry_delay)

                conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET status = ?, attempt = ?, http_status = ?, response_body = ?, next_retry_at = ?
                    WHERE delivery_id = ?
                    """,
                    (
                        WebhookDeliveryStatus.RETRYING.value,
                        attempt + 1,
                        http_status,
                        error,
                        next_retry,
                        delivery_id,
                    ),
                )

            conn.commit()

    def get_deliveries(
        self,
        webhook_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """Get webhook delivery history."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            if webhook_id:
                cursor = conn.execute(
                    """
                    SELECT delivery_id, event_type, status, attempt, created_at, delivered_at
                    FROM webhook_deliveries
                    WHERE webhook_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (webhook_id, limit),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT delivery_id, event_type, status, attempt, created_at, delivered_at
                    FROM webhook_deliveries
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                )

            for row in cursor:
                results.append({
                    "delivery_id": row[0],
                    "event_type": row[1],
                    "status": row[2],
                    "attempt": row[3],
                    "created_at": row[4],
                    "delivered_at": row[5],
                })

        return results

    def list_webhooks(self, tenant_id: str) -> List[Dict]:
        """List webhooks for a tenant."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT webhook_id, url, events, active, created_at, last_triggered
                FROM webhooks
                WHERE tenant_id = ?
                ORDER BY created_at DESC
                """,
                (tenant_id,),
            )

            for row in cursor:
                results.append({
                    "webhook_id": row[0],
                    "url": row[1],
                    "events": json.loads(row[2]),
                    "active": row[3],
                    "created_at": row[4],
                    "last_triggered": row[5],
                })

        return results

    def delete_webhook(self, webhook_id: str) -> bool:
        """Delete a webhook."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM webhooks WHERE webhook_id = ?",
                (webhook_id,),
            )
            conn.commit()

        return True

    def get_statistics(self, tenant_id: Optional[str] = None) -> Dict:
        """Get webhook statistics."""
        with sqlite3.connect(self.db_path) as conn:
            if tenant_id:
                where = "WHERE w.tenant_id = ?"
                params = (tenant_id,)
            else:
                where = ""
                params = ()

            cursor = conn.execute(
                f"SELECT COUNT(*) FROM webhooks w {where}",
                params,
            )
            total_webhooks = cursor.fetchone()[0]

            cursor = conn.execute(
                f"""
                SELECT COUNT(*) FROM webhook_deliveries wd
                JOIN webhooks w ON wd.webhook_id = w.webhook_id
                {where} AND wd.status = ?
                """,
                (*params, WebhookDeliveryStatus.DELIVERED.value),
            )
            successful = cursor.fetchone()[0]

            cursor = conn.execute(
                f"""
                SELECT COUNT(*) FROM webhook_deliveries wd
                JOIN webhooks w ON wd.webhook_id = w.webhook_id
                {where} AND wd.status = ?
                """,
                (*params, WebhookDeliveryStatus.FAILED.value),
            )
            failed = cursor.fetchone()[0]

        return {
            "total_webhooks": total_webhooks,
            "successful_deliveries": successful,
            "failed_deliveries": failed,
            "pending_in_queue": len(self._event_queue),
        }

    def to_json(self) -> Dict:
        """Export state as JSON."""
        stats = self.get_statistics()

        return {
            "statistics": stats,
            "storage_path": str(self.storage_path),
            "max_retries": self.max_retries,
            "queue_size": len(self._event_queue),
        }
