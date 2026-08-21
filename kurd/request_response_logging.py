"""
Request/Response Logging for Kurd

Logs all requests and responses for debugging, auditing, and compliance.

Usage:
    from kurd.request_response_logging import RequestResponseLogger

    logger = RequestResponseLogger(storage_path="/var/log/kurd")

    # Log request
    logger.log_request(
        request_id="req-123",
        tenant_id="customer-1",
        tool_name="add",
        arguments={"a": 1, "b": 2},
        headers={"user-agent": "..."},
    )

    # Log response
    logger.log_response(
        request_id="req-123",
        status_code=200,
        result={"value": 3},
        latency_ms=25.5,
    )

    # Query logs
    logs = logger.query_by_tenant("customer-1", limit=100)
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Any


class RequestResponseLogger:
    """Logs and stores request/response data."""

    def __init__(self, storage_path: str = "/var/log/kurd", retention_days: int = 90):
        """
        Initialize logger.

        Args:
            storage_path: Directory to store logs
            retention_days: Delete logs older than this
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self.db_path = self.storage_path / "requests.db"

        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    arguments TEXT,
                    headers TEXT,
                    client_ip TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    received_at TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS responses (
                    request_id TEXT PRIMARY KEY,
                    status_code INTEGER,
                    result TEXT,
                    error TEXT,
                    latency_ms FLOAT,
                    response_size_bytes INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(request_id) REFERENCES requests(request_id)
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tenant ON requests(tenant_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tool ON requests(tool_name)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON requests(timestamp)
            """)

            conn.commit()

    def log_request(
        self,
        request_id: str,
        tenant_id: str,
        tool_name: str,
        arguments: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        client_ip: Optional[str] = None,
    ) -> None:
        """Log an incoming request."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO requests
                (request_id, tenant_id, tool_name, arguments, headers, client_ip, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    tenant_id,
                    tool_name,
                    json.dumps(arguments) if arguments else None,
                    json.dumps(headers) if headers else None,
                    client_ip,
                    datetime.utcnow(),
                ),
            )
            conn.commit()

    def log_response(
        self,
        request_id: str,
        status_code: int,
        result: Optional[Any] = None,
        error: Optional[str] = None,
        latency_ms: float = 0,
        response_size_bytes: int = 0,
    ) -> None:
        """Log a response."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO responses
                (request_id, status_code, result, error, latency_ms, response_size_bytes)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    status_code,
                    json.dumps(result) if result else None,
                    error,
                    latency_ms,
                    response_size_bytes,
                ),
            )
            conn.commit()

    def query_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Get request and response by ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    r.request_id, r.tenant_id, r.tool_name, r.arguments,
                    r.headers, r.client_ip, r.received_at,
                    resp.status_code, resp.result, resp.error,
                    resp.latency_ms, resp.response_size_bytes, resp.timestamp
                FROM requests r
                LEFT JOIN responses resp ON r.request_id = resp.request_id
                WHERE r.request_id = ?
                """,
                (request_id,),
            )

            row = cursor.fetchone()
            if not row:
                return None

            return {
                "request_id": row[0],
                "tenant_id": row[1],
                "tool_name": row[2],
                "arguments": json.loads(row[3]) if row[3] else None,
                "headers": json.loads(row[4]) if row[4] else None,
                "client_ip": row[5],
                "received_at": row[6],
                "status_code": row[7],
                "result": json.loads(row[8]) if row[8] else None,
                "error": row[9],
                "latency_ms": row[10],
                "response_size_bytes": row[11],
                "response_timestamp": row[12],
            }

    def query_by_tenant(
        self,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict]:
        """Get requests for a tenant."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    r.request_id, r.tool_name, r.timestamp,
                    resp.status_code, resp.latency_ms, resp.error
                FROM requests r
                LEFT JOIN responses resp ON r.request_id = resp.request_id
                WHERE r.tenant_id = ?
                ORDER BY r.timestamp DESC
                LIMIT ? OFFSET ?
                """,
                (tenant_id, limit, offset),
            )

            for row in cursor:
                results.append({
                    "request_id": row[0],
                    "tool_name": row[1],
                    "timestamp": row[2],
                    "status_code": row[3],
                    "latency_ms": row[4],
                    "error": row[5],
                })

        return results

    def query_by_tool(
        self,
        tool_name: str,
        limit: int = 100,
    ) -> List[Dict]:
        """Get requests for a tool."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    r.request_id, r.tenant_id, r.timestamp,
                    resp.status_code, resp.latency_ms
                FROM requests r
                LEFT JOIN responses resp ON r.request_id = resp.request_id
                WHERE r.tool_name = ?
                ORDER BY r.timestamp DESC
                LIMIT ?
                """,
                (tool_name, limit),
            )

            for row in cursor:
                results.append({
                    "request_id": row[0],
                    "tenant_id": row[1],
                    "timestamp": row[2],
                    "status_code": row[3],
                    "latency_ms": row[4],
                })

        return results

    def get_error_logs(self, limit: int = 100) -> List[Dict]:
        """Get failed requests."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    r.request_id, r.tenant_id, r.tool_name, r.timestamp,
                    resp.status_code, resp.error, resp.latency_ms
                FROM requests r
                JOIN responses resp ON r.request_id = resp.request_id
                WHERE resp.status_code >= 400 OR resp.error IS NOT NULL
                ORDER BY r.timestamp DESC
                LIMIT ?
                """,
                (limit,),
            )

            for row in cursor:
                results.append({
                    "request_id": row[0],
                    "tenant_id": row[1],
                    "tool_name": row[2],
                    "timestamp": row[3],
                    "status_code": row[4],
                    "error": row[5],
                    "latency_ms": row[6],
                })

        return results

    def get_slow_requests(self, latency_threshold_ms: float = 1000, limit: int = 100) -> List[Dict]:
        """Get requests exceeding latency threshold."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    r.request_id, r.tenant_id, r.tool_name, r.timestamp,
                    resp.latency_ms
                FROM requests r
                JOIN responses resp ON r.request_id = resp.request_id
                WHERE resp.latency_ms > ?
                ORDER BY resp.latency_ms DESC
                LIMIT ?
                """,
                (latency_threshold_ms, limit),
            )

            for row in cursor:
                results.append({
                    "request_id": row[0],
                    "tenant_id": row[1],
                    "tool_name": row[2],
                    "timestamp": row[3],
                    "latency_ms": row[4],
                })

        return results

    def get_statistics(self, tenant_id: Optional[str] = None) -> Dict:
        """Get statistics on requests."""
        with sqlite3.connect(self.db_path) as conn:
            if tenant_id:
                where = "WHERE r.tenant_id = ?"
                params = (tenant_id,)
            else:
                where = ""
                params = ()

            # Total requests
            cursor = conn.execute(f"SELECT COUNT(*) FROM requests {where}", params)
            total_requests = cursor.fetchone()[0]

            # Success rate
            cursor = conn.execute(
                f"""
                SELECT COUNT(*) FROM responses
                WHERE (SELECT COUNT(*) FROM requests {where}) > 0
                AND status_code < 400
                """,
                params,
            )
            successful = cursor.fetchone()[0]

            # Average latency
            cursor = conn.execute(
                f"""
                SELECT AVG(latency_ms) FROM responses r
                JOIN requests req ON r.request_id = req.request_id
                {where}
                """,
                params,
            )
            avg_latency = cursor.fetchone()[0] or 0

            # Top tools
            cursor = conn.execute(
                f"""
                SELECT tool_name, COUNT(*) FROM requests
                {where}
                GROUP BY tool_name
                ORDER BY COUNT(*) DESC
                LIMIT 10
                """,
                params,
            )
            top_tools = dict(cursor.fetchall())

            return {
                "total_requests": total_requests,
                "successful_requests": successful,
                "success_rate": (successful / total_requests * 100) if total_requests > 0 else 0,
                "average_latency_ms": avg_latency,
                "top_tools": top_tools,
            }

    def cleanup_old_logs(self) -> int:
        """Delete logs older than retention period. Returns count deleted."""
        cutoff_date = datetime.utcnow() - timedelta(days=self.retention_days)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM responses
                WHERE request_id IN (
                    SELECT request_id FROM requests
                    WHERE timestamp < ?
                )
                """,
                (cutoff_date,),
            )

            cursor = conn.execute(
                "DELETE FROM requests WHERE timestamp < ?",
                (cutoff_date,),
            )

            count = cursor.rowcount
            conn.commit()

        return count

    def export_logs(
        self,
        tenant_id: Optional[str] = None,
        format: str = "jsonl",
    ) -> str:
        """Export logs as JSONL or CSV."""
        results = []

        with sqlite3.connect(self.db_path) as conn:
            if tenant_id:
                where = "WHERE r.tenant_id = ?"
                params = (tenant_id,)
            else:
                where = ""
                params = ()

            cursor = conn.execute(
                f"""
                SELECT
                    r.request_id, r.tenant_id, r.tool_name, r.arguments,
                    r.client_ip, r.received_at,
                    resp.status_code, resp.result, resp.error,
                    resp.latency_ms, resp.response_size_bytes
                FROM requests r
                LEFT JOIN responses resp ON r.request_id = resp.request_id
                {where}
                ORDER BY r.timestamp DESC
                """,
                params,
            )

            for row in cursor:
                record = {
                    "request_id": row[0],
                    "tenant_id": row[1],
                    "tool_name": row[2],
                    "arguments": json.loads(row[3]) if row[3] else None,
                    "client_ip": row[4],
                    "received_at": row[5],
                    "status_code": row[6],
                    "result": json.loads(row[7]) if row[7] else None,
                    "error": row[8],
                    "latency_ms": row[9],
                    "response_size_bytes": row[10],
                }
                results.append(record)

        if format == "jsonl":
            return "\n".join(json.dumps(r) for r in results)
        elif format == "csv":
            import csv
            from io import StringIO

            output = StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=results[0].keys() if results else [],
            )
            writer.writeheader()
            for record in results:
                writer.writerow(record)
            return output.getvalue()

        return ""

    def to_json(self) -> Dict:
        """Export state as JSON."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM requests")
            total_requests = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM responses")
            total_responses = cursor.fetchone()[0]

        return {
            "total_requests": total_requests,
            "total_responses": total_responses,
            "storage_path": str(self.storage_path),
            "retention_days": self.retention_days,
        }
