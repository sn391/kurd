"""
Audit Logging for Kurd

Tracks all tool calls with: who, what, when, result, latency.
Can export to: file, syslog, JSON endpoint.

Usage:
    from kurd import Router
    from kurd.audit_logging import AuditLogger

    logger = AuditLogger(output='file', path='/var/log/kurd-audit.jsonl')
    router = Router()
    router.set_audit_logger(logger)
"""

import json
import time
import logging.handlers
from datetime import datetime
from typing import Any, Optional, Dict
from pathlib import Path
import logging


class AuditLogEntry:
    """Single audit log entry."""

    def __init__(
        self,
        request_id: str,
        client_ip: Optional[str],
        tool_name: str,
        arguments: Dict[str, Any],
        result: Any,
        error: Optional[str],
        latency_ms: float,
        timestamp: datetime,
    ):
        self.request_id = request_id
        self.client_ip = client_ip
        self.tool_name = tool_name
        self.arguments = arguments
        self.result = result
        self.error = error
        self.latency_ms = latency_ms
        self.timestamp = timestamp

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "request_id": self.request_id,
            "client_ip": self.client_ip,
            "timestamp": self.timestamp.isoformat(),
            "tool_name": self.tool_name,
            "arguments": self.arguments,
            "result": self.result,
            "error": self.error,
            "latency_ms": self.latency_ms,
        }

    def to_json(self) -> str:
        """Convert to JSON line."""
        return json.dumps(self.to_dict())


class AuditLogger:
    """Audit logging for Kurd gateway."""

    def __init__(
        self,
        output: str = "stdout",
        path: Optional[str] = None,
        log_arguments: bool = True,
        log_result: bool = True,
        log_errors: bool = True,
    ):
        """
        Initialize audit logger.

        Args:
            output: 'stdout', 'file', 'syslog', or 'none'
            path: File path for 'file' output
            log_arguments: Include tool arguments in logs
            log_result: Include tool results in logs
            log_errors: Include errors in logs
        """
        self.output = output
        self.path = Path(path) if path else None
        self.log_arguments = log_arguments
        self.log_result = log_result
        self.log_errors = log_errors
        self.entries: list[AuditLogEntry] = []

        if output == "file" and not path:
            raise ValueError("path required for file output")

        if output == "file":
            self.path.parent.mkdir(parents=True, exist_ok=True)

        if output == "syslog":
            self.logger = logging.getLogger("kurd.audit")
            handler = logging.handlers.SysLogHandler()
            self.logger.addHandler(handler)

    def log(
        self,
        request_id: str,
        client_ip: Optional[str],
        tool_name: str,
        arguments: Dict[str, Any],
        result: Any,
        error: Optional[str],
        latency_ms: float,
    ) -> None:
        """Log a tool call."""
        entry = AuditLogEntry(
            request_id=request_id,
            client_ip=client_ip,
            tool_name=tool_name,
            arguments=arguments if self.log_arguments else {},
            result=result if self.log_result else None,
            error=error if self.log_errors else None,
            latency_ms=latency_ms,
            timestamp=datetime.utcnow(),
        )

        self.entries.append(entry)

        if self.output == "stdout":
            print(entry.to_json())

        elif self.output == "file":
            with open(self.path, "a") as f:
                f.write(entry.to_json() + "\n")

        elif self.output == "syslog":
            self.logger.info(entry.to_json())

    def get_entries(
        self,
        tool_name: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query logged entries."""
        entries = self.entries

        if tool_name:
            entries = [e for e in entries if e.tool_name == tool_name]

        return [e.to_dict() for e in entries[-limit:]]

    def get_entry_count(self) -> int:
        """Get total audit log entries."""
        return len(self.entries)


# Integration helper for Router
def setup_audit_logging(router, output: str = "stdout", path: Optional[str] = None) -> AuditLogger:
    """
    Setup audit logging on a router.

    Example:
        from kurd import Router
        from kurd.audit_logging import setup_audit_logging

        router = Router()
        audit = setup_audit_logging(router, output='file', path='/var/log/kurd-audit.jsonl')
    """
    logger = AuditLogger(output=output, path=path)
    router._audit_logger = logger
    return logger
