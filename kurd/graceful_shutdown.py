"""
Graceful Shutdown for Kurd

Drains connections and finishes in-flight requests before shutting down.

Usage:
    from kurd.graceful_shutdown import GracefulShutdownManager

    shutdown = GracefulShutdownManager(timeout_seconds=30)

    # On SIGTERM/SIGINT
    shutdown.initiate_shutdown()

    # Wait for completion
    shutdown.wait_for_completion()
"""

import signal
import time
from datetime import datetime, timedelta
from typing import Optional, Callable, List
from dataclasses import dataclass


@dataclass
class ShutdownState:
    """Current shutdown state."""

    initiated: bool
    draining: bool
    active_requests: int
    completed_requests: int
    cancelled_requests: int
    start_time: Optional[datetime]
    deadline: Optional[datetime]


class GracefulShutdownManager:
    """Manages graceful shutdown with connection draining."""

    def __init__(self, timeout_seconds: int = 30):
        """
        Initialize graceful shutdown manager.

        Args:
            timeout_seconds: Maximum time to wait for connections to drain
        """
        self.timeout_seconds = timeout_seconds
        self.initiated = False
        self.draining = False
        self.start_time: Optional[datetime] = None
        self.deadline: Optional[datetime] = None

        self.active_requests = 0
        self.completed_requests = 0
        self.cancelled_requests = 0

        self.shutdown_callbacks: List[Callable] = []
        self.pre_shutdown_callbacks: List[Callable] = []

    def register_shutdown_callback(self, callback: Callable) -> None:
        """Register callback to run during shutdown."""
        self.shutdown_callbacks.append(callback)

    def register_pre_shutdown_callback(self, callback: Callable) -> None:
        """Register callback to run before shutdown starts."""
        self.pre_shutdown_callbacks.append(callback)

    def initiate_shutdown(self) -> None:
        """Begin graceful shutdown."""
        if self.initiated:
            return

        self.initiated = True
        self.draining = True
        self.start_time = datetime.utcnow()
        self.deadline = self.start_time + timedelta(seconds=self.timeout_seconds)

        print(f"[Shutdown] Initiating graceful shutdown (timeout: {self.timeout_seconds}s)")

        # Run pre-shutdown callbacks
        for callback in self.pre_shutdown_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"[Shutdown] Pre-shutdown callback error: {e}")

    def register_request(self) -> bool:
        """
        Register a new request.

        Returns:
            True if request accepted, False if shutdown draining
        """
        if self.draining:
            self.cancelled_requests += 1
            return False

        self.active_requests += 1
        return True

    def complete_request(self) -> None:
        """Mark request as completed."""
        if self.active_requests > 0:
            self.active_requests -= 1
        self.completed_requests += 1

    def cancel_request(self) -> None:
        """Mark request as cancelled."""
        if self.active_requests > 0:
            self.active_requests -= 1
        self.cancelled_requests += 1

    def stop_accepting_requests(self) -> None:
        """Stop accepting new requests (start draining)."""
        self.draining = True
        print("[Shutdown] Stopped accepting new requests")

    def wait_for_completion(self) -> bool:
        """
        Wait for all in-flight requests to complete.

        Returns:
            True if all completed, False if timeout
        """
        if not self.initiated:
            return True

        while True:
            if self.active_requests == 0:
                print(f"[Shutdown] All requests completed ({self.completed_requests} completed)")
                return True

            if datetime.utcnow() >= self.deadline:
                print(
                    f"[Shutdown] Timeout reached. "
                    f"Cancelling {self.active_requests} in-flight requests"
                )
                self.active_requests = 0
                return False

            # Wait a bit before checking again
            time.sleep(0.1)

    def get_state(self) -> ShutdownState:
        """Get current shutdown state."""
        return ShutdownState(
            initiated=self.initiated,
            draining=self.draining,
            active_requests=self.active_requests,
            completed_requests=self.completed_requests,
            cancelled_requests=self.cancelled_requests,
            start_time=self.start_time,
            deadline=self.deadline,
        )

    def finalize(self) -> None:
        """Perform final shutdown steps."""
        # Run shutdown callbacks
        for callback in self.shutdown_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"[Shutdown] Callback error: {e}")

        print("[Shutdown] Graceful shutdown complete")

    def to_json(self) -> dict:
        """Export state as JSON."""
        state = self.get_state()
        return {
            "initiated": state.initiated,
            "draining": state.draining,
            "active_requests": state.active_requests,
            "completed_requests": state.completed_requests,
            "cancelled_requests": state.cancelled_requests,
            "start_time": state.start_time.isoformat() if state.start_time else None,
            "deadline": state.deadline.isoformat() if state.deadline else None,
            "timeout_seconds": self.timeout_seconds,
        }


# Global instance for signal handling
_shutdown_manager: Optional[GracefulShutdownManager] = None


def setup_signal_handlers(manager: GracefulShutdownManager) -> None:
    """Setup SIGTERM/SIGINT handlers for graceful shutdown."""
    global _shutdown_manager
    _shutdown_manager = manager

    def handle_shutdown_signal(signum, frame):
        print(f"\n[Shutdown] Received signal {signum}")
        if _shutdown_manager:
            _shutdown_manager.initiate_shutdown()

    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)
