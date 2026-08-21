"""
Error Recovery Strategies for Kurd

Implements configurable retry logic, fallbacks, and circuit breakers per tool.

Usage:
    from kurd.error_recovery import ErrorRecoveryManager, RetryStrategy

    recovery = ErrorRecoveryManager()

    # Configure retry strategy for tool
    strategy = RetryStrategy(
        max_retries=3,
        backoff_type="exponential",
        initial_delay_ms=100,
    )
    recovery.set_tool_strategy("add", strategy)
"""

from typing import Optional, Callable, Dict, Any
from dataclasses import dataclass
from enum import Enum
import time


class BackoffType(Enum):
    """Backoff strategies."""

    LINEAR = "linear"  # delay, delay*2, delay*3
    EXPONENTIAL = "exponential"  # delay, delay*2, delay*4
    FIBONACCI = "fibonacci"  # delay, delay, delay*2, delay*3, delay*5


@dataclass
class RetryStrategy:
    """Configuration for retry strategy."""

    max_retries: int = 3
    backoff_type: str = "exponential"
    initial_delay_ms: int = 100
    max_delay_ms: int = 5000
    jitter: bool = True  # Add randomness to backoff


class ErrorRecoveryManager:
    """Manages error recovery and retry strategies."""

    def __init__(self):
        self.tool_strategies: Dict[str, RetryStrategy] = {}
        self.default_strategy = RetryStrategy()
        self.fallback_handlers: Dict[str, Callable] = {}
        self.error_counts: Dict[str, int] = {}
        self.last_error_time: Dict[str, float] = {}

    def set_tool_strategy(self, tool_name: str, strategy: RetryStrategy) -> None:
        """Set retry strategy for a tool."""
        self.tool_strategies[tool_name] = strategy

    def register_fallback_handler(self, tool_name: str, handler: Callable) -> None:
        """Register fallback handler for tool."""
        self.fallback_handlers[tool_name] = handler

    def get_retry_delay(self, tool_name: str, attempt: int) -> int:
        """Calculate retry delay in milliseconds."""
        strategy = self.tool_strategies.get(tool_name, self.default_strategy)

        if attempt == 0:
            return 0

        backoff_type = strategy.backoff_type.lower()

        if backoff_type == "linear":
            delay_ms = strategy.initial_delay_ms * attempt
        elif backoff_type == "exponential":
            delay_ms = strategy.initial_delay_ms * (2 ** (attempt - 1))
        elif backoff_type == "fibonacci":
            fib = self._fibonacci(attempt)
            delay_ms = strategy.initial_delay_ms * fib
        else:
            delay_ms = strategy.initial_delay_ms

        # Cap at max delay
        delay_ms = min(delay_ms, strategy.max_delay_ms)

        # Add jitter if enabled
        if strategy.jitter:
            import random
            jitter = int(delay_ms * 0.1 * random.random())
            delay_ms += jitter

        return delay_ms

    def should_retry(self, tool_name: str, error: Exception) -> bool:
        """Check if error is retryable."""
        # Retryable errors
        retryable_exceptions = (
            TimeoutError,
            ConnectionError,
            OSError,
        )

        if isinstance(error, retryable_exceptions):
            return True

        # Check error message for common retryable patterns
        error_str = str(error).lower()
        if any(pattern in error_str for pattern in ["timeout", "connection", "temporarily"]):
            return True

        return False

    def execute_with_retry(
        self,
        tool_name: str,
        func: Callable,
        *args,
        **kwargs
    ) -> tuple[bool, Any, Optional[str]]:
        """
        Execute function with retry logic.

        Returns:
            (success, result, error_message)
        """
        strategy = self.tool_strategies.get(tool_name, self.default_strategy)

        for attempt in range(strategy.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                self.error_counts[tool_name] = 0
                return True, result, None

            except Exception as e:
                self.error_counts[tool_name] = self.error_counts.get(tool_name, 0) + 1
                self.last_error_time[tool_name] = time.time()

                if not self.should_retry(tool_name, e):
                    return False, None, str(e)

                if attempt < strategy.max_retries:
                    delay_ms = self.get_retry_delay(tool_name, attempt + 1)
                    time.sleep(delay_ms / 1000.0)
                else:
                    # All retries exhausted, try fallback
                    if tool_name in self.fallback_handlers:
                        try:
                            fallback_result = self.fallback_handlers[tool_name](*args, **kwargs)
                            return True, fallback_result, None
                        except Exception as fallback_error:
                            return False, None, f"Fallback failed: {str(fallback_error)}"

                    return False, None, str(e)

    def get_error_count(self, tool_name: str) -> int:
        """Get error count for tool."""
        return self.error_counts.get(tool_name, 0)

    def get_last_error_time(self, tool_name: str) -> Optional[float]:
        """Get last error time for tool."""
        return self.last_error_time.get(tool_name)

    def reset_error_count(self, tool_name: str) -> None:
        """Reset error count for tool."""
        self.error_counts[tool_name] = 0

    def _fibonacci(self, n: int) -> int:
        """Calculate Fibonacci number."""
        if n <= 0:
            return 0
        elif n == 1:
            return 1
        a, b = 0, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def to_json(self) -> Dict:
        """Export state as JSON."""
        return {
            "tool_strategies": {
                tool: {
                    "max_retries": strategy.max_retries,
                    "backoff_type": strategy.backoff_type,
                    "initial_delay_ms": strategy.initial_delay_ms,
                    "max_delay_ms": strategy.max_delay_ms,
                }
                for tool, strategy in self.tool_strategies.items()
            },
            "error_counts": self.error_counts,
            "last_error_times": self.last_error_time,
        }
