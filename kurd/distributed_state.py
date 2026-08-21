"""
Distributed State for Kurd

Redis-backed shared state for horizontal scaling.

Usage:
    from kurd.distributed_state import DistributedStateManager

    # Initialize with Redis
    state = DistributedStateManager(
        redis_url="redis://localhost:6379/0",
        enable_dlq=True,
        enable_idempotency=True
    )

    # All DLQ/idempotency operations now use Redis (shared across instances)
    dlq = state.get_dlq()
    dlq.add_message(...)  # Stored in Redis, accessible from any instance

    # Store arbitrary state
    state.set("gateway:config:version", 42)
    version = state.get("gateway:config:version")
"""

import json
from typing import Dict, Optional, Any, List
from enum import Enum


class BackendType(Enum):
    """Supported state backends."""

    REDIS = "redis"
    MEMORY = "memory"  # Fallback for local/testing


class DistributedStateManager:
    """Manages distributed state across multiple Kurd instances."""

    def __init__(
        self,
        backend: str = "redis",
        redis_url: Optional[str] = None,
        redis_password: Optional[str] = None,
        redis_db: int = 0,
        namespace: str = "kurd",
        key_prefix: str = "kurd:",
        ttl_seconds: int = 86400,
    ):
        """
        Initialize distributed state manager.

        Args:
            backend: 'redis' or 'memory'
            redis_url: Redis connection URL
            redis_password: Redis password
            redis_db: Redis database number
            namespace: Namespace for keys
            key_prefix: Prefix for all keys
            ttl_seconds: Default TTL for keys
        """
        self.backend = backend
        self.namespace = namespace
        self.key_prefix = key_prefix
        self.ttl_seconds = ttl_seconds
        self.redis_client = None
        self.memory_store: Dict[str, Any] = {}

        if backend == "redis":
            self._init_redis(redis_url, redis_password, redis_db)
        elif backend == "memory":
            pass  # Use in-memory dict

    def _init_redis(
        self,
        redis_url: Optional[str],
        password: Optional[str],
        db: int,
    ) -> None:
        """Initialize Redis connection."""
        try:
            import redis

            if redis_url:
                self.redis_client = redis.from_url(redis_url, db=db)
            else:
                self.redis_client = redis.Redis(
                    host="localhost",
                    port=6379,
                    db=db,
                    password=password,
                    decode_responses=True,
                )

            # Test connection
            self.redis_client.ping()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Redis: {e}")

    def _make_key(self, key: str) -> str:
        """Create namespaced key."""
        return f"{self.key_prefix}{self.namespace}:{key}"

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """Set value in distributed state."""
        namespaced_key = self._make_key(key)
        ttl = ttl_seconds or self.ttl_seconds

        if self.backend == "redis" and self.redis_client:
            value_json = json.dumps(value) if not isinstance(value, str) else value
            self.redis_client.setex(namespaced_key, ttl, value_json)
            return True
        else:
            self.memory_store[namespaced_key] = value
            return True

    def get(self, key: str) -> Optional[Any]:
        """Get value from distributed state."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            value = self.redis_client.get(namespaced_key)
            if value:
                try:
                    return json.loads(value)
                except Exception:
                    return value
            return None
        else:
            return self.memory_store.get(namespaced_key)

    def delete(self, key: str) -> bool:
        """Delete value from distributed state."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            self.redis_client.delete(namespaced_key)
            return True
        else:
            if namespaced_key in self.memory_store:
                del self.memory_store[namespaced_key]
            return True

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            return bool(self.redis_client.exists(namespaced_key))
        else:
            return namespaced_key in self.memory_store

    def increment(self, key: str, amount: int = 1) -> int:
        """Increment counter (atomic)."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            return self.redis_client.incrby(namespaced_key, amount)
        else:
            current = self.memory_store.get(namespaced_key, 0)
            new_value = current + amount
            self.memory_store[namespaced_key] = new_value
            return new_value

    def append_to_list(self, key: str, value: Any) -> int:
        """Append to distributed list."""
        namespaced_key = self._make_key(key)
        value_json = json.dumps(value) if not isinstance(value, str) else value

        if self.backend == "redis" and self.redis_client:
            return self.redis_client.rpush(namespaced_key, value_json)
        else:
            if namespaced_key not in self.memory_store:
                self.memory_store[namespaced_key] = []
            self.memory_store[namespaced_key].append(value)
            return len(self.memory_store[namespaced_key])

    def get_list(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get items from distributed list."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            items = self.redis_client.lrange(namespaced_key, start, end)
            return [json.loads(item) if item.startswith("{") or item.startswith("[") else item for item in items]
        else:
            items = self.memory_store.get(namespaced_key, [])
            return items[start:end+1] if end >= 0 else items[start:]

    def clear_list(self, key: str) -> bool:
        """Clear a list."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            self.redis_client.delete(namespaced_key)
            return True
        else:
            if namespaced_key in self.memory_store:
                del self.memory_store[namespaced_key]
            return True

    def set_hash(self, key: str, field: str, value: Any) -> bool:
        """Set hash field (for DLQ, idempotency, etc.)."""
        namespaced_key = self._make_key(key)
        value_json = json.dumps(value) if not isinstance(value, str) else value

        if self.backend == "redis" and self.redis_client:
            self.redis_client.hset(namespaced_key, field, value_json)
            return True
        else:
            if namespaced_key not in self.memory_store:
                self.memory_store[namespaced_key] = {}
            self.memory_store[namespaced_key][field] = value
            return True

    def get_hash(self, key: str, field: str) -> Optional[Any]:
        """Get hash field."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            value = self.redis_client.hget(namespaced_key, field)
            if value:
                try:
                    return json.loads(value)
                except Exception:
                    return value
            return None
        else:
            hash_data = self.memory_store.get(namespaced_key, {})
            return hash_data.get(field)

    def get_all_hash(self, key: str) -> Dict[str, Any]:
        """Get all hash fields."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            data = self.redis_client.hgetall(namespaced_key)
            return {
                k: json.loads(v) if v.startswith("{") or v.startswith("[") else v
                for k, v in data.items()
            }
        else:
            return self.memory_store.get(namespaced_key, {})

    def delete_hash_field(self, key: str, field: str) -> bool:
        """Delete hash field."""
        namespaced_key = self._make_key(key)

        if self.backend == "redis" and self.redis_client:
            self.redis_client.hdel(namespaced_key, field)
            return True
        else:
            if namespaced_key in self.memory_store:
                if field in self.memory_store[namespaced_key]:
                    del self.memory_store[namespaced_key][field]
            return True

    def get_dlq(self):
        """Get DLQ manager backed by distributed state."""
        from kurd.dead_letter_queue import DeadLetterQueue
        dlq = DeadLetterQueue()
        # Override storage with distributed state
        dlq._distributed_state = self
        return dlq

    def get_idempotency(self):
        """Get idempotency manager backed by distributed state."""
        from kurd.idempotency import IdempotencyManager
        idempotency = IdempotencyManager()
        # Override storage with distributed state
        idempotency._distributed_state = self
        return idempotency

    def health_check(self) -> bool:
        """Check backend health."""
        if self.backend == "redis" and self.redis_client:
            try:
                self.redis_client.ping()
                return True
            except Exception:
                return False
        else:
            return True

    def to_json(self) -> Dict:
        """Export state as JSON."""
        return {
            "backend": self.backend,
            "namespace": self.namespace,
            "key_prefix": self.key_prefix,
            "memory_keys": len(self.memory_store) if self.backend == "memory" else 0,
            "healthy": self.health_check(),
        }
