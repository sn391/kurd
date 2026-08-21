"""
Secrets Management for Kurd

Integrates with external secret stores (Kubernetes, HashiCorp Vault, AWS Secrets Manager).

Usage:
    from kurd.secrets_management import SecretsManager, VaultBackend

    # Kubernetes secrets (in-cluster)
    manager = SecretsManager(backend="kubernetes")

    # HashiCorp Vault
    manager = SecretsManager(
        backend="vault",
        vault_addr="https://vault.example.com",
        vault_token="s.xxxxx"
    )

    # AWS Secrets Manager
    manager = SecretsManager(backend="aws")

    # Get secret
    secret = manager.get_secret("db_password")

    # Store secret
    manager.put_secret("db_password", "supersecret123")

    # List secrets
    secrets = manager.list_secrets()
"""

import os
from typing import Dict, Optional, List, Any
from enum import Enum


class SecretBackend(Enum):
    """Supported secret backends."""

    KUBERNETES = "kubernetes"
    VAULT = "vault"
    AWS = "aws"
    ENV = "env"  # Environment variables fallback


class SecretsManager:
    """Manages secrets from various backends."""

    def __init__(
        self,
        backend: str = "kubernetes",
        vault_addr: Optional[str] = None,
        vault_token: Optional[str] = None,
        vault_namespace: str = "secret",
        aws_region: str = "us-east-1",
    ):
        """
        Initialize secrets manager.

        Args:
            backend: 'kubernetes', 'vault', 'aws', or 'env'
            vault_addr: Vault server address (for vault backend)
            vault_token: Vault auth token
            vault_namespace: Vault namespace/mount path
            aws_region: AWS region
        """
        self.backend = SecretBackend(backend.lower())
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.vault_namespace = vault_namespace
        self.aws_region = aws_region
        self.cache: Dict[str, Any] = {}
        self._init_backend()

    def _init_backend(self) -> None:
        """Initialize backend connection."""
        if self.backend == SecretBackend.KUBERNETES:
            self._init_kubernetes()
        elif self.backend == SecretBackend.VAULT:
            self._init_vault()
        elif self.backend == SecretBackend.AWS:
            self._init_aws()
        elif self.backend == SecretBackend.ENV:
            pass  # No initialization needed

    def _init_kubernetes(self) -> None:
        """Initialize Kubernetes backend."""
        try:
            import kubernetes
            from kubernetes import client, config
            config.load_incluster_config()
            self.k8s_client = client.CoreV1Api()
            self.k8s_namespace = os.getenv("KURD_K8S_NAMESPACE", "default")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kubernetes backend: {e}")

    def _init_vault(self) -> None:
        """Initialize Vault backend."""
        if not self.vault_addr or not self.vault_token:
            raise ValueError("vault_addr and vault_token required for Vault backend")

        try:
            import hvac
            self.vault_client = hvac.Client(
                url=self.vault_addr,
                token=self.vault_token,
                namespace=self.vault_namespace,
            )
            # Test connection
            self.vault_client.is_authenticated()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Vault backend: {e}")

    def _init_aws(self) -> None:
        """Initialize AWS Secrets Manager backend."""
        try:
            import boto3
            self.aws_client = boto3.client(
                "secretsmanager",
                region_name=self.aws_region,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize AWS backend: {e}")

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value.

        Args:
            secret_name: Name of the secret
            default: Default value if not found

        Returns:
            Secret value or default
        """
        # Check cache first
        if secret_name in self.cache:
            return self.cache[secret_name]

        if self.backend == SecretBackend.KUBERNETES:
            return self._get_kubernetes_secret(secret_name, default)
        elif self.backend == SecretBackend.VAULT:
            return self._get_vault_secret(secret_name, default)
        elif self.backend == SecretBackend.AWS:
            return self._get_aws_secret(secret_name, default)
        elif self.backend == SecretBackend.ENV:
            return self._get_env_secret(secret_name, default)

        return default

    def _get_kubernetes_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from Kubernetes."""
        try:
            secret = self.k8s_client.read_namespaced_secret(
                name=secret_name,
                namespace=self.k8s_namespace,
            )

            if secret.data and secret_name in secret.data:
                value = secret.data[secret_name]
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                self.cache[secret_name] = value
                return value

            return default
        except Exception:
            return default

    def _get_vault_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from Vault."""
        try:
            response = self.vault_client.secrets.kv.read_secret_version(
                path=secret_name,
            )
            data = response["data"]["data"]

            if "value" in data:
                value = data["value"]
                self.cache[secret_name] = value
                return value

            return default
        except Exception:
            return default

    def _get_aws_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from AWS Secrets Manager."""
        try:
            response = self.aws_client.get_secret_value(SecretId=secret_name)

            if "SecretString" in response:
                value = response["SecretString"]
                self.cache[secret_name] = value
                return value

            return default
        except Exception:
            return default

    def _get_env_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from environment variables."""
        value = os.getenv(secret_name.upper(), default)
        if value:
            self.cache[secret_name] = value
        return value

    def put_secret(self, secret_name: str, value: str, metadata: Optional[Dict] = None) -> bool:
        """
        Store a secret.

        Args:
            secret_name: Name of the secret
            value: Secret value
            metadata: Optional metadata

        Returns:
            True if successful
        """
        if self.backend == SecretBackend.KUBERNETES:
            return self._put_kubernetes_secret(secret_name, value, metadata)
        elif self.backend == SecretBackend.VAULT:
            return self._put_vault_secret(secret_name, value, metadata)
        elif self.backend == SecretBackend.AWS:
            return self._put_aws_secret(secret_name, value, metadata)
        elif self.backend == SecretBackend.ENV:
            return self._put_env_secret(secret_name, value)

        return False

    def _put_kubernetes_secret(
        self,
        secret_name: str,
        value: str,
        metadata: Optional[Dict] = None,
    ) -> bool:
        """Store secret in Kubernetes."""
        try:
            from kubernetes import client

            secret = client.V1Secret(
                metadata=client.V1ObjectMeta(name=secret_name),
                data={secret_name: value},
            )

            try:
                self.k8s_client.patch_namespaced_secret(
                    name=secret_name,
                    namespace=self.k8s_namespace,
                    body=secret,
                )
            except Exception:
                self.k8s_client.create_namespaced_secret(
                    namespace=self.k8s_namespace,
                    body=secret,
                )

            self.cache[secret_name] = value
            return True
        except Exception:
            return False

    def _put_vault_secret(
        self,
        secret_name: str,
        value: str,
        metadata: Optional[Dict] = None,
    ) -> bool:
        """Store secret in Vault."""
        try:
            secret_data = {"value": value}
            if metadata:
                secret_data.update(metadata)

            self.vault_client.secrets.kv.create_or_update_secret(
                path=secret_name,
                secret=secret_data,
            )

            self.cache[secret_name] = value
            return True
        except Exception:
            return False

    def _put_aws_secret(
        self,
        secret_name: str,
        value: str,
        metadata: Optional[Dict] = None,
    ) -> bool:
        """Store secret in AWS Secrets Manager."""
        try:
            try:
                self.aws_client.update_secret(
                    SecretId=secret_name,
                    SecretString=value,
                )
            except Exception:
                self.aws_client.create_secret(
                    Name=secret_name,
                    SecretString=value,
                )

            self.cache[secret_name] = value
            return True
        except Exception:
            return False

    def _put_env_secret(self, secret_name: str, value: str) -> bool:
        """Store secret in environment (not recommended for production)."""
        try:
            os.environ[secret_name.upper()] = value
            self.cache[secret_name] = value
            return True
        except Exception:
            return False

    def delete_secret(self, secret_name: str) -> bool:
        """Delete a secret."""
        if self.backend == SecretBackend.KUBERNETES:
            return self._delete_kubernetes_secret(secret_name)
        elif self.backend == SecretBackend.VAULT:
            return self._delete_vault_secret(secret_name)
        elif self.backend == SecretBackend.AWS:
            return self._delete_aws_secret(secret_name)

        # Clear from cache
        if secret_name in self.cache:
            del self.cache[secret_name]

        return False

    def _delete_kubernetes_secret(self, secret_name: str) -> bool:
        """Delete secret from Kubernetes."""
        try:
            self.k8s_client.delete_namespaced_secret(
                name=secret_name,
                namespace=self.k8s_namespace,
            )
            if secret_name in self.cache:
                del self.cache[secret_name]
            return True
        except Exception:
            return False

    def _delete_vault_secret(self, secret_name: str) -> bool:
        """Delete secret from Vault."""
        try:
            self.vault_client.secrets.kv.delete_secret_version(
                path=secret_name,
            )
            if secret_name in self.cache:
                del self.cache[secret_name]
            return True
        except Exception:
            return False

    def _delete_aws_secret(self, secret_name: str) -> bool:
        """Delete secret from AWS Secrets Manager."""
        try:
            self.aws_client.delete_secret(SecretId=secret_name)
            if secret_name in self.cache:
                del self.cache[secret_name]
            return True
        except Exception:
            return False

    def list_secrets(self, pattern: Optional[str] = None) -> List[str]:
        """
        List available secrets.

        Args:
            pattern: Optional filter pattern

        Returns:
            List of secret names
        """
        if self.backend == SecretBackend.KUBERNETES:
            return self._list_kubernetes_secrets(pattern)
        elif self.backend == SecretBackend.VAULT:
            return self._list_vault_secrets(pattern)
        elif self.backend == SecretBackend.AWS:
            return self._list_aws_secrets(pattern)
        elif self.backend == SecretBackend.ENV:
            return self._list_env_secrets(pattern)

        return []

    def _list_kubernetes_secrets(self, pattern: Optional[str] = None) -> List[str]:
        """List secrets from Kubernetes."""
        try:
            secrets = self.k8s_client.list_namespaced_secret(
                namespace=self.k8s_namespace,
            )

            names = [s.metadata.name for s in secrets.items]

            if pattern:
                names = [n for n in names if pattern.lower() in n.lower()]

            return names
        except Exception:
            return []

    def _list_vault_secrets(self, pattern: Optional[str] = None) -> List[str]:
        """List secrets from Vault."""
        try:
            response = self.vault_client.secrets.kv.list_secrets(
                path="",
            )
            names = response.get("keys", [])

            if pattern:
                names = [n for n in names if pattern.lower() in n.lower()]

            return names
        except Exception:
            return []

    def _list_aws_secrets(self, pattern: Optional[str] = None) -> List[str]:
        """List secrets from AWS Secrets Manager."""
        try:
            response = self.aws_client.list_secrets()
            names = [s["Name"] for s in response.get("SecretList", [])]

            if pattern:
                names = [n for n in names if pattern.lower() in n.lower()]

            return names
        except Exception:
            return []

    def _list_env_secrets(self, pattern: Optional[str] = None) -> List[str]:
        """List secrets from environment."""
        names = list(os.environ.keys())

        if pattern:
            names = [n for n in names if pattern.lower() in n.lower()]

        return names

    def clear_cache(self) -> None:
        """Clear local cache."""
        self.cache.clear()

    def to_json(self) -> Dict:
        """Export state as JSON."""
        return {
            "backend": self.backend.value,
            "cache_entries": len(self.cache),
            "vault_namespace": self.vault_namespace if self.backend == SecretBackend.VAULT else None,
            "aws_region": self.aws_region if self.backend == SecretBackend.AWS else None,
        }
