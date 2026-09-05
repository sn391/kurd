
try:
    from importlib.metadata import version as _pkg_version
    __version__: str = _pkg_version("kurd")
except Exception:
    __version__ = "0.0.0"

from ._kurd import fast_parse, fast_parse_batch, set_ip_allowlist, clear_ip_allowlist, set_admin_token, clear_admin_token, configure_otel, clear_otel
from .router import Router, RuntimeConfig
from .telemetry import setup_otel, OTELTracer, OTELConfig
from .authorization import AuthorizationManager, Role, Permission
from .health_checks import HealthCheckManager
from .multitenancy import TenantManager, Tenant

__all__ = [
    "__version__",
    "fast_parse",
    "fast_parse_batch",
    "set_ip_allowlist",
    "clear_ip_allowlist",
    "Router",
    "RuntimeConfig",
    "setup_otel",
    "OTELTracer",
    "OTELConfig",
    "AuthorizationManager",
    "Role",
    "Permission",
    "HealthCheckManager",
    "TenantManager",
    "Tenant",
    "set_admin_token",
    "clear_admin_token",
    "configure_otel",
    "clear_otel",
]