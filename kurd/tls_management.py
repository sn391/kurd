"""
TLS/SSL Management for Kurd

Handles certificate management and HTTPS configuration.

Usage:
    from kurd.tls_management import TLSManager

    tls = TLSManager()

    # Load certificate
    tls.load_certificate(
        cert_path="/etc/certs/server.crt",
        key_path="/etc/certs/server.key"
    )

    # Configure HTTPS
    tls.enable_https(port=9443)
"""

from typing import Optional, Dict, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
import ssl
import socket


@dataclass
class CertificateInfo:
    """Information about a certificate."""

    subject: str
    issuer: str
    not_before: datetime
    not_after: datetime
    serial_number: str
    fingerprint: str
    is_valid: bool
    days_until_expiry: int


class TLSManager:
    """Manages TLS/SSL certificates and HTTPS configuration."""

    def __init__(self):
        self.cert_path: Optional[str] = None
        self.key_path: Optional[str] = None
        self.ca_cert_path: Optional[str] = None
        self.https_port = 9443
        self.http_port = 9200
        self.require_https = False
        self.min_tls_version = ssl.TLSVersion.TLSv1_2
        self.ssl_context: Optional[ssl.SSLContext] = None
        self.certificate_info: Optional[CertificateInfo] = None

    def load_certificate(
        self,
        cert_path: str,
        key_path: str,
        ca_cert_path: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Load certificate and key files.

        Args:
            cert_path: Path to certificate file (.crt)
            key_path: Path to private key file (.key)
            ca_cert_path: Optional path to CA certificate

        Returns:
            (success, error_message)
        """
        if not os.path.exists(cert_path):
            return False, f"Certificate file not found: {cert_path}"

        if not os.path.exists(key_path):
            return False, f"Key file not found: {key_path}"

        if ca_cert_path and not os.path.exists(ca_cert_path):
            return False, f"CA certificate file not found: {ca_cert_path}"

        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_cert_path = ca_cert_path

        # Create SSL context
        try:
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            self.ssl_context.minimum_version = self.min_tls_version
            self.ssl_context.load_cert_chain(cert_path, key_path)

            if ca_cert_path:
                self.ssl_context.load_verify_locations(ca_cert_path)
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
            else:
                self.ssl_context.verify_mode = ssl.CERT_NONE

            # Extract certificate info
            self.certificate_info = self._extract_cert_info(cert_path)

            return True, None
        except Exception as e:
            return False, f"Failed to load certificate: {str(e)}"

    def enable_https(self, port: int = 9443, require_https: bool = False) -> None:
        """
        Enable HTTPS support.

        Args:
            port: HTTPS port
            require_https: If True, reject HTTP connections
        """
        self.https_port = port
        self.require_https = require_https

    def get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Get configured SSL context."""
        return self.ssl_context

    def is_https_enabled(self) -> bool:
        """Check if HTTPS is enabled."""
        return self.ssl_context is not None

    def is_certificate_valid(self) -> bool:
        """Check if certificate is currently valid."""
        if not self.certificate_info:
            return False

        now = datetime.utcnow()
        return self.certificate_info.not_before <= now <= self.certificate_info.not_after

    def get_certificate_info(self) -> Optional[CertificateInfo]:
        """Get certificate information."""
        return self.certificate_info

    def get_days_until_expiry(self) -> Optional[int]:
        """Get days until certificate expires."""
        if not self.certificate_info:
            return None

        days = (self.certificate_info.not_after - datetime.utcnow()).days
        return max(0, days)

    def needs_renewal(self, days_threshold: int = 30) -> bool:
        """Check if certificate needs renewal."""
        days_left = self.get_days_until_expiry()
        if days_left is None:
            return False

        return days_left <= days_threshold

    def _extract_cert_info(self, cert_path: str) -> Optional[CertificateInfo]:
        """Extract information from certificate file."""
        try:
            import ssl
            from OpenSSL import crypto

            with open(cert_path, 'rb') as f:
                cert_data = f.read()

            cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert_data)

            subject = str(cert.get_subject())
            issuer = str(cert.get_issuer())
            not_before = datetime.strptime(
                cert.get_notBefore().decode(), '%Y%m%d%H%M%SZ'
            )
            not_after = datetime.strptime(
                cert.get_notAfter().decode(), '%Y%m%d%H%M%SZ'
            )
            serial = hex(cert.get_serial_number())

            days_until = (not_after - datetime.utcnow()).days

            return CertificateInfo(
                subject=subject,
                issuer=issuer,
                not_before=not_before,
                not_after=not_after,
                serial_number=serial,
                fingerprint=self._get_cert_fingerprint(cert_path),
                is_valid=not_before <= datetime.utcnow() <= not_after,
                days_until_expiry=max(0, days_until),
            )
        except Exception as e:
            print(f"Failed to extract certificate info: {e}")
            return None

    def _get_cert_fingerprint(self, cert_path: str) -> str:
        """Get certificate fingerprint (SHA-256)."""
        try:
            import hashlib
            with open(cert_path, 'rb') as f:
                cert_data = f.read()
            return hashlib.sha256(cert_data).hexdigest()
        except Exception:
            return "unknown"

    def validate_client_cert(self, client_cert_path: str) -> Tuple[bool, Optional[str]]:
        """Validate a client certificate."""
        if not self.ssl_context:
            return False, "HTTPS not configured"

        try:
            if not os.path.exists(client_cert_path):
                return False, "Client certificate not found"

            # Try to load it
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_verify_locations(client_cert_path)

            return True, None
        except Exception as e:
            return False, f"Invalid client certificate: {str(e)}"

    def to_json(self) -> Dict:
        """Export TLS configuration as JSON."""
        cert_info = self.certificate_info

        return {
            "https_enabled": self.is_https_enabled(),
            "https_port": self.https_port,
            "http_port": self.http_port,
            "require_https": self.require_https,
            "min_tls_version": str(self.min_tls_version),
            "certificate": {
                "subject": cert_info.subject if cert_info else None,
                "issuer": cert_info.issuer if cert_info else None,
                "valid": cert_info.is_valid if cert_info else None,
                "expires_at": cert_info.not_after.isoformat() if cert_info else None,
                "days_until_expiry": cert_info.days_until_expiry if cert_info else None,
                "fingerprint": cert_info.fingerprint if cert_info else None,
            } if cert_info else None,
        }
