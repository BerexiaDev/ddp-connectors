"""Secure, read-only SFTP source connector built on Paramiko."""

import base64
import hashlib
import posixpath
import socket
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

from loguru import logger
import paramiko

from .file_connector import FileConnector, FileMetadata


class SftpConnectorError(RuntimeError):
    """Raised when an SFTP operation cannot be completed safely."""


class SftpConfigurationError(ValueError):
    """Raised for invalid or insecure SFTP settings."""


class SftpPathError(SftpConnectorError):
    """Raised when a requested path is outside the configured root."""


@dataclass(frozen=True)
class SftpConnectionDiagnostic:
    """A non-sensitive classification of the most recent ``ping`` failure."""

    category: str
    message: str


class SftpConnector(FileConnector):
    """A synchronous, read-only SFTP connector.

    A connector has at most one lazily-created session and is not thread-safe.
    It verifies the host through either a known-hosts file or a pinned SHA-256
    host-key fingerprint.  It never enables Paramiko's auto-add host-key policy.
    """

    def __init__(
        self,
        host: str,
        user: str,
        remote_root: str,
        *,
        port: int = 22,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        known_hosts: Optional[str] = None,
        host_key_fingerprint: Optional[str] = None,
        connect_timeout: float = 10,
        auth_timeout: float = 10,
        socket_timeout: float = 30,
    ) -> None:
        self.host = self._required_text(host, "host")
        self.user = self._required_text(user, "user")
        self.port = self._port(port)
        self.remote_root = self._normalize_root(remote_root)
        self.password = password
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.known_hosts = known_hosts
        self.host_key_fingerprint = host_key_fingerprint
        self.connect_timeout = self._timeout(connect_timeout, "connect_timeout")
        self.auth_timeout = self._timeout(auth_timeout, "auth_timeout")
        self.socket_timeout = self._timeout(socket_timeout, "socket_timeout")
        self._client = None
        self._transport = None
        self._sftp = None
        self.last_connection_diagnostic = None
        self._validate_auth_and_trust()

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "SftpConnector":
        """Build a connector from the factory's SFTP configuration dictionary."""
        if not isinstance(settings, dict):
            raise SftpConfigurationError("SFTP connector settings must be a dictionary.")
        try:
            return cls(
                settings["host"], settings["user"], settings["remote_root"],
                port=settings.get("port", 22), password=settings.get("password"),
                private_key_path=settings.get("private_key_path"),
                private_key_passphrase=settings.get("private_key_passphrase"),
                known_hosts=settings.get("known_hosts"),
                host_key_fingerprint=settings.get("host_key_fingerprint"),
                connect_timeout=settings.get("connect_timeout", 10),
                auth_timeout=settings.get("auth_timeout", 10),
                socket_timeout=settings.get("socket_timeout", 30),
            )
        except KeyError as exc:
            raise SftpConfigurationError("Missing required SFTP setting: {}.".format(exc.args[0])) from None

    @staticmethod
    def _required_text(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise SftpConfigurationError("SFTP setting '{}' must be a non-empty string.".format(field))
        return value.strip()

    @staticmethod
    def _port(value: Any) -> int:
        try:
            port = int(value)
        except (TypeError, ValueError):
            raise SftpConfigurationError("SFTP setting 'port' must be an integer.") from None
        if not 1 <= port <= 65535:
            raise SftpConfigurationError("SFTP setting 'port' must be between 1 and 65535.")
        return port

    @staticmethod
    def _timeout(value: Any, field: str) -> float:
        try:
            timeout = float(value)
        except (TypeError, ValueError):
            raise SftpConfigurationError("SFTP setting '{}' must be a number.".format(field)) from None
        if timeout <= 0:
            raise SftpConfigurationError("SFTP setting '{}' must be greater than zero.".format(field))
        return timeout

    @staticmethod
    def _normalize_root(value: Any) -> str:
        root = SftpConnector._required_text(value, "remote_root")
        if not root.startswith("/"):
            raise SftpConfigurationError("SFTP setting 'remote_root' must be an absolute POSIX path.")
        normalized = posixpath.normpath(root)
        if normalized == "/":
            return normalized
        return normalized.rstrip("/")

    def _validate_auth_and_trust(self) -> None:
        if bool(self.password) == bool(self.private_key_path):
            raise SftpConfigurationError("Configure exactly one of password or private_key_path for SFTP authentication.")
        if bool(self.known_hosts) == bool(self.host_key_fingerprint):
            raise SftpConfigurationError(
                "Configure exactly one of known_hosts or host_key_fingerprint for SFTP host-key verification."
            )

    def __enter__(self) -> "SftpConnector":
        self._ensure_connected()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def ping(self) -> bool:
        """Return whether an authenticated and host-key-verified session can be opened."""
        opened_here = self._sftp is None
        try:
            self._ensure_connected()
            self.last_connection_diagnostic = None
            return True
        except Exception as exc:
            self.last_connection_diagnostic = self._connection_diagnostic(exc)
            logger.warning(
                "SFTP connection test failed for host {}: {}",
                self.host,
                self.last_connection_diagnostic.category,
            )
            return False
        finally:
            if opened_here:
                self.close()

    def _ensure_connected(self) -> None:
        if self._sftp is not None:
            return
        try:
            if self.known_hosts:
                self._connect_with_known_hosts()
            else:
                self._connect_with_pinned_fingerprint()
        except Exception as exc:
            self.close()
            if isinstance(exc, SftpConnectorError):
                raise
            raise SftpConnectorError("Unable to establish a verified SFTP connection to {}.".format(self.host)) from exc

    def _connect_with_known_hosts(self) -> None:
        client = paramiko.SSHClient()
        # Own the client before every fallible operation. _ensure_connected and
        # this method's exception path can therefore always close it.
        self._client = client
        try:
            client.load_host_keys(self.known_hosts)
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
            client.connect(
                hostname=self.host, port=self.port, username=self.user, password=self.password,
                key_filename=self.private_key_path, passphrase=self.private_key_passphrase,
                timeout=self.connect_timeout, auth_timeout=self.auth_timeout,
                banner_timeout=self.connect_timeout, channel_timeout=self.socket_timeout,
                allow_agent=False, look_for_keys=False,
            )
            transport = client.get_transport()
            if transport is None:
                raise SftpConnectorError("SFTP SSH transport was not established.")
            self._transport = transport
            self._sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception:
            self.close()
            raise

    def _connect_with_pinned_fingerprint(self) -> None:
        # Transport does not expose a connect timeout when constructed from an
        # address tuple, so create and configure the socket explicitly.
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        transport = None
        try:
            sock.settimeout(self.socket_timeout)
            transport = paramiko.Transport(sock)
            # Assign ownership before SSH negotiation/authentication/channel setup.
            self._transport = transport
            transport.banner_timeout = self.connect_timeout
            transport.auth_timeout = self.auth_timeout
            transport.start_client(timeout=self.connect_timeout)
            actual = self._sha256_fingerprint(transport.get_remote_server_key())
            if not self._fingerprints_match(actual, self.host_key_fingerprint):
                raise SftpConnectorError("SFTP server host key does not match the configured fingerprint.")
            if self.password:
                transport.auth_password(self.user, self.password)
            else:
                key = self._load_private_key(self.private_key_path, self.private_key_passphrase)
                transport.auth_publickey(self.user, key)
            if not transport.is_authenticated():
                raise SftpConnectorError("SFTP authentication was rejected.")
            self._sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception:
            # If Transport construction itself failed, it cannot close the socket.
            if transport is None:
                sock.close()
            else:
                self.close()
            raise

    @staticmethod
    def _load_private_key(path: str, passphrase: Optional[str]):
        last_error = None
        for key_type in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key, paramiko.DSSKey):
            try:
                return key_type.from_private_key_file(path, password=passphrase)
            except paramiko.SSHException as exc:
                last_error = exc
        raise SftpConfigurationError("Unable to load the configured SFTP private key.") from last_error

    @staticmethod
    def _sha256_fingerprint(key) -> str:
        encoded = base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode("ascii").rstrip("=")
        return "SHA256:" + encoded

    @staticmethod
    def _fingerprints_match(actual: str, configured: Optional[str]) -> bool:
        if not configured:
            return False
        return actual == configured.strip().rstrip("=")

    def _resolve_path(self, remote_path: str) -> str:
        if remote_path is None:
            raise SftpPathError("A remote path is required.")
        raw = str(remote_path).strip()
        candidate = posixpath.normpath(raw if raw.startswith("/") else posixpath.join(self.remote_root, raw or "."))
        root = self.remote_root
        if root != "/" and candidate != root and not candidate.startswith(root + "/"):
            raise SftpPathError("Requested remote path is outside the configured remote_root.")
        if root == "/" and not candidate.startswith("/"):
            raise SftpPathError("Requested remote path is invalid.")
        return candidate

    @staticmethod
    def _metadata(path: str, attributes) -> FileMetadata:
        mode = getattr(attributes, "st_mode", 0)
        return FileMetadata(
            path=path, size=getattr(attributes, "st_size", 0),
            modified_at=datetime.fromtimestamp(getattr(attributes, "st_mtime", 0), tz=timezone.utc),
            is_file=stat.S_ISREG(mode), is_directory=stat.S_ISDIR(mode), is_symlink=stat.S_ISLNK(mode),
        )

    def get_file_metadata(self, remote_path: str) -> FileMetadata:
        path = self._resolve_path(remote_path)
        try:
            attributes = self._ensure_sftp().lstat(path)
            return self._metadata(path, attributes)
        except SftpConnectorError:
            raise
        except Exception as exc:
            raise SftpConnectorError("Unable to read SFTP metadata for {}.".format(path)) from exc

    def list_files(self, remote_path: str = "", *, recursive: bool = False) -> Iterator[FileMetadata]:
        """Yield entries below ``remote_path``; symlinks are returned but never followed."""
        root = self._resolve_path(remote_path)
        sftp = self._ensure_sftp()
        try:
            initial = self._metadata(root, sftp.lstat(root))
            if initial.is_symlink:
                raise SftpPathError("Refusing to list through an SFTP symlink.")
            if initial.is_file:
                yield initial
                return
            for item in self._walk_directory(root, recursive):
                yield item
        except SftpConnectorError:
            raise
        except Exception as exc:
            raise SftpConnectorError("Unable to list SFTP path {}.".format(root)) from exc

    def _walk_directory(self, directory: str, recursive: bool) -> Iterator[FileMetadata]:
        sftp = self._ensure_sftp()
        for attributes in sftp.listdir_attr(directory):
            name = getattr(attributes, "filename", "")
            if not name or name in (".", ".."):
                continue
            if name.startswith("/") or "/" in name:
                raise SftpPathError("SFTP directory entry has an invalid filename.")
            path = self._resolve_path(posixpath.join(directory, name))
            metadata = self._metadata(path, attributes)
            yield metadata
            if recursive and metadata.is_directory and not metadata.is_symlink:
                # listdir_attr metadata is not a security boundary: verify the
                # item immediately before using it as a recursive directory.
                current = self._metadata(path, sftp.lstat(path))
                if current.is_directory and not current.is_symlink:
                    for child in self._walk_directory(path, recursive):
                        yield child

    def open_file(self, remote_path: str):
        path = self._resolve_path(remote_path)
        metadata = self.get_file_metadata(path)
        if metadata.is_symlink:
            raise SftpPathError("Refusing to open an SFTP symlink.")
        if not metadata.is_file:
            raise SftpPathError("Requested SFTP path is not a regular file.")
        try:
            return self._ensure_sftp().open(path, "rb", bufsize=32 * 1024)
        except Exception as exc:
            raise SftpConnectorError("Unable to open SFTP file {}.".format(path)) from exc

    def _ensure_sftp(self):
        self._ensure_connected()
        return self._sftp

    @staticmethod
    def _connection_diagnostic(exc: Exception) -> SftpConnectionDiagnostic:
        """Classify connection-test failures without surfacing driver text."""
        chain = []
        current = exc
        while current is not None and len(chain) < 8:
            chain.append(current)
            current = current.__cause__ or current.__context__
        names = {item.__class__.__name__.lower() for item in chain}
        message = "SFTP connection failed."
        category = "connection"
        if any(isinstance(item, SftpConfigurationError) for item in chain):
            category, message = "configuration", "SFTP configuration is invalid."
        elif any("hostkey" in name or "host_key" in name for name in names) or any(
            "host key" in str(item).lower() for item in chain if isinstance(item, SftpConnectorError)
        ):
            category, message = "host_key", "SFTP host-key verification failed."
        elif any("authentication" in name or "auth" in name for name in names):
            category, message = "authentication", "SFTP authentication failed."
        elif any(isinstance(item, (TimeoutError, socket.timeout)) or "timeout" in item.__class__.__name__.lower() for item in chain):
            category, message = "timeout", "SFTP connection timed out."
        return SftpConnectionDiagnostic(category=category, message=message)

    def close(self) -> None:
        """Close the SFTP channel and SSH transport. Safe to call repeatedly."""
        sftp, client, transport = self._sftp, self._client, self._transport
        self._sftp = self._client = self._transport = None
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        elif transport is not None:
            try:
                transport.close()
            except Exception:
                pass
