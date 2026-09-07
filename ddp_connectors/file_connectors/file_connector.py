"""Public contract for read-only file source connectors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO, Iterator


@dataclass(frozen=True)
class FileMetadata:
    """Metadata returned for an item in a remote file source."""

    path: str
    size: int
    modified_at: datetime
    is_file: bool
    is_directory: bool
    is_symlink: bool = False


class FileConnector(ABC):
    """A synchronous, read-only file source.

    ``open_file`` returns a binary stream owned by its caller.  The caller must
    close that stream when finished.  Closing the connector invalidates any
    still-open streams and releases its network session.
    """

    @abstractmethod
    def ping(self) -> bool:
        """Test authenticated access without raising for an expected failure."""

    @abstractmethod
    def list_files(self, remote_path: str = "", *, recursive: bool = False) -> Iterator[FileMetadata]:
        """Yield file and directory metadata below an allowed remote path."""

    @abstractmethod
    def get_file_metadata(self, remote_path: str) -> FileMetadata:
        """Return metadata for one allowed remote path."""

    @abstractmethod
    def open_file(self, remote_path: str) -> BinaryIO:
        """Open one regular remote file in binary read-only mode."""

    @abstractmethod
    def close(self) -> None:
        """Release all network resources held by the connector."""
