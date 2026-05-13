from __future__ import annotations

import hashlib
from pathlib import Path


class ChecksumError(Exception):
    """Raised when checksum calculation fails."""


def compute_file_checksum(path: str | Path, algorithm: str = "sha256") -> str:
    """Compute a checksum for a file."""
    file_path = Path(path)

    if not file_path.exists():
        raise ChecksumError(f"File not found: {file_path}")

    if not file_path.is_file():
        raise ChecksumError(f"Path is not a file: {file_path}")

    try:
        hasher = hashlib.new(algorithm)
    except ValueError as exc:
        raise ChecksumError(f"Unsupported checksum algorithm: {algorithm}") from exc

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def compute_bytes_checksum(content: bytes, algorithm: str = "sha256") -> str:
    """Compute a checksum for bytes."""
    try:
        hasher = hashlib.new(algorithm)
    except ValueError as exc:
        raise ChecksumError(f"Unsupported checksum algorithm: {algorithm}") from exc

    hasher.update(content)
    return hasher.hexdigest()
