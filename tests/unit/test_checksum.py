import hashlib

import pytest

from src.utils.checksum import ChecksumError, compute_bytes_checksum, compute_file_checksum


def test_compute_bytes_checksum():
    content = b"hello climate risk"
    expected = hashlib.sha256(content).hexdigest()

    assert compute_bytes_checksum(content) == expected


def test_compute_file_checksum(tmp_path):
    file_path = tmp_path / "sample.txt"
    file_path.write_text("hello", encoding="utf-8")

    expected = hashlib.sha256(b"hello").hexdigest()

    assert compute_file_checksum(file_path) == expected


def test_compute_file_checksum_missing_file(tmp_path):
    missing_file = tmp_path / "missing.txt"

    with pytest.raises(ChecksumError):
        compute_file_checksum(missing_file)


def test_compute_bytes_checksum_invalid_algorithm():
    with pytest.raises(ChecksumError):
        compute_bytes_checksum(b"abc", algorithm="not_real")
