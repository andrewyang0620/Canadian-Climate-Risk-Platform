from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def current_extract_date() -> str:
    """Return current UTC date as YYYY-MM-DD."""
    return datetime.now(UTC).strftime("%Y-%m-%d")


def generate_run_id() -> str:
    """Generate a unique run id."""
    return str(uuid4())
