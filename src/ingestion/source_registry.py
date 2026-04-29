from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.utils.config import ConfigError, load_project_config


REQUIRED_SOURCE_FIELDS = {
    "display_name",
    "source_group",
    "provider",
    "source_url",
    "access_method",
    "expected_frequency",
    "file_format",
    "spatial_grain",
    "temporal_grain",
    "target_bronze_table",
    "target_silver_table",
    "downstream_use",
    "required_fields",
    "validation_checks",
    "redistribution",
}

ALLOWED_SOURCE_GROUPS = {"national", "provincial", "municipal"}
ALLOWED_REDISPLAY = {"public", "restricted", "unknown"}

ALLOWED_FREQUENCIES = {
    "daily",
    "monthly",
    "quarterly",
    "annual",
    "seasonal",
    "periodic",
}


@dataclass(frozen=True)
class SourceDefinition:
    """Structured metadata for one source in the registry."""

    name: str
    display_name: str
    source_group: str
    provider: str
    source_url: str
    access_method: str
    expected_frequency: str
    file_format: str
    spatial_grain: str
    temporal_grain: str
    target_bronze_table: str
    target_silver_table: str
    downstream_use: list[str]
    required_fields: list[str]
    validation_checks: list[str]
    redistribution: str
    raw: dict[str, Any]


class SourceRegistry:
    """Config-driven source registry for ingestion and audit planning."""

    def __init__(self, config_path: str = "source_config.yml") -> None:
        self.config = load_project_config(config_path)
        self.sources = self._load_sources()

    def _load_sources(self) -> dict[str, SourceDefinition]:
        raw_sources = self.config.get("sources")

        if not isinstance(raw_sources, dict) or not raw_sources:
            raise ConfigError("source_config.yml must contain a non-empty 'sources' mapping.")

        sources: dict[str, SourceDefinition] = {}

        for source_name, metadata in raw_sources.items():
            if not isinstance(metadata, dict):
                raise ConfigError(f"Source '{source_name}' metadata must be a mapping.")

            self._validate_source(source_name, metadata)

            sources[source_name] = SourceDefinition(
                name=source_name,
                display_name=str(metadata["display_name"]),
                source_group=str(metadata["source_group"]),
                provider=str(metadata["provider"]),
                source_url=str(metadata["source_url"]),
                access_method=str(metadata["access_method"]),
                expected_frequency=str(metadata["expected_frequency"]),
                file_format=str(metadata["file_format"]),
                spatial_grain=str(metadata["spatial_grain"]),
                temporal_grain=str(metadata["temporal_grain"]),
                target_bronze_table=str(metadata["target_bronze_table"]),
                target_silver_table=str(metadata["target_silver_table"]),
                downstream_use=list(metadata["downstream_use"]),
                required_fields=list(metadata["required_fields"]),
                validation_checks=list(metadata["validation_checks"]),
                redistribution=str(metadata["redistribution"]),
                raw=metadata,
            )

        return sources

    @staticmethod
    def _validate_source(source_name: str, metadata: dict[str, Any]) -> None:
        missing_fields = REQUIRED_SOURCE_FIELDS - set(metadata)

        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ConfigError(f"Source '{source_name}' is missing required fields: {missing}")

        source_group = metadata["source_group"]
        if source_group not in ALLOWED_SOURCE_GROUPS:
            raise ConfigError(
                f"Source '{source_name}' has invalid source_group '{source_group}'. "
                f"Allowed values: {sorted(ALLOWED_SOURCE_GROUPS)}"
            )

        expected_frequency = metadata["expected_frequency"]
        if expected_frequency not in ALLOWED_FREQUENCIES:
            raise ConfigError(
                f"Source '{source_name}' has invalid expected_frequency '{expected_frequency}'. "
                f"Allowed values: {sorted(ALLOWED_FREQUENCIES)}"
            )

        redistribution = metadata["redistribution"]
        if redistribution not in ALLOWED_REDISPLAY:
            raise ConfigError(
                f"Source '{source_name}' has invalid redistribution '{redistribution}'. "
                f"Allowed values: {sorted(ALLOWED_REDISPLAY)}"
            )

        for list_field in ("downstream_use", "required_fields", "validation_checks"):
            value = metadata[list_field]
            if not isinstance(value, list) or not value:
                raise ConfigError(
                    f"Source '{source_name}' field '{list_field}' must be a non-empty list."
                )

        for table_field in ("target_bronze_table", "target_silver_table"):
            value = metadata[table_field]
            if not isinstance(value, str) or not value.strip():
                raise ConfigError(f"Source '{source_name}' field '{table_field}' must be non-empty.")

        source_url = metadata["source_url"]
        if not isinstance(source_url, str) or not source_url.startswith("http"):
            raise ConfigError(f"Source '{source_name}' must have a valid HTTP source_url.")

    def list_sources(self) -> list[str]:
        """Return all source names."""
        return sorted(self.sources)

    def get_source(self, source_name: str) -> SourceDefinition:
        """Return one source definition by name."""
        try:
            return self.sources[source_name]
        except KeyError as exc:
            raise KeyError(f"Unknown source: {source_name}") from exc

    def filter_by_group(self, source_group: str) -> list[SourceDefinition]:
        """Return sources that belong to a source group."""
        return [
            source
            for source in self.sources.values()
            if source.source_group == source_group
        ]

    def bronze_tables(self) -> set[str]:
        """Return all configured Bronze table names."""
        return {source.target_bronze_table for source in self.sources.values()}

    def silver_tables(self) -> set[str]:
        """Return all configured Silver table names."""
        return {source.target_silver_table for source in self.sources.values()}
