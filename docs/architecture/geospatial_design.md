# CRS Strategy

This document defines the coordinate reference system rules for raw ingestion, processing, distance/area calculation, serving, and BI compatibility.

## Spatial Source Selection Policy

For grid-level climate and hazard modeling, area-based spatial sources are preferred over point-only sources when available.

Preferred source types include:

- basin polygons
- fire perimeter polygons
- flood hazard polygons
- parcel polygons
- property polygons
- administrative boundaries

Point-only datasets are still allowed, but they must not be treated as complete grid coverage by default. When a point-only source is used, the Gold feature logic must clearly preserve whether each grid value is:

- directly observed inside the grid cell
- interpolated from nearby observations
- spatially allocated from an area-based source
- missing because no usable source coverage exists

Missing data must not be interpreted as low risk.

This policy is architectural. Source registration, matching keys, join-rate thresholds, IDW parameters, and scoring inputs are intentionally not finalized here. Those decisions belong to the later source-specific implementation branches after profiling and validation.
