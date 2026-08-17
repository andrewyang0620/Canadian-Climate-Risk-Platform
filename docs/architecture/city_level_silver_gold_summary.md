# Phase D — City Silver to Gold Serving Summary

Phase D standardizes Vancouver and Calgary municipal datasets into city-level Gold serving tables. The city Gold layer keeps each source at its natural business grain and uses explicit bridge or spatial relationships where a direct one-to-one join is not valid.

## Vancouver

### Silver inputs

| Silver table | Grain | Main content |
|---|---|---|
| `silver_property_parcel` | one row per `property_parcel_key` | Parcel identity, address, `source_tax_coord`, parcel geometry |
| `silver_property_tax_assessment` | source property-tax assessment record | Land coordinate, folio/PID, assessed values, tax levy, zoning, neighbourhood, assessment/report year |
| `silver_building_permit` | one row per `building_permit_key` | Permit type/class, project description, cost, issue date, permit point geometry |
| `silver_flood_hazard_zone` | one row per source flood-zone polygon | Vancouver flood-zone/scenario geometry |

### Gold serving outputs

| Serving table | Grain | Provides |
|---|---|---|
| `gold_vancouver_land_coordinate_assessment` | `source_land_coordinate × report_year` | Aggregated land/improvement/total assessed value, tax levy, folio/PID counts, zoning and neighbourhood context |
| `gold_vancouver_parcel_assessment_context` | one row per `property_parcel_key` | Latest assessment context attached to the current parcel universe, with explicit exact-vs-ambiguous mapping flags |
| `gold_vancouver_parcel_flood_exposure` | one row per `property_parcel_key` | Parcel flood-exposure flag plus scenario-specific overlap area/ratio for Designated Floodplain, Fraser Risk Today, Still Creek Floodplain and Wave Effect Zone |
| `gold_vancouver_building_permit_context` | one row per `building_permit_key` | Permit/housing classification plus exact parcel, flood and assessment context |

Current serving row counts:

- `gold_vancouver_land_coordinate_assessment`: 655,171
- `gold_vancouver_parcel_assessment_context`: 99,726
- `gold_vancouver_parcel_flood_exposure`: 99,726
- `gold_vancouver_building_permit_context`: 50,610

### Core transformation and join logic

**Property tax → land-coordinate assessment**

```text
silver_property_tax_assessment
    GROUP BY source_land_coordinate, report_year
        ↓
gold_vancouver_land_coordinate_assessment
```

Assessment values are summed at land-coordinate/year grain. Categorical fields are retained only when the group has a single unique value; ambiguity is explicitly flagged.

**Parcel ↔ property tax**

```text
silver_property_parcel.source_tax_coord
    =
silver_property_tax_assessment.source_land_coordinate
```

The relationship is materialized through `gold_vancouver_property_parcel_bridge`.

Bridge grain:

```text
source_land_coordinate × property_parcel_key
```

One land coordinate may map to multiple parcels. These mappings are retained as ambiguous; assessed value is not arbitrarily split across parcels.

`gold_vancouver_parcel_assessment_context` joins:

```text
parcel
    → bridge on property_parcel_key
    → latest land-coordinate assessment on source_land_coordinate
```

Land-coordinate values remain available for ambiguous mappings. Exact parcel-level assessed-value fields are populated only when one land coordinate maps to exactly one parcel.

**Parcel → flood exposure**

Parcel polygons and flood-zone polygons are spatially intersected in `EPSG:3347`. Only positive-area intersections are retained. Source zones are rolled up to the four Vancouver flood scenarios and then pivoted to one row per parcel.

```text
silver_property_parcel.geometry
    ×
silver_flood_hazard_zone.geometry
        ↓
parcel-zone overlay
        ↓
parcel-scenario exposure
        ↓
gold_vancouver_parcel_flood_exposure
```

All parcels are retained, including parcels with no flood overlap.

**Building permit → parcel context**

Permit point geometry is spatially matched to the current parcel polygons.

```text
silver_building_permit.geometry_wkt
    → spatial match
silver_property_parcel.geometry_wkt
    → property_parcel_key
    → flood + assessment serving tables
```

Only exact one-to-one parcel matches receive parcel-level context; ambiguous or unmatched permits remain in the serving table without forced assignment. Housing-related permits are identified from the exact `Dwelling Uses` token in `permit_class_group`.

---

## Calgary

### Silver inputs

| Silver table | Grain | Main content |
|---|---|---|
| `silver_property_assessment` | source assessment/unit record | `source_parcel_id`, assessed values, property/community/land-use attributes, property geometry |
| `silver_building_permit` | one row per `building_permit_key` | Permit classification, work class, housing units, project cost, permit point geometry |
| `silver_development_permit` | one row per `development_permit_key` | Development status/use/land-use fields plus single and multi-location spatial information |
| `silver_flood_hazard_zone` | one row per source flood-zone polygon | Calgary Flood Fringe, Floodplain, Floodway, Normal River Channel and Overland Flow geometry |

### Gold serving outputs

| Serving table | Grain | Provides |
|---|---|---|
| `gold_calgary_property_location_assessment` | `source_parcel_id × assessment_year` | Consolidated property-location assessment values and canonical property/community/land-use context |
| `gold_calgary_property_location_flood_exposure` | one row per `source_parcel_id` | Property-location flood flags and per-class overlap metrics |
| `gold_calgary_building_permit_context` | one row per `building_permit_key` | Building-permit/housing metrics plus exact property assessment and flood context |
| `gold_calgary_development_permit_context` | one row per `development_permit_key` | Development-permit attributes plus mapped-property counts, aggregated assessment context and flood exposure |

Current serving row counts:

- `gold_calgary_property_location_assessment`: 410,049
- `gold_calgary_property_location_flood_exposure`: 410,049
- `gold_calgary_building_permit_context`: 489,276
- `gold_calgary_development_permit_context`: 190,399

### Core transformation and join logic

**Property assessment → property location**

Multiple Calgary assessment/unit rows can share the same `source_parcel_id`, especially condominium/unit records.

```text
silver_property_assessment
    GROUP BY source_parcel_id, assessment_year
        ↓
gold_calgary_property_location_assessment
```

Assessment values and row counts are aggregated. Property/community/land-use attributes are retained only when single-valued within the location/year group.

The current input contains assessment year 2026 only, so the current serving output is one row per `source_parcel_id`.

**Property location → flood exposure**

Property-location polygons are spatially intersected with Calgary flood polygons in `EPSG:3347`.

```text
gold_calgary_property_location_assessment.geometry
    ×
silver_flood_hazard_zone.geometry
        ↓
gold_calgary_property_location_flood_overlay
        ↓
gold_calgary_property_location_flood_exposure
```

Flood semantics:

```text
Flood Fringe          → flood exposure
Floodplain            → flood exposure
Floodway               → flood exposure
Overland Flow          → flood exposure
Normal River Channel  → retained as spatial context, not flood exposure
```

Per-class intersection geometries are unioned before final overlap area/ratio calculation so overlapping source polygons are not double counted.

**Building permit → property location**

Permit points are spatially matched to current Calgary property-location polygons.

```text
silver_building_permit.geometry_wkt
    → spatial match
gold_calgary_property_location_assessment.geometry_wkt
    → source_parcel_id
    → assessment + flood context
```

Only exact one-to-one matches receive `source_parcel_id`; ambiguous and unmatched permits remain unassigned.

Housing semantics are kept separate:

```text
is_residential_permit
    = permit_class_mapped == "Residential"

creates_new_housing_units
    = housing_units > 0

is_housing_related
    = is_residential_permit OR creates_new_housing_units
```

Negative `housing_units` values are retained as anomalies but excluded from the derived new-housing-unit total.

**Development permit → property locations**

A Calgary Development Permit can contain multiple source locations. `geometry_wkt` is therefore not used as the sole representative location.

```text
silver_development_permit.locations_wkt
    → explode Point / MultiPoint
    → deduplicate coordinates
    → spatially match each unique point
      to current property polygons
        ↓
gold_calgary_development_permit_location_bridge
```

Bridge grain:

```text
development_permit_key × source_parcel_id
```

The bridge is then rolled back to one permit:

```text
development permit
    → bridge on development_permit_key
    → assessment/flood context on source_parcel_id
    → gold_calgary_development_permit_context
```

A permit may map to one property, multiple properties, no property, or only partially map. These cases remain explicit rather than forcing a one-to-one relationship.

---

## Final Phase D serving structure

```text
Vancouver
├── gold_vancouver_land_coordinate_assessment
├── gold_vancouver_parcel_assessment_context
├── gold_vancouver_parcel_flood_exposure
└── gold_vancouver_building_permit_context

Calgary
├── gold_calgary_property_location_assessment
├── gold_calgary_property_location_flood_exposure
├── gold_calgary_building_permit_context
└── gold_calgary_development_permit_context
```

Phase D closes with **8 city-level serving tables**: four for Vancouver and four for Calgary. Supporting bridge/overlay tables remain available for lineage, spatial relationships and auditability, but are not the primary downstream serving interface.
