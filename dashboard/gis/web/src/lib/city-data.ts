import { geojson as flatgeobuf } from "flatgeobuf";
import type { Feature, FeatureCollection, Geometry } from "geojson";

import { gisDataUrl } from "./gis-data-url";
import { padBounds, type ViewportBounds } from "./grid-geometry";

export const CITY_SCOPES = ["vancouver", "calgary"] as const;

export type CityScope = (typeof CITY_SCOPES)[number];

export type ExplorerScope = "national" | CityScope;

export function isCityScope(scope: ExplorerScope): scope is CityScope {
  return (CITY_SCOPES as readonly string[]).includes(scope);
}

export interface CityBounds {
  west: number;
  south: number;
  east: number;
  north: number;
}

export interface CityLayerManifest {
  file: string;
  format: string;
  source_table: string;
  key: string;
  geometry_type: string;
  crs: string;
  source_row_count: number;
  feature_count: number;
  non_spatial_count: number;
  geometry_repair_count: number;
  bounds: CityBounds;
  properties: string[];
  file_size_bytes: number;
}

export interface CityEntry {
  label: string;
  bounds: CityBounds;
  property_layer: CityLayerManifest;
  building_permits?: CityLayerManifest;
  development_permits?: CityLayerManifest;
}

export interface CityManifest {
  version: number;
  generated_at_utc: string;
  crs: string;
  cities: Record<CityScope, CityEntry>;
}

export interface CityFeatureProperties {
  [key: string]: unknown;
}

export type CityFeature = Feature<Geometry, CityFeatureProperties>;

export type CityActivityKind = "building_permits" | "development_permits";

// The single mutually-exclusive city map mode: property grid alone, property
// colored by flood exposure, property + one activity layer, or nothing.
// Flood/BP/DP never show together — matches the on-map legend 1:1.
export type CityLayerId = "property" | "flood" | CityActivityKind | "none";

export const CITY_LAYER_IDS: CityLayerId[] = [
  "property",
  "flood",
  "building_permits",
  "development_permits",
  "none",
];

export interface DevelopmentPermitPropertyLinksPayload {
  version: number;
  direction: string;
  permit_count: number;
  relationship_count: number;
  links: Record<string, string[]>;
}

let manifestCache: Promise<CityManifest> | null = null;
let developmentPermitLinksCache:
  | Promise<DevelopmentPermitPropertyLinksPayload>
  | null = null;

export function cityBoundsToMapBounds(
  bounds: CityBounds,
): [[number, number], [number, number]] {
  return [
    [bounds.west, bounds.south],
    [bounds.east, bounds.north],
  ];
}

export function loadCityManifest(): Promise<CityManifest> {
  if (!manifestCache) {
    // On failure, drop the cached (now-rejected) promise so the next call
    // retries the fetch instead of replaying the same rejection forever.
    manifestCache = fetch(gisDataUrl("cities/manifest.json"))
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(
            `Failed to load city GIS manifest: ${response.status}`,
          );
        }

        return (await response.json()) as CityManifest;
      })
      .catch((error) => {
        manifestCache = null;

        throw error;
      });
  }

  return manifestCache;
}

export async function loadCityFeatures(
  layer: CityLayerManifest,
  bounds: ViewportBounds,
  // Checked between features so a caller can stop an in-flight stream early
  // (component unmounted, layer toggled off) instead of paying for the
  // remaining range requests — some of these files run to hundreds of MB —
  // just to discard the result.
  shouldContinue?: () => boolean,
): Promise<FeatureCollection<Geometry, CityFeatureProperties>> {
  const features: CityFeature[] = [];
  const paddedBounds = padBounds(bounds);

  const iterator = flatgeobuf.deserialize(
    gisDataUrl(layer.file),
    paddedBounds,
  );

  for await (const feature of iterator) {
    if (shouldContinue && !shouldContinue()) {
      await iterator.return?.(undefined);
      break;
    }

    features.push(feature as CityFeature);
  }

  return {
    type: "FeatureCollection",
    features,
  };
}

// Calgary-only: DP -> property relationships for development permits mapped
// to more than one property. Fetched once (6.67 MB), then memory-cached —
// single-property DPs resolve from single_source_parcel_id instead, so most
// permits never trigger this fetch at all.
export function loadDevelopmentPermitPropertyLinks(): Promise<DevelopmentPermitPropertyLinksPayload> {
  if (!developmentPermitLinksCache) {
    // On failure, drop the cached (now-rejected) promise so the next call
    // retries the fetch instead of replaying the same rejection forever.
    developmentPermitLinksCache = fetch(
      gisDataUrl("cities/calgary/development_permit_property_links.json"),
    )
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(
            `Failed to load development permit property links: ${response.status}`,
          );
        }

        return (await response.json()) as DevelopmentPermitPropertyLinksPayload;
      })
      .catch((error) => {
        developmentPermitLinksCache = null;

        throw error;
      });
  }

  return developmentPermitLinksCache;
}

function activityFeatureStringProperty(
  feature: CityFeature,
  propertyName: string,
): string | null {
  const value = feature.properties?.[propertyName];

  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function activityFeatureNumberProperty(
  feature: CityFeature,
  propertyName: string,
): number | null {
  const value = feature.properties?.[propertyName];

  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

// Resolves the property (parcel) keys an activity feature (BP/DP) is linked
// to, for cross-grain highlighting. Only uses relationships already mapped
// in Gold — a permit with no property match (e.g. parcel_mapping_status =
// no_match) stays selected with zero related properties; we never guess a
// nearest parcel.
export async function resolveActivityPropertyKeys(
  scope: CityScope,
  kind: CityActivityKind,
  feature: CityFeature,
): Promise<string[]> {
  if (kind === "building_permits") {
    const propertyField =
      scope === "vancouver" ? "property_parcel_key" : "source_parcel_id";

    const propertyKey = activityFeatureStringProperty(feature, propertyField);

    return propertyKey ? [propertyKey] : [];
  }

  if (scope !== "calgary") {
    return [];
  }

  const mappedCount = activityFeatureNumberProperty(
    feature,
    "mapped_property_location_count",
  );

  if (mappedCount !== null && mappedCount <= 0) {
    return [];
  }

  const singlePropertyKey = activityFeatureStringProperty(
    feature,
    "single_source_parcel_id",
  );

  if (singlePropertyKey) {
    return [singlePropertyKey];
  }

  const developmentPermitKey = activityFeatureStringProperty(
    feature,
    "development_permit_key",
  );

  if (!developmentPermitKey) {
    return [];
  }

  const relationship = await loadDevelopmentPermitPropertyLinks();

  return relationship.links[developmentPermitKey] ?? [];
}
