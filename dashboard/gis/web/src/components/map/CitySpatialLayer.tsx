import { useEffect, useMemo, useRef, useState } from "react";
import type { LayersList } from "@deck.gl/core";
import { GeoJsonLayer } from "@deck.gl/layers";
import { AnimatePresence, motion } from "motion/react";

import {
  loadCityFeatures,
  loadCityManifest,
  type CityActivityKind,
  type CityFeature,
  type CityFeatureProperties,
  type CityLayerManifest,
  type CityManifest,
  type CityScope,
} from "../../lib/city-data";
import {
  boundsKey,
  containsBounds,
  padBounds,
  type ViewportBounds,
} from "../../lib/grid-geometry";
interface CitySpatialLayerProps {
  scope: CityScope;
  visible: boolean;
  // Whether flood-exposure coloring/tooltip info applies. When false, every
  // parcel renders with the same neutral style regardless of is_flood_exposed
  // — the "pure property grid" layer mode carries no flood semantics at all.
  showFlood: boolean;
  beforeId: string | null;
  viewportBounds: ViewportBounds;
  selectedFeatureKey: string | null;
  onFeatureSelect: (featureKey: string, feature: CityFeature) => void;
  relatedFeatureKeys: string[];
  relatedFeatureKind: CityActivityKind | null;
  infoVisible: boolean;
  interactionEnabled: boolean;
  // Reports this component's deck.gl layers up to MapCanvas instead of
  // owning its own DeckGLOverlay/MapboxOverlay. Multiple simultaneous
  // interleaved MapboxOverlay instances on one map (Property + BP + DP each
  // with their own) turned out to silently corrupt each other's rendering —
  // confirmed by isolating Property alone, where flood fill rendered fine.
  // One shared overlay per city map (assembled in MapCanvas) avoids that.
  onLayersChange: (layers: LayersList) => void;
}

interface HoverState {
  x: number;
  y: number;
  featureKey: string;
  feature: CityFeature;
}

type InterleavedLayerProps = {
  beforeId?: string;
};

const MAX_GEOMETRY_CACHE_SIZE = 6;

type RgbaColor = [number, number, number, number];

const CITY_COLORS = {
  neutralFill: [82, 111, 124, 8] as RgbaColor,
  neutralLine: [151, 171, 180, 42] as RgbaColor,
  exposedFill: [53, 183, 205, 58] as RgbaColor,
  exposedLine: [105, 219, 236, 185] as RgbaColor,
  contextualFill: [108, 128, 152, 24] as RgbaColor,
  contextualLine: [147, 169, 194, 135] as RgbaColor,
  hoverLine: [232, 242, 245, 240] as RgbaColor,
  selectedLine: [255, 196, 108, 255] as RgbaColor,
};

interface GeometryState {
  bounds: ViewportBounds;
  features: CityFeature[];
}

interface PendingGeometryRequest {
  bounds: ViewportBounds;
  activeLayer: CityLayerManifest;
}

// The manifest carries the feature-key field name per city (property_parcel_key
// for Vancouver parcels, source_parcel_id for Calgary properties) — resolve
// keys from that instead of guessing field names.
const NAME_FIELD_CANDIDATES = ["address_text", "community_name", "property_type"];

function booleanProperty(feature: CityFeature, propertyName: string): boolean {
  const value = feature.properties?.[propertyName];

  return value === true || value === 1;
}

function numericProperty(
  feature: CityFeature,
  propertyName: string,
): number | null {
  const value = feature.properties?.[propertyName];

  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function isFloodExposed(feature: CityFeature): boolean {
  return booleanProperty(feature, "is_flood_exposed");
}

function isNormalRiverChannelOnly(
  scope: CityScope,
  feature: CityFeature,
): boolean {
  if (scope !== "calgary" || isFloodExposed(feature)) {
    return false;
  }

  return booleanProperty(feature, "intersects_normal_river_channel");
}

// Vancouver has no municipal flood status distinct from is_flood_exposed;
// Calgary keeps Normal River Channel as contextual-only per Phase D.
function floodStatusLabel(scope: CityScope, feature: CityFeature): string {
  if (isFloodExposed(feature)) {
    return scope === "vancouver"
      ? "Mapped flood exposure"
      : "Regulatory flood exposure";
  }

  if (isNormalRiverChannelOnly(scope, feature)) {
    return "Normal River Channel context";
  }

  return "No mapped flood exposure";
}

function floodMembershipCount(
  scope: CityScope,
  feature: CityFeature,
): number | null {
  return numericProperty(
    feature,
    scope === "vancouver" ? "scenario_count" : "flood_zone_membership_count",
  );
}

function resolveFeatureKey(
  scope: CityScope,
  layer: CityLayerManifest | null,
  properties: CityFeatureProperties,
  fallbackIndex: number,
): string {
  const value = layer ? properties[layer.key] : undefined;

  if (value !== undefined && value !== null && value !== "") {
    return String(value);
  }

  return `${scope}-${fallbackIndex}`;
}

// Direct key lookup for click/selection matching — unlike resolveFeatureKey
// (used for the per-render hover/cache key) this has no index fallback,
// since a selected feature must resolve to the same key across viewport
// reloads. A parcel missing its manifest key field can't be selected by key
// at all (mirrors CityActivityLayer's activityFeatureKey for BP/DP).
function stableFeatureKey(
  layer: CityLayerManifest | null,
  properties: CityFeatureProperties,
): string | null {
  const value = layer ? properties[layer.key] : undefined;

  if (value === undefined || value === null || value === "") {
    return null;
  }

  return String(value);
}

function propertyDisplayName(
  scope: CityScope,
  layer: CityLayerManifest | null,
  feature: CityFeature,
): string {
  const properties = feature.properties ?? {};

  for (const field of NAME_FIELD_CANDIDATES) {
    const value = properties[field];

    if (typeof value === "string" && value.trim().length > 0) {
      return value;
    }
  }

  const keyValue = layer ? properties[layer.key] : null;

  if (keyValue !== undefined && keyValue !== null && keyValue !== "") {
    return String(keyValue);
  }

  return scope === "vancouver" ? "Unnamed parcel" : "Unnamed property";
}

function resolveCityLayer(
  manifest: CityManifest | null,
  scope: CityScope,
): CityLayerManifest | null {
  if (!manifest) {
    return null;
  }

  return manifest.cities[scope]?.property_layer ?? null;
}

function tooltipPosition(x: number, y: number) {
  const width = 170;
  const height = 110;
  const gap = 14;

  return {
    left: x + width + gap > window.innerWidth ? x - width - gap : x + gap,
    top: y + height + gap > window.innerHeight ? y - height - gap : y + gap,
  };
}

export function CitySpatialLayer({
  scope,
  visible,
  showFlood,
  beforeId,
  viewportBounds,
  selectedFeatureKey,
  onFeatureSelect,
  relatedFeatureKeys,
  relatedFeatureKind,
  infoVisible,
  interactionEnabled,
  onLayersChange,
}: CitySpatialLayerProps) {
  const [manifest, setManifest] = useState<CityManifest | null>(null);
  const [manifestLoading, setManifestLoading] = useState(false);
  const [geometryState, setGeometryState] = useState<GeometryState | null>(
    null,
  );
  const [geometryLoading, setGeometryLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const [hoverFeature, setHoverFeature] = useState<CityFeature | null>(null);
  const hoverTimerRef = useRef<number | null>(null);
  const hoverCandidateRef = useRef<string | null>(null);
  const hoverFeatureKeyRef = useRef<string | null>(null);
  const infoVisibleRef = useRef(infoVisible);
  const geometryStateRef = useRef<GeometryState | null>(null);
  const geometryCacheRef = useRef(new Map<string, GeometryState>());
  const geometryLoadInFlightRef = useRef(false);
  const pendingGeometryRequestRef = useRef<PendingGeometryRequest | null>(null);
  const visibleRef = useRef(visible);
  const unmountedRef = useRef(false);
  const featureKeysRef = useRef(new WeakMap<CityFeature, string>());

  const layer = useMemo(
    () => resolveCityLayer(manifest, scope),
    [manifest, scope],
  );

  function clearHoverTimer() {
    if (hoverTimerRef.current) {
      window.clearTimeout(hoverTimerRef.current);
      hoverTimerRef.current = null;
    }
  }

  function featureKeyFor(feature: CityFeature, index: number): string {
    const cached = featureKeysRef.current.get(feature);

    if (cached) {
      return cached;
    }

    const key = resolveFeatureKey(scope, layer, feature.properties ?? {}, index);

    featureKeysRef.current.set(feature, key);

    return key;
  }

  function setLoadedGeometry(nextGeometryState: GeometryState) {
    geometryStateRef.current = nextGeometryState;
    geometryCacheRef.current.set(
      boundsKey(nextGeometryState.bounds),
      nextGeometryState,
    );

    if (geometryCacheRef.current.size > MAX_GEOMETRY_CACHE_SIZE) {
      const oldestKey = geometryCacheRef.current.keys().next().value;

      if (oldestKey) {
        geometryCacheRef.current.delete(oldestKey);
      }
    }

    setGeometryState(nextGeometryState);
  }

  function findCachedGeometry(bounds: ViewportBounds): GeometryState | null {
    for (const cachedGeometry of geometryCacheRef.current.values()) {
      if (
        cachedGeometry.features.length > 0 &&
        containsBounds(cachedGeometry.bounds, bounds)
      ) {
        return cachedGeometry;
      }
    }

    return null;
  }

  function requestGeometry(bounds: ViewportBounds, activeLayer: CityLayerManifest) {
    const currentGeometry = geometryStateRef.current;

    if (
      currentGeometry &&
      currentGeometry.features.length > 0 &&
      containsBounds(currentGeometry.bounds, bounds)
    ) {
      setLoadError(null);
      setGeometryLoading(false);
      return;
    }

    const cachedGeometry = findCachedGeometry(bounds);

    if (cachedGeometry) {
      setLoadedGeometry(cachedGeometry);
      setLoadError(null);
      setGeometryLoading(false);
      return;
    }

    if (geometryLoadInFlightRef.current) {
      pendingGeometryRequestRef.current = {
        bounds,
        activeLayer,
      };
      setGeometryLoading(true);
      return;
    }

    geometryLoadInFlightRef.current = true;
    setGeometryLoading(true);
    setLoadError(null);

    loadCityFeatures(activeLayer, bounds, () => !unmountedRef.current)
      .then((collection) => {
        const loadedGeometry = {
          bounds: padBounds(bounds),
          features: collection.features as CityFeature[],
        };

        geometryCacheRef.current.set(
          boundsKey(loadedGeometry.bounds),
          loadedGeometry,
        );

        if (!unmountedRef.current) {
          setLoadedGeometry(loadedGeometry);
          setLoadError(null);
        }
      })
      .catch((error) => {
        console.error("Failed to load city viewport geometry", error);

        if (
          !unmountedRef.current &&
          visibleRef.current &&
          !pendingGeometryRequestRef.current
        ) {
          setLoadError("This area could not be loaded.");
        }
      })
      .finally(() => {
        geometryLoadInFlightRef.current = false;

        const pendingRequest = pendingGeometryRequestRef.current;
        pendingGeometryRequestRef.current = null;

        if (unmountedRef.current) {
          return;
        }

        if (pendingRequest && visibleRef.current) {
          requestGeometry(
            pendingRequest.bounds,
            pendingRequest.activeLayer,
          );
          return;
        }

        setGeometryLoading(false);
      });
  }

  useEffect(() => {
    unmountedRef.current = false;
    setLoadError(null);
    setManifestLoading(true);

    loadCityManifest()
      .then((loaded) => {
        if (!unmountedRef.current) {
          setManifest(loaded);
        }
      })
      .catch((error) => {
        console.error("Failed to load city manifest", error);

        if (!unmountedRef.current) {
          setLoadError("City data could not be loaded.");
        }
      })
      .finally(() => {
        if (!unmountedRef.current) {
          setManifestLoading(false);
        }
      });

    return () => {
      unmountedRef.current = true;
      clearHoverTimer();
    };
  }, [scope]);

  useEffect(() => {
    // Skip the fetch while toggled off (same reasoning as CityActivityLayer):
    // re-included in deps so flipping visible back on resumes correctly.
    if (!layer || !visible) {
      return;
    }

    requestGeometry(viewportBounds, layer);
  }, [
    layer,
    visible,
    viewportBounds.minX,
    viewportBounds.minY,
    viewportBounds.maxX,
    viewportBounds.maxY,
  ]);

  useEffect(() => {
    infoVisibleRef.current = infoVisible;

    if (!infoVisible) {
      clearHoverTimer();
      hoverCandidateRef.current = null;
      setHover(null);
    }
  }, [infoVisible]);

  useEffect(() => {
    if (!interactionEnabled) {
      clearHoverTimer();
      hoverCandidateRef.current = null;
      hoverFeatureKeyRef.current = null;
      setHoverFeature(null);
      setHover(null);
    }
  }, [interactionEnabled]);

  useEffect(() => {
    visibleRef.current = visible;

    if (!visible) {
      pendingGeometryRequestRef.current = null;
      setGeometryLoading(false);
      setLoadError(null);
      clearHoverTimer();
      hoverCandidateRef.current = null;
      hoverFeatureKeyRef.current = null;
      setHoverFeature(null);
      setHover(null);
    }
  }, [visible]);

  const geometry = geometryState?.features ?? [];

  const selectedFeature = useMemo(
    () =>
      selectedFeatureKey
        ? geometry.find(
            (feature) =>
              stableFeatureKey(layer, feature.properties ?? {}) ===
              selectedFeatureKey,
          ) ?? null
        : null,
    [
      geometry,
      layer,
      selectedFeatureKey,
    ],
  );

  const relatedFeatureKeySet = useMemo(
    () => new Set(relatedFeatureKeys),
    [relatedFeatureKeys],
  );

  // Cross-grain highlight: properties linked to the selected BP/DP activity.
  // Only highlights whatever is already loaded in the current viewport —
  // panning to a related property elsewhere picks it up automatically via
  // this same set, without forcing a city-wide preload.
  const relatedFeatures = useMemo(() => {
    if (!layer || relatedFeatureKeySet.size === 0) {
      return [];
    }

    return geometry.filter((feature) => {
      const key = stableFeatureKey(layer, feature.properties ?? {});

      return key !== null && relatedFeatureKeySet.has(key);
    });
  }, [geometry, layer, relatedFeatureKeySet]);

  const layers = useMemo(() => {
    if (!visible || geometry.length === 0) {
      return [];
    }

    const layerPlacement = beforeId
      ? {
          beforeId,
        }
      : {};

    const baseLayer = new GeoJsonLayer<CityFeatureProperties>({
      id: `city-spatial-${scope}-fill`,
      data: geometry,
      ...layerPlacement,
      filled: true,
      stroked: true,
      pickable: interactionEnabled,
      lineWidthUnits: "pixels",
      lineWidthMinPixels: 0.6,
      getFillColor: (feature: CityFeature) => {
        if (showFlood) {
          if (isFloodExposed(feature)) {
            return CITY_COLORS.exposedFill;
          }

          if (isNormalRiverChannelOnly(scope, feature)) {
            return CITY_COLORS.contextualFill;
          }

          return CITY_COLORS.neutralFill;
        }

        return CITY_COLORS.neutralFill;
      },
      getLineColor: (feature: CityFeature) => {
        if (showFlood) {
          if (isFloodExposed(feature)) {
            return CITY_COLORS.exposedLine;
          }

          if (isNormalRiverChannelOnly(scope, feature)) {
            return CITY_COLORS.contextualLine;
          }

          return CITY_COLORS.neutralLine;
        }

        return CITY_COLORS.neutralLine;
      },
      getLineWidth: (feature: CityFeature) => {
        if (showFlood) {
          if (isFloodExposed(feature)) {
            return 0.8;
          }

          if (isNormalRiverChannelOnly(scope, feature)) {
            return 0.55;
          }

          return 0.35;
        }

        return 0.35;
      },
      onClick: (info) => {
        const feature = info.object as CityFeature | null;

        if (!feature) {
          return;
        }

        const key = stableFeatureKey(layer, feature.properties ?? {});

        if (!key) {
          return;
        }

        onFeatureSelect(key, feature);
      },
      onHover: (info) => {
        const feature = info.object as CityFeature | null;

        if (!feature) {
          clearHoverTimer();
          hoverCandidateRef.current = null;
          hoverFeatureKeyRef.current = null;
          setHoverFeature(null);
          setHover(null);
          return;
        }

        const featureKey = featureKeyFor(feature, info.index ?? 0);

        if (hoverFeatureKeyRef.current !== featureKey) {
          hoverFeatureKeyRef.current = featureKey;
          setHoverFeature(feature);
        }

        clearHoverTimer();
        hoverCandidateRef.current = featureKey;
        setHover(null);

        if (!infoVisible) {
          return;
        }

        hoverTimerRef.current = window.setTimeout(() => {
          if (
            !infoVisibleRef.current ||
            hoverCandidateRef.current !== featureKey
          ) {
            return;
          }

          setHover({
            x: info.x,
            y: info.y,
            featureKey,
            feature,
          });

          hoverTimerRef.current = null;
        }, 300);
      },
      updateTriggers: {
        getFillColor: [scope, showFlood],
        getLineColor: [scope, showFlood],
        getLineWidth: [scope, showFlood],
      },
    } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
      InterleavedLayerProps);

    // Cross-grain highlight from a selected BP/DP — a distinct, low-alpha
    // tint per activity kind so it never reads as flood exposure (cyan) or
    // direct property selection (amber).
    const relatedLayer =
      relatedFeatures.length > 0 && relatedFeatureKind
        ? new GeoJsonLayer<CityFeatureProperties>({
            id: `city-spatial-${scope}-related`,
            data: relatedFeatures,
            ...layerPlacement,
            filled: true,
            stroked: true,
            pickable: false,
            lineWidthUnits: "pixels",
            lineWidthMinPixels: 2.2,
            getFillColor:
              relatedFeatureKind === "development_permits"
                ? [184, 122, 232, 34]
                : [244, 166, 74, 34],
            getLineColor:
              relatedFeatureKind === "development_permits"
                ? [184, 122, 232, 210]
                : [244, 166, 74, 210],
            getLineWidth: 2.2,
          } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
            InterleavedLayerProps)
        : null;

    const hoverLayer = hoverFeature
      ? new GeoJsonLayer<CityFeatureProperties>({
          id: `city-spatial-${scope}-hover`,
          data: [hoverFeature],
          ...layerPlacement,
          filled: false,
          stroked: true,
          pickable: false,
          lineWidthUnits: "pixels",
          lineWidthMinPixels: 1.4,
          getLineColor: CITY_COLORS.hoverLine,
          getLineWidth: 1.4,
        } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
          InterleavedLayerProps)
      : null;

    const selectedLayer = selectedFeature
      ? new GeoJsonLayer<CityFeatureProperties>({
          id: `city-spatial-${scope}-selected`,
          data: [selectedFeature],
          ...layerPlacement,
          filled: false,
          stroked: true,
          pickable: false,
          lineWidthUnits: "pixels",
          lineWidthMinPixels: 2.4,
          getLineColor: CITY_COLORS.selectedLine,
          getLineWidth: 2.4,
        } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
          InterleavedLayerProps)
      : null;

    return [
      baseLayer,
      relatedLayer,
      hoverLayer,
      selectedLayer,
    ].filter(Boolean);
  }, [
    scope,
    visible,
    showFlood,
    geometry,
    beforeId,
    selectedFeature,
    relatedFeatures,
    relatedFeatureKind,
    hoverFeature,
    onFeatureSelect,
    infoVisible,
    interactionEnabled,
  ]);

  useEffect(() => {
    onLayersChange(layers);

    return () => {
      onLayersChange([]);
    };
  }, [layers, onLayersChange]);

  const hoverPosition = hover ? tooltipPosition(hover.x, hover.y) : null;
  const hoverKey = hover?.featureKey ?? null;
  const hoverFloodStatus =
    hover && showFlood ? floodStatusLabel(scope, hover.feature) : null;
  const hoverFloodCount =
    hover && showFlood ? floodMembershipCount(scope, hover.feature) : null;
  const isLoading =
    visible && (manifestLoading || (geometryLoading && geometry.length === 0));

  return (
    <>
      <AnimatePresence>
        {isLoading && (
          <motion.div
            className="map-loading-indicator"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <span />
          </motion.div>
        )}
      </AnimatePresence>

      <AnimatePresence>
        {visible && loadError && (
          <motion.div
            className="map-status-message glass-panel"
            initial={{
              opacity: 0,
              y: -4,
            }}
            animate={{
              opacity: 1,
              y: 0,
            }}
            exit={{
              opacity: 0,
            }}
          >
            {loadError}
          </motion.div>
        )}
      </AnimatePresence>

      {infoVisible &&
        hover &&
        hoverPosition && (
          <div
            className="map-tooltip glass-panel"
            style={
              hoverPosition
            }
          >
            <div className="tooltip-eyebrow">
              {scope ===
              "vancouver"
                ? "Vancouver · Parcel"
                : "Calgary · Property"}
            </div>

            <div className="tooltip-value">
              {propertyDisplayName(
                scope,
                layer,
                hover.feature,
              )}
            </div>

            {hoverFloodStatus && (
              <div className="tooltip-grid">
                {hoverFloodStatus}
              </div>
            )}

            {hoverFloodCount !== null && hoverFloodCount > 0 && (
              <div className="tooltip-grid">
                {scope === "vancouver"
                  ? `${hoverFloodCount} flood scenario${
                      hoverFloodCount === 1 ? "" : "s"
                    }`
                  : `${hoverFloodCount} flood zone membership${
                      hoverFloodCount === 1 ? "" : "s"
                    }`}
              </div>
            )}

            {hoverKey && (
              <div className="tooltip-grid">
                {hoverKey}
              </div>
            )}
          </div>
        )}
    </>
  );
}
