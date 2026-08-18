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

interface CityActivityLayerProps {
  scope: CityScope;
  kind: CityActivityKind;
  visible: boolean;
  beforeId: string | null;
  viewportBounds: ViewportBounds;
  selectedActivityKey: string | null;
  onActivitySelect: (key: string, feature: CityFeature) => void;
  infoVisible: boolean;
  interactionEnabled: boolean;
  // See CitySpatialLayer — reports layers up to MapCanvas's single shared
  // overlay instead of owning its own DeckGLOverlay/MapboxOverlay.
  onLayersChange: (layers: LayersList) => void;
}

interface HoverState {
  x: number;
  y: number;
  activityKey: string;
  feature: CityFeature;
}

type InterleavedLayerProps = {
  beforeId?: string;
};

const MAX_GEOMETRY_CACHE_SIZE = 6;

type RgbaColor = [number, number, number, number];

const ACTIVITY_COLORS = {
  housingBuildingPermit: [246, 148, 60, 210] as RgbaColor,
  otherBuildingPermit: [148, 156, 163, 170] as RgbaColor,
  developmentPermit: [168, 123, 219, 210] as RgbaColor,
  hoverRing: [255, 255, 255, 235] as RgbaColor,
};

interface GeometryState {
  bounds: ViewportBounds;
  features: CityFeature[];
}

interface PendingGeometryRequest {
  bounds: ViewportBounds;
  activeLayer: CityLayerManifest;
}

function stringProperty(feature: CityFeature, name: string): string | null {
  const value = feature.properties?.[name];

  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function numberProperty(feature: CityFeature, name: string): number | null {
  const value = feature.properties?.[name];

  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function booleanProperty(feature: CityFeature, name: string): boolean {
  const value = feature.properties?.[name];

  return value === true || value === 1;
}

function isHousingRelated(feature: CityFeature): boolean {
  return booleanProperty(feature, "is_housing_related");
}

function isFloodExposed(feature: CityFeature): boolean {
  return booleanProperty(feature, "is_flood_exposed");
}

function kindLabel(kind: CityActivityKind): string {
  return kind === "development_permits" ? "Development Permit" : "Building Permit";
}

function formatSnakeCase(value: string): string {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatDate(raw: string): string | null {
  const parsed = new Date(raw);

  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return new Intl.DateTimeFormat("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(parsed);
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-CA", {
    style: "currency",
    currency: "CAD",
    maximumFractionDigits: 0,
  }).format(value);
}

function activityTitle(feature: CityFeature): string {
  return (
    stringProperty(feature, "address_text") ??
    stringProperty(feature, "project_description") ??
    stringProperty(feature, "description") ??
    stringProperty(feature, "category") ??
    "Unnamed permit"
  );
}

function permitNumberOf(feature: CityFeature): string | null {
  return stringProperty(feature, "permit_number");
}

function housingActivityOf(feature: CityFeature): string | null {
  if (!isHousingRelated(feature)) {
    return null;
  }

  const activityType = stringProperty(feature, "housing_activity_type");

  return activityType ? formatSnakeCase(activityType) : "Housing-related";
}

function proposedUseOf(feature: CityFeature): string | null {
  return stringProperty(feature, "proposed_use_description");
}

function statusOf(feature: CityFeature): string | null {
  const status =
    stringProperty(feature, "permit_status") ??
    stringProperty(feature, "status_current");

  return status ? formatSnakeCase(status) : null;
}

function issueDateOf(kind: CityActivityKind, feature: CityFeature): string | null {
  const raw =
    kind === "development_permits"
      ? stringProperty(feature, "applied_date") ??
        stringProperty(feature, "decision_date")
      : stringProperty(feature, "issue_date");

  return raw ? formatDate(raw) : null;
}

function estimatedCostOf(feature: CityFeature): string | null {
  const value = numberProperty(feature, "estimated_project_cost");

  return value === null ? null : formatCurrency(value);
}

function mappedPropertiesOf(feature: CityFeature): number | null {
  return numberProperty(feature, "mapped_property_location_count");
}

function resolveActivityLayer(
  manifest: CityManifest | null,
  scope: CityScope,
  kind: CityActivityKind,
): CityLayerManifest | null {
  if (!manifest) {
    return null;
  }

  return manifest.cities[scope]?.[kind] ?? null;
}

function resolveActivityKey(
  layer: CityLayerManifest | null,
  properties: CityFeatureProperties,
  fallbackIndex: number,
): string {
  const value = layer ? properties[layer.key] : undefined;

  if (value !== undefined && value !== null && value !== "") {
    return String(value);
  }

  return `activity-${fallbackIndex}`;
}

// Direct key lookup for click/selection matching — unlike resolveActivityKey
// (used for hover debouncing) this has no per-render fallback index, since a
// selected feature must resolve to the same key across viewport reloads.
function activityFeatureKey(
  feature: CityFeature,
  keyField: string,
): string | null {
  const value = feature.properties?.[keyField];

  if (value === null || value === undefined) {
    return null;
  }

  return String(value);
}

function tooltipPosition(x: number, y: number) {
  const width = 190;
  const height = 150;
  const gap = 14;

  return {
    left: x + width + gap > window.innerWidth ? x - width - gap : x + gap,
    top: y + height + gap > window.innerHeight ? y - height - gap : y + gap,
  };
}

export function CityActivityLayer({
  scope,
  kind,
  visible,
  beforeId,
  viewportBounds,
  selectedActivityKey,
  onActivitySelect,
  infoVisible,
  interactionEnabled,
  onLayersChange,
}: CityActivityLayerProps) {
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
  const hoverActivityKeyRef = useRef<string | null>(null);
  const infoVisibleRef = useRef(infoVisible);
  const geometryStateRef = useRef<GeometryState | null>(null);
  const geometryCacheRef = useRef(new Map<string, GeometryState>());
  const geometryLoadInFlightRef = useRef(false);
  const pendingGeometryRequestRef = useRef<PendingGeometryRequest | null>(null);
  const visibleRef = useRef(visible);
  const unmountedRef = useRef(false);
  const activityKeysRef = useRef(new WeakMap<CityFeature, string>());

  const layer = useMemo(
    () => resolveActivityLayer(manifest, scope, kind),
    [manifest, scope, kind],
  );

  function clearHoverTimer() {
    if (hoverTimerRef.current) {
      window.clearTimeout(hoverTimerRef.current);
      hoverTimerRef.current = null;
    }
  }

  function activityKeyFor(feature: CityFeature, index: number): string {
    const cached = activityKeysRef.current.get(feature);

    if (cached) {
      return cached;
    }

    const key = resolveActivityKey(layer, feature.properties ?? {}, index);

    activityKeysRef.current.set(feature, key);

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
        console.error("Failed to load city activity geometry", error);

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
  }, [scope, kind]);

  useEffect(() => {
    // Skip the fetch while toggled off — BP/DP default OFF specifically to
    // avoid pulling Calgary's 269MB/119MB layers until the user asks for
    // them. Re-included in deps so flipping visible back on resumes (cache
    // hit if the viewport was already loaded, fresh fetch otherwise).
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
      hoverActivityKeyRef.current = null;
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
      hoverActivityKeyRef.current = null;
      setHoverFeature(null);
      setHover(null);
    }
  }, [visible]);

  const geometry = geometryState?.features ?? [];

  const selectedFeature = useMemo(() => {
    if (!selectedActivityKey || !layer) {
      return null;
    }

    return (
      geometry.find(
        (feature) =>
          activityFeatureKey(feature, layer.key) === selectedActivityKey,
      ) ?? null
    );
  }, [geometry, layer, selectedActivityKey]);

  const layers = useMemo(() => {
    // The component (and its underlying MapboxOverlay control) stays mounted
    // continuously while in city scope — only the rendered layer list toggles
    // with `visible`. Unmounting/remounting per-toggle was corrupting deck.gl's
    // layer-group bookkeeping for the other simultaneously-mounted city
    // overlays (Property/BP/DP each own a separate interleaved MapboxOverlay).
    if (!visible || geometry.length === 0) {
      return [];
    }

    const layerPlacement = beforeId
      ? {
          beforeId,
        }
      : {};

    const baseLayer = new GeoJsonLayer<CityFeatureProperties>({
      id: `city-activity-${scope}-${kind}-points`,
      data: geometry,
      ...layerPlacement,
      pointType: "circle",
      filled: true,
      stroked: false,
      pickable: interactionEnabled,
      // World-space (meters) radius so points scale with zoom like real
      // ground features, instead of staying a fixed pixel size — clamped so
      // they don't vanish zoomed out or balloon zoomed in.
      pointRadiusUnits: "meters",
      getPointRadius: 6,
      pointRadiusMinPixels: 2,
      pointRadiusMaxPixels: 12,
      getFillColor: (feature: CityFeature) => {
        if (kind === "development_permits") {
          return ACTIVITY_COLORS.developmentPermit;
        }

        return isHousingRelated(feature)
          ? ACTIVITY_COLORS.housingBuildingPermit
          : ACTIVITY_COLORS.otherBuildingPermit;
      },
      onClick: (info) => {
        const feature = info.object as CityFeature | null;

        if (!feature || !layer) {
          return;
        }

        const key = activityFeatureKey(feature, layer.key);

        if (!key) {
          return;
        }

        onActivitySelect(key, feature);
      },
      onHover: (info) => {
        const feature = info.object as CityFeature | null;

        if (!feature) {
          clearHoverTimer();
          hoverCandidateRef.current = null;
          hoverActivityKeyRef.current = null;
          setHoverFeature(null);
          setHover(null);
          return;
        }

        const activityKey = activityKeyFor(feature, info.index ?? 0);

        if (hoverActivityKeyRef.current !== activityKey) {
          hoverActivityKeyRef.current = activityKey;
          setHoverFeature(feature);
        }

        clearHoverTimer();
        hoverCandidateRef.current = activityKey;
        setHover(null);

        if (!infoVisible) {
          return;
        }

        hoverTimerRef.current = window.setTimeout(() => {
          if (
            !infoVisibleRef.current ||
            hoverCandidateRef.current !== activityKey
          ) {
            return;
          }

          setHover({
            x: info.x,
            y: info.y,
            activityKey,
            feature,
          });

          hoverTimerRef.current = null;
        }, 300);
      },
      updateTriggers: {
        getFillColor: [kind],
      },
    } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
      InterleavedLayerProps);

    const hoverLayer = hoverFeature
      ? new GeoJsonLayer<CityFeatureProperties>({
          id: `city-activity-${scope}-${kind}-hover`,
          data: [hoverFeature],
          ...layerPlacement,
          pointType: "circle",
          filled: false,
          stroked: true,
          pickable: false,
          // Matches the base dot's radius exactly so the ring traces its
          // outline at any zoom.
          pointRadiusUnits: "meters",
          getPointRadius: 6,
          pointRadiusMinPixels: 2,
          pointRadiusMaxPixels: 12,
          lineWidthUnits: "pixels",
          lineWidthMinPixels: 1.6,
          getLineColor: ACTIVITY_COLORS.hoverRing,
          getLineWidth: 1.6,
        } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
          InterleavedLayerProps)
      : null;

    const selectedLayer = selectedFeature
      ? new GeoJsonLayer<CityFeatureProperties>({
          id: `city-activity-${scope}-${kind}-selected`,
          data: [selectedFeature],
          ...layerPlacement,
          pointType: "circle",
          filled: false,
          stroked: true,
          pickable: false,
          // Visibly larger than the base dot at any zoom (9m vs 6m) so a
          // selected permit still reads as "highlighted", not just "hovered".
          pointRadiusUnits: "meters",
          getPointRadius: 9,
          pointRadiusMinPixels: 4,
          pointRadiusMaxPixels: 16,
          lineWidthUnits: "pixels",
          lineWidthMinPixels: 2.2,
          getLineColor: [255, 222, 160, 255],
          getLineWidth: 2.2,
        } as ConstructorParameters<typeof GeoJsonLayer<CityFeatureProperties>>[0] &
          InterleavedLayerProps)
      : null;

    return [baseLayer, hoverLayer, selectedLayer].filter(Boolean);
  }, [
    scope,
    kind,
    visible,
    geometry,
    beforeId,
    hoverFeature,
    selectedFeature,
    onActivitySelect,
    infoVisible,
    interactionEnabled,
  ]);

  const hoverPosition = hover ? tooltipPosition(hover.x, hover.y) : null;
  const permitNumber = hover ? permitNumberOf(hover.feature) : null;
  const housingActivity = hover ? housingActivityOf(hover.feature) : null;
  const proposedUse = hover ? proposedUseOf(hover.feature) : null;
  const status = hover ? statusOf(hover.feature) : null;
  const issueDate = hover ? issueDateOf(kind, hover.feature) : null;
  const estimatedCost = hover ? estimatedCostOf(hover.feature) : null;
  const mappedProperties = hover ? mappedPropertiesOf(hover.feature) : null;
  const floodExposed = hover ? isFloodExposed(hover.feature) : false;
  const isLoading =
    visible && (manifestLoading || (geometryLoading && geometry.length === 0));

  useEffect(() => {
    onLayersChange(layers);

    return () => {
      onLayersChange([]);
    };
  }, [layers, onLayersChange]);

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
              {scope === "vancouver" ? "Vancouver" : "Calgary"} ·{" "}
              {kindLabel(kind)}
            </div>

            <div className="tooltip-value">
              {activityTitle(
                hover.feature,
              )}
            </div>

            {permitNumber && (
              <div className="tooltip-grid">
                {permitNumber}
              </div>
            )}

            {housingActivity && (
              <div className="tooltip-grid">
                {housingActivity}
              </div>
            )}

            {proposedUse && (
              <div className="tooltip-grid">
                {proposedUse}
              </div>
            )}

            {status && (
              <div className="tooltip-grid">
                {status}
              </div>
            )}

            {issueDate && (
              <div className="tooltip-grid">
                Issued {issueDate}
              </div>
            )}

            {estimatedCost && (
              <div className="tooltip-grid">
                {estimatedCost}
              </div>
            )}

            {mappedProperties !==
              null && (
              <div className="tooltip-grid">
                {mappedProperties.toLocaleString(
                  "en-CA",
                )}{" "}
                mapped properties
              </div>
            )}

            <div className="tooltip-grid">
              {floodExposed
                ? "Flood-exposed location"
                : "No mapped flood exposure"}
            </div>
          </div>
        )}
    </>
  );
}
