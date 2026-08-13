import { useEffect, useMemo, useRef, useState } from "react";
import { GeoJsonLayer } from "@deck.gl/layers";
import { AnimatePresence, motion } from "motion/react";

import { DeckGLOverlay } from "./DeckGLOverlay";
import {
  getNumericValue,
  loadMonthlyDataset,
  type MonthlyDataset,
} from "../../lib/gis-data";
import {
  boundsKey,
  containsBounds,
  loadGridGeometry,
  padBounds,
  type GridFeature,
  type GridProperties,
  type ViewportBounds,
} from "../../lib/grid-geometry";
import {
  formatLayerValue,
  layerColor,
  resolveLayerDefinition,
  type LayerId,
} from "../../lib/layer-registry";

interface NationalRiskLayerProps {
  referenceMonth: string;
  activeLayerId: LayerId;
  beforeId: string | null;
  regionProvinceKey: string | null;
  selectedGridKey: string | null;
  onGridSelect: (gridCellKey: string) => void;
  viewportBounds: ViewportBounds;
  hideLayerFill: boolean;
  gridInfoVisible: boolean;
  gridInteractionEnabled: boolean;
}

interface HoverState {
  x: number;
  y: number;
  gridCellKey: string;
  provinceKey: string;
  value: number | null;
  feature: GridFeature;
}

type InterleavedLayerProps = {
  beforeId?: string;
};

const MAX_GEOMETRY_CACHE_SIZE = 6;

interface GeometryState {
  bounds: ViewportBounds;
  features: GridFeature[];
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

export function NationalRiskLayer({
  referenceMonth,
  activeLayerId,
  beforeId,
  regionProvinceKey,
  selectedGridKey,
  onGridSelect,
  viewportBounds,
  hideLayerFill,
  gridInfoVisible,
  gridInteractionEnabled,
}: NationalRiskLayerProps) {
  const [dataset, setDataset] = useState<MonthlyDataset | null>(null);
  const [geometryState, setGeometryState] = useState<GeometryState | null>(null);
  const [loading, setLoading] = useState(false);
  const [geometryLoading, setGeometryLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const [hoverFeature, setHoverFeature] = useState<GridFeature | null>(null);
  const hoverTimerRef = useRef<number | null>(null);
  const hoverCandidateRef = useRef<string | null>(null);
  const hoverFeatureKeyRef = useRef<string | null>(null);
  const gridInfoVisibleRef = useRef(gridInfoVisible);
  const geometryStateRef = useRef<GeometryState | null>(null);
  const geometryCacheRef = useRef(new Map<string, GeometryState>());
  const geometryLoadInFlightRef = useRef(false);
  const pendingViewportBoundsRef = useRef<ViewportBounds | null>(null);
  const unmountedRef = useRef(false);

  const definition = useMemo(
    () =>
      resolveLayerDefinition(
        activeLayerId,
        dataset?.displayStatistics,
      ),
    [
      activeLayerId,
      dataset?.displayStatistics,
    ],
  );

  function clearHoverTimer() {
    if (hoverTimerRef.current) {
      window.clearTimeout(hoverTimerRef.current);
      hoverTimerRef.current = null;
    }
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

  function requestGeometry(bounds: ViewportBounds) {
    const currentGeometry = geometryStateRef.current;

    if (
      currentGeometry &&
      currentGeometry.features.length > 0 &&
      containsBounds(currentGeometry.bounds, bounds)
    ) {
      setGeometryLoading(false);
      return;
    }

    const cachedGeometry = findCachedGeometry(bounds);

    if (cachedGeometry) {
      setLoadedGeometry(cachedGeometry);
      setGeometryLoading(false);
      return;
    }

    if (geometryLoadInFlightRef.current) {
      pendingViewportBoundsRef.current = bounds;
      setGeometryLoading(true);
      return;
    }

    geometryLoadInFlightRef.current = true;
    setGeometryLoading(true);

    loadGridGeometry(bounds)
      .then((collection) => {
        const loadedGeometry = {
          bounds: padBounds(bounds),
          features: collection.features as GridFeature[],
        };

        geometryCacheRef.current.set(
          boundsKey(loadedGeometry.bounds),
          loadedGeometry,
        );

        if (!unmountedRef.current) {
          setLoadedGeometry(loadedGeometry);
        }
      })
      .catch((error) => {
        console.error("Failed to load viewport geometry", error);
      })
      .finally(() => {
        geometryLoadInFlightRef.current = false;

        const pendingBounds = pendingViewportBoundsRef.current;
        pendingViewportBoundsRef.current = null;

        if (unmountedRef.current) {
          return;
        }

        if (pendingBounds) {
          requestGeometry(pendingBounds);
          return;
        }

        setGeometryLoading(false);
      });
  }

  useEffect(() => {
    let cancelled = false;

    setLoading(true);
    setLoadError(null);

    loadMonthlyDataset(referenceMonth)
      .then((loaded) => {
        if (!cancelled) {
          setDataset(loaded);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setLoadError("This month could not be loaded.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [referenceMonth]);

  useEffect(() => {
    unmountedRef.current = false;

    return () => {
      unmountedRef.current = true;
      clearHoverTimer();
    };
  }, []);

  useEffect(() => {
    requestGeometry(viewportBounds);
  }, [
    viewportBounds.minX,
    viewportBounds.minY,
    viewportBounds.maxX,
    viewportBounds.maxY,
  ]);

  useEffect(() => {
    gridInfoVisibleRef.current = gridInfoVisible;

    if (!gridInfoVisible) {
      clearHoverTimer();
      hoverCandidateRef.current = null;
      setHover(null);
    }
  }, [gridInfoVisible]);

  useEffect(() => {
    if (!gridInteractionEnabled) {
      clearHoverTimer();
      hoverCandidateRef.current = null;
      hoverFeatureKeyRef.current = null;
      setHoverFeature(null);
      setHover(null);
    }
  }, [gridInteractionEnabled]);

  const geometry = geometryState?.features ?? [];

  const selectedFeature = useMemo(
    () =>
      selectedGridKey
        ? geometry.find(
            (feature) => feature.properties.grid_cell_key === selectedGridKey,
          ) ?? null
        : null,
    [
      geometry,
      selectedGridKey,
    ],
  );

  const layers = useMemo(() => {
    if (!dataset || geometry.length === 0) {
      return [];
    }

    const layerPlacement = beforeId
      ? {
          beforeId,
        }
      : {};

    const baseLayer = new GeoJsonLayer<GridProperties>({
      id: "national-risk-grid-fill",
      data: geometry,
      ...layerPlacement,
      filled: true,
      stroked: false,
      pickable: gridInteractionEnabled,
      getFillColor: (feature) => {
        const properties = feature.properties;

        if (regionProvinceKey && properties.province_key !== regionProvinceKey) {
          return [0, 0, 0, 0];
        }

        if (hideLayerFill) {
          return [0, 0, 0, 0];
        }

        const value = getNumericValue(
          dataset,
          properties.grid_cell_key,
          definition.column,
        );

        return layerColor(definition, value);
      },
      onClick: (info) => {
        const feature = info.object;

        if (!feature) {
          return;
        }

        const properties = feature.properties;

        if (regionProvinceKey && properties.province_key !== regionProvinceKey) {
          return;
        }

        onGridSelect(properties.grid_cell_key);
      },
      onHover: (info) => {
        const feature = info.object as GridFeature | null;

        if (!feature) {
          clearHoverTimer();
          hoverCandidateRef.current = null;
          hoverFeatureKeyRef.current = null;
          setHoverFeature(null);
          setHover(null);
          return;
        }

        const properties = feature.properties;

        if (regionProvinceKey && properties.province_key !== regionProvinceKey) {
          clearHoverTimer();
          hoverCandidateRef.current = null;
          hoverFeatureKeyRef.current = null;
          setHoverFeature(null);
          setHover(null);
          return;
        }

        if (hoverFeatureKeyRef.current !== properties.grid_cell_key) {
          hoverFeatureKeyRef.current = properties.grid_cell_key;
          setHoverFeature(feature);
        }

        clearHoverTimer();
        hoverCandidateRef.current = properties.grid_cell_key;
        setHover(null);

        if (!gridInfoVisible) {
          return;
        }

        hoverTimerRef.current = window.setTimeout(() => {
          if (
            !gridInfoVisibleRef.current ||
            hoverCandidateRef.current !== properties.grid_cell_key
          ) {
            return;
          }

          const value = getNumericValue(
            dataset,
            properties.grid_cell_key,
            definition.column,
          );

          setHover({
            x: info.x,
            y: info.y,
            gridCellKey: properties.grid_cell_key,
            provinceKey: properties.province_key,
            value,
            feature,
          });

          hoverTimerRef.current = null;
        }, 300);
      },
      updateTriggers: {
        getFillColor: [
          dataset,
          definition,
          regionProvinceKey,
          hideLayerFill,
        ],
      },
    } as ConstructorParameters<typeof GeoJsonLayer<GridProperties>>[0] &
      InterleavedLayerProps);

    const hoverLayer = hoverFeature
      ? new GeoJsonLayer<GridProperties>({
          id: "national-risk-grid-hover",
          data: [hoverFeature],
          ...layerPlacement,
          filled: false,
          stroked: true,
          pickable: false,
          lineWidthUnits: "pixels",
          lineWidthMinPixels: 1.2,
          getLineColor: [44, 53, 49, 210],
          getLineWidth: 1.2,
        } as ConstructorParameters<typeof GeoJsonLayer<GridProperties>>[0] &
          InterleavedLayerProps)
      : null;

    const selectedLayer = selectedFeature
      ? new GeoJsonLayer<GridProperties>({
          id: "national-risk-grid-selected",
          data: [selectedFeature],
          ...layerPlacement,
          filled: false,
          stroked: true,
          pickable: false,
          lineWidthUnits: "pixels",
          lineWidthMinPixels: 2.4,
          getLineColor: [23, 31, 27, 255],
          getLineWidth: 2.4,
        } as ConstructorParameters<typeof GeoJsonLayer<GridProperties>>[0] &
          InterleavedLayerProps)
      : null;

    return [
      baseLayer,
      hoverLayer,
      selectedLayer,
    ].filter(Boolean);
  }, [
    dataset,
    geometry,
    beforeId,
    definition,
    regionProvinceKey,
    selectedFeature,
    hoverFeature,
    hover,
    onGridSelect,
    hideLayerFill,
    gridInfoVisible,
    gridInteractionEnabled,
  ]);

  const hoverPosition = hover ? tooltipPosition(hover.x, hover.y) : null;
  const isLoading = loading || (geometryLoading && geometry.length === 0);

  return (
    <>
      <DeckGLOverlay
        interleaved
        layers={layers}
        getCursor={({ isHovering }) =>
          gridInteractionEnabled && isHovering ? "pointer" : "grab"
        }
      />

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
        {loadError && (
          <motion.div
            className="map-status-message glass-panel"
            initial={{ opacity: 0, y: -4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }}
          >
            {loadError}
          </motion.div>
        )}
      </AnimatePresence>

      {gridInfoVisible && hover && hoverPosition && (
        <div
          className="map-tooltip glass-panel"
          style={hoverPosition}
        >
          <div className="tooltip-eyebrow">{hover.provinceKey} · 10 km grid</div>

          <div className="tooltip-value">
            {formatLayerValue(definition, hover.value)}
          </div>

          <div className="tooltip-label">{definition.label}</div>

          <div className="tooltip-grid">{hover.gridCellKey}</div>
        </div>
      )}
    </>
  );
}
