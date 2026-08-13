import { useEffect, useMemo, useState } from "react";
import { GeoJsonLayer } from "@deck.gl/layers";
import { AnimatePresence, motion } from "motion/react";

import { DeckGLOverlay } from "./DeckGLOverlay";
import {
  getNumericValue,
  loadMonthlyDataset,
  type MonthlyDataset,
} from "../../lib/gis-data";
import {
  loadGridGeometry,
  type GridFeature,
  type GridProperties,
  type ViewportBounds,
} from "../../lib/grid-geometry";
import {
  formatLayerValue,
  getLayerDefinition,
  layerColor,
  type LayerId,
} from "../../lib/layer-registry";

interface NationalRiskLayerProps {
  referenceMonth: string;
  activeLayerId: LayerId;
  beforeId: string;
  regionProvinceKey: string | null;
  selectedGridKey: string | null;
  onGridSelect: (gridCellKey: string) => void;
  viewportBounds: ViewportBounds;
}

interface HoverState {
  x: number;
  y: number;
  gridCellKey: string;
  provinceKey: string;
  value: number | null;
}

type InterleavedLayerProps = {
  beforeId: string;
};

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
}: NationalRiskLayerProps) {
  const [dataset, setDataset] = useState<MonthlyDataset | null>(null);
  const [geometry, setGeometry] = useState<GridFeature[]>([]);
  const [loading, setLoading] = useState(false);
  const [geometryLoading, setGeometryLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const definition = getLayerDefinition(activeLayerId);

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
    let cancelled = false;

    setGeometryLoading(true);

    loadGridGeometry(viewportBounds)
      .then((collection) => {
        if (cancelled) {
          return;
        }

        setGeometry(collection.features as GridFeature[]);
      })
      .catch((error) => {
        console.error("Failed to load viewport geometry", error);
      })
      .finally(() => {
        if (!cancelled) {
          setGeometryLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [
    viewportBounds.minX,
    viewportBounds.minY,
    viewportBounds.maxX,
    viewportBounds.maxY,
  ]);

  const layers = useMemo(() => {
    if (!dataset || geometry.length === 0) {
      return [];
    }

    return [
      new GeoJsonLayer<GridProperties>({
        id: "national-risk-grid",
        data: geometry,
        beforeId,
        filled: true,
        stroked: true,
        pickable: true,
        lineWidthUnits: "pixels",
        lineWidthMinPixels: 0,
        getFillColor: (feature) => {
          const properties = feature.properties;

          if (regionProvinceKey && properties.province_key !== regionProvinceKey) {
            return [0, 0, 0, 0];
          }

          const value = getNumericValue(
            dataset,
            properties.grid_cell_key,
            definition.column,
          );

          return layerColor(definition, value);
        },
        getLineColor: (feature) => {
          const key = feature.properties.grid_cell_key;

          if (key === selectedGridKey) {
            return [23, 31, 27, 255];
          }

          return [44, 53, 49, 210];
        },
        getLineWidth: (feature) => {
          const key = feature.properties.grid_cell_key;

          if (key === selectedGridKey) {
            return 2.4;
          }

          if (key === hover?.gridCellKey) {
            return 1.2;
          }

          return 0;
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
          const feature = info.object;

          if (!feature) {
            setHover(null);
            return;
          }

          const properties = feature.properties;

          if (regionProvinceKey && properties.province_key !== regionProvinceKey) {
            setHover(null);
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
          });
        },
        transitions: {
          getFillColor: {
            duration: 280,
          },
        },
        updateTriggers: {
          getFillColor: [dataset, activeLayerId, regionProvinceKey],
          getLineWidth: [hover?.gridCellKey, selectedGridKey],
          getLineColor: [selectedGridKey],
        },
      } as ConstructorParameters<typeof GeoJsonLayer<GridProperties>>[0] &
        InterleavedLayerProps),
    ];
  }, [
    dataset,
    geometry,
    activeLayerId,
    beforeId,
    definition,
    regionProvinceKey,
    hover?.gridCellKey,
    selectedGridKey,
    onGridSelect,
  ]);

  const hoverPosition = hover ? tooltipPosition(hover.x, hover.y) : null;
  const isLoading = loading || geometryLoading;

  return (
    <>
      <DeckGLOverlay
        interleaved
        layers={layers}
        getCursor={({ isHovering }) => (isHovering ? "pointer" : "grab")}
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

      {hover && hoverPosition && (
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
