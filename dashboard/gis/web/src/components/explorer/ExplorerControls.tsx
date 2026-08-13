import { useEffect, useRef, useState } from "react";
import { CalendarDays, Check, ChevronDown, Layers3 } from "lucide-react";
import { AnimatePresence, motion } from "motion/react";

import {
  LAYER_DEFINITIONS,
  LAYER_GROUPS,
  getLayerDefinition,
  layerGradient,
  type LayerId,
} from "../../lib/layer-registry";
import type { RegionId } from "../../lib/gis-data";
import { formatMonth } from "./TimelineControl";

interface ExplorerControlsProps {
  activeLayerId: LayerId;
  onLayerChange: (layerId: LayerId) => void;
  referenceMonth: string;
  region: RegionId;
  onRegionChange: (region: RegionId) => void;
}

export function ExplorerControls({
  activeLayerId,
  onLayerChange,
  referenceMonth,
  region,
  onRegionChange,
}: ExplorerControlsProps) {
  const [openMenu, setOpenMenu] = useState<"layer" | "region" | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node;

      if (!containerRef.current?.contains(target)) {
        setOpenMenu(null);
      }
    };

    document.addEventListener("pointerdown", handlePointerDown);

    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
    };
  }, []);

  const activeLayer = getLayerDefinition(activeLayerId);
  const regionLabel = region === "all" ? "AB + BC" : region;

  return (
    <div ref={containerRef} className="control-island glass-panel">
      <div className="control-segment">
        <button
          className="control-button primary-control"
          onClick={() => {
            setOpenMenu(openMenu === "layer" ? null : "layer");
          }}
        >
          <Layers3 size={16} />
          <span>{activeLayer.label}</span>
          <ChevronDown size={14} />
        </button>

        <AnimatePresence>
          {openMenu === "layer" && (
          <motion.div
            className="control-menu layer-menu"
            initial={{ opacity: 0, y: -5, scale: 0.985 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -3, scale: 0.99 }}
          >
            {LAYER_GROUPS.map((group) => {
              const layers = Object.values(LAYER_DEFINITIONS).filter(
                (layer) => layer.group === group.id,
              );

              return (
                <div key={group.id} className="layer-group">
                  <div className="menu-section-label">{group.label}</div>

                  {layers.map((layer) => (
                    <button
                      key={layer.id}
                      className={`layer-option ${
                        layer.id === activeLayerId ? "active" : ""
                      }`}
                      onClick={() => {
                        onLayerChange(layer.id);
                        setOpenMenu(null);
                      }}
                    >
                      {layer.id === activeLayerId && (
                        <motion.span
                          layoutId="active-layer-surface"
                          className="active-layer-surface"
                        />
                      )}

                      <span className="layer-option-content">
                        <span
                          className="layer-swatch"
                          style={{ background: layerGradient(layer) }}
                        />

                        <span className="layer-option-label">{layer.label}</span>

                        {layer.id === activeLayerId && <Check size={14} />}
                      </span>
                    </button>
                  ))}
                </div>
              );
            })}
          </motion.div>
          )}
        </AnimatePresence>
      </div>

      <div className="control-divider" />

      <button className="control-button control-readout">
        <CalendarDays size={16} />
        <span>{formatMonth(referenceMonth)}</span>
      </button>

      <div className="control-divider" />

      <div className="control-segment">
        <button
          className="control-button"
          onClick={() => {
            setOpenMenu(openMenu === "region" ? null : "region");
          }}
        >
          <span>{regionLabel}</span>
          <ChevronDown size={14} />
        </button>

        <AnimatePresence>
          {openMenu === "region" && (
          <motion.div
            className="control-menu region-menu"
            initial={{ opacity: 0, y: -5, scale: 0.985 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -3, scale: 0.99 }}
          >
            {[
              { id: "all" as const, label: "Alberta + British Columbia" },
              { id: "BC" as const, label: "British Columbia" },
              { id: "AB" as const, label: "Alberta" },
            ].map((option) => (
              <button
                key={option.id}
                className={`region-option ${
                  option.id === region ? "active" : ""
                }`}
                onClick={() => {
                  onRegionChange(option.id);
                  setOpenMenu(null);
                }}
              >
                <span>{option.label}</span>
                {option.id === region && <Check size={14} />}
              </button>
            ))}
          </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

export type { RegionId };
