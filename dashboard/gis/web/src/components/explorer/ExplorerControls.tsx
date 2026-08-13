import { useEffect, useRef, useState } from "react";
import {
  CalendarDays,
  Check,
  ChevronDown,
  Eye,
  EyeOff,
  Info,
  Layers3,
} from "lucide-react";
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
  months: string[];
  referenceMonth: string;
  onMonthChange: (month: string) => void;
  region: RegionId;
  onRegionChange: (region: RegionId) => void;
  layersVisible: boolean;
  onLayersVisibleChange: (visible: boolean) => void;
  gridInfoVisible: boolean;
  onGridInfoVisibleChange: (visible: boolean) => void;
  onAboutHoverChange: (hovering: boolean) => void;
  showAbout: boolean;
}

export function ExplorerControls({
  activeLayerId,
  onLayerChange,
  months,
  referenceMonth,
  onMonthChange,
  region,
  onRegionChange,
  layersVisible,
  onLayersVisibleChange,
  gridInfoVisible,
  onGridInfoVisibleChange,
  onAboutHoverChange,
  showAbout,
}: ExplorerControlsProps) {
  const [openMenu, setOpenMenu] = useState<"layer" | "date" | "region" | null>(
    null,
  );
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
  const monthsByYear = months.reduce<Record<string, string[]>>((groups, month) => {
    const year = month.slice(0, 4);

    groups[year] = [...(groups[year] ?? []), month];

    return groups;
  }, {});
  const years = Object.keys(monthsByYear).sort((left, right) =>
    right.localeCompare(left),
  );

  return (
    <div ref={containerRef} className="control-island glass-panel">
      <div className="control-segment layer-control-segment">
        <button
          className={`control-button grid-info-control ${
            gridInfoVisible ? "active" : ""
          }`}
          aria-label={gridInfoVisible ? "Hide grid info box" : "Show grid info box"}
          title={gridInfoVisible ? "Hide grid info box" : "Show grid info box"}
          onClick={() => {
            onGridInfoVisibleChange(!gridInfoVisible);
          }}
        >
          {gridInfoVisible ? <Eye size={16} /> : <EyeOff size={16} />}
        </button>

        <button
          className="control-button primary-control"
          onClick={() => {
            setOpenMenu(openMenu === "layer" ? null : "layer");
          }}
        >
          <Layers3 size={16} />
          <span>{layersVisible ? activeLayer.label : "No Layer"}</span>
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
                  {group.id !== "composite" && (
                    <div className="menu-section-label">{group.label}</div>
                  )}

                  {layers.map((layer) => (
                    <button
                      key={layer.id}
                      className={`layer-option ${
                        layersVisible && layer.id === activeLayerId ? "active" : ""
                      }`}
                      onClick={() => {
                        onLayerChange(layer.id);
                        onLayersVisibleChange(true);
                        setOpenMenu(null);
                      }}
                    >
                      {layersVisible && layer.id === activeLayerId && (
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

                        {layersVisible && layer.id === activeLayerId && (
                          <Check size={14} />
                        )}
                      </span>
                    </button>
                  ))}
                </div>
              );
            })}

            <div className="layer-group layer-none-group">
              <button
                className={`layer-option ${!layersVisible ? "active" : ""}`}
                onClick={() => {
                  onLayersVisibleChange(false);
                  setOpenMenu(null);
                }}
              >
                {!layersVisible && (
                  <motion.span
                    layoutId="active-layer-surface"
                    className="active-layer-surface"
                  />
                )}

                <span className="layer-option-content">
                  <span className="layer-swatch layer-swatch-empty" />

                  <span className="layer-option-label">No Layer</span>

                  {!layersVisible && <Check size={14} />}
                </span>
              </button>
            </div>
          </motion.div>
          )}
        </AnimatePresence>
      </div>

      <div className="control-divider" />

      <div className="control-segment">
        <button
          className="control-button control-readout"
          onClick={() => {
            setOpenMenu(openMenu === "date" ? null : "date");
          }}
        >
          <CalendarDays size={16} />
          <span>{formatMonth(referenceMonth)}</span>
          <ChevronDown size={14} />
        </button>

        <AnimatePresence>
          {openMenu === "date" && (
          <motion.div
            className="control-menu date-menu"
            initial={{ opacity: 0, y: -5, scale: 0.985 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -3, scale: 0.99 }}
          >
            {years.map((year) => (
              <div key={year} className="date-year-group">
                <div className="menu-section-label">{year}</div>

                <div className="date-month-grid">
                  {[...monthsByYear[year]]
                    .sort((left, right) => left.localeCompare(right))
                    .map((month) => (
                    <button
                      key={month}
                      className={`date-month-option ${
                        month === referenceMonth ? "active" : ""
                      }`}
                      onClick={() => {
                        onMonthChange(month);
                        setOpenMenu(null);
                      }}
                    >
                      {new Intl.DateTimeFormat("en", {
                        month: "short",
                      }).format(new Date(`${month}-01T00:00:00`))}
                    </button>
                  ))}
                </div>
              </div>
            ))}
          </motion.div>
          )}
        </AnimatePresence>
      </div>

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

      {showAbout && layersVisible && (
        <>
          <div className="control-divider" />

          <button
            className="control-button about-control"
            aria-label={`About ${activeLayer.label}`}
            title={`About ${activeLayer.label}`}
            onPointerEnter={() => {
              onAboutHoverChange(true);
            }}
            onPointerLeave={() => {
              onAboutHoverChange(false);
            }}
            onFocus={() => {
              onAboutHoverChange(true);
            }}
            onBlur={() => {
              onAboutHoverChange(false);
            }}
          >
            <Info size={16} />
          </button>

          <div
            className="layer-about-popover"
            onPointerEnter={() => {
              onAboutHoverChange(true);
            }}
            onPointerLeave={() => {
              onAboutHoverChange(false);
            }}
          >
            <div className="layer-about-title">{activeLayer.label}</div>
            <div className="layer-about-copy">{activeLayer.about}</div>
          </div>
        </>
      )}
    </div>
  );
}

export type { RegionId };
