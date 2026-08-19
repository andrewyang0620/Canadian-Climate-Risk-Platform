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
import type { CityLayerId, CityScope, ExplorerScope } from "../../lib/city-data";
import { formatMonth } from "./TimelineControl";

interface CityLayerOption {
  id: CityLayerId;
  label: string;
  swatch: string | null;
  // Mirrors National's per-layer `about` copy (layer-registry.ts) — omitted
  // for "none" the same way National has no about text while layersVisible
  // is false.
  about?: string;
}

function propertyAboutCopy(scope: CityScope): string {
  return scope === "vancouver"
    ? "Property renders every Vancouver parcel as a selectable, neutral-colored shape — no flood or activity styling. It's the base layer for browsing and clicking into individual parcels; switch to Flood Exposure or Building Permits for those overlays."
    : "Property renders every Calgary property as a selectable, neutral-colored shape — no flood or activity styling. It's the base layer for browsing and clicking into individual properties; switch to Flood Exposure, Building Permits, or Development Permits for those overlays.";
}

function floodAboutCopy(scope: CityScope): string {
  return scope === "vancouver"
    ? "Flood Exposure colors each parcel by mapped flood exposure, drawn from the same floodplain and Fraser Basin scenario overlays used to score properties. Parcels intersecting one or more mapped scenarios highlight in cyan; the scenario count shows on hover."
    : "Flood Exposure colors each parcel by regulatory flood exposure, combining floodway, floodplain, flood fringe, and overland flow zone overlaps. Parcels that only intersect the Normal River Channel — not a hazard zone — render in a separate context-only color and are not counted as exposed.";
}

const BUILDING_PERMITS_ABOUT =
  "Building Permits plots each issued permit as a point, colored by whether it's housing-related (orange) or not (grey). Selecting a permit highlights its linked parcel using the relationship already resolved in Gold — permits with no mapped parcel select with no highlight, never a nearest-parcel guess.";

const DEVELOPMENT_PERMITS_ABOUT =
  "Development Permits plots each Calgary development permit as a point in purple. A permit mapped to one property links directly; permits mapped to several resolve through a dedicated development-permit-to-property relationship table, fetched once and cached in memory.";

function vancouverLayerOptions(): CityLayerOption[] {
  return [
    { id: "property", label: "Property", swatch: "rgba(120, 132, 145, 0.7)", about: propertyAboutCopy("vancouver") },
    { id: "flood", label: "Flood Exposure", swatch: "rgba(53, 183, 205, 0.85)", about: floodAboutCopy("vancouver") },
    { id: "building_permits", label: "Building Permits", swatch: "rgba(246, 148, 60, 0.9)", about: BUILDING_PERMITS_ABOUT },
    { id: "none", label: "No Layer", swatch: null },
  ];
}

function calgaryLayerOptions(): CityLayerOption[] {
  return [
    { id: "property", label: "Property", swatch: "rgba(120, 132, 145, 0.7)", about: propertyAboutCopy("calgary") },
    { id: "flood", label: "Flood Exposure", swatch: "rgba(53, 183, 205, 0.85)", about: floodAboutCopy("calgary") },
    { id: "building_permits", label: "Building Permits", swatch: "rgba(246, 148, 60, 0.9)", about: BUILDING_PERMITS_ABOUT },
    { id: "development_permits", label: "Development Permits", swatch: "rgba(168, 123, 219, 0.9)", about: DEVELOPMENT_PERMITS_ABOUT },
    { id: "none", label: "No Layer", swatch: null },
  ];
}

interface ExplorerControlsProps {
  scope: ExplorerScope;
  onScopeChange: (scope: ExplorerScope) => void;
  cityLayerId: CityLayerId;
  onCityLayerChange: (layerId: CityLayerId) => void;
  activeLayerId: LayerId;
  onLayerChange: (layerId: LayerId) => void;
  months: string[];
  referenceMonth: string;
  onMonthChange: (month: string) => void;
  onRegionChange: (region: RegionId) => void;
  layersVisible: boolean;
  onLayersVisibleChange: (visible: boolean) => void;
  gridInfoVisible: boolean;
  onGridInfoVisibleChange: (visible: boolean) => void;
  onAboutHoverChange: (hovering: boolean) => void;
  // Hovering the bar/buttons themselves — distinct from onAboutHoverChange,
  // which only covers the About popover (positioned below the bar, outside
  // its own bounding box).
  onControlsHoverChange: (hovering: boolean) => void;
  showAbout: boolean;
}

export function ExplorerControls({
  scope,
  onScopeChange,
  cityLayerId,
  onCityLayerChange,
  activeLayerId,
  onLayerChange,
  months,
  referenceMonth,
  onMonthChange,
  onRegionChange,
  layersVisible,
  onLayersVisibleChange,
  gridInfoVisible,
  onGridInfoVisibleChange,
  onAboutHoverChange,
  onControlsHoverChange,
  showAbout,
}: ExplorerControlsProps) {
  const [openMenu, setOpenMenu] = useState<
    "layer" | "citylayer" | "date" | "region" | null
  >(null);
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

  // Selecting a dropdown option closes the menu while the pointer is still
  // sitting exactly where that option was — the menu item unmounts out from
  // under the cursor without ever firing pointerleave on the outer bar, so
  // onControlsHoverChange never gets its "false" and map picking stays
  // blocked until the user happens to move the mouse again. Closing via this
  // helper (instead of a bare setOpenMenu(null)) clears the hover flag right
  // away, since the option is closing specifically because the user is done
  // with the bar.
  function closeMenu() {
    setOpenMenu(null);
    onControlsHoverChange(false);
  }

  const activeLayer = getLayerDefinition(activeLayerId);
  const scopeRegionLabel =
    scope === "vancouver" ? "Vancouver" : scope === "calgary" ? "Calgary" : "AB + BC";
  const scopeRegionOptions = [
    { id: "national" as const, label: "AB + BC" },
    { id: "vancouver" as const, label: "Vancouver" },
    { id: "calgary" as const, label: "Calgary" },
  ];
  const cityLayerOptions =
    scope === "calgary" ? calgaryLayerOptions() : vancouverLayerOptions();
  const activeCityLayerOption = cityLayerOptions.find(
    (option) => option.id === cityLayerId,
  );
  const cityLayerLabel = activeCityLayerOption?.label ?? "Property";
  const monthsByYear = months.reduce<Record<string, string[]>>((groups, month) => {
    const year = month.slice(0, 4);

    groups[year] = [...(groups[year] ?? []), month];

    return groups;
  }, {});
  const years = Object.keys(monthsByYear).sort((left, right) =>
    right.localeCompare(left),
  );

  return (
    <div
      ref={containerRef}
      className="control-island glass-panel"
      onPointerEnter={() => {
        onControlsHoverChange(true);
      }}
      onPointerLeave={() => {
        onControlsHoverChange(false);
      }}
    >
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

        <div className="control-segment">
          <button
            className="control-button"
            onClick={() => {
              setOpenMenu(openMenu === "region" ? null : "region");
            }}
          >
            <span>{scopeRegionLabel}</span>
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
              {scopeRegionOptions.map((option) => (
                <button
                  key={option.id}
                  className={`region-option ${
                    option.id === scope ? "active" : ""
                  }`}
                  onClick={() => {
                    onScopeChange(option.id);

                    if (option.id === "national") {
                      onRegionChange("all");
                    }

                    closeMenu();
                  }}
                >
                  <span>{option.label}</span>
                  {option.id === scope && <Check size={14} />}
                </button>
              ))}
            </motion.div>
            )}
          </AnimatePresence>
        </div>

        {scope === "national" ? (
          <>
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
                            closeMenu();
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
                      closeMenu();
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
          </>
        ) : (
          <>
            <button
              className="control-button primary-control"
              onClick={() => {
                setOpenMenu(openMenu === "citylayer" ? null : "citylayer");
              }}
            >
              <Layers3 size={16} />
              <span>{cityLayerLabel}</span>
              <ChevronDown size={14} />
            </button>

            <AnimatePresence>
              {openMenu === "citylayer" && (
              <motion.div
                className="control-menu layer-menu"
                initial={{ opacity: 0, y: -5, scale: 0.985 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: -3, scale: 0.99 }}
              >
                <div className="layer-group">
                  {cityLayerOptions.map((option) => (
                    <button
                      key={option.id}
                      className={`layer-option ${
                        option.id === cityLayerId ? "active" : ""
                      }`}
                      onClick={() => {
                        onCityLayerChange(option.id);
                        closeMenu();
                      }}
                    >
                      {option.id === cityLayerId && (
                        <motion.span
                          layoutId="active-city-layer-surface"
                          className="active-layer-surface"
                        />
                      )}

                      <span className="layer-option-content">
                        <span
                          className={`layer-swatch ${
                            option.swatch ? "" : "layer-swatch-empty"
                          }`}
                          style={
                            option.swatch
                              ? { background: option.swatch }
                              : undefined
                          }
                        />

                        <span className="layer-option-label">{option.label}</span>

                        {option.id === cityLayerId && <Check size={14} />}
                      </span>
                    </button>
                  ))}
                </div>
              </motion.div>
              )}
            </AnimatePresence>
          </>
        )}
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
                        closeMenu();
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

      {(() => {
        const about =
          scope === "national"
            ? layersVisible
              ? { label: activeLayer.label, text: activeLayer.about }
              : null
            : activeCityLayerOption?.about
              ? { label: activeCityLayerOption.label, text: activeCityLayerOption.about }
              : null;

        if (!showAbout || !about) {
          return null;
        }

        return (
          <>
            <div className="control-divider" />

            <button
              className="control-button about-control"
              aria-label={`About ${about.label}`}
              title={`About ${about.label}`}
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
              <div className="layer-about-title">{about.label}</div>
              <div className="layer-about-copy">{about.text}</div>
            </div>
          </>
        );
      })()}
    </div>
  );
}

export type { RegionId };
