import { AnimatePresence, motion } from "motion/react";

import type { CityLayerId, CityScope } from "../../lib/city-data";

interface CityLegendPanelProps {
  scope: CityScope;
  cityLayerId: CityLayerId;
}

interface CityLegendDefinition {
  label: string;
  gradient?: string;
  labels?: string[];
}

const CITY_LEGENDS: Record<CityLayerId, CityLegendDefinition | null> = {
  property: {
    label: "Property",
  },
  flood: {
    label: "Flood Exposure",
    gradient:
      "linear-gradient(90deg, rgba(82, 111, 124, 0.08), rgba(53, 183, 205, 0.75))",
    labels: ["No mapped exposure", "Exposed"],
  },
  building_permits: {
    label: "Building Permits",
  },
  development_permits: {
    label: "Development Permits",
  },
  none: null,
};

export function CityLegendPanel({ scope, cityLayerId }: CityLegendPanelProps) {
  const definition = CITY_LEGENDS[cityLayerId];

  if (!definition) {
    return null;
  }

  return (
    <AnimatePresence mode="wait">
      <motion.section
        key={cityLayerId}
        className="legend-panel glass-panel"
        initial={{ opacity: 0, y: 4 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: 3 }}
        transition={{ duration: 0.14 }}
      >
        <div className="legend-heading">
          {definition.label}
        </div>

        {definition.gradient && (
          <div
            className="legend-gradient"
            style={{
              background: definition.gradient,
            }}
          />
        )}

        {definition.labels && (
          <div className="legend-labels">
            {definition.labels.map((label) => (
              <span key={label}>
                {label}
              </span>
            ))}
          </div>
        )}

        <div className="legend-items city-legend-items">
          {cityLayerId === "property" && (
            <div className="legend-item">
              <span>Selectable property parcels</span>
            </div>
          )}

          {cityLayerId === "flood" && (
            scope === "calgary" && (
              <div className="legend-item">
                <span>Normal River Channel - context only</span>
              </div>
            )
          )}

          {cityLayerId === "building_permits" && (
            <>
              <div className="legend-item">
                <span
                  className="legend-swatch"
                  style={{ background: "rgba(246, 148, 60, 0.9)" }}
                />

                <span>Housing-related permit</span>
              </div>

              <div className="legend-item">
                <span
                  className="legend-swatch"
                  style={{ background: "rgba(148, 156, 163, 0.85)" }}
                />

                <span>Other building permit</span>
              </div>
            </>
          )}

          {cityLayerId === "development_permits" && (
            <div className="legend-item">
              <span
                className="legend-swatch"
                style={{ background: "rgba(168, 123, 219, 0.9)" }}
              />

              <span>Development permit</span>
            </div>
          )}
        </div>
      </motion.section>
    </AnimatePresence>
  );
}
