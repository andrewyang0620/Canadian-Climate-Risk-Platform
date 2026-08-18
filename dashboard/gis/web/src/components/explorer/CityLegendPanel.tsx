import type { CityLayerId, CityScope } from "../../lib/city-data";

interface CityLegendPanelProps {
  scope: CityScope;
  cityLayerId: CityLayerId;
}

export function CityLegendPanel({ scope, cityLayerId }: CityLegendPanelProps) {
  return (
    <div className="legend-panel glass-panel">
      <div className="legend-heading">City Layers</div>

      <div className="legend-items">
        {cityLayerId === "flood" && (
          <>
            <div className="legend-item">
              <span
                className="legend-swatch"
                style={{ background: "rgba(53, 183, 205, 0.75)" }}
              />

              <span>Flood exposure</span>
            </div>

            {scope === "calgary" && (
              <div className="legend-item">
                <span
                  className="legend-swatch"
                  style={{ background: "rgba(108, 128, 152, 0.65)" }}
                />

                <span>Normal River Channel · context only</span>
              </div>
            )}
          </>
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
    </div>
  );
}
