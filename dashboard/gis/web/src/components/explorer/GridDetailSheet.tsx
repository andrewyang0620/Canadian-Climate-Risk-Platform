import { useEffect, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { X } from "lucide-react";

import {
  loadGridDetail,
  type GridDetailData,
} from "../../lib/grid-detail";
import { HazardSignal } from "./HazardSignal";
import { MetricRow } from "./MetricRow";
import { formatMonth } from "./TimelineControl";

interface GridDetailSheetProps {
  gridCellKey: string | null;
  referenceMonth: string;
  onClose: () => void;
  onHoverChange: (hovering: boolean) => void;
}

function formatNumber(value: number | null, digits = 1): string {
  return value === null ? "-" : value.toFixed(digits);
}

function formatPercent(value: number | null, digits = 0): string {
  return value === null ? "-" : `${(value * 100).toFixed(digits)}%`;
}

function formatTier(value: string | null): string {
  if (!value) {
    return "Insufficient data";
  }

  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) =>
    letter.toUpperCase(),
  );
}

export function GridDetailSheet({
  gridCellKey,
  referenceMonth,
  onClose,
  onHoverChange,
}: GridDetailSheetProps) {
  const [detail, setDetail] = useState<GridDetailData | null>(null);
  const [detailVersion, setDetailVersion] = useState<string | null>(null);
  const [loadedMonth, setLoadedMonth] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // Safety net: if the sheet unmounts (closes) while the pointer is still
  // over it, the pointerleave that would normally clear the hover flag never
  // fires — without this, map interaction would stay blocked.
  useEffect(() => {
    return () => {
      onHoverChange(false);
    };
  }, [onHoverChange]);

  useEffect(() => {
    if (!gridCellKey) {
      setDetail(null);
      return;
    }

    let cancelled = false;

    setLoading(true);

    loadGridDetail(gridCellKey, referenceMonth)
      .then((result) => {
        if (!cancelled) {
          setDetail(result);
          setDetailVersion(`${gridCellKey}-${referenceMonth}`);
          setLoadedMonth(referenceMonth);
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
  }, [gridCellKey, referenceMonth]);

  return (
    <AnimatePresence>
      {gridCellKey && (
        <motion.aside
          className="detail-sheet glass-panel"
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: 16 }}
          onPointerEnter={() => {
            onHoverChange(true);
          }}
          onPointerLeave={() => {
            onHoverChange(false);
          }}
        >
          <div className="detail-sheet-header">
            <div>
              <div className="detail-eyebrow">
                {detail?.identity.provinceName ?? "10 km grid"}
                {detail?.identity.municipality
                  ? ` · ${detail.identity.municipality}`
                  : ""}
              </div>

              <div className="detail-title">10 km Grid</div>

              <div className="detail-subtitle">
                {formatMonth(loadedMonth ?? referenceMonth)} ·{" "}
                {detail?.identity.gridCellKey ?? gridCellKey}
              </div>
            </div>

            <button
              className="detail-close-button"
              onClick={onClose}
              aria-label="Close grid details"
            >
              <X size={17} />
            </button>
          </div>

          {loading && !detail && (
            <div className="detail-loading">Loading grid details...</div>
          )}

          <AnimatePresence mode="wait">
            {detail && detailVersion && (
            <motion.div
              key={detailVersion}
              className="detail-scroll"
              initial={{ opacity: 0, y: 3 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -2 }}
              transition={{ duration: 0.14 }}
            >
              <section className="detail-risk-summary">
                <div className="section-eyebrow">Composite Risk</div>

                <div className="risk-score-line">
                  <span className="risk-score-number">
                    {detail.risk.composite === null
                      ? "-"
                      : detail.risk.composite.toFixed(2)}
                  </span>

                  <div className="risk-meta">
                    <span className="risk-tier">
                      {formatTier(detail.risk.tier)}
                    </span>

                    <span>
                      {detail.risk.percentile === null
                        ? "No percentile"
                        : `${(detail.risk.percentile * 100).toFixed(
                            0,
                          )}th percentile`}
                    </span>
                  </div>
                </div>

                <div className="confidence-line">
                  <span>Score confidence</span>

                  <strong>{formatPercent(detail.risk.confidence)}</strong>
                </div>
              </section>

              <div className="detail-divider" />

              <section className="detail-section">
                <div className="section-title">Hazard Signals</div>

                <div className="hazard-list">
                  <HazardSignal
                    label="Climate"
                    value={detail.risk.climate}
                    tone="climate"
                  />
                  <HazardSignal
                    label="Hydro"
                    value={detail.risk.hydro}
                    tone="hydro"
                  />
                  <HazardSignal
                    label="Wildfire"
                    value={detail.risk.wildfire}
                    tone="wildfire"
                  />
                </div>
              </section>

              <div className="detail-divider" />

              <section className="detail-section">
                <div className="section-title">Observed Conditions</div>

                <div className="evidence-group">
                  <div className="evidence-heading climate">Climate</div>

                  <MetricRow
                    label="Mean temperature"
                    value={
                      detail.climate.meanTemp === null
                        ? "-"
                        : `${formatNumber(detail.climate.meanTemp)} °C`
                    }
                  />
                  <MetricRow
                    label="Total precipitation"
                    value={
                      detail.climate.precipitation === null
                        ? "-"
                        : `${formatNumber(detail.climate.precipitation)} mm`
                    }
                  />
                  <MetricRow
                    label="Extreme heat"
                    value={
                      detail.climate.extremeHeatDays === null
                        ? "-"
                        : `${formatNumber(detail.climate.extremeHeatDays, 0)} days`
                    }
                  />
                  <MetricRow
                    label="Extreme cold"
                    value={
                      detail.climate.extremeColdDays === null
                        ? "-"
                        : `${formatNumber(detail.climate.extremeColdDays, 0)} days`
                    }
                  />
                  <MetricRow
                    label="Heavy precipitation"
                    value={
                      detail.climate.heavyPrecipDays === null
                        ? "-"
                        : `${formatNumber(detail.climate.heavyPrecipDays, 0)} days`
                    }
                  />
                </div>

                <div className="evidence-group">
                  <div className="evidence-heading hydro">Hydro</div>

                  <MetricRow
                    label="Mean flow"
                    value={formatNumber(detail.hydro.meanFlow, 2)}
                  />
                  <MetricRow
                    label="P95 flow"
                    value={formatNumber(detail.hydro.p95Flow, 2)}
                  />
                  <MetricRow
                    label="Mean water level"
                    value={formatNumber(detail.hydro.meanLevel, 2)}
                  />
                  <MetricRow
                    label="P95 water level"
                    value={formatNumber(detail.hydro.p95Level, 2)}
                  />
                </div>

                <div className="evidence-group">
                  <div className="evidence-heading wildfire">Wildfire</div>

                  <MetricRow
                    label="Observed perimeters"
                    value={formatNumber(detail.wildfire.perimeterCount, 0)}
                  />
                  <MetricRow
                    label="Grid overlap"
                    value={formatPercent(detail.wildfire.overlapRatio, 1)}
                  />
                  <MetricRow
                    label="Intersected area"
                    value={
                      detail.wildfire.intersectionAreaSqKm === null
                        ? "-"
                        : `${formatNumber(
                            detail.wildfire.intersectionAreaSqKm,
                          )} sq km`
                    }
                  />
                </div>
              </section>

              <div className="detail-divider" />

              <section className="detail-section quality-section">
                <div className="section-title">Data Quality</div>

                <div className="quality-group">
                  <div className="quality-heading">Climate</div>

                  <MetricRow
                    label="Stations"
                    value={formatNumber(detail.quality.climateStationCount, 0)}
                  />
                  <MetricRow
                    label="Mapping"
                    value={detail.quality.climateMappingMethod ?? "-"}
                  />
                  <MetricRow
                    label="Nearest station"
                    value={
                      detail.quality.nearestClimateStationKm === null
                        ? "-"
                        : `${formatNumber(
                            detail.quality.nearestClimateStationKm,
                          )} km`
                    }
                  />
                  <MetricRow
                    label="IDW confidence"
                    value={formatPercent(detail.quality.climateIdwConfidence)}
                  />
                  <MetricRow
                    label="Temperature coverage"
                    value={formatPercent(detail.quality.temperatureCompleteness)}
                  />
                </div>

                <div className="quality-group">
                  <div className="quality-heading">Hydro</div>

                  <MetricRow
                    label="Stations"
                    value={formatNumber(detail.quality.hydroStationCount, 0)}
                  />
                  <MetricRow
                    label="Assignment"
                    value={detail.quality.hydroAssignmentMethod ?? "-"}
                  />
                  <MetricRow
                    label="Basin coverage"
                    value={formatPercent(detail.quality.hydroBasinCoverage)}
                  />
                  <MetricRow
                    label="Flow completeness"
                    value={formatPercent(detail.quality.flowCompleteness)}
                  />
                  <MetricRow
                    label="Level completeness"
                    value={formatPercent(detail.quality.levelCompleteness)}
                  />
                </div>

                <div className="quality-group">
                  <div className="quality-heading">Overall</div>

                  <MetricRow
                    label="Domains available"
                    value={
                      detail.quality.domainCoverageCount === null
                        ? "-"
                        : `${formatNumber(
                            detail.quality.domainCoverageCount,
                            0,
                          )} / 3`
                    }
                  />
                  <MetricRow
                    label="Domain coverage"
                    value={formatPercent(detail.quality.domainCoverageRatio)}
                  />
                </div>
              </section>
            </motion.div>
            )}
          </AnimatePresence>
        </motion.aside>
      )}
    </AnimatePresence>
  );
}
