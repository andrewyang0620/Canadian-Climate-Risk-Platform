import { useEffect, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { X } from "lucide-react";

import { loadGridDetail, type GridDetailData } from "../../lib/grid-detail";
import type { CityFeature, CityScope } from "../../lib/city-data";
import { HazardSignal } from "./HazardSignal";
import { MetricRow } from "./MetricRow";
import { formatMonth } from "./TimelineControl";

interface CityDetailSheetProps {
  scope: CityScope;
  featureKey: string | null;
  feature: CityFeature | null;
  referenceMonth: string;
  onClose: () => void;
  onHoverChange: (hovering: boolean) => void;
}

function featureProperty(feature: CityFeature, name: string): unknown {
  return feature.properties?.[name];
}

// Presence-defaulting reader: used for exposure classification, where a
// missing flag should behave like "false" rather than "unknown".
function booleanProperty(feature: CityFeature, name: string): boolean {
  const value = featureProperty(feature, name);

  return value === true || value === 1;
}

// Tri-state reader: used for display, where a missing flag should render
// as "-" rather than silently collapsing to "No".
function optionalBooleanProperty(
  feature: CityFeature,
  name: string,
): boolean | null {
  const value = featureProperty(feature, name);

  if (value === undefined || value === null) {
    return null;
  }

  return value === true || value === 1;
}

function numberProperty(feature: CityFeature, name: string): number | null {
  const value = featureProperty(feature, name);

  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringProperty(feature: CityFeature, name: string): string | null {
  const value = featureProperty(feature, name);

  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

// Mirrors CitySpatialLayer's exposure classification: Calgary's Normal
// River Channel stays contextual-only and never overrides a regulatory
// exposure flag (Phase D).
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

function floodStatusLabel(scope: CityScope, feature: CityFeature): string {
  if (isFloodExposed(feature)) {
    return scope === "vancouver"
      ? "Mapped flood exposure"
      : "Regulatory flood exposure";
  }

  if (isNormalRiverChannelOnly(scope, feature)) {
    return "Normal River Channel context only";
  }

  return "No mapped flood exposure";
}

function formatNumber(value: number | null, digits = 1): string {
  return value === null ? "-" : value.toFixed(digits);
}

function formatPercent(value: number | null, digits = 0): string {
  return value === null ? "-" : `${(value * 100).toFixed(digits)}%`;
}

function formatCurrency(value: number | null): string {
  if (value === null) {
    return "-";
  }

  return new Intl.NumberFormat("en-CA", {
    style: "currency",
    currency: "CAD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatFlag(value: boolean | null): string {
  if (value === null) {
    return "-";
  }

  return value ? "Yes" : "No";
}

function formatFlagWithOverlap(value: boolean | null, overlapRatio: number | null): string {
  return `${formatFlag(value)} · ${formatPercent(overlapRatio)}`;
}

function formatYearRange(min: number | null, max: number | null): string {
  if (min === null && max === null) {
    return "-";
  }

  if (min !== null && max !== null && min !== max) {
    return `${min}–${max}`;
  }

  return String(min ?? max);
}

function propertyTitle(feature: CityFeature, fallback: string): string {
  const address = stringProperty(feature, "address_text");

  if (address) {
    return address;
  }

  const community = stringProperty(feature, "community_name");
  const propertyType = stringProperty(feature, "property_type");

  if (community && propertyType) {
    return `${community} · ${propertyType}`;
  }

  if (community) {
    return community;
  }

  return fallback;
}

function CityAssessmentSection({
  scope,
  feature,
}: {
  scope: CityScope;
  feature: CityFeature;
}) {
  if (scope === "vancouver") {
    const hasLatestAssessment = booleanProperty(feature, "has_latest_assessment");
    const mappingStatus = !hasLatestAssessment
      ? "No current assessment"
      : booleanProperty(feature, "assessment_mapping_exact_1_to_1")
        ? "Exact 1:1 match"
        : booleanProperty(feature, "assessment_mapping_ambiguous")
          ? "Ambiguous match"
          : "Mapped";

    return (
      <section className="detail-section">
        <div className="section-title">Assessment &amp; Land Context</div>

        <div className="evidence-group">
          <MetricRow
            label="Zoning district"
            value={stringProperty(feature, "zoning_district") ?? "-"}
          />
          <MetricRow
            label="Zoning classification"
            value={stringProperty(feature, "zoning_classification") ?? "-"}
          />
          <MetricRow
            label="Neighbourhood"
            value={stringProperty(feature, "neighbourhood_code") ?? "-"}
          />
        </div>

        <div className="evidence-group">
          <MetricRow
            label="Assessment year"
            value={formatNumber(numberProperty(feature, "report_year"), 0)}
          />
          <MetricRow
            label="Land-coordinate assessed value"
            value={formatCurrency(
              numberProperty(
                feature,
                "land_coordinate_current_total_assessed_value",
              ),
            )}
          />
          <MetricRow
            label="Exact-mapped assessed value"
            value={formatCurrency(
              numberProperty(
                feature,
                "exact_mapped_current_total_assessed_value",
              ),
            )}
          />
          <MetricRow label="Mapping" value={mappingStatus} />
        </div>
      </section>
    );
  }

  return (
    <section className="detail-section">
      <div className="section-title">Assessment &amp; Land Context</div>

      <div className="evidence-group">
        <MetricRow
          label="Property type"
          value={stringProperty(feature, "property_type") ?? "-"}
        />
        <MetricRow
          label="Land use designation"
          value={stringProperty(feature, "land_use_designation") ?? "-"}
        />
        <MetricRow
          label="Community"
          value={stringProperty(feature, "community_name") ?? "-"}
        />
        <MetricRow
          label="Year built"
          value={formatYearRange(
            numberProperty(feature, "year_of_construction_min"),
            numberProperty(feature, "year_of_construction_max"),
          )}
        />
      </div>

      <div className="evidence-group">
        <MetricRow
          label="Assessment year"
          value={formatNumber(numberProperty(feature, "assessment_year"), 0)}
        />
        <MetricRow
          label="Assessment class"
          value={stringProperty(feature, "assessment_class") ?? "-"}
        />
        <MetricRow
          label="Total assessed value"
          value={formatCurrency(numberProperty(feature, "assessed_value_total_sum"))}
        />
        <MetricRow
          label="Residential value"
          value={formatCurrency(
            numberProperty(feature, "assessed_value_residential_sum"),
          )}
        />
        <MetricRow
          label="Non-residential value"
          value={formatCurrency(
            numberProperty(feature, "assessed_value_non_residential_sum"),
          )}
        />
        <MetricRow
          label="Farmland value"
          value={formatCurrency(
            numberProperty(feature, "assessed_value_farmland_sum"),
          )}
        />
      </div>
    </section>
  );
}

function CityFloodSection({
  scope,
  feature,
}: {
  scope: CityScope;
  feature: CityFeature;
}) {
  const membershipCount = numberProperty(
    feature,
    scope === "vancouver" ? "scenario_count" : "flood_zone_membership_count",
  );

  return (
    <section className="detail-section">
      <div className="section-title">Local Municipal Flood</div>

      <div className="evidence-group">
        <MetricRow label="Status" value={floodStatusLabel(scope, feature)} />
        <MetricRow
          label={
            scope === "vancouver"
              ? "Flood scenarios"
              : "Flood zone memberships"
          }
          value={membershipCount === null ? "-" : formatNumber(membershipCount, 0)}
        />
      </div>

      {scope === "vancouver" ? (
        <div className="evidence-group">
          <MetricRow
            label="Designated floodplain"
            value={formatFlagWithOverlap(
              optionalBooleanProperty(feature, "designated_floodplain_flag"),
              numberProperty(feature, "designated_floodplain_overlap_ratio"),
            )}
          />
          <MetricRow
            label="Fraser Risk Today"
            value={formatFlagWithOverlap(
              optionalBooleanProperty(feature, "fraser_risk_today_flag"),
              numberProperty(feature, "fraser_risk_today_overlap_ratio"),
            )}
          />
          <MetricRow
            label="Still Creek floodplain"
            value={formatFlagWithOverlap(
              optionalBooleanProperty(feature, "still_creek_floodplain_flag"),
              numberProperty(feature, "still_creek_floodplain_overlap_ratio"),
            )}
          />
          <MetricRow
            label="Wave effect zone"
            value={formatFlagWithOverlap(
              optionalBooleanProperty(feature, "wave_effect_zone_flag"),
              numberProperty(feature, "wave_effect_zone_overlap_ratio"),
            )}
          />
        </div>
      ) : (
        <>
          <div className="evidence-group">
            <MetricRow
              label="Flood Fringe"
              value={formatFlagWithOverlap(
                optionalBooleanProperty(feature, "flood_fringe_flag"),
                numberProperty(feature, "flood_fringe_overlap_ratio"),
              )}
            />
            <MetricRow
              label="Floodplain"
              value={formatFlagWithOverlap(
                optionalBooleanProperty(feature, "floodplain_flag"),
                numberProperty(feature, "floodplain_overlap_ratio"),
              )}
            />
            <MetricRow
              label="Floodway"
              value={formatFlagWithOverlap(
                optionalBooleanProperty(feature, "floodway_flag"),
                numberProperty(feature, "floodway_overlap_ratio"),
              )}
            />
            <MetricRow
              label="Overland Flow"
              value={formatFlagWithOverlap(
                optionalBooleanProperty(feature, "overland_flow_flag"),
                numberProperty(feature, "overland_flow_overlap_ratio"),
              )}
            />
          </div>

          <div className="evidence-group">
            <div className="evidence-heading">
              Normal River Channel · context only
            </div>

            <MetricRow
              label="Intersects channel"
              value={formatFlagWithOverlap(
                optionalBooleanProperty(feature, "normal_river_channel_flag"),
                numberProperty(feature, "normal_river_channel_overlap_ratio"),
              )}
            />
          </div>
        </>
      )}
    </section>
  );
}

function NationalContextSection({
  feature,
  referenceMonth,
  detail,
  loading,
  error,
}: {
  feature: CityFeature;
  referenceMonth: string;
  detail: GridDetailData | null;
  loading: boolean;
  error: string | null;
}) {
  const gridKey = stringProperty(feature, "national_grid_cell_key");

  return (
    <section className="detail-section">
      <div className="section-title">
        Selected-Month National 10 km Context
      </div>

      {!gridKey && (
        <div className="detail-loading">No matched national grid cell.</div>
      )}

      {gridKey && loading && !detail && (
        <div className="detail-loading">Loading national context...</div>
      )}

      {gridKey && error && !detail && !loading && (
        <div className="detail-loading">{error}</div>
      )}

      {gridKey && detail && (
        <>
          <div className="evidence-group">
            <MetricRow label="10 km grid" value={detail.identity.gridCellKey} />
            <MetricRow
              label="Grid overlap"
              value={formatPercent(
                numberProperty(feature, "national_grid_overlap_ratio"),
              )}
            />
            <MetricRow
              label={formatMonth(referenceMonth)}
              value={
                detail.risk.composite === null
                  ? "-"
                  : detail.risk.composite.toFixed(2)
              }
            />
          </div>

          <div className="hazard-list">
            <HazardSignal label="Climate" value={detail.risk.climate} tone="climate" />
            <HazardSignal label="Hydro" value={detail.risk.hydro} tone="hydro" />
            <HazardSignal
              label="Wildfire"
              value={detail.risk.wildfire}
              tone="wildfire"
            />
          </div>
        </>
      )}
    </section>
  );
}

export function CityDetailSheet({
  scope,
  featureKey,
  feature,
  referenceMonth,
  onClose,
  onHoverChange,
}: CityDetailSheetProps) {
  const [nationalDetail, setNationalDetail] = useState<GridDetailData | null>(
    null,
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Safety net: if the sheet unmounts (closes) while the pointer is still
  // over it, the pointerleave that would normally clear the hover flag never
  // fires — without this, map interaction would stay blocked.
  useEffect(() => {
    return () => {
      onHoverChange(false);
    };
  }, [onHoverChange]);

  useEffect(() => {
    if (!feature) {
      setNationalDetail(null);
      setError(null);
      return;
    }

    const gridKey = stringProperty(feature, "national_grid_cell_key");

    if (!gridKey) {
      setNationalDetail(null);
      setError(null);
      return;
    }

    let cancelled = false;

    setLoading(true);
    setError(null);

    loadGridDetail(gridKey, referenceMonth)
      .then((result) => {
        if (!cancelled) {
          setNationalDetail(result);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setError("National context could not be loaded.");
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
  }, [feature, referenceMonth]);

  const title = feature
    ? propertyTitle(feature, featureKey ?? "Selected property")
    : "";

  return (
    <AnimatePresence>
      {feature && featureKey && (
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
                {scope === "vancouver"
                  ? "Vancouver · Parcel"
                  : "Calgary · Property"}
              </div>

              <div className="detail-title">{title}</div>

              <div className="detail-subtitle">{featureKey}</div>
            </div>

            <button
              className="detail-close-button"
              onClick={onClose}
              aria-label="Close property details"
            >
              <X size={17} />
            </button>
          </div>

          <div className="detail-scroll">
            <CityAssessmentSection scope={scope} feature={feature} />

            <div className="detail-divider" />

            <CityFloodSection scope={scope} feature={feature} />

            <div className="detail-divider" />

            <NationalContextSection
              feature={feature}
              referenceMonth={referenceMonth}
              detail={nationalDetail}
              loading={loading}
              error={error}
            />
          </div>
        </motion.aside>
      )}
    </AnimatePresence>
  );
}
