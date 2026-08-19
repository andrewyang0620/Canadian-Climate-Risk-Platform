import { useEffect } from "react";
import { AnimatePresence, motion } from "motion/react";
import { X } from "lucide-react";

import type {
  CityActivityKind,
  CityFeature,
  CityScope,
} from "../../lib/city-data";

import { MetricRow } from "./MetricRow";

interface CityActivityDetailSheetProps {
  scope: CityScope;
  kind: CityActivityKind | null;
  featureKey: string | null;
  feature: CityFeature | null;
  propertyKeys: string[];
  resolving: boolean;
  onClose: () => void;
  onHoverChange: (hovering: boolean) => void;
}

function stringProperty(feature: CityFeature, name: string): string | null {
  const value = feature.properties?.[name];

  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();

  return text || null;
}

function numberProperty(feature: CityFeature, name: string): number | null {
  const value = feature.properties?.[name];

  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function booleanProperty(
  feature: CityFeature,
  name: string,
): boolean | null {
  const value = feature.properties?.[name];

  if (value === undefined || value === null) {
    return null;
  }

  return value === true || value === 1;
}

function formatSnakeCase(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
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

function kindLabel(kind: CityActivityKind): string {
  return kind === "development_permits"
    ? "Development Permit"
    : "Building Permit";
}

function activityTitle(feature: CityFeature): string {
  return (
    stringProperty(feature, "address_text") ??
    stringProperty(feature, "category") ??
    "Unnamed permit"
  );
}

// Vancouver BP carries parcel_mapping_status; Calgary BP and Calgary DP both
// carry location_mapping_status instead — same concept, different Gold column.
function mappingStatusField(scope: CityScope, kind: CityActivityKind): string {
  if (kind === "development_permits") {
    return "location_mapping_status";
  }

  return scope === "vancouver"
    ? "parcel_mapping_status"
    : "location_mapping_status";
}

function isFloodExposed(feature: CityFeature): boolean {
  return booleanProperty(feature, "is_flood_exposed") ?? false;
}

// Identity + classification fields only — permit_type/class/work_class are
// short coded labels, safe to show in full. The free-text project
// description field is deliberately left out: it can run past 2000
// characters and would turn this into a database-row viewer.
function PermitIdentitySection({
  kind,
  feature,
}: {
  kind: CityActivityKind;
  feature: CityFeature;
}) {
  const permitNumber = stringProperty(feature, "permit_number");
  const status =
    stringProperty(feature, "permit_status") ??
    stringProperty(feature, "status_current");
  const permitType = stringProperty(feature, "permit_type_mapped");
  const permitClass =
    stringProperty(feature, "permit_class_mapped") ??
    stringProperty(feature, "category");
  const workClass = stringProperty(feature, "work_class_mapped");
  const proposedUse = stringProperty(feature, "proposed_use_description");
  const housingActivity = booleanProperty(feature, "is_housing_related")
    ? stringProperty(feature, "housing_activity_type")
    : null;
  const dateRaw =
    kind === "development_permits"
      ? stringProperty(feature, "decision_date") ??
        stringProperty(feature, "applied_date")
      : stringProperty(feature, "issue_date");
  const date = dateRaw ? formatDate(dateRaw) : null;
  const estimatedCost = numberProperty(feature, "estimated_project_cost");

  return (
    <section className="detail-section">
      <div className="section-title">Permit</div>

      <div className="evidence-group">
        {permitNumber && (
          <MetricRow label="Permit number" value={permitNumber} />
        )}

        {status && (
          <MetricRow label="Status" value={formatSnakeCase(status)} />
        )}

        {permitType && <MetricRow label="Type" value={permitType} />}

        {permitClass && <MetricRow label="Class" value={permitClass} />}

        {workClass && (
          <MetricRow label="Work class" value={workClass} />
        )}

        {proposedUse && (
          <MetricRow label="Proposed use" value={proposedUse} />
        )}

        {housingActivity && (
          <MetricRow
            label="Housing activity"
            value={formatSnakeCase(housingActivity)}
          />
        )}

        {date && (
          <MetricRow
            label={
              kind === "development_permits" ? "Decision date" : "Issue date"
            }
            value={date}
          />
        )}

        {kind === "building_permits" && estimatedCost !== null && (
          <MetricRow
            label="Estimated cost"
            value={formatCurrency(estimatedCost)}
          />
        )}
      </div>

      <div className="evidence-group">
        <MetricRow
          label="Flood exposure"
          value={
            isFloodExposed(feature)
              ? "Flood-exposed location"
              : "No mapped flood exposure"
          }
        />
      </div>
    </section>
  );
}

// Summary only — a DP with 261 mapped properties does not get 261 rows here.
// The map is the right place to show that many spatial relationships; this
// sheet states the count and mapping status, then points at the map.
function LinkedPropertiesSection({
  scope,
  kind,
  feature,
  propertyKeys,
  resolving,
}: {
  scope: CityScope;
  kind: CityActivityKind;
  feature: CityFeature;
  propertyKeys: string[];
  resolving: boolean;
}) {
  const mappingStatus = stringProperty(
    feature,
    mappingStatusField(scope, kind),
  );
  const partialMapping = booleanProperty(feature, "has_partial_spatial_mapping");

  return (
    <section className="detail-section">
      <div className="section-title">Linked Properties</div>

      {resolving ? (
        <div className="detail-loading">Resolving linked properties...</div>
      ) : (
        <>
          <div className="evidence-group">
            <MetricRow
              label="Linked properties"
              value={
                propertyKeys.length === 0
                  ? "0 properties"
                  : `${propertyKeys.length.toLocaleString("en-CA")} ${
                      propertyKeys.length === 1 ? "property" : "properties"
                    }`
              }
            />

            {mappingStatus && (
              <MetricRow label="Mapping" value={formatSnakeCase(mappingStatus)} />
            )}

            {partialMapping === true && (
              <MetricRow label="Coverage" value="Partial spatial mapping" />
            )}
          </div>

          <div className="detail-note">
            {propertyKeys.length === 0
              ? "No mapped property to highlight for this permit."
              : "Highlighted on the map, wherever currently loaded in the viewport."}
          </div>
        </>
      )}
    </section>
  );
}

export function CityActivityDetailSheet({
  scope,
  kind,
  featureKey,
  feature,
  propertyKeys,
  resolving,
  onClose,
  onHoverChange,
}: CityActivityDetailSheetProps) {
  const title = feature ? activityTitle(feature) : "";

  // Safety net: if the sheet unmounts (closes) while the pointer is still
  // over it, the pointerleave that would normally clear the hover flag never
  // fires — without this, map interaction would stay blocked.
  useEffect(() => {
    return () => {
      onHoverChange(false);
    };
  }, [onHoverChange]);

  return (
    <AnimatePresence>
      {kind && featureKey && feature && (
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
                {scope === "vancouver" ? "Vancouver" : "Calgary"} ·{" "}
                {kindLabel(kind)}
              </div>

              <div className="detail-title">{title}</div>

              <div className="detail-subtitle">{featureKey}</div>
            </div>

            <button
              className="detail-close-button"
              onClick={onClose}
              aria-label="Close permit details"
            >
              <X size={17} />
            </button>
          </div>

          <div className="detail-scroll">
            <PermitIdentitySection kind={kind} feature={feature} />

            <div className="detail-divider" />

            <LinkedPropertiesSection
              scope={scope}
              kind={kind}
              feature={feature}
              propertyKeys={propertyKeys}
              resolving={resolving}
            />
          </div>
        </motion.aside>
      )}
    </AnimatePresence>
  );
}
