import { useEffect, useState } from "react";
import { MapPinned } from "lucide-react";
import { motion } from "motion/react";

import { ExplorerControls, type RegionId } from "./components/explorer/ExplorerControls";
import { CityActivityDetailSheet } from "./components/explorer/CityActivityDetailSheet";
import { CityDetailSheet } from "./components/explorer/CityDetailSheet";
import { CityLegendPanel } from "./components/explorer/CityLegendPanel";
import { GridDetailSheet } from "./components/explorer/GridDetailSheet";
import { LegendPanel } from "./components/explorer/LegendPanel";
import { TimelineControl } from "./components/explorer/TimelineControl";
import { MapCanvas } from "./components/map/MapCanvas";
import { readExplorerUrl, writeExplorerUrl } from "./lib/explorer-url";
import { loadManifest } from "./lib/gis-data";
import type { LayerId } from "./lib/layer-registry";
import {
  isCityScope,
  resolveActivityPropertyKeys,
  type CityActivityKind,
  type CityFeature,
  type CityLayerId,
  type ExplorerScope,
} from "./lib/city-data";

interface CitySelection {
  key: string;
  feature: CityFeature;
}

interface CityActivitySelection {
  kind: CityActivityKind;
  key: string;
  feature: CityFeature;
  propertyKeys: string[];
  resolving: boolean;
}

export default function App() {
  const initialUrlState = readExplorerUrl();
  const [months, setMonths] = useState<string[]>([]);
  const [scope, setScope] = useState<ExplorerScope>(
    initialUrlState.scope ?? "national",
  );
  const [referenceMonth, setReferenceMonth] = useState(
    initialUrlState.month ?? "2023-08",
  );
  const [activeLayerId, setActiveLayerId] = useState<LayerId>(
    initialUrlState.layer ?? "composite_risk_score",
  );
  const [layersVisible, setLayersVisible] = useState(true);
  const [gridInfoVisible, setGridInfoVisible] = useState(true);
  const [aboutHovered, setAboutHovered] = useState(false);
  // Pointer over the control bar/buttons or an open detail sheet — blocks
  // grid/property picking underneath so hovering UI chrome never triggers a
  // hover tooltip or selection on the map layer behind it.
  const [chromeHovered, setChromeHovered] = useState(false);
  const [region, setRegion] = useState<RegionId>(
    initialUrlState.region ?? "all",
  );
  const [selectedGridKey, setSelectedGridKey] = useState<string | null>(
    initialUrlState.grid,
  );
  const [selectedCityFeature, setSelectedCityFeature] =
    useState<CitySelection | null>(null);
  const [selectedCityActivity, setSelectedCityActivity] =
    useState<CityActivitySelection | null>(null);
  const [cityLayerId, setCityLayerId] = useState<CityLayerId>(
    initialUrlState.cityLayer ?? "flood",
  );

  // Scope changes only happen through this handler (ExplorerControls' picker
  // calls onScopeChange, which is wired to this) — never through a [scope]
  // effect. That distinction matters: an effect would also fire on initial
  // mount and wipe out clayer=... restored from the URL before the page ever
  // renders. Reset logic belongs here, on the interactive transition only.
  function handleScopeChange(nextScope: ExplorerScope) {
    if (nextScope === scope) {
      return;
    }

    setScope(nextScope);

    if (nextScope !== "national") {
      setSelectedGridKey(null);
    } else {
      setLayersVisible(true);
    }

    setSelectedCityFeature(null);
    setSelectedCityActivity(null);
    // "development_permits" isn't a valid option for Vancouver, and a fresh
    // city visit should default to the flood read anyway.
    setCityLayerId("flood");
  }

  // Switching the city layer mode away from a BP/DP activity's kind should
  // also drop its selection — otherwise the detail sheet and the linked-
  // property highlight stay on for a layer that's no longer even rendered.
  useEffect(() => {
    if (selectedCityActivity && cityLayerId !== selectedCityActivity.kind) {
      setSelectedCityActivity(null);
    }
  }, [cityLayerId, selectedCityActivity]);

  // Selecting a BP/DP resolves its linked property key(s) from Gold-mapped
  // relationships only (never a nearest-parcel guess). Guards against a
  // stale async response overwriting a newer selection: if the user clicks
  // permit B while permit A's lookup is still in flight, A's result is
  // discarded on arrival rather than clobbering B's selection.
  async function handleCityActivitySelect(
    kind: CityActivityKind,
    key: string,
    feature: CityFeature,
  ) {
    if (!isCityScope(scope)) {
      return;
    }

    setSelectedCityFeature(null);

    setSelectedCityActivity({
      kind,
      key,
      feature,
      propertyKeys: [],
      resolving: true,
    });

    try {
      const propertyKeys = await resolveActivityPropertyKeys(
        scope,
        kind,
        feature,
      );

      setSelectedCityActivity((current) => {
        if (!current || current.kind !== kind || current.key !== key) {
          return current;
        }

        return {
          ...current,
          propertyKeys,
          resolving: false,
        };
      });
    } catch (error) {
      console.error("Failed to resolve activity property links", error);

      setSelectedCityActivity((current) => {
        if (!current || current.kind !== kind || current.key !== key) {
          return current;
        }

        return {
          ...current,
          resolving: false,
        };
      });
    }
  }

  useEffect(() => {
    loadManifest().then((manifest) => {
      const available = manifest.monthly_data.months;

      setMonths(available);

      if (!available.includes(referenceMonth)) {
        setReferenceMonth(available[available.length - 1]);
      }
    });
  }, [referenceMonth]);

  useEffect(() => {
    writeExplorerUrl({
      scope,
      month: referenceMonth,
      layer: activeLayerId,
      region,
      grid: selectedGridKey,
      cityLayerId,
    });
  }, [
    scope,
    referenceMonth,
    activeLayerId,
    region,
    selectedGridKey,
    cityLayerId,
  ]);

  const detailOpen =
    (scope === "national" && selectedGridKey) ||
    (isCityScope(scope) && (selectedCityFeature || selectedCityActivity));

  return (
    <main className={detailOpen ? "app-shell detail-open" : "app-shell"}>
      <div className="map-stage">
        <MapCanvas
          scope={scope}
          referenceMonth={referenceMonth}
          activeLayerId={activeLayerId}
          region={region}
          selectedGridKey={selectedGridKey}
          onGridSelect={setSelectedGridKey}
          selectedCityFeatureKey={selectedCityFeature?.key ?? null}
          onCityFeatureSelect={(key, feature) => {
            setSelectedCityActivity(null);
            setSelectedCityFeature({ key, feature });
          }}
          selectedActivityKind={selectedCityActivity?.kind ?? null}
          selectedActivityKey={selectedCityActivity?.key ?? null}
          relatedCityFeatureKeys={selectedCityActivity?.propertyKeys ?? []}
          onCityActivitySelect={handleCityActivitySelect}
          cityLayerId={cityLayerId}
          layersVisible={layersVisible}
          gridInfoVisible={gridInfoVisible && !aboutHovered && !chromeHovered}
          gridInteractionEnabled={!aboutHovered && !chromeHovered}
        />
      </div>

      <motion.header
        className="brand-panel glass-panel"
        initial={{
          opacity: 0,
          y: -8,
        }}
        animate={{
          opacity: 1,
          y: 0,
        }}
      >
        <div className="brand-mark">
          <MapPinned size={17} />
        </div>

        <div>
          <div className="brand-title">Canadian Climate Risk</div>

          <div className="brand-subtitle">Alberta · British Columbia</div>
        </div>
      </motion.header>

      <ExplorerControls
        scope={scope}
        onScopeChange={handleScopeChange}
        cityLayerId={cityLayerId}
        onCityLayerChange={setCityLayerId}
        activeLayerId={activeLayerId}
        onLayerChange={setActiveLayerId}
        months={months}
        referenceMonth={referenceMonth}
        onMonthChange={setReferenceMonth}
        onRegionChange={(nextRegion) => {
          setRegion(nextRegion);
          setSelectedGridKey(null);
        }}
        layersVisible={layersVisible}
        onLayersVisibleChange={setLayersVisible}
        gridInfoVisible={gridInfoVisible}
        onGridInfoVisibleChange={setGridInfoVisible}
        onAboutHoverChange={setAboutHovered}
        onControlsHoverChange={setChromeHovered}
        showAbout={!selectedGridKey && !selectedCityFeature && !selectedCityActivity}
      />

      {scope === "national" && layersVisible && (
        <LegendPanel
          layerId={activeLayerId}
          referenceMonth={referenceMonth}
        />
      )}

      {isCityScope(scope) && cityLayerId !== "none" && (
        <CityLegendPanel scope={scope} cityLayerId={cityLayerId} />
      )}

      {scope === "national" && months.length > 0 && (
        <TimelineControl
          months={months}
          referenceMonth={referenceMonth}
          onMonthChange={setReferenceMonth}
        />
      )}

      {scope === "national" && (
        <GridDetailSheet
          gridCellKey={selectedGridKey}
          referenceMonth={referenceMonth}
          onClose={() => {
            setSelectedGridKey(null);
          }}
          onHoverChange={setChromeHovered}
        />
      )}

      {isCityScope(scope) && (
        <CityDetailSheet
          scope={scope}
          featureKey={selectedCityFeature?.key ?? null}
          feature={selectedCityFeature?.feature ?? null}
          referenceMonth={referenceMonth}
          onClose={() => {
            setSelectedCityFeature(null);
          }}
          onHoverChange={setChromeHovered}
        />
      )}

      {isCityScope(scope) && (
        <CityActivityDetailSheet
          scope={scope}
          kind={selectedCityActivity?.kind ?? null}
          featureKey={selectedCityActivity?.key ?? null}
          feature={selectedCityActivity?.feature ?? null}
          propertyKeys={selectedCityActivity?.propertyKeys ?? []}
          resolving={selectedCityActivity?.resolving ?? false}
          onClose={() => {
            setSelectedCityActivity(null);
          }}
          onHoverChange={setChromeHovered}
        />
      )}
    </main>
  );
}
