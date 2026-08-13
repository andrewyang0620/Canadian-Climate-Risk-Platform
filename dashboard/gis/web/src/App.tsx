import { useEffect, useState } from "react";
import { Info, MapPinned } from "lucide-react";
import { motion } from "motion/react";

import { ExplorerControls, type RegionId } from "./components/explorer/ExplorerControls";
import { GridDetailSheet } from "./components/explorer/GridDetailSheet";
import { LegendPanel } from "./components/explorer/LegendPanel";
import { TimelineControl } from "./components/explorer/TimelineControl";
import { MapCanvas } from "./components/map/MapCanvas";
import { readExplorerUrl, writeExplorerUrl } from "./lib/explorer-url";
import { loadManifest } from "./lib/gis-data";
import type { LayerId } from "./lib/layer-registry";

export default function App() {
  const initialUrlState = readExplorerUrl();
  const [months, setMonths] = useState<string[]>([]);
  const [referenceMonth, setReferenceMonth] = useState(
    initialUrlState.month ?? "2023-08",
  );
  const [activeLayerId, setActiveLayerId] = useState<LayerId>(
    initialUrlState.layer ?? "composite_risk_score",
  );
  const [region, setRegion] = useState<RegionId>(
    initialUrlState.region ?? "all",
  );
  const [selectedGridKey, setSelectedGridKey] = useState<string | null>(
    initialUrlState.grid,
  );

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
      month: referenceMonth,
      layer: activeLayerId,
      region,
      grid: selectedGridKey,
    });
  }, [referenceMonth, activeLayerId, region, selectedGridKey]);

  return (
    <main className={selectedGridKey ? "app-shell detail-open" : "app-shell"}>
      <div className="map-stage">
        <MapCanvas
          referenceMonth={referenceMonth}
          activeLayerId={activeLayerId}
          region={region}
          selectedGridKey={selectedGridKey}
          onGridSelect={setSelectedGridKey}
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
        activeLayerId={activeLayerId}
        onLayerChange={setActiveLayerId}
        referenceMonth={referenceMonth}
        region={region}
        onRegionChange={(nextRegion) => {
          setRegion(nextRegion);
          setSelectedGridKey(null);
        }}
      />

      {!selectedGridKey && (
        <button className="info-button glass-panel" aria-label="About this explorer">
          <Info size={18} />
        </button>
      )}

      <LegendPanel layerId={activeLayerId} />

      {months.length > 0 && (
        <TimelineControl
          months={months}
          referenceMonth={referenceMonth}
          onMonthChange={setReferenceMonth}
        />
      )}

      <GridDetailSheet
        gridCellKey={selectedGridKey}
        referenceMonth={referenceMonth}
        onClose={() => {
          setSelectedGridKey(null);
        }}
      />
    </main>
  );
}
