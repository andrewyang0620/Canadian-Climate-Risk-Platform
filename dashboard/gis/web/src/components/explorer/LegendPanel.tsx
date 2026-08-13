import { useEffect, useState } from "react";
import { AnimatePresence, motion } from "motion/react";

import {
  loadMonthlyDataset,
  type MonthlyDisplayStatistics,
} from "../../lib/gis-data";
import {
  layerGradient,
  resolveLayerDefinition,
  type LayerId,
} from "../../lib/layer-registry";

interface LegendPanelProps {
  layerId: LayerId;
  referenceMonth: string;
}

interface StatisticsState {
  referenceMonth: string;
  values: MonthlyDisplayStatistics;
}

export function LegendPanel({
  layerId,
  referenceMonth,
}: LegendPanelProps) {
  const [statisticsState, setStatisticsState] =
    useState<StatisticsState | null>(null);

  useEffect(() => {
    let cancelled = false;

    loadMonthlyDataset(referenceMonth)
      .then((dataset) => {
        if (cancelled) {
          return;
        }

        setStatisticsState({
          referenceMonth,
          values: dataset.displayStatistics,
        });
      })
      .catch(() => {
        if (cancelled) {
          return;
        }

        setStatisticsState({
          referenceMonth,
          values: {},
        });
      });

    return () => {
      cancelled = true;
    };
  }, [referenceMonth]);

  const displayStatistics =
    statisticsState?.referenceMonth === referenceMonth
      ? statisticsState.values
      : undefined;

  const definition = resolveLayerDefinition(
    layerId,
    displayStatistics,
  );

  return (
    <AnimatePresence mode="wait">
      <motion.section
        key={layerId}
        className="legend-panel glass-panel"
        initial={{ opacity: 0, y: 4 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: 3 }}
        transition={{ duration: 0.14 }}
      >
        <div className="legend-heading">
          {definition.label}
        </div>

        <div
          className="legend-gradient"
          style={{
            background: layerGradient(definition),
          }}
        />

        <div className="legend-labels">
          {definition.legendLabels.map((label) => (
            <span key={label}>
              {label}
            </span>
          ))}
        </div>
      </motion.section>
    </AnimatePresence>
  );
}
