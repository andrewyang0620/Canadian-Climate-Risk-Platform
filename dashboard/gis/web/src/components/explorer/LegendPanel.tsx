import { AnimatePresence, motion } from "motion/react";

import {
  getLayerDefinition,
  layerGradient,
  type LayerId,
} from "../../lib/layer-registry";

interface LegendPanelProps {
  layerId: LayerId;
}

export function LegendPanel({ layerId }: LegendPanelProps) {
  const definition = getLayerDefinition(layerId);

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
        <div className="legend-heading">{definition.label}</div>

        <div
          className="legend-gradient"
          style={{ background: layerGradient(definition) }}
        />

        <div className="legend-labels">
          {definition.legendLabels.map((label) => (
            <span key={label}>{label}</span>
          ))}
        </div>
      </motion.section>
    </AnimatePresence>
  );
}
