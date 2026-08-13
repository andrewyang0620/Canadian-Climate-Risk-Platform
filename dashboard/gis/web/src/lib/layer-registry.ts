export type RGB = [number, number, number];

export type RGBA = [number, number, number, number];

export type LayerGroup = "risk" | "climate" | "hydro" | "wildfire";

export type LayerId =
  | "composite_risk_score"
  | "score_confidence"
  | "climate_sub_score"
  | "climate_mean_temp_c"
  | "climate_total_precip_mm"
  | "climate_extreme_heat_days"
  | "hydro_sub_score"
  | "wildfire_sub_score"
  | "wildfire_intersection_area_ratio_of_grid";

interface ColorStop {
  position: number;
  color: RGB;
}

export interface LayerDefinition {
  id: LayerId;
  column: string;
  label: string;
  group: LayerGroup;
  domain: [number, number];
  unit?: string;
  stops: ColorStop[];
  alphaMode: "value" | "fixed" | "nonzero";
  alphaMin: number;
  alphaMax: number;
  legendLabels: [string, string, string];
}

export const LAYER_DEFINITIONS: Record<LayerId, LayerDefinition> = {
  composite_risk_score: {
    id: "composite_risk_score",
    column: "composite_risk_score",
    label: "Composite Risk",
    group: "risk",
    domain: [0, 0.8],
    stops: [
      { position: 0, color: [244, 234, 200] },
      { position: 0.35, color: [242, 175, 91] },
      { position: 0.65, color: [217, 112, 65] },
      { position: 1, color: [112, 35, 52] },
    ],
    alphaMode: "value",
    alphaMin: 55,
    alphaMax: 205,
    legendLabels: ["Low", "Elevated", "High"],
  },
  score_confidence: {
    id: "score_confidence",
    column: "score_confidence",
    label: "Score Confidence",
    group: "risk",
    domain: [0, 1],
    stops: [
      { position: 0, color: [84, 86, 124] },
      { position: 0.5, color: [65, 137, 139] },
      { position: 1, color: [154, 210, 174] },
    ],
    alphaMode: "value",
    alphaMin: 55,
    alphaMax: 185,
    legendLabels: ["Low", "Medium", "High"],
  },
  climate_sub_score: {
    id: "climate_sub_score",
    column: "climate_sub_score",
    label: "Climate Risk",
    group: "climate",
    domain: [0, 1],
    stops: [
      { position: 0, color: [242, 232, 206] },
      { position: 0.5, color: [229, 145, 94] },
      { position: 1, color: [157, 50, 55] },
    ],
    alphaMode: "value",
    alphaMin: 50,
    alphaMax: 195,
    legendLabels: ["Low", "Elevated", "High"],
  },
  climate_mean_temp_c: {
    id: "climate_mean_temp_c",
    column: "climate_mean_temp_c",
    label: "Mean Temperature",
    group: "climate",
    domain: [-30, 30],
    unit: "deg C",
    stops: [
      { position: 0, color: [54, 83, 177] },
      { position: 0.5, color: [238, 235, 223] },
      { position: 1, color: [194, 70, 61] },
    ],
    alphaMode: "fixed",
    alphaMin: 145,
    alphaMax: 145,
    legendLabels: ["-30 deg C", "0 deg C", "30 deg C"],
  },
  climate_total_precip_mm: {
    id: "climate_total_precip_mm",
    column: "climate_total_precip_mm",
    label: "Total Precipitation",
    group: "climate",
    domain: [0, 400],
    unit: "mm",
    stops: [
      { position: 0, color: [221, 236, 232] },
      { position: 0.5, color: [86, 158, 185] },
      { position: 1, color: [25, 70, 116] },
    ],
    alphaMode: "value",
    alphaMin: 45,
    alphaMax: 190,
    legendLabels: ["0 mm", "200 mm", "400+ mm"],
  },
  climate_extreme_heat_days: {
    id: "climate_extreme_heat_days",
    column: "climate_extreme_heat_days",
    label: "Extreme Heat Days",
    group: "climate",
    domain: [0, 31],
    unit: "days",
    stops: [
      { position: 0, color: [244, 237, 217] },
      { position: 0.5, color: [236, 143, 76] },
      { position: 1, color: [152, 43, 45] },
    ],
    alphaMode: "value",
    alphaMin: 35,
    alphaMax: 205,
    legendLabels: ["0", "15 days", "31 days"],
  },
  hydro_sub_score: {
    id: "hydro_sub_score",
    column: "hydro_sub_score",
    label: "Hydro Risk",
    group: "hydro",
    domain: [0, 1],
    stops: [
      { position: 0, color: [219, 239, 235] },
      { position: 0.5, color: [59, 145, 178] },
      { position: 1, color: [25, 57, 111] },
    ],
    alphaMode: "value",
    alphaMin: 45,
    alphaMax: 200,
    legendLabels: ["Low", "Elevated", "High"],
  },
  wildfire_sub_score: {
    id: "wildfire_sub_score",
    column: "wildfire_sub_score",
    label: "Wildfire Risk",
    group: "wildfire",
    domain: [0, 1],
    stops: [
      { position: 0, color: [245, 231, 197] },
      { position: 0.45, color: [235, 139, 65] },
      { position: 1, color: [123, 28, 43] },
    ],
    alphaMode: "value",
    alphaMin: 35,
    alphaMax: 210,
    legendLabels: ["Low", "Elevated", "High"],
  },
  wildfire_intersection_area_ratio_of_grid: {
    id: "wildfire_intersection_area_ratio_of_grid",
    column: "wildfire_intersection_area_ratio_of_grid",
    label: "Wildfire Grid Overlap",
    group: "wildfire",
    domain: [0, 1],
    stops: [
      { position: 0, color: [245, 188, 83] },
      { position: 0.5, color: [227, 82, 48] },
      { position: 1, color: [112, 20, 38] },
    ],
    alphaMode: "nonzero",
    alphaMin: 110,
    alphaMax: 225,
    legendLabels: ["None", "50%", "100%"],
  },
};

export const LAYER_GROUPS: Array<{
  id: LayerGroup;
  label: string;
}> = [
  { id: "risk", label: "Risk" },
  { id: "climate", label: "Climate" },
  { id: "hydro", label: "Hydro" },
  { id: "wildfire", label: "Wildfire" },
];

export function getLayerDefinition(layerId: LayerId): LayerDefinition {
  return LAYER_DEFINITIONS[layerId];
}

export function isLayerId(value: string | null): value is LayerId {
  if (!value) {
    return false;
  }

  return value in LAYER_DEFINITIONS;
}

function interpolateColor(left: RGB, right: RGB, progress: number): RGB {
  return left.map((channel, index) =>
    Math.round(channel + (right[index] - channel) * progress),
  ) as RGB;
}

export function normalizeLayerValue(
  definition: LayerDefinition,
  value: number,
): number {
  const [minimum, maximum] = definition.domain;

  return Math.max(0, Math.min(1, (value - minimum) / (maximum - minimum)));
}

export function layerColor(
  definition: LayerDefinition,
  value: number | null,
): RGBA {
  if (value === null) {
    return [90, 102, 96, 38];
  }

  if (definition.alphaMode === "nonzero" && value <= 0) {
    return [0, 0, 0, 0];
  }

  const normalized = normalizeLayerValue(definition, value);
  let rgb = definition.stops[definition.stops.length - 1].color;

  for (let index = 0; index < definition.stops.length - 1; index += 1) {
    const left = definition.stops[index];
    const right = definition.stops[index + 1];

    if (normalized >= left.position && normalized <= right.position) {
      const progress =
        (normalized - left.position) / (right.position - left.position);

      rgb = interpolateColor(left.color, right.color, progress);
      break;
    }
  }

  let alpha = definition.alphaMin;

  if (definition.alphaMode === "value" || definition.alphaMode === "nonzero") {
    alpha = Math.round(
      definition.alphaMin + normalized * (definition.alphaMax - definition.alphaMin),
    );
  }

  return [rgb[0], rgb[1], rgb[2], alpha];
}

export function layerGradient(definition: LayerDefinition): string {
  const stops = definition.stops
    .map(({ position, color }) => `rgb(${color.join(",")}) ${position * 100}%`)
    .join(", ");

  return `linear-gradient(90deg, ${stops})`;
}

export function formatLayerValue(
  definition: LayerDefinition,
  value: number | null,
): string {
  if (value === null) {
    return "No data";
  }

  if (definition.unit === "deg C") {
    return `${value.toFixed(1)} deg C`;
  }

  if (definition.unit === "mm") {
    return `${value.toFixed(1)} mm`;
  }

  if (definition.unit === "days") {
    return `${Math.round(value)} days`;
  }

  if (definition.id.includes("score")) {
    return value.toFixed(3);
  }

  if (definition.id === "wildfire_intersection_area_ratio_of_grid") {
    return `${(value * 100).toFixed(1)}%`;
  }

  return value.toFixed(2);
}
