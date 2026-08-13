import type { MonthlyDisplayStatistics } from "./gis-data";

export type RGB = [number, number, number];

export type RGBA = [number, number, number, number];

export type LayerGroup = "composite" | "climate" | "hydro" | "wildfire";

export type LayerId =
  | "composite_risk_score"
  | "climate_sub_score"
  | "climate_mean_temp_c"
  | "climate_total_precip_mm"
  | "climate_extreme_heat_days"
  | "climate_extreme_cold_days"
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
  transparentBelow?: number;
  legendLabels: [string, string, string];
  about: string;
}

export const LAYER_DEFINITIONS: Record<LayerId, LayerDefinition> = {
  composite_risk_score: {
    id: "composite_risk_score",
    column: "composite_risk_score",
    label: "Composite Risk",
    group: "composite",
    domain: [0, 0.8],
    stops: [
      { position: 0, color: [252, 250, 244] },
      { position: 0.28, color: [246, 190, 106] },
      { position: 0.58, color: [203, 58, 48] },
      { position: 1, color: [78, 8, 30] },
    ],
    alphaMode: "value",
    alphaMin: 78,
    alphaMax: 252,
    transparentBelow: 0.15,
    legendLabels: ["Low", "Elevated", "High"],
    about:
      "Composite Risk combines Climate, Hydro, and Wildfire scores with base weights of 35%, 35%, and 30%. Missing domains are not treated as zero; available weights are renormalized, and at least two domains are required.",
  },
  climate_sub_score: {
    id: "climate_sub_score",
    column: "climate_sub_score",
    label: "Climate Risk",
    group: "climate",
    domain: [0, 1],
    stops: [
      { position: 0, color: [252, 250, 244] },
      { position: 0.32, color: [244, 181, 96] },
      { position: 0.62, color: [196, 49, 47] },
      { position: 1, color: [84, 9, 34] },
    ],
    alphaMode: "value",
    alphaMin: 76,
    alphaMax: 250,
    transparentBelow: 0.15,
    legendLabels: ["Low", "Elevated", "High"],
    about:
      "Climate Risk combines extreme heat, heavy precipitation, freeze-thaw, extreme cold, and total precipitation signals. Each signal is ranked within the same province and calendar month, preserving true zero values as zero.",
  },
  climate_mean_temp_c: {
    id: "climate_mean_temp_c",
    column: "climate_mean_temp_c",
    label: "Mean Temperature",
    group: "climate",
    domain: [-30, 30],
    unit: "°C",
    stops: [
      { position: 0, color: [54, 83, 177] },
      { position: 0.5, color: [238, 235, 223] },
      { position: 1, color: [194, 70, 61] },
    ],
    alphaMode: "fixed",
    alphaMin: 205,
    alphaMax: 205,
    legendLabels: ["-30 °C", "0 °C", "30 °C"],
    about:
      "Mean Temperature is a physical climate indicator, not an input to Climate Risk. Station daily mean temperatures are averaged to a monthly value, then mapped to each 10 km grid by direct station values or 150 km IDW interpolation.",
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
    alphaMin: 96,
    alphaMax: 238,
    legendLabels: ["0 mm", "200 mm", "400+ mm"],
    about:
      "Total Precipitation is the monthly sum of daily precipitation at stations, mapped to each grid by direct stations or 150 km IDW interpolation. It also contributes 15% to Climate Risk after province-month percentile ranking.",
  },
  climate_extreme_heat_days: {
    id: "climate_extreme_heat_days",
    column: "climate_extreme_heat_days",
    label: "Extreme Heat Days",
    group: "climate",
    domain: [0, 31],
    unit: "days",
    stops: [
      { position: 0, color: [253, 250, 242] },
      { position: 0.18, color: [246, 181, 86] },
      { position: 0.48, color: [171, 34, 43] },
      { position: 1, color: [74, 7, 28] },
    ],
    alphaMode: "value",
    alphaMin: 76,
    alphaMax: 250,
    legendLabels: ["0", "15 days", "31 days"],
    about:
      "Extreme Heat Days count days where daily maximum temperature is at least 30 °C. Grid values can be fractional after station averaging or IDW interpolation, then feed Climate Risk with a 30% weight.",
  },
  climate_extreme_cold_days: {
    id: "climate_extreme_cold_days",
    column: "climate_extreme_cold_days",
    label: "Extreme Cold Days",
    group: "climate",
    domain: [0, 31],
    unit: "days",
    stops: [
      { position: 0, color: [249, 252, 255] },
      { position: 0.18, color: [147, 190, 222] },
      { position: 0.48, color: [42, 83, 162] },
      { position: 1, color: [13, 28, 90] },
    ],
    alphaMode: "value",
    alphaMin: 76,
    alphaMax: 250,
    legendLabels: ["0", "15 days", "31 days"],
    about:
      "Extreme Cold Days count days where daily minimum temperature is at or below -20 °C. The mapped grid value contributes 15% to Climate Risk after zero-preserving province-month percentile ranking.",
  },
  hydro_sub_score: {
    id: "hydro_sub_score",
    column: "hydro_sub_score",
    label: "Hydro Risk",
    group: "hydro",
    domain: [0, 1],
    stops: [
      { position: 0, color: [205, 235, 231] },
      { position: 0.5, color: [39, 139, 186] },
      { position: 1, color: [12, 49, 128] },
    ],
    alphaMode: "value",
    alphaMin: 100,
    alphaMax: 245,
    transparentBelow: 0.15,
    legendLabels: ["Low", "Elevated", "High"],
    about:
      "Hydro Risk combines flow P95, flow variability, zero-flow ratio, level P95, and level variability. Historical percentiles are calculated within each grid and calendar month; missing signals are renormalized.",
  },
  wildfire_sub_score: {
    id: "wildfire_sub_score",
    column: "wildfire_sub_score",
    label: "Wildfire Risk",
    group: "wildfire",
    domain: [0, 1],
    stops: [
      { position: 0, color: [252, 250, 244] },
      { position: 0.28, color: [246, 190, 106] },
      { position: 0.58, color: [203, 58, 48] },
      { position: 1, color: [78, 8, 30] },
    ],
    alphaMode: "value",
    alphaMin: 76,
    alphaMax: 252,
    legendLabels: ["Low", "Elevated", "High"],
    about:
      "Wildfire Risk is derived from wildfire-grid overlap. Positive overlaps are ranked within the province across the dataset; zero overlap remains exactly zero rather than a low percentile.",
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
    alphaMin: 172,
    alphaMax: 252,
    legendLabels: ["None", "50%", "100%"],
    about:
      "Wildfire Grid Overlap is the raw share of a grid cell intersected by wildfire perimeter polygons in the selected month. It is an exposure indicator, not a percentile risk score.",
  },
};

export const LAYER_GROUPS: Array<{
  id: LayerGroup;
  label: string;
}> = [
  { id: "composite", label: "Composite" },
  { id: "climate", label: "Climate" },
  { id: "hydro", label: "Hydro" },
  { id: "wildfire", label: "Wildfire" },
];

export function getLayerDefinition(layerId: LayerId): LayerDefinition {
  return LAYER_DEFINITIONS[layerId];
}

export function resolveLayerDefinition(
  layerId: LayerId,
  displayStatistics?: MonthlyDisplayStatistics,
): LayerDefinition {
  const baseDefinition = getLayerDefinition(layerId);

  if (!displayStatistics) {
    return baseDefinition;
  }

  if (layerId === "climate_mean_temp_c") {
    const statistics = displayStatistics.climate_mean_temp_c;

    if (
      !statistics ||
      statistics.p02 === null ||
      statistics.median === null ||
      statistics.p98 === null ||
      statistics.p98 <= statistics.p02
    ) {
      return baseDefinition;
    }

    const medianPosition = Math.max(
      0,
      Math.min(
        1,
        (statistics.median - statistics.p02) /
          (statistics.p98 - statistics.p02),
      ),
    );

    return {
      ...baseDefinition,
      domain: [statistics.p02, statistics.p98],
      stops: [
        {
          position: 0,
          color: [54, 83, 177],
        },
        {
          position: medianPosition,
          color: [238, 235, 223],
        },
        {
          position: 1,
          color: [194, 70, 61],
        },
      ],
      legendLabels: [
        `<= ${statistics.p02.toFixed(1)} °C`,
        `${statistics.median.toFixed(1)} °C`,
        `>= ${statistics.p98.toFixed(1)} °C`,
      ],
    };
  }

  if (layerId === "climate_total_precip_mm") {
    const statistics = displayStatistics.climate_total_precip_mm;

    if (!statistics || statistics.p98 === null || statistics.p98 <= 0) {
      return baseDefinition;
    }

    const midpoint = statistics.p98 / 2;

    return {
      ...baseDefinition,
      domain: [0, statistics.p98],
      legendLabels: [
        "0 mm",
        `${midpoint.toFixed(1)} mm`,
        `>= ${statistics.p98.toFixed(1)} mm`,
      ],
    };
  }

  return baseDefinition;
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

  if (
    definition.transparentBelow !== undefined &&
    value < definition.transparentBelow
  ) {
    return [0, 0, 0, 0];
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

  if (definition.unit === "°C") {
    return `${value.toFixed(1)} °C`;
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
