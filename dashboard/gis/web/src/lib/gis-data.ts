import {
  gisDataUrl
} from "./gis-data-url"


export interface GisManifest {
  geometry: {
    file: string;
    crs: string;
    feature_count: number;
  };

  monthly_data: {
    directory: string;
    months: string[];
    files: Record<string, string>;
    columns: string[];
  };
}

export interface DisplayStatisticSummary {
  valid_count: number;
  min: number | null;
  p02: number | null;
  median: number | null;
  p98: number | null;
  max: number | null;
}

export type MonthlyDisplayStatistics = Record<
  string,
  DisplayStatisticSummary
>;

export interface MonthlyDataset {
  columns: string[];
  columnIndex: Map<string, number>;
  rowsByGrid: Map<string, unknown[]>;
  displayStatistics: MonthlyDisplayStatistics;
}

export interface GridMetadataDataset {
  columns: string[];
  columnIndex: Map<string, number>;
  rowsByGrid: Map<string, unknown[]>;
}

export type RegionId = "all" | "BC" | "AB";

interface RegionContext {
  provinceKey: string | null;
  bounds: [[number, number], [number, number]];
}

let manifestCache: GisManifest | null = null;
const monthlyDatasetCache = new Map<string, Promise<MonthlyDataset>>();
const MAX_MONTHLY_DATASET_CACHE_SIZE = 8;
let gridMetadataCache: Promise<GridMetadataDataset> | null = null;
let regionContextCache: Promise<Record<RegionId, RegionContext>> | null = null;

export async function loadManifest(): Promise<GisManifest> {
  if (manifestCache) {
    return manifestCache;
  }

  const response = await fetch(gisDataUrl("manifest.json"));

  if (!response.ok) {
    throw new Error(`Failed to load GIS manifest: ${response.status}`);
  }

  manifestCache = (await response.json()) as GisManifest;

  return manifestCache;
}

export function loadMonthlyDataset(referenceMonth: string): Promise<MonthlyDataset> {
  const cached = monthlyDatasetCache.get(referenceMonth);

  if (cached) {
    monthlyDatasetCache.delete(referenceMonth);
    monthlyDatasetCache.set(referenceMonth, cached);

    return cached;
  }

  const request = loadMonthlyDatasetInternal(referenceMonth);

  monthlyDatasetCache.set(referenceMonth, request);

  if (monthlyDatasetCache.size > MAX_MONTHLY_DATASET_CACHE_SIZE) {
    const oldestKey = monthlyDatasetCache.keys().next().value;

    if (oldestKey) {
      monthlyDatasetCache.delete(oldestKey);
    }
  }

  return request;
}

async function loadMonthlyDatasetInternal(
  referenceMonth: string,
): Promise<MonthlyDataset> {
  const manifest = await loadManifest();

  const relativePath = manifest.monthly_data.files[referenceMonth];

  if (!relativePath) {
    throw new Error(`No GIS data for ${referenceMonth}`);
  }

  const response = await fetch(gisDataUrl(relativePath));

  if (!response.ok) {
    throw new Error(`Failed to load ${referenceMonth}: ${response.status}`);
  }

  const payload = (await response.json()) as {
    reference_month: string;
    grid_cell_count: number;
    display_statistics?: MonthlyDisplayStatistics;
    rows: unknown[][];
  };

  const columns = manifest.monthly_data.columns;

  const columnIndex = new Map<string, number>(
    columns.map((column, index) => [column, index]),
  );

  const gridKeyIndex = columnIndex.get("grid_cell_key");

  if (gridKeyIndex === undefined) {
    throw new Error("grid_cell_key is missing from monthly schema");
  }

  const rowsByGrid = new Map<string, unknown[]>();

  for (const row of payload.rows) {
    const gridKey = String(row[gridKeyIndex]);

    rowsByGrid.set(gridKey, row);
  }

  return {
    columns,
    columnIndex,
    rowsByGrid,
    displayStatistics: payload.display_statistics ?? {},
  };
}

export function getNumericValue(
  dataset: MonthlyDataset,
  gridCellKey: string,
  column: string,
): number | null {
  return getNumberValue(dataset, gridCellKey, column);
}

type TabularDataset = {
  columnIndex: Map<string, number>;
  rowsByGrid: Map<string, unknown[]>;
};

export function getValue(
  dataset: TabularDataset,
  gridCellKey: string,
  column: string,
): unknown | null {
  const row = dataset.rowsByGrid.get(gridCellKey);
  const index = dataset.columnIndex.get(column);

  if (!row || index === undefined) {
    return null;
  }

  const value = row[index];

  return value === undefined ? null : value;
}

export function getStringValue(
  dataset: TabularDataset,
  gridCellKey: string,
  column: string,
): string | null {
  const value = getValue(dataset, gridCellKey, column);

  if (value === null || value === undefined) {
    return null;
  }

  return String(value);
}

export function getNumberValue(
  dataset: TabularDataset,
  gridCellKey: string,
  column: string,
): number | null {
  const value = getValue(dataset, gridCellKey, column);

  if (value === null || value === undefined) {
    return null;
  }

  const numericValue = Number(value);

  return Number.isFinite(numericValue) ? numericValue : null;
}

export function loadGridMetadata(): Promise<GridMetadataDataset> {
  if (gridMetadataCache) {
    return gridMetadataCache;
  }

  gridMetadataCache = loadGridMetadataInternal();

  return gridMetadataCache;
}

async function loadGridMetadataInternal(): Promise<GridMetadataDataset> {
  const response = await fetch(gisDataUrl("grid_metadata.json"));

  if (!response.ok) {
    throw new Error(`Failed to load grid metadata: ${response.status}`);
  }

  const payload = (await response.json()) as {
    columns: string[];
    rows: unknown[][];
  };

  const columnIndex = new Map<string, number>(
    payload.columns.map((column, index) => [column, index]),
  );

  const gridKeyIndex = columnIndex.get("grid_cell_key");

  if (gridKeyIndex === undefined) {
    throw new Error("grid_cell_key is missing from grid metadata");
  }

  const rowsByGrid = new Map<string, unknown[]>();

  for (const row of payload.rows) {
    rowsByGrid.set(String(row[gridKeyIndex]), row);
  }

  return {
    columns: payload.columns,
    columnIndex,
    rowsByGrid,
  };
}

export async function loadRegionContexts(): Promise<
  Record<RegionId, RegionContext>
> {
  if (regionContextCache) {
    return regionContextCache;
  }

  regionContextCache = buildRegionContexts();

  return regionContextCache;
}

async function buildRegionContexts(): Promise<Record<RegionId, RegionContext>> {
  const payload = await loadGridMetadata();
  const index = payload.columnIndex;

  const keyIndex = index.get("province_key");
  const codeIndex = index.get("province_code");
  const longitudeIndex = index.get("centroid_longitude");
  const latitudeIndex = index.get("centroid_latitude");

  if (
    keyIndex === undefined ||
    codeIndex === undefined ||
    longitudeIndex === undefined ||
    latitudeIndex === undefined
  ) {
    throw new Error("Grid metadata is missing region fields.");
  }

  const provinceKeyIndex = keyIndex;
  const provinceCodeIndex = codeIndex;
  const centroidLongitudeIndex = longitudeIndex;
  const centroidLatitudeIndex = latitudeIndex;

  function contextFor(regionId: RegionId): RegionContext {
    const rows =
      regionId === "all"
        ? Array.from(payload.rowsByGrid.values())
        : Array.from(payload.rowsByGrid.values()).filter(
            (row) =>
              row[provinceKeyIndex] === regionId ||
              row[provinceCodeIndex] === regionId,
          );

    if (rows.length === 0) {
      throw new Error(`No grid metadata found for region ${regionId}`);
    }

    const longitudes = rows.map((row) => Number(row[centroidLongitudeIndex]));
    const latitudes = rows.map((row) => Number(row[centroidLatitudeIndex]));

    return {
      provinceKey:
        regionId === "all" ? null : String(rows[0][provinceKeyIndex]),
      bounds: [
        [Math.min(...longitudes), Math.min(...latitudes)],
        [Math.max(...longitudes), Math.max(...latitudes)],
      ],
    };
  }

  return {
    all: contextFor("all"),
    BC: contextFor("BC"),
    AB: contextFor("AB"),
  };
}
