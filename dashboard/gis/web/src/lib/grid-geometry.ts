import { geojson as flatgeobuf } from "flatgeobuf";

import type { Feature, FeatureCollection, Geometry } from "geojson";

export interface GridProperties {
  grid_cell_key: string;
  province_key: string;
}

export type GridFeature = Feature<Geometry, GridProperties>;

export interface ViewportBounds {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
}

const GEOMETRY_URL = "/grid_geometry.fgb";

export function padBounds(
  bounds: ViewportBounds,
  ratio = 0.12,
): ViewportBounds {
  const width = bounds.maxX - bounds.minX;
  const height = bounds.maxY - bounds.minY;

  return {
    minX: bounds.minX - width * ratio,
    minY: bounds.minY - height * ratio,
    maxX: bounds.maxX + width * ratio,
    maxY: bounds.maxY + height * ratio,
  };
}

export function containsBounds(
  outer: ViewportBounds,
  inner: ViewportBounds,
): boolean {
  return (
    inner.minX >= outer.minX &&
    inner.minY >= outer.minY &&
    inner.maxX <= outer.maxX &&
    inner.maxY <= outer.maxY
  );
}

export function boundsKey(bounds: ViewportBounds): string {
  return [
    bounds.minX,
    bounds.minY,
    bounds.maxX,
    bounds.maxY,
  ]
    .map((value) => value.toFixed(4))
    .join(",");
}

export async function loadGridGeometry(
  bounds: ViewportBounds,
): Promise<FeatureCollection<Geometry, GridProperties>> {
  const features: GridFeature[] = [];
  const paddedBounds = padBounds(bounds);
  const iterator = flatgeobuf.deserialize(GEOMETRY_URL, paddedBounds);

  for await (const feature of iterator) {
    features.push(feature as GridFeature);
  }

  return {
    type: "FeatureCollection",
    features,
  };
}
