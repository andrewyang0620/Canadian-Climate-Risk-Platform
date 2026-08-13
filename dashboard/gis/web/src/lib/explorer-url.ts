import { isLayerId, type LayerId } from "./layer-registry";
import type { RegionId } from "./gis-data";

export interface ExplorerUrlState {
  month: string | null;
  layer: LayerId | null;
  region: RegionId | null;
  grid: string | null;
}

const VALID_REGIONS = new Set<RegionId>(["all", "BC", "AB"]);

export function readExplorerUrl(): ExplorerUrlState {
  const params = new URLSearchParams(window.location.search);
  const layer = params.get("layer");
  const region = params.get("region");

  return {
    month: params.get("month"),
    layer: isLayerId(layer) ? layer : null,
    region:
      region && VALID_REGIONS.has(region as RegionId)
        ? (region as RegionId)
        : null,
    grid: params.get("grid"),
  };
}

export function writeExplorerUrl({
  month,
  layer,
  region,
  grid,
}: {
  month: string;
  layer: LayerId;
  region: RegionId;
  grid: string | null;
}) {
  const url = new URL(window.location.href);

  url.searchParams.set("month", month);
  url.searchParams.set("layer", layer);
  url.searchParams.set("region", region);

  if (grid) {
    url.searchParams.set("grid", grid);
  } else {
    url.searchParams.delete("grid");
  }

  window.history.replaceState(null, "", url);
}
