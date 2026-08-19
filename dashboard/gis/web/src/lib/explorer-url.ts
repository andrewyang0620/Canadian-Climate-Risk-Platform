import { isLayerId, type LayerId } from "./layer-registry";
import type { RegionId } from "./gis-data";
import {
  CITY_LAYER_IDS,
  CITY_SCOPES,
  type CityLayerId,
  type ExplorerScope,
} from "./city-data";

export interface ExplorerUrlState {
  scope: ExplorerScope | null;
  month: string | null;
  layer: LayerId | null;
  region: RegionId | null;
  grid: string | null;
  cityLayer: CityLayerId | null;
}

const VALID_REGIONS = new Set<RegionId>(["all", "BC", "AB"]);
const VALID_SCOPES = new Set<ExplorerScope>(["national", ...CITY_SCOPES]);
const VALID_CITY_LAYERS = new Set<CityLayerId>(CITY_LAYER_IDS);

export function readExplorerUrl(): ExplorerUrlState {
  const params = new URLSearchParams(window.location.search);
  const scope = params.get("scope");
  const layer = params.get("layer");
  const region = params.get("region");
  const cityLayer = params.get("clayer");

  return {
    scope:
      scope && VALID_SCOPES.has(scope as ExplorerScope)
        ? (scope as ExplorerScope)
        : null,
    month: params.get("month"),
    layer: isLayerId(layer) ? layer : null,
    region:
      region && VALID_REGIONS.has(region as RegionId)
        ? (region as RegionId)
        : null,
    grid: params.get("grid"),
    cityLayer:
      cityLayer && VALID_CITY_LAYERS.has(cityLayer as CityLayerId)
        ? (cityLayer as CityLayerId)
        : null,
  };
}

export function writeExplorerUrl({
  scope,
  month,
  layer,
  region,
  grid,
  cityLayerId,
}: {
  scope: ExplorerScope;
  month: string;
  layer: LayerId;
  region: RegionId;
  grid: string | null;
  cityLayerId: CityLayerId;
}) {
  const url = new URL(window.location.href);

  url.searchParams.set("scope", scope);
  url.searchParams.set("month", month);
  url.searchParams.set("layer", layer);
  url.searchParams.set("region", region);

  if (grid) {
    url.searchParams.set("grid", grid);
  } else {
    url.searchParams.delete("grid");
  }

  // Only simple, reliably-restorable UI state goes in the URL. Property/
  // activity selection is deliberately excluded — there is no by-key FGB
  // lookup endpoint, so a selected feature can't be reconstructed from just
  // its key after a reload.
  if (scope !== "national") {
    url.searchParams.set("clayer", cityLayerId);
  } else {
    url.searchParams.delete("clayer");
  }

  window.history.replaceState(null, "", url);
}
