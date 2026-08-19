import type { ExplorerScope } from "./city-data";

const NATIONAL_STYLE_URL = import.meta.env.VITE_MAP_STYLE_URL;
const CITY_STYLE_URL = import.meta.env.VITE_CITY_MAP_STYLE_URL;

function configuredStyleUrl(scope: ExplorerScope): string {
  const styleUrl =
    scope === "national"
      ? NATIONAL_STYLE_URL
      : CITY_STYLE_URL || NATIONAL_STYLE_URL;

  if (!styleUrl) {
    throw new Error("VITE_MAP_STYLE_URL is required.");
  }

  return styleUrl;
}

export function getMapTilerStyleConfig(scope: ExplorerScope = "national") {
  const configuredUrl = configuredStyleUrl(scope);
  const styleUrl = new URL(configuredUrl);
  const styleId = styleUrl.pathname.match(
    /\/maps\/([^/]+)(?:\/style\.json|\/)?$/,
  )?.[1];
  const apiKey = styleUrl.searchParams.get("key");

  if (!styleId) {
    throw new Error("Map style URL must be a MapTiler map style URL.");
  }

  if (!apiKey) {
    throw new Error("MapTiler map style URL must include an API key.");
  }

  return {
    apiKey,
    styleId,
  };
}
