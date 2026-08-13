const CUSTOM_STYLE_URL = import.meta.env.VITE_MAP_STYLE_URL;

export function getMapStyle(): string {
  if (!CUSTOM_STYLE_URL) {
    throw new Error("VITE_MAP_STYLE_URL is required.");
  }

  return CUSTOM_STYLE_URL;
}

export function getMapTilerStyleConfig() {
  const styleUrl = new URL(getMapStyle());
  const styleId = styleUrl.pathname.match(
    /\/maps\/([^/]+)(?:\/style\.json|\/)?$/,
  )?.[1];
  const apiKey = styleUrl.searchParams.get("key");

  if (!styleId) {
    throw new Error("VITE_MAP_STYLE_URL must be a MapTiler map style URL.");
  }

  if (!apiKey) {
    throw new Error("VITE_MAP_STYLE_URL must include a MapTiler API key.");
  }

  return {
    apiKey,
    styleId,
  };
}
