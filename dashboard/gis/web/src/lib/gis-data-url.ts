const GIS_DATA_BASE_URL = (
  import.meta.env.VITE_GIS_DATA_BASE_URL ?? ""
)
  .trim()
  .replace(/\/+$/, "");


export function gisDataUrl(
  path: string,
): string {
  const normalizedPath =
    path.replace(/^\/+/, "");

  if (!GIS_DATA_BASE_URL) {
    return `/${normalizedPath}`;
  }

  return (
    `${GIS_DATA_BASE_URL}/${normalizedPath}`
  );
}