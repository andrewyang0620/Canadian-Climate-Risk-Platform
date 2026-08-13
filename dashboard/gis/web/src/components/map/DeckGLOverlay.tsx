import type { DeckProps } from "@deck.gl/core";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { useControl } from "react-map-gl/maplibre";

type DeckGLOverlayProps = DeckProps & {
  interleaved?: boolean;
};

export function DeckGLOverlay(props: DeckGLOverlayProps) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay(props));

  overlay.setProps(props);

  return null;
}
