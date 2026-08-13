import { useEffect, useRef, useState } from "react";
import Map, {
  NavigationControl,
  ScaleControl,
  type MapRef,
} from "react-map-gl/maplibre";
import * as maptilersdk from "@maptiler/sdk";

import "@maptiler/sdk/dist/maptiler-sdk.css";

import {
  loadRegionContexts,
  type RegionId,
} from "../../lib/gis-data";
import type { ViewportBounds } from "../../lib/grid-geometry";
import type { LayerId } from "../../lib/layer-registry";
import { getMapTilerStyleConfig } from "../../lib/map-style";
import { NationalRiskLayer } from "./NationalRiskLayer";

interface MapCanvasProps {
  referenceMonth: string;
  activeLayerId: LayerId;
  region: RegionId;
  selectedGridKey: string | null;
  onGridSelect: (key: string) => void;
}

export function MapCanvas({
  referenceMonth,
  activeLayerId,
  region,
  selectedGridKey,
  onGridSelect,
}: MapCanvasProps) {
  const mapRef = useRef<MapRef>(null);
  const [labelLayerId, setLabelLayerId] = useState<string | null>(null);
  const [regionProvinceKey, setRegionProvinceKey] = useState<string | null>(null);
  const [viewportBounds, setViewportBounds] = useState<ViewportBounds | null>(
    null,
  );
  const { apiKey, styleId } = getMapTilerStyleConfig();

  maptilersdk.config.apiKey = apiKey;

  useEffect(() => {
    let cancelled = false;

    loadRegionContexts().then((contexts) => {
      if (cancelled) {
        return;
      }

      const context = contexts[region];

      setRegionProvinceKey(context.provinceKey);

      mapRef.current?.fitBounds(context.bounds, {
        padding: {
          top: 110,
          right: 90,
          bottom: 105,
          left: 90,
        },
        duration: 650,
      });
    });

    return () => {
      cancelled = true;
    };
  }, [region]);

  function captureBounds(map: {
    getBounds: () => {
      getWest: () => number;
      getSouth: () => number;
      getEast: () => number;
      getNorth: () => number;
    };
  }) {
    const bounds = map.getBounds();

    setViewportBounds({
      minX: bounds.getWest(),
      minY: bounds.getSouth(),
      maxX: bounds.getEast(),
      maxY: bounds.getNorth(),
    });
  }

  return (
    <Map
      ref={mapRef}
      mapLib={maptilersdk as never}
      initialViewState={{
        longitude: -122.9,
        latitude: 54.1,
        zoom: 4.3,
        bearing: 0,
        pitch: 0,
      }}
      mapStyle={styleId}
      attributionControl={{}}
      canvasContextAttributes={{ antialias: true }}
      minZoom={3}
      maxZoom={15}
      maxPitch={65}
      dragRotate
      touchPitch
      cursor="grab"
      {...({
        language: maptilersdk.Language.STYLE_LOCK,
        maptilerLogo: true,
      } as Record<string, unknown>)}
      onLoad={(event) => {
        const style = event.target.getStyle();
        const firstSymbol = style.layers?.find((layer) => layer.type === "symbol");

        if (firstSymbol) {
          setLabelLayerId(firstSymbol.id);
        }

        captureBounds(event.target);
      }}
      onMoveEnd={(event) => {
        captureBounds(event.target);
      }}
    >
      {labelLayerId && viewportBounds && (
        <NationalRiskLayer
          referenceMonth={referenceMonth}
          activeLayerId={activeLayerId}
          beforeId={labelLayerId}
          regionProvinceKey={regionProvinceKey}
          selectedGridKey={selectedGridKey}
          onGridSelect={onGridSelect}
          viewportBounds={viewportBounds}
        />
      )}

      <NavigationControl position="bottom-right" visualizePitch />

      <ScaleControl position="bottom-left" unit="metric" maxWidth={120} />
    </Map>
  );
}
