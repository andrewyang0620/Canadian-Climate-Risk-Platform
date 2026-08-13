import { useEffect, useRef, useState } from "react";
import Map, {
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

const DETAIL_BASEMAP_ZOOM = 12;

interface MapCanvasProps {
  referenceMonth: string;
  activeLayerId: LayerId;
  region: RegionId;
  selectedGridKey: string | null;
  onGridSelect: (key: string) => void;
  layersVisible: boolean;
  gridInfoVisible: boolean;
  gridInteractionEnabled: boolean;
}

export function MapCanvas({
  referenceMonth,
  activeLayerId,
  region,
  selectedGridKey,
  onGridSelect,
  layersVisible,
  gridInfoVisible,
  gridInteractionEnabled,
}: MapCanvasProps) {
  const mapRef = useRef<MapRef>(null);
  const [labelLayerId, setLabelLayerId] = useState<string | null>(null);
  const [mapReady, setMapReady] = useState(false);
  const [regionProvinceKey, setRegionProvinceKey] = useState<string | null>(null);
  const [viewportBounds, setViewportBounds] = useState<ViewportBounds | null>(
    null,
  );
  const [hideLayerFill, setHideLayerFill] = useState(false);
  const [isMapMoving, setIsMapMoving] = useState(false);
  const viewportBoundsRef = useRef<ViewportBounds | null>(null);
  const hideLayerFillRef = useRef(false);
  const fitBoundsCaptureTimeoutRef = useRef<number | null>(null);
  const { apiKey, styleId } = getMapTilerStyleConfig();

  maptilersdk.config.apiKey = apiKey;

  useEffect(() => {
    if (!mapReady) {
      return undefined;
    }

    let cancelled = false;

    loadRegionContexts().then((contexts) => {
      if (cancelled) {
        return;
      }

      const context = contexts[region];

      setRegionProvinceKey(context.provinceKey);

      const map = mapRef.current;

      if (!map) {
        return;
      }

      map.fitBounds(context.bounds, {
        padding: {
          top: 80,
          right: 60,
          bottom: 75,
          left: 60,
        },
        duration: 650,
      });

      if (fitBoundsCaptureTimeoutRef.current) {
        window.clearTimeout(fitBoundsCaptureTimeoutRef.current);
      }

      fitBoundsCaptureTimeoutRef.current = window.setTimeout(() => {
        captureBounds(map);
        captureZoom(map);
      }, 720);
    });

    return () => {
      cancelled = true;

      if (fitBoundsCaptureTimeoutRef.current) {
        window.clearTimeout(fitBoundsCaptureTimeoutRef.current);
        fitBoundsCaptureTimeoutRef.current = null;
      }
    };
  }, [region, mapReady]);

  function shouldUpdateBounds(nextBounds: ViewportBounds): boolean {
    const currentBounds = viewportBoundsRef.current;

    if (!currentBounds) {
      return true;
    }

    const width = currentBounds.maxX - currentBounds.minX;
    const height = currentBounds.maxY - currentBounds.minY;
    const thresholdX = Math.max(width * 0.005, 0.005);
    const thresholdY = Math.max(height * 0.005, 0.005);

    return (
      Math.abs(nextBounds.minX - currentBounds.minX) > thresholdX ||
      Math.abs(nextBounds.maxX - currentBounds.maxX) > thresholdX ||
      Math.abs(nextBounds.minY - currentBounds.minY) > thresholdY ||
      Math.abs(nextBounds.maxY - currentBounds.maxY) > thresholdY
    );
  }

  function captureBounds(map: {
    getBounds: () => {
      getWest: () => number;
      getSouth: () => number;
      getEast: () => number;
      getNorth: () => number;
    };
  }) {
    const bounds = map.getBounds();
    const nextBounds = {
      minX: bounds.getWest(),
      minY: bounds.getSouth(),
      maxX: bounds.getEast(),
      maxY: bounds.getNorth(),
    };

    if (!shouldUpdateBounds(nextBounds)) {
      return;
    }

    viewportBoundsRef.current = nextBounds;
    setViewportBounds(nextBounds);
  }

  function captureZoom(map: { getZoom: () => number }) {
    const nextHideLayerFill = map.getZoom() >= DETAIL_BASEMAP_ZOOM;

    if (nextHideLayerFill === hideLayerFillRef.current) {
      return;
    }

    hideLayerFillRef.current = nextHideLayerFill;
    setHideLayerFill(nextHideLayerFill);
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
        logoPosition: "bottom-right",
      } as Record<string, unknown>)}
      onLoad={(event) => {
        const style = event.target.getStyle();
        const firstSymbol = style.layers?.find((layer) => layer.type === "symbol");

        if (firstSymbol) {
          setLabelLayerId(firstSymbol.id);
        }

        setMapReady(true);

        window.requestAnimationFrame(() => {
          event.target.resize();
          captureBounds(event.target);
          captureZoom(event.target);
        });
      }}
      onIdle={(event) => {
        setIsMapMoving(false);
        captureBounds(event.target);
        captureZoom(event.target);
      }}
      onMoveStart={() => {
        setIsMapMoving(true);
      }}
      onMoveEnd={(event) => {
        setIsMapMoving(false);
        captureBounds(event.target);
        captureZoom(event.target);
      }}
    >
      {layersVisible && viewportBounds && (
        <NationalRiskLayer
          referenceMonth={referenceMonth}
          activeLayerId={activeLayerId}
          beforeId={labelLayerId}
          regionProvinceKey={regionProvinceKey}
          selectedGridKey={selectedGridKey}
          onGridSelect={onGridSelect}
          viewportBounds={viewportBounds}
          hideLayerFill={hideLayerFill}
          gridInfoVisible={gridInfoVisible}
          gridInteractionEnabled={gridInteractionEnabled && !isMapMoving}
        />
      )}

      <ScaleControl position="bottom-left" unit="metric" maxWidth={120} />
    </Map>
  );
}
