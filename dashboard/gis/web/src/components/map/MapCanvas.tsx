import { useEffect, useRef, useState } from "react";
import Map, {
  ScaleControl,
  type MapRef,
} from "react-map-gl/maplibre";
import * as maptilersdk from "@maptiler/sdk";
import type { LayersList } from "@deck.gl/core";

import "@maptiler/sdk/dist/maptiler-sdk.css";

import {
  loadRegionContexts,
  type RegionId,
} from "../../lib/gis-data";
import type { ViewportBounds } from "../../lib/grid-geometry";
import type { LayerId } from "../../lib/layer-registry";
import { getMapTilerStyleConfig } from "../../lib/map-style";
import {
  isCityScope,
  type CityActivityKind,
  type CityFeature,
  type CityLayerId,
  type CityScope,
  type ExplorerScope,
} from "../../lib/city-data";
import { NationalRiskLayer } from "./NationalRiskLayer";
import { CitySpatialLayer } from "./CitySpatialLayer";
import { CityActivityLayer } from "./CityActivityLayer";
import { DeckGLOverlay } from "./DeckGLOverlay";

const DETAIL_BASEMAP_ZOOM = 12;
const CITY_PROPERTY_MIN_ZOOM = 12;
const CITY_ACTIVITY_MIN_ZOOM = 13.5;

interface StyleLayerLike {
  id: string;
  type: string;
  "source-layer"?: string;
}

interface OverlayPlacement {
  national: string | null;
  property: string | null;
  activity: string | null;
}

// ground -> parcel/property flood fill -> 3D buildings -> BP/DP points -> labels
function findOverlayPlacement(
  layers: StyleLayerLike[] | undefined,
): OverlayPlacement {
  if (!layers) {
    return {
      national: null,
      property: null,
      activity: null,
    };
  }

  const firstSymbol = layers.find((layer) => layer.type === "symbol")?.id ?? null;

  const buildingLayer =
    layers.find(
      (layer) =>
        layer.type === "fill-extrusion" &&
        (layer["source-layer"] === "building" ||
          layer.id.toLowerCase().includes("building")),
    ) ?? layers.find((layer) => layer.type === "fill-extrusion");

  return {
    national: firstSymbol,
    property: buildingLayer?.id ?? firstSymbol,
    activity: firstSymbol,
  };
}

// Hand-tuned starting camera per city — center is hardcoded (not derived
// from city.bounds) specifically so it can target a representative area
// (e.g. downtown) instead of the whole municipality's geometric centroid.
// center starts at each city's bbox centroid; tweak freely.
const CITY_CAMERA: Record<
  CityScope,
  {
    center: [number, number];
    bearing: number;
    pitch: number;
    zoom: number;
  }
> = {
  vancouver: {
    center: [-123.128055, 49.288164],
    bearing: -160,
    pitch: 70,
    zoom: 15.2,
  },
  calgary: {
    // 4 Ave SE @ 1 St SE, downtown Calgary
    center: [-114.069100, 51.047922],
    bearing: -125,
    pitch: 70,
    zoom: 14.6,
  },
};

interface MapCanvasProps {
  scope: ExplorerScope;
  referenceMonth: string;
  activeLayerId: LayerId;
  region: RegionId;
  selectedGridKey: string | null;
  onGridSelect: (key: string) => void;
  selectedCityFeatureKey: string | null;
  onCityFeatureSelect: (featureKey: string, feature: CityFeature) => void;
  selectedActivityKind: CityActivityKind | null;
  selectedActivityKey: string | null;
  relatedCityFeatureKeys: string[];
  onCityActivitySelect: (
    kind: CityActivityKind,
    key: string,
    feature: CityFeature,
  ) => void;
  // City-only layer mode (property/flood/BP/DP/none) — independent of
  // layersVisible, which stays National's alone.
  cityLayerId: CityLayerId;
  layersVisible: boolean;
  gridInfoVisible: boolean;
  gridInteractionEnabled: boolean;
}

export function MapCanvas({
  scope,
  referenceMonth,
  activeLayerId,
  region,
  selectedGridKey,
  onGridSelect,
  selectedCityFeatureKey,
  onCityFeatureSelect,
  selectedActivityKind,
  selectedActivityKey,
  relatedCityFeatureKeys,
  onCityActivitySelect,
  cityLayerId,
  layersVisible,
  gridInfoVisible,
  gridInteractionEnabled,
}: MapCanvasProps) {
  const cityPropertyVisible = cityLayerId !== "none";
  const cityShowFlood = cityLayerId === "flood";
  const cityBuildingPermitsVisible = cityLayerId === "building_permits";
  const cityDevelopmentPermitsVisible = cityLayerId === "development_permits";
  const mapRef = useRef<MapRef>(null);
  const [overlayPlacement, setOverlayPlacement] = useState<OverlayPlacement>({
    national: null,
    property: null,
    activity: null,
  });
  const [mapReady, setMapReady] = useState(false);
  const [regionProvinceKey, setRegionProvinceKey] = useState<string | null>(null);
  const [viewportBounds, setViewportBounds] = useState<ViewportBounds | null>(
    null,
  );
  const [hideLayerFill, setHideLayerFill] = useState(false);
  const [mapZoom, setMapZoom] = useState(4.3);
  const [isMapMoving, setIsMapMoving] = useState(false);
  // Property/BP/DP each report their deck.gl layers here instead of owning
  // their own DeckGLOverlay — see CitySpatialLayer's onLayersChange comment
  // for why multiple simultaneous interleaved MapboxOverlay instances on one
  // map turned out to silently corrupt each other's rendering.
  const [propertyLayers, setPropertyLayers] = useState<LayersList>([]);
  const [buildingPermitLayers, setBuildingPermitLayers] = useState<LayersList>(
    [],
  );
  const [developmentPermitLayers, setDevelopmentPermitLayers] =
    useState<LayersList>([]);
  const viewportBoundsRef = useRef<ViewportBounds | null>(null);
  const hideLayerFillRef = useRef(false);
  const fitBoundsCaptureTimeoutRef = useRef<number | null>(null);
  const { apiKey, styleId } = getMapTilerStyleConfig(scope);

  maptilersdk.config.apiKey = apiKey;

  useEffect(() => {
    if (!mapReady || scope !== "national") {
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
        bearing: 0,
        pitch: 0,
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
  }, [region, mapReady, scope]);

  useEffect(() => {
    if (!mapReady || !isCityScope(scope)) {
      return undefined;
    }

    const map = mapRef.current;

    if (!map) {
      return undefined;
    }

    // Hardcoded per-city camera (CITY_CAMERA) — no manifest fetch needed to
    // fly the initial view, so this applies immediately on scope change.
    const camera = CITY_CAMERA[scope];

    map.flyTo({
      center: camera.center,
      zoom: camera.zoom,
      bearing: camera.bearing,
      pitch: camera.pitch,
      duration: 850,
    });

    if (fitBoundsCaptureTimeoutRef.current) {
      window.clearTimeout(fitBoundsCaptureTimeoutRef.current);
    }

    fitBoundsCaptureTimeoutRef.current = window.setTimeout(() => {
      captureBounds(map);
      captureZoom(map);
    }, 920);

    return () => {
      if (fitBoundsCaptureTimeoutRef.current) {
        window.clearTimeout(fitBoundsCaptureTimeoutRef.current);
        fitBoundsCaptureTimeoutRef.current = null;
      }
    };
  }, [scope, mapReady]);

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
    const nextZoom = map.getZoom();

    setMapZoom(nextZoom);

    const nextHideLayerFill = nextZoom >= DETAIL_BASEMAP_ZOOM;

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
      maxZoom={18}
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

        setOverlayPlacement(
          findOverlayPlacement(style.layers as StyleLayerLike[]),
        );

        setMapReady(true);

        window.requestAnimationFrame(() => {
          event.target.resize();
          captureBounds(event.target);
          captureZoom(event.target);
        });
      }}
      onStyleData={(event) => {
        const style = event.target.getStyle();

        setOverlayPlacement(
          findOverlayPlacement(style.layers as StyleLayerLike[]),
        );
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
      {scope === "national" && layersVisible && viewportBounds && (
        <NationalRiskLayer
          referenceMonth={referenceMonth}
          activeLayerId={activeLayerId}
          beforeId={overlayPlacement.national}
          regionProvinceKey={regionProvinceKey}
          selectedGridKey={selectedGridKey}
          onGridSelect={onGridSelect}
          viewportBounds={viewportBounds}
          hideLayerFill={hideLayerFill}
          gridInfoVisible={gridInfoVisible}
          gridInteractionEnabled={gridInteractionEnabled && !isMapMoving}
        />
      )}

      {isCityScope(scope) &&
        viewportBounds &&
        mapZoom >= CITY_PROPERTY_MIN_ZOOM && (
          <CitySpatialLayer
            key={scope}
            scope={scope}
            visible={cityPropertyVisible}
            showFlood={cityShowFlood}
            beforeId={overlayPlacement.property}
            viewportBounds={viewportBounds}
            selectedFeatureKey={selectedCityFeatureKey}
            onFeatureSelect={onCityFeatureSelect}
            relatedFeatureKeys={relatedCityFeatureKeys}
            relatedFeatureKind={selectedActivityKind}
            infoVisible={gridInfoVisible}
            interactionEnabled={gridInteractionEnabled && !isMapMoving}
            onLayersChange={setPropertyLayers}
          />
        )}

      {isCityScope(scope) &&
        viewportBounds &&
        mapZoom >= CITY_ACTIVITY_MIN_ZOOM && (
          <CityActivityLayer
            key={`${scope}-building-permits`}
            scope={scope}
            kind="building_permits"
            visible={cityBuildingPermitsVisible}
            selectedActivityKey={
              selectedActivityKind === "building_permits"
                ? selectedActivityKey
                : null
            }
            onActivitySelect={(key, feature) => {
              onCityActivitySelect("building_permits", key, feature);
            }}
            beforeId={overlayPlacement.activity}
            viewportBounds={viewportBounds}
            infoVisible={gridInfoVisible}
            interactionEnabled={gridInteractionEnabled && !isMapMoving}
            onLayersChange={setBuildingPermitLayers}
          />
        )}

      {scope === "calgary" &&
        viewportBounds &&
        mapZoom >= CITY_ACTIVITY_MIN_ZOOM && (
          <CityActivityLayer
            key="calgary-development-permits"
            scope="calgary"
            kind="development_permits"
            visible={cityDevelopmentPermitsVisible}
            selectedActivityKey={
              selectedActivityKind === "development_permits"
                ? selectedActivityKey
                : null
            }
            onActivitySelect={(key, feature) => {
              onCityActivitySelect("development_permits", key, feature);
            }}
            beforeId={overlayPlacement.activity}
            viewportBounds={viewportBounds}
            infoVisible={gridInfoVisible}
            interactionEnabled={gridInteractionEnabled && !isMapMoving}
            onLayersChange={setDevelopmentPermitLayers}
          />
        )}

      {isCityScope(scope) && (
        <DeckGLOverlay
          interleaved
          layers={[
            ...propertyLayers,
            ...buildingPermitLayers,
            ...developmentPermitLayers,
          ]}
          getCursor={({ isHovering }) => (isHovering ? "pointer" : "grab")}
        />
      )}

      <ScaleControl position="bottom-left" unit="metric" maxWidth={120} />
    </Map>
  );
}
