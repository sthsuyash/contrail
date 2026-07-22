"use client";

import { useEffect, useRef } from "react";
import maplibregl, { type Map as MapLibreMap } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import type { Aircraft } from "@/lib/api";

/**
 * Live aircraft positions.
 *
 * Aircraft are drawn from a GeoJSON source updated in place rather than by
 * re-rendering markers. At a couple of thousand aircraft, one DOM node each
 * would stall the main thread on every refresh; a single source lets MapLibre
 * render them on the GPU and makes the update cost independent of count.
 *
 * The basemap is a plain dark background with no raster tiles. That is
 * deliberate: it keeps the dashboard working with no external tile provider,
 * no API key, and no network beyond the local API, the same reasoning that
 * makes the rest of the pipeline runnable from a fresh clone.
 */
export function AircraftMap({ aircraft }: { aircraft: Aircraft[] }) {
  const container = useRef<HTMLDivElement>(null);
  const map = useRef<MapLibreMap | null>(null);
  const ready = useRef(false);

  useEffect(() => {
    if (!container.current || map.current) return;

    map.current = new maplibregl.Map({
      container: container.current,
      style: {
        version: 8,
        sources: {},
        layers: [
          { id: "bg", type: "background", paint: { "background-color": "#0e1428" } },
        ],
        // `glyphs` is omitted rather than set to undefined: MapLibre validates
        // the style object and rejects an explicit undefined with
        // "glyphs: string expected, undefined found". Nothing here renders
        // text, so no glyph source is needed at all.
      },
      center: [5.5, 51.5],
      zoom: 5.2,
      attributionControl: false,
    });

    map.current.addControl(new maplibregl.NavigationControl({}), "top-right");

    map.current.on("load", () => {
      if (!map.current) return;
      map.current.addSource("aircraft", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });

      map.current.addLayer({
        id: "aircraft-dots",
        type: "circle",
        source: "aircraft",
        paint: {
          // Radius by altitude: higher aircraft read as larger, which makes
          // departure and arrival streams visually separable around a hub.
          "circle-radius": [
            "interpolate", ["linear"], ["coalesce", ["get", "altitude"], 0],
            0, 2.5,
            3000, 3.5,
            11000, 5,
          ],
          "circle-color": [
            "case",
            ["get", "onGround"], "#7a86a8",
            [
              "interpolate", ["linear"], ["coalesce", ["get", "altitude"], 0],
              0, "#f0b429",
              4000, "#4dd4ac",
              11000, "#5aa9ff",
            ],
          ],
          "circle-opacity": 0.85,
          "circle-stroke-width": 0.5,
          "circle-stroke-color": "#0b1020",
        },
      });
      ready.current = true;
    });

    return () => {
      map.current?.remove();
      map.current = null;
      ready.current = false;
    };
  }, []);

  // Fit the view to the data once, on the first non-empty payload.
  //
  // Coverage is configurable: a run polling only the Benelux should not open
  // at a Europe-wide zoom showing one small cluster. Fitting only once means
  // subsequent refreshes do not yank the camera away from wherever the user has
  // panned to.
  const fitted = useRef(false);

  useEffect(() => {
    const instance = map.current;
    if (!instance) return;

    const apply = () => {
      const source = instance.getSource("aircraft") as maplibregl.GeoJSONSource | undefined;
      if (!source) return;

      if (!fitted.current && aircraft.length > 0) {
        const bounds = new maplibregl.LngLatBounds();
        for (const a of aircraft) bounds.extend([a.longitude, a.latitude]);
        instance.fitBounds(bounds, { padding: 48, duration: 0, maxZoom: 9 });
        fitted.current = true;
      }
      source.setData({
        type: "FeatureCollection",
        features: aircraft.map((a) => ({
          type: "Feature" as const,
          geometry: { type: "Point" as const, coordinates: [a.longitude, a.latitude] },
          properties: {
            icao24: a.icao24,
            callsign: a.callsign ?? "",
            // null altitude is passed through as null rather than 0 so the
            // paint expression can coalesce it explicitly.
            altitude: a.altitude_m,
            onGround: a.on_ground,
          },
        })),
      });
    };

    if (ready.current) apply();
    else instance.once("load", apply);
  }, [aircraft]);

  return <div id="map" ref={container} />;
}
