/**
 * Typed client for the Contrail API.
 *
 * The types here mirror the FastAPI response models. Notably, every optional
 * measurement is `number | null` rather than `number | undefined`: the pipeline
 * preserves the difference between "not reported" and zero all the way from the
 * OpenSky state vector, and flattening it in the UI would undo that.
 */

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

export interface Aircraft {
  icao24: string;
  callsign: string | null;
  origin_country: string;
  latitude: number;
  longitude: number;
  altitude_m: number | null;
  velocity_knots: number | null;
  true_track: number | null;
  on_ground: boolean;
  observed_at: string;
  position_age_seconds: number;
}

export interface Flight {
  flight_id: string;
  icao24: string;
  callsign: string | null;
  departure_time: string;
  arrival_time: string;
  duration_minutes: number;
  departure_airport: string | null;
  arrival_airport: string | null;
  max_altitude_m: number | null;
  max_velocity_knots: number | null;
  endpoint_distance_km: number | null;
  observation_count: number;
  is_complete: boolean;
  reconstruction_quality: "low" | "medium" | "high";
}

export interface TrafficPoint {
  traffic_hour: string;
  region: string;
  distinct_aircraft: number;
  distinct_airborne: number;
  observations: number;
  duplicate_rate_pct: number | null;
}

export interface PipelineStats {
  raw_rows: number;
  distinct_observations: number;
  duplicate_rate_pct: number;
  distinct_aircraft: number;
  flights_reconstructed: number;
  complete_flights: number;
  latest_observation: string | null;
  regions: string[];
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${path} responded ${response.status}`);
  }
  return (await response.json()) as T;
}

export const api = {
  stats: () => get<PipelineStats>("/api/stats"),
  liveAircraft: (limit = 2000) =>
    get<Aircraft[]>(`/api/aircraft/live?limit=${limit}`),
  flights: (limit = 50) => get<Flight[]>(`/api/flights?limit=${limit}`),
  hourlyTraffic: (hours = 48) =>
    get<TrafficPoint[]>(`/api/traffic/hourly?hours=${hours}`),
};
