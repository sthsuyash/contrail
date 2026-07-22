"use client";

import { useCallback, useEffect, useState } from "react";
import { AircraftMap } from "@/components/AircraftMap";
import { api, type Aircraft, type Flight, type PipelineStats } from "@/lib/api";

const REFRESH_MS = 15_000;

export default function Dashboard() {
  const [stats, setStats] = useState<PipelineStats | null>(null);
  const [aircraft, setAircraft] = useState<Aircraft[]>([]);
  const [flights, setFlights] = useState<Flight[]>([]);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const [nextStats, nextAircraft, nextFlights] = await Promise.all([
        api.stats(),
        api.liveAircraft(),
        api.flights(50),
      ]);
      setStats(nextStats);
      setAircraft(nextAircraft);
      setFlights(nextFlights);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = setInterval(() => void load(), REFRESH_MS);
    return () => clearInterval(timer);
  }, [load]);

  return (
    <div className="shell">
      <header className="masthead">
        <h1>Contrail</h1>
        <p>
          Flight telemetry reconstructed from OpenSky Network state vectors.
          {stats?.latest_observation ? (
            <>
              {" "}
              Latest observation{" "}
              <span className="muted">
                {new Date(stats.latest_observation).toUTCString()}
              </span>
              .
            </>
          ) : null}
        </p>
      </header>

      {error ? (
        <div className="panel">
          <div className="error">
            Could not reach the API ({error}). Is it running on port 8000?
          </div>
        </div>
      ) : null}

      <section className="tiles">
        <Tile
          label="Observations"
          value={stats?.distinct_observations}
          note="deduplicated on (icao24, time_position)"
        />
        <Tile
          label="Duplicate rate"
          value={stats ? `${stats.duplicate_rate_pct}%` : undefined}
          note="measured at ingest; the warehouse erases it"
        />
        <Tile label="Aircraft" value={stats?.distinct_aircraft} note="distinct airframes" />
        <Tile
          label="Flights"
          value={stats?.flights_reconstructed}
          note={stats ? `${stats.complete_flights} fully observed` : undefined}
        />
        <Tile label="Tracking now" value={aircraft.length} note="with a usable position fix" />
      </section>

      <div className="grid">
        <div className="panel">
          <h2>Live positions</h2>
          <AircraftMap aircraft={aircraft} />
        </div>

        <div className="panel">
          <h2>Reconstructed flights</h2>
          <div className="scroll">
            <table>
              <thead>
                <tr>
                  <th>Callsign</th>
                  <th>Route</th>
                  <th className="num">Min</th>
                  <th className="num">Alt</th>
                  <th>Quality</th>
                </tr>
              </thead>
              <tbody>
                {flights.map((flight) => (
                  <tr key={flight.flight_id}>
                    <td>{flight.callsign ?? <span className="muted">unknown</span>}</td>
                    <td>
                      {flight.departure_airport ?? "···"}
                      <span className="muted"> → </span>
                      {flight.arrival_airport ?? "···"}
                    </td>
                    <td className="num">{flight.duration_minutes.toFixed(1)}</td>
                    <td className="num">
                      {flight.max_altitude_m === null
                        ? "—"
                        : `${Math.round(flight.max_altitude_m).toLocaleString()} m`}
                    </td>
                    <td>
                      <span className={`pill ${flight.reconstruction_quality}`}>
                        {flight.reconstruction_quality}
                      </span>
                    </td>
                  </tr>
                ))}
                {flights.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="muted">
                      No flights yet. Run the ingester and sink, then `dbt build`.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

function Tile({
  label,
  value,
  note,
}: {
  label: string;
  value: number | string | undefined;
  note?: string;
}) {
  return (
    <div className="tile">
      <div className="label">{label}</div>
      <div className="value">
        {value === undefined ? "—" : typeof value === "number" ? value.toLocaleString() : value}
      </div>
      {note ? <div className="note">{note}</div> : null}
    </div>
  );
}
