// Package sink writes observed state vectors to a destination.
package sink

import (
	"context"
	"strings"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/opensky"
)

// Record is the wire format between the ingester and everything downstream.
//
// It flattens the API's positional array into named fields and adds the
// provenance the pipeline needs but the API does not supply: which region's
// poll produced the row, when that poll happened, and when it was ingested.
//
// Nullable measurements stay pointers so they serialise to JSON null rather
// than zero. The distinction survives all the way into ClickHouse as Nullable
// columns, because collapsing it at the edge would be unrecoverable: no
// downstream model can tell an aircraft stationary on a taxiway from one whose
// velocity was never reported.
type Record struct {
	// Observation identity.
	ICAO24     string `json:"icao24"`
	DedupKey   string `json:"dedup_key"`
	ObservedAt int64  `json:"observed_at"`

	// Reported state.
	Callsign       *string  `json:"callsign"`
	OriginCountry  string   `json:"origin_country"`
	TimePosition   *int64   `json:"time_position"`
	LastContact    int64    `json:"last_contact"`
	Longitude      *float64 `json:"longitude"`
	Latitude       *float64 `json:"latitude"`
	BaroAltitude   *float64 `json:"baro_altitude"`
	OnGround       bool     `json:"on_ground"`
	Velocity       *float64 `json:"velocity"`
	TrueTrack      *float64 `json:"true_track"`
	VerticalRate   *float64 `json:"vertical_rate"`
	GeoAltitude    *float64 `json:"geo_altitude"`
	Squawk         *string  `json:"squawk"`
	SPI            bool     `json:"spi"`
	PositionSource int      `json:"position_source"`
	Category       *int     `json:"category"`

	// Ingestion provenance.
	Region     string `json:"region"`
	PollTime   int64  `json:"poll_time"`
	IngestedAt int64  `json:"ingested_at"`

	// PositionAgeSeconds is how far the position fix lags the last contact.
	//
	// Precomputed here because it is the field that decides whether an
	// observation can be trusted for geospatial work, and measurements show it
	// reaching several minutes on a small fraction of vectors. Downstream
	// models filter on it constantly; deriving it once at the edge keeps that
	// definition in one place.
	PositionAgeSeconds int64 `json:"position_age_seconds"`
}

// NewRecord projects a state vector into a Record.
func NewRecord(sv *opensky.StateVector, region string, pollTime int64, ingestedAt time.Time) Record {
	rec := Record{
		ICAO24:         sv.ICAO24,
		DedupKey:       sv.DedupKey(),
		ObservedAt:     sv.ObservedAt().Unix(),
		Callsign:       trimCallsign(sv.Callsign),
		OriginCountry:  sv.OriginCountry,
		TimePosition:   sv.TimePosition,
		LastContact:    sv.LastContact,
		Longitude:      sv.Longitude,
		Latitude:       sv.Latitude,
		BaroAltitude:   sv.BaroAltitude,
		OnGround:       sv.OnGround,
		Velocity:       sv.Velocity,
		TrueTrack:      sv.TrueTrack,
		VerticalRate:   sv.VerticalRate,
		GeoAltitude:    sv.GeoAltitude,
		Squawk:         sv.Squawk,
		SPI:            sv.SPI,
		PositionSource: int(sv.PositionSource),
		Category:       sv.Category,
		Region:         region,
		PollTime:       pollTime,
		IngestedAt:     ingestedAt.Unix(),
	}
	if sv.TimePosition != nil {
		rec.PositionAgeSeconds = sv.LastContact - *sv.TimePosition
	}
	return rec
}

// trimCallsign strips the fixed-width padding the API pads callsigns with.
// Callsigns arrive in an 8-character field, so "SWR123" is transmitted as
// "SWR123  ". Joining against flight schedules on the padded form fails
// silently, so the padding is removed once, here.
func trimCallsign(cs *string) *string {
	if cs == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*cs)
	if trimmed == "" {
		// An all-blank callsign carries no information; it is absence, not a
		// value, and modelling it as an empty string would put a meaningless
		// category into every group-by downstream.
		return nil
	}
	return &trimmed
}

// Sink accepts batches of records.
type Sink interface {
	// Write persists a batch. It must be safe to retry on error.
	Write(ctx context.Context, records []Record) error
	// Close flushes anything buffered.
	Close() error
	// Describe names the sink for logs.
	Describe() string
}
