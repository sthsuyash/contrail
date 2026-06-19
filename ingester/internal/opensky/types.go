package opensky

import (
	"encoding/json"
	"fmt"
	"time"
)

// PositionSource identifies how a state vector's position was derived.
type PositionSource int

const (
	SourceADSB    PositionSource = 0
	SourceASTERIX PositionSource = 1
	SourceMLAT    PositionSource = 2
	SourceFLARM   PositionSource = 3
)

// StateVector is one aircraft's reported state at a point in time.
//
// The API encodes these as heterogeneous positional JSON arrays rather than
// objects, so the index of each field is load-bearing. Only icao24,
// origin_country, last_contact, on_ground and spi are guaranteed present;
// everything else is nullable and is modelled as a pointer so that "absent"
// stays distinguishable from a legitimate zero. An aircraft on the ground
// genuinely reports velocity 0, and an aircraft with no position fix reports
// nothing at all. Collapsing both to 0 would silently corrupt every
// downstream aggregate.
type StateVector struct {
	ICAO24         string         `json:"icao24"`
	Callsign       *string        `json:"callsign"`
	OriginCountry  string         `json:"origin_country"`
	TimePosition   *int64         `json:"time_position"`
	LastContact    int64          `json:"last_contact"`
	Longitude      *float64       `json:"longitude"`
	Latitude       *float64       `json:"latitude"`
	BaroAltitude   *float64       `json:"baro_altitude"`
	OnGround       bool           `json:"on_ground"`
	Velocity       *float64       `json:"velocity"`
	TrueTrack      *float64       `json:"true_track"`
	VerticalRate   *float64       `json:"vertical_rate"`
	Sensors        []int32        `json:"sensors"`
	GeoAltitude    *float64       `json:"geo_altitude"`
	Squawk         *string        `json:"squawk"`
	SPI            bool           `json:"spi"`
	PositionSource PositionSource `json:"position_source"`
	Category       *int           `json:"category"`
}

// minStateFields is the number of entries present on every state vector.
// Index 17 (category) only appears when the request sets extended=1, so
// vectors arrive at either 17 or 18 elements depending on the query.
const minStateFields = 17

// UnmarshalJSON decodes the positional array form into named fields.
func (s *StateVector) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("state vector is not a JSON array: %w", err)
	}
	if len(raw) < minStateFields {
		return fmt.Errorf("state vector has %d fields, want at least %d", len(raw), minStateFields)
	}

	// Each decode target is addressed by its documented index. Pointer targets
	// are left nil when the element is JSON null.
	decoders := []func() error{
		func() error { return decodeInto(raw[0], &s.ICAO24) },
		func() error { return decodePtr(raw[1], &s.Callsign) },
		func() error { return decodeInto(raw[2], &s.OriginCountry) },
		func() error { return decodePtr(raw[3], &s.TimePosition) },
		func() error { return decodeInto(raw[4], &s.LastContact) },
		func() error { return decodePtr(raw[5], &s.Longitude) },
		func() error { return decodePtr(raw[6], &s.Latitude) },
		func() error { return decodePtr(raw[7], &s.BaroAltitude) },
		func() error { return decodeInto(raw[8], &s.OnGround) },
		func() error { return decodePtr(raw[9], &s.Velocity) },
		func() error { return decodePtr(raw[10], &s.TrueTrack) },
		func() error { return decodePtr(raw[11], &s.VerticalRate) },
		func() error { return decodeInto(raw[12], &s.Sensors) },
		func() error { return decodePtr(raw[13], &s.GeoAltitude) },
		func() error { return decodePtr(raw[14], &s.Squawk) },
		func() error { return decodeInto(raw[15], &s.SPI) },
		func() error { return decodeInto(raw[16], &s.PositionSource) },
	}
	for i, decode := range decoders {
		if err := decode(); err != nil {
			return fmt.Errorf("field %d: %w", i, err)
		}
	}
	if len(raw) > minStateFields {
		if err := decodePtr(raw[minStateFields], &s.Category); err != nil {
			return fmt.Errorf("field %d: %w", minStateFields, err)
		}
	}
	return nil
}

// decodeInto unmarshals a non-nullable element, tolerating an explicit null by
// leaving the zero value in place. The API occasionally emits null for fields
// its own schema marks as required (sensors is the common offender).
func decodeInto(raw json.RawMessage, target any) error {
	if isNull(raw) {
		return nil
	}
	return json.Unmarshal(raw, target)
}

// decodePtr allocates and fills **T only when the element is not null.
func decodePtr[T any](raw json.RawMessage, target **T) error {
	if isNull(raw) {
		*target = nil
		return nil
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	*target = &v
	return nil
}

func isNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// HasPosition reports whether the vector carries a usable lat/lon fix.
// Vectors without one still matter (they prove the aircraft was contactable)
// but they cannot participate in any geospatial model.
func (s *StateVector) HasPosition() bool {
	return s.Latitude != nil && s.Longitude != nil
}

// DedupKey identifies a distinct observation.
//
// Consecutive polls overlap almost entirely: the same aircraft is returned
// every time, and when its transponder has not reported since the last poll it
// comes back carrying an identical time_position. Keying on
// (icao24, time_position) is what separates a genuinely new observation from
// the same one seen again, and is the difference between ~10M real rows a day
// and a table of mostly duplicates.
//
// When time_position is absent the vector has no position fix and no
// observation instant of its own; last_contact is used so the row remains
// addressable, with a marker so downstream models can tell the two apart.
func (s *StateVector) DedupKey() string {
	if s.TimePosition != nil {
		return fmt.Sprintf("%s:%d", s.ICAO24, *s.TimePosition)
	}
	return fmt.Sprintf("%s:c%d", s.ICAO24, s.LastContact)
}

// ObservedAt returns the instant the position was measured, falling back to
// last contact when there is no position fix.
func (s *StateVector) ObservedAt() time.Time {
	if s.TimePosition != nil {
		return time.Unix(*s.TimePosition, 0).UTC()
	}
	return time.Unix(s.LastContact, 0).UTC()
}

// StatesResponse is the envelope returned by /states/all.
type StatesResponse struct {
	Time   int64         `json:"time"`
	States []StateVector `json:"states"`
}
