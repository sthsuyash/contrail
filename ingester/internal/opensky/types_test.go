package opensky

import (
	"encoding/json"
	"testing"
	"time"
)

// A complete state vector as the API emits it: a positional array, 17 fields,
// no category because extended=1 was not set.
const fullVector = `["4b1815","SWR123  ","Switzerland",1754563200,1754563201,` +
	`8.5554,47.4515,11277.6,false,231.5,89.2,0.0,[1234,5678],11582.4,"1000",false,0]`

func decodeOne(t *testing.T, raw string) StateVector {
	t.Helper()
	var sv StateVector
	if err := json.Unmarshal([]byte(raw), &sv); err != nil {
		t.Fatalf("unmarshalling state vector: %v", err)
	}
	return sv
}

func TestUnmarshalFullVector(t *testing.T) {
	sv := decodeOne(t, fullVector)

	if sv.ICAO24 != "4b1815" {
		t.Errorf("ICAO24 = %q, want %q", sv.ICAO24, "4b1815")
	}
	if sv.Callsign == nil || *sv.Callsign != "SWR123  " {
		t.Errorf("Callsign = %v, want %q (trailing padding preserved)", sv.Callsign, "SWR123  ")
	}
	if sv.OriginCountry != "Switzerland" {
		t.Errorf("OriginCountry = %q, want %q", sv.OriginCountry, "Switzerland")
	}
	if sv.TimePosition == nil || *sv.TimePosition != 1754563200 {
		t.Errorf("TimePosition = %v, want 1754563200", sv.TimePosition)
	}
	if sv.LastContact != 1754563201 {
		t.Errorf("LastContact = %d, want 1754563201", sv.LastContact)
	}
	if sv.Longitude == nil || *sv.Longitude != 8.5554 {
		t.Errorf("Longitude = %v, want 8.5554", sv.Longitude)
	}
	if sv.Latitude == nil || *sv.Latitude != 47.4515 {
		t.Errorf("Latitude = %v, want 47.4515", sv.Latitude)
	}
	if sv.OnGround {
		t.Error("OnGround = true, want false")
	}
	if len(sv.Sensors) != 2 {
		t.Errorf("Sensors = %v, want 2 entries", sv.Sensors)
	}
	if sv.Squawk == nil || *sv.Squawk != "1000" {
		t.Errorf("Squawk = %v, want %q", sv.Squawk, "1000")
	}
	if sv.PositionSource != SourceADSB {
		t.Errorf("PositionSource = %d, want ADS-B", sv.PositionSource)
	}
	if sv.Category != nil {
		t.Errorf("Category = %v, want nil when extended was not requested", sv.Category)
	}
}

// TestNullsSurviveAsNil is the correctness guarantee that matters most: a
// missing measurement must not become a zero measurement. An aircraft with no
// position fix and an aircraft parked at sea level are different facts.
func TestNullsSurviveAsNil(t *testing.T) {
	const noFix = `["3c6444",null,"Germany",null,1754563201,null,null,null,` +
		`true,null,null,null,[],null,null,false,0]`

	sv := decodeOne(t, noFix)

	if sv.Callsign != nil {
		t.Errorf("Callsign = %v, want nil", sv.Callsign)
	}
	if sv.TimePosition != nil {
		t.Errorf("TimePosition = %v, want nil", sv.TimePosition)
	}
	if sv.Latitude != nil || sv.Longitude != nil {
		t.Errorf("position = (%v, %v), want both nil", sv.Latitude, sv.Longitude)
	}
	if sv.Velocity != nil {
		t.Errorf("Velocity = %v, want nil; absent must not collapse to 0", sv.Velocity)
	}
	if sv.BaroAltitude != nil || sv.GeoAltitude != nil {
		t.Errorf("altitudes = (%v, %v), want both nil", sv.BaroAltitude, sv.GeoAltitude)
	}
	if sv.HasPosition() {
		t.Error("HasPosition() = true for a vector with no fix")
	}
	// Non-nullable fields still decode normally.
	if sv.ICAO24 != "3c6444" || !sv.OnGround {
		t.Errorf("required fields mangled: icao24=%q on_ground=%v", sv.ICAO24, sv.OnGround)
	}
}

// A grounded aircraft genuinely reporting zero must stay distinguishable from
// one reporting nothing, the inverse of the case above.
func TestZeroIsNotNull(t *testing.T) {
	const grounded = `["484506","KLM45   ","Netherlands",1754563200,1754563200,` +
		`4.7639,52.3086,0.0,true,0.0,0.0,0.0,[99],0.0,"2000",false,0]`

	sv := decodeOne(t, grounded)

	if sv.Velocity == nil {
		t.Fatal("Velocity = nil, want a pointer to 0 for a stationary aircraft")
	}
	if *sv.Velocity != 0 {
		t.Errorf("Velocity = %v, want 0", *sv.Velocity)
	}
	if sv.BaroAltitude == nil || *sv.BaroAltitude != 0 {
		t.Errorf("BaroAltitude = %v, want a pointer to 0", sv.BaroAltitude)
	}
	if !sv.HasPosition() {
		t.Error("HasPosition() = false despite a valid fix at sea level")
	}
}

func TestExtendedVectorCarriesCategory(t *testing.T) {
	// extended=1 appends an 18th element.
	const extended = `["4b1815","SWR123  ","Switzerland",1754563200,1754563201,` +
		`8.5554,47.4515,11277.6,false,231.5,89.2,0.0,[1234],11582.4,"1000",false,0,4]`

	sv := decodeOne(t, extended)
	if sv.Category == nil {
		t.Fatal("Category = nil, want 4")
	}
	if *sv.Category != 4 {
		t.Errorf("Category = %d, want 4", *sv.Category)
	}
}

func TestTruncatedVectorIsRejected(t *testing.T) {
	const truncated = `["4b1815","SWR123  ","Switzerland",1754563200]`

	var sv StateVector
	if err := json.Unmarshal([]byte(truncated), &sv); err == nil {
		t.Error("unmarshalling a truncated vector succeeded, want an error")
	}
}

// The API sometimes emits null for sensors despite its schema marking the field
// as required. That must degrade to an empty slice, not fail the whole poll:
// one malformed vector should never cost the other 12,000 in the response.
func TestNullSensorsDegradeGracefully(t *testing.T) {
	const nullSensors = `["4b1815","SWR123  ","Switzerland",1754563200,1754563201,` +
		`8.5554,47.4515,11277.6,false,231.5,89.2,0.0,null,11582.4,"1000",false,0]`

	sv := decodeOne(t, nullSensors)
	if len(sv.Sensors) != 0 {
		t.Errorf("Sensors = %v, want empty", sv.Sensors)
	}
	if sv.ICAO24 != "4b1815" {
		t.Error("a null sensors field derailed decoding of the rest of the vector")
	}
}

// TestDedupKeyDistinguishesObservations covers the property the entire bronze
// layer depends on. Consecutive polls return the same aircraft repeatedly; only
// a changed time_position marks a genuinely new observation.
func TestDedupKeyDistinguishesObservations(t *testing.T) {
	first := decodeOne(t, fullVector)

	// Same aircraft, same transponder report, seen again on the next poll.
	repeat := decodeOne(t, fullVector)
	if first.DedupKey() != repeat.DedupKey() {
		t.Error("re-observing an unchanged vector produced a different key")
	}

	// last_contact advancing without time_position does not make a new
	// observation: the aircraft is reachable but has not reported a new position.
	staleFix := decodeOne(t, fullVector)
	staleFix.LastContact = 1754563299
	if first.DedupKey() != staleFix.DedupKey() {
		t.Error("a bumped last_contact alone created a spurious new observation")
	}

	// A new transponder report is a new observation.
	moved := decodeOne(t, fullVector)
	newTime := int64(1754563210)
	moved.TimePosition = &newTime
	if first.DedupKey() == moved.DedupKey() {
		t.Error("a new time_position did not produce a distinct key")
	}
}

// Vectors with no position fix have no observation instant of their own, so
// they fall back to last_contact, but must not collide with real fixes.
func TestDedupKeyForPositionlessVectors(t *testing.T) {
	var sv StateVector
	sv.ICAO24 = "3c6444"
	sv.LastContact = 1754563200

	withFix := StateVector{ICAO24: "3c6444"}
	ts := int64(1754563200)
	withFix.TimePosition = &ts

	if sv.DedupKey() == withFix.DedupKey() {
		t.Errorf("positionless vector collided with a real fix at the same instant: %q", sv.DedupKey())
	}
}

func TestObservedAtPrefersPositionTime(t *testing.T) {
	sv := decodeOne(t, fullVector)
	if got, want := sv.ObservedAt(), time.Unix(1754563200, 0).UTC(); !got.Equal(want) {
		t.Errorf("ObservedAt() = %s, want %s", got, want)
	}

	sv.TimePosition = nil
	if got, want := sv.ObservedAt(), time.Unix(1754563201, 0).UTC(); !got.Equal(want) {
		t.Errorf("ObservedAt() without a fix = %s, want last contact %s", got, want)
	}
}

func TestUnmarshalStatesResponse(t *testing.T) {
	raw := `{"time":1754563201,"states":[` + fullVector + `,` + fullVector + `]}`

	var resp StatesResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshalling response envelope: %v", err)
	}
	if resp.Time != 1754563201 {
		t.Errorf("Time = %d, want 1754563201", resp.Time)
	}
	if len(resp.States) != 2 {
		t.Fatalf("States = %d entries, want 2", len(resp.States))
	}
}

// An empty result is normal for a small bounding box at a quiet hour and must
// not be treated as an error.
func TestUnmarshalEmptyStates(t *testing.T) {
	var resp StatesResponse
	if err := json.Unmarshal([]byte(`{"time":1754563201,"states":null}`), &resp); err != nil {
		t.Fatalf("unmarshalling an empty response: %v", err)
	}
	if len(resp.States) != 0 {
		t.Errorf("States = %v, want empty", resp.States)
	}
}
