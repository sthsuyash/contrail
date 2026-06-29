package opensky

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
)

// fixtureFS builds an in-memory capture of n responses, each one second apart,
// with a single aircraft drifting north-east.
func fixtureFS(t *testing.T, n int) fstest.MapFS {
	t.Helper()
	fsys := fstest.MapFS{}
	for i := range n {
		ts := 1786107000 + i
		lat, lon := 47.0+float64(i)*0.1, 8.0+float64(i)*0.1
		body := `{"time":` + itoa(ts) + `,"states":[` +
			`["4b1815","SWR123  ","Switzerland",` + itoa(ts) + `,` + itoa(ts) + `,` +
			ftoa(lon) + `,` + ftoa(lat) + `,11277.6,false,231.5,89.2,0.0,null,11582.4,"1000",false,0]]}`
		fsys[fixtureName(i)] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

func TestReplayServesRecordingsInOrder(t *testing.T) {
	r, err := NewReplay(fixtureFS(t, 3), "states-*.json", WithLoop(false))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}
	if r.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", r.Len())
	}

	for i, want := range []int64{1786107000, 1786107001, 1786107002} {
		resp, err := r.Fetch(context.Background(), budget.BoundingBox{})
		if err != nil {
			t.Fatalf("Fetch %d: %v", i, err)
		}
		if resp.Time != want {
			t.Errorf("response %d Time = %d, want %d", i, resp.Time, want)
		}
	}
}

// Filenames must sort into capture order, which is why the recorder
// zero-pads sequence numbers.
func TestReplayOrdersByFilenameNotGlobOrder(t *testing.T) {
	fsys := fstest.MapFS{
		"states-010.json": &fstest.MapFile{Data: []byte(`{"time":10,"states":[]}`)},
		"states-002.json": &fstest.MapFile{Data: []byte(`{"time":2,"states":[]}`)},
		"states-001.json": &fstest.MapFile{Data: []byte(`{"time":1,"states":[]}`)},
	}
	r, err := NewReplay(fsys, "states-*.json", WithLoop(false))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	for _, want := range []int64{1, 2, 10} {
		resp, err := r.Fetch(context.Background(), budget.BoundingBox{})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if resp.Time != want {
			t.Errorf("Time = %d, want %d", resp.Time, want)
		}
	}
}

func TestReplayLoopsWhenAsked(t *testing.T) {
	r, err := NewReplay(fixtureFS(t, 2), "states-*.json", WithLoop(true))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	var times []int64
	for range 5 {
		resp, err := r.Fetch(context.Background(), budget.BoundingBox{})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		times = append(times, resp.Time)
	}

	want := []int64{1786107000, 1786107001, 1786107000, 1786107001, 1786107000}
	for i := range want {
		if times[i] != want[i] {
			t.Errorf("fetch %d Time = %d, want %d", i, times[i], want[i])
		}
	}
}

func TestReplayExhaustsWhenNotLooping(t *testing.T) {
	r, err := NewReplay(fixtureFS(t, 2), "states-*.json", WithLoop(false))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	for range 2 {
		if _, err := r.Fetch(context.Background(), budget.BoundingBox{}); err != nil {
			t.Fatalf("Fetch: %v", err)
		}
	}
	if _, err := r.Fetch(context.Background(), budget.BoundingBox{}); !errors.Is(err, ErrReplayExhausted) {
		t.Errorf("Fetch after exhaustion = %v, want ErrReplayExhausted", err)
	}
}

// Replay has to behave like the API when handed a narrower box, or the two
// sources would diverge in a way that only shows up in production.
func TestReplayFiltersByBoundingBox(t *testing.T) {
	r, err := NewReplay(fixtureFS(t, 3), "states-*.json", WithLoop(false))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	// The first recording sits at (47.0, 8.0); this box excludes it.
	box := budget.BoundingBox{LatMin: 50, LonMin: 10, LatMax: 55, LonMax: 15}
	resp, err := r.Fetch(context.Background(), box)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(resp.States) != 0 {
		t.Errorf("States = %d, want 0 outside the box", len(resp.States))
	}

	// A box containing the second recording's position keeps it.
	box = budget.BoundingBox{LatMin: 47, LonMin: 8, LatMax: 48, LonMax: 9}
	resp, err = r.Fetch(context.Background(), box)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(resp.States) != 1 {
		t.Errorf("States = %d, want 1 inside the box", len(resp.States))
	}
}

// Vectors with no fix cannot be located, and the live API returns them
// regardless of the box, so replay must too.
func TestReplayKeepsPositionlessVectors(t *testing.T) {
	fsys := fstest.MapFS{
		"states-001.json": &fstest.MapFile{Data: []byte(
			`{"time":1,"states":[["3c6444",null,"Germany",null,1,null,null,null,true,null,null,null,null,null,null,false,0]]}`)},
	}
	r, err := NewReplay(fsys, "states-*.json", WithLoop(false))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	box := budget.BoundingBox{LatMin: 50, LonMin: 10, LatMax: 55, LonMax: 15}
	resp, err := r.Fetch(context.Background(), box)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(resp.States) != 1 {
		t.Errorf("States = %d, want the positionless vector retained", len(resp.States))
	}
}

func TestReplayTimeShiftRebasesOntoNow(t *testing.T) {
	r, err := NewReplay(fixtureFS(t, 3), "states-*.json",
		WithLoop(false), WithTimeShift(true))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	resp, err := r.Fetch(context.Background(), budget.BoundingBox{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	age := time.Since(time.Unix(resp.Time, 0))
	if age > time.Minute {
		t.Errorf("shifted timestamp is %s old, want near the current clock", age)
	}
	if resp.States[0].TimePosition == nil {
		t.Fatal("TimePosition = nil after shifting")
	}
	if got := time.Since(time.Unix(*resp.States[0].TimePosition, 0)); got > time.Minute {
		t.Errorf("shifted TimePosition is %s old, want near the current clock", got)
	}
}

// Time shifting must not write through the pointers held by the loaded
// fixtures, or each lap of a looping replay would drift further from the
// original recording.
func TestReplayTimeShiftDoesNotCorruptFixtures(t *testing.T) {
	r, err := NewReplay(fixtureFS(t, 2), "states-*.json",
		WithLoop(true), WithTimeShift(true))
	if err != nil {
		t.Fatalf("NewReplay: %v", err)
	}

	first, err := r.Fetch(context.Background(), budget.BoundingBox{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	firstPos := *first.States[0].TimePosition

	// Consume a full lap and return to the same recording.
	if _, err := r.Fetch(context.Background(), budget.BoundingBox{}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	again, err := r.Fetch(context.Background(), budget.BoundingBox{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if got := *again.States[0].TimePosition; got != firstPos {
		t.Errorf("TimePosition on second lap = %d, want %d (the fixture was mutated in place)",
			got, firstPos)
	}
}

func TestNewReplayRejectsEmptyPattern(t *testing.T) {
	if _, err := NewReplay(fstest.MapFS{}, "states-*.json"); err == nil {
		t.Error("NewReplay succeeded with no matching fixtures, want an error")
	}
}

func TestNewReplayRejectsMalformedFixture(t *testing.T) {
	fsys := fstest.MapFS{
		"states-001.json": &fstest.MapFile{Data: []byte(`{"time":1,"states":[[`)},
	}
	if _, err := NewReplay(fsys, "states-*.json"); err == nil {
		t.Error("NewReplay succeeded on malformed JSON, want an error")
	}
}

// TestReplayDecodesRecordedTraffic runs the decoder over genuine captures
// rather than hand-written vectors. Real responses carry quirks the
// documentation does not describe, most notably a sensors field that is null
// on every vector, and this is what catches a decoder that only handles the
// documented shape.
func TestReplayDecodesRecordedTraffic(t *testing.T) {
	const dir = "../../../fixtures"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skip("no recorded fixtures; run `make record-fixtures`")
	}

	r, err := NewReplay(os.DirFS(dir), "states-*.json", WithLoop(false))
	if err != nil {
		t.Skipf("no recorded fixtures loaded: %v", err)
	}
	t.Logf("loaded %d recorded responses", r.Len())

	var total, positioned int
	for {
		resp, err := r.Fetch(context.Background(), budget.BoundingBox{})
		if errors.Is(err, ErrReplayExhausted) {
			break
		}
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if resp.Time == 0 {
			t.Error("recorded response has no timestamp")
		}
		for i := range resp.States {
			sv := &resp.States[i]
			total++
			if sv.ICAO24 == "" {
				t.Fatal("decoded a vector with no ICAO24; the positional decode is misaligned")
			}
			if sv.DedupKey() == "" {
				t.Fatal("decoded a vector with no dedup key")
			}
			if sv.HasPosition() {
				positioned++
				if *sv.Latitude < -90 || *sv.Latitude > 90 {
					t.Errorf("latitude %f out of range; field indices are off", *sv.Latitude)
				}
				if *sv.Longitude < -180 || *sv.Longitude > 180 {
					t.Errorf("longitude %f out of range; field indices are off", *sv.Longitude)
				}
			}
		}
	}

	if total == 0 {
		t.Fatal("recorded fixtures contained no state vectors")
	}
	t.Logf("decoded %d vectors, %d with a position fix", total, positioned)
}

func itoa(v int) string     { return strconv.Itoa(v) }
func ftoa(v float64) string { return formatDegrees(v) }

// fixtureName zero-pads the sequence number so filename order matches capture
// order, the property TestReplayOrdersByFilenameNotGlobOrder depends on.
func fixtureName(i int) string {
	return fmt.Sprintf("states-%03d.json", i)
}
