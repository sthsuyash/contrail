package opensky

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
)

// Replay serves recorded /states/all responses instead of calling the API.
//
// It exists so the pipeline runs for someone who has just cloned the repo and
// has no OpenSky account, and so CI never depends on a third-party service
// being reachable or on a credit allowance that a busy afternoon could exhaust.
// Because it satisfies the same Source interface as the live client, nothing
// downstream of ingestion can tell the difference.
type Replay struct {
	responses []*StatesResponse
	names     []string
	loop      bool
	timeShift bool

	mu     sync.Mutex
	cursor int
	// epoch anchors wall-clock time to the first recorded timestamp so that
	// replayed observations advance at the same cadence they were captured.
	epoch     time.Time
	baseTime  int64
	exhausted bool
}

// ReplayOption customises a Replay.
type ReplayOption func(*Replay)

// WithLoop restarts from the first recording once the last is served, so a
// short capture can drive a long-running demo.
func WithLoop(loop bool) ReplayOption {
	return func(r *Replay) { r.loop = loop }
}

// WithTimeShift rebases recorded timestamps onto the current wall clock,
// preserving the intervals between them.
//
// This is what makes a recorded capture look live on a dashboard. It is off by
// default because tests need recorded data to decode to the same values every
// run: a shifting clock would make every assertion about timestamps unstable.
func WithTimeShift(shift bool) ReplayOption {
	return func(r *Replay) { r.timeShift = shift }
}

// ErrReplayExhausted is returned when a non-looping replay runs out.
var ErrReplayExhausted = fmt.Errorf("opensky: replay fixtures exhausted")

// NewReplay loads every file matching pattern from fsys, in filename order.
func NewReplay(fsys fs.FS, pattern string, opts ...ReplayOption) (*Replay, error) {
	matches, err := fs.Glob(fsys, pattern)
	if err != nil {
		return nil, fmt.Errorf("globbing replay fixtures %q: %w", pattern, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no replay fixtures matched %q", pattern)
	}
	// Fixtures are named with a sequence number; sorting recovers capture order.
	sort.Strings(matches)

	r := &Replay{loop: true}
	for _, opt := range opts {
		opt(r)
	}

	for _, name := range matches {
		raw, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("reading replay fixture %s: %w", name, err)
		}
		var resp StatesResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("parsing replay fixture %s: %w", name, err)
		}
		r.responses = append(r.responses, &resp)
		r.names = append(r.names, path.Base(name))
	}

	r.baseTime = r.responses[0].Time
	r.epoch = time.Now().UTC()
	return r, nil
}

// Describe implements Source.
func (r *Replay) Describe() string {
	return fmt.Sprintf("replay of %d recorded responses (%s … %s)",
		len(r.responses), r.names[0], r.names[len(r.names)-1])
}

// Len reports how many responses are loaded.
func (r *Replay) Len() int { return len(r.responses) }

// Fetch implements Source, returning the next recorded response.
//
// The bounding box is applied as a filter rather than ignored: a fixture
// captured over one region must still behave like the API when a caller asks
// for a sub-region of it, or replay would silently return data the live source
// never would.
func (r *Replay) Fetch(ctx context.Context, box budget.BoundingBox) (*StatesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.exhausted {
		r.mu.Unlock()
		return nil, ErrReplayExhausted
	}
	current := r.responses[r.cursor]
	r.cursor++
	if r.cursor >= len(r.responses) {
		if r.loop {
			r.cursor = 0
		} else {
			r.exhausted = true
		}
	}
	shift := r.timeShift
	epoch, baseTime := r.epoch, r.baseTime
	r.mu.Unlock()

	// Copy before mutating: the loaded responses are reused on every lap.
	out := &StatesResponse{Time: current.Time}
	for i := range current.States {
		sv := current.States[i]
		if !withinBox(&sv, box) {
			continue
		}
		out.States = append(out.States, sv)
	}
	if shift {
		rebase(out, epoch, baseTime)
	}
	return out, nil
}

// withinBox reports whether a vector falls inside the query window. Vectors
// with no position fix are always included: the live API returns them too, and
// dropping them here would understate how often aircraft go dark.
func withinBox(sv *StateVector, box budget.BoundingBox) bool {
	if box.IsGlobal() || !sv.HasPosition() {
		return true
	}
	return *sv.Latitude >= box.LatMin && *sv.Latitude <= box.LatMax &&
		*sv.Longitude >= box.LonMin && *sv.Longitude <= box.LonMax
}

// rebase moves recorded timestamps forward so the capture appears to be
// happening now, preserving the offsets between observations.
func rebase(resp *StatesResponse, epoch time.Time, baseTime int64) {
	delta := epoch.Unix() - baseTime
	resp.Time += delta
	for i := range resp.States {
		sv := &resp.States[i]
		sv.LastContact += delta
		if sv.TimePosition != nil {
			shifted := *sv.TimePosition + delta
			// Assign to a fresh variable: the pointer is shared with the
			// stored fixture, and mutating through it would corrupt the
			// recording for every subsequent lap.
			sv.TimePosition = &shifted
		}
	}
}
