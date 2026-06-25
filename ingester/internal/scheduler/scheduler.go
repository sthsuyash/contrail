// Package scheduler decides which region to poll next.
//
// With a single global query there is no decision to make. With several regions
// at different credit prices there is, and it matters: the API charges by the
// area a query covers, but the value of a query is the number of aircraft it
// returns, and those two quantities are almost unrelated. A 6 sq° window over
// London costs 1 credit and returns several hundred aircraft; a 400 sq° window
// over the North Atlantic costs 3 and returns a handful. Spending the daily
// allowance uniformly across regions therefore wastes most of it.
//
// The scheduler ranks regions by observed yield per credit, scaled by how long
// each has gone unpolled. The first term concentrates spending where the data
// is; the second stops a quiet region from being starved forever, since its
// score rises without bound until it is eventually chosen. Neither term alone
// is sufficient: yield alone starves, staleness alone ignores cost.
package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/config"
)

// smoothing is the EWMA weight applied to each new yield observation.
// Traffic density changes over hours, not minutes, so recent polls are
// weighted meaningfully but a single unusual one cannot dominate.
const smoothing = 0.3

// optimisticYield is the assumed yield of a region that has never been polled.
// Setting it high forces every region to be sampled before the scheduler
// commits to a ranking; starting at zero would let the first region polled
// monopolise the budget purely because nothing else had a measurement yet.
const optimisticYield = 1000.0

// DefaultMaxStaleness bounds how long a region may go unpolled.
//
// Ranking on yield per credit alone is unbounded: given a region returning 800
// aircraft per credit against one returning 1.7, the ratio drives the quiet
// region to roughly one poll in every 250. At realistic pacing that is a gap of
// several hours, which is not merely a thin sample but a useless one. Flights
// cannot be reconstructed from positions hours apart, so those credits produce
// nothing at all. A poll frequency below what sessionization needs is worse
// value than not covering the region, because it still costs credits.
//
// This ceiling converts the ranking from an optimisation into an optimisation
// under a coverage constraint: economics decide the order, but no region waits
// longer than this.
const DefaultMaxStaleness = 10 * time.Minute

// regionState tracks a region's measured productivity.
type regionState struct {
	region     config.Region
	cost       int
	yield      float64
	polls      int
	lastPolled time.Time
}

// score ranks a region for selection. Higher is better.
func (r *regionState) score(now time.Time) float64 {
	idle := now.Sub(r.lastPolled).Seconds()
	if idle < 0 {
		idle = 0
	}
	return (r.yield / float64(r.cost)) * idle
}

// Scheduler selects regions to poll. It is safe for concurrent use.
type Scheduler struct {
	mu           sync.Mutex
	regions      []*regionState
	now          func() time.Time
	maxStaleness time.Duration
}

// Option customises a Scheduler.
type Option func(*Scheduler)

// WithClock replaces the time source, for tests.
func WithClock(now func() time.Time) Option {
	return func(s *Scheduler) { s.now = now }
}

// WithMaxStaleness overrides the coverage ceiling. Zero disables it, restoring
// pure yield-per-credit ranking.
func WithMaxStaleness(d time.Duration) Option {
	return func(s *Scheduler) { s.maxStaleness = d }
}

// New builds a scheduler over the given regions.
func New(regions []config.Region, opts ...Option) (*Scheduler, error) {
	if len(regions) == 0 {
		return nil, fmt.Errorf("scheduler needs at least one region")
	}
	s := &Scheduler{now: time.Now, maxStaleness: DefaultMaxStaleness}
	for _, opt := range opts {
		opt(s)
	}

	start := s.now()
	for _, r := range regions {
		s.regions = append(s.regions, &regionState{
			region: r,
			cost:   r.Cost(),
			yield:  optimisticYield,
			// Seeding every region with the same instant means the first pass
			// is a plain round-robin in declaration order, which keeps startup
			// behaviour predictable and easy to reason about in logs.
			lastPolled: start,
		})
	}
	return s, nil
}

// Next returns the region that should be polled now, and its credit cost.
//
// Regions past the staleness ceiling are served first, stalest before the rest;
// only when every region is within the ceiling does yield per credit decide.
func (s *Scheduler) Next() (config.Region, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	if s.maxStaleness > 0 {
		var stalest *regionState
		for _, r := range s.regions {
			if now.Sub(r.lastPolled) <= s.maxStaleness {
				continue
			}
			if stalest == nil || r.lastPolled.Before(stalest.lastPolled) {
				stalest = r
			}
		}
		if stalest != nil {
			return stalest.region, stalest.cost
		}
	}

	best := s.regions[0]
	bestScore := best.score(now)
	for _, r := range s.regions[1:] {
		if sc := r.score(now); sc > bestScore {
			best, bestScore = r, sc
		}
	}
	return best.region, best.cost
}

// Observe records the outcome of a poll so future rankings improve.
func (s *Scheduler) Observe(name string, vectors int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.regions {
		if r.region.Name != name {
			continue
		}
		r.lastPolled = s.now()
		r.polls++
		if r.polls == 1 {
			// Replace the optimistic seed outright rather than blending it,
			// so one real measurement immediately displaces the guess.
			r.yield = float64(vectors)
		} else {
			r.yield = smoothing*float64(vectors) + (1-smoothing)*r.yield
		}
		return
	}
}

// Stat reports a region's measured productivity.
type Stat struct {
	Name           string
	Cost           int
	Yield          float64
	Polls          int
	YieldPerCredit float64
}

// Stats returns per-region measurements, for logging and the README's numbers.
func (s *Scheduler) Stats() []Stat {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := make([]Stat, 0, len(s.regions))
	for _, r := range s.regions {
		stats = append(stats, Stat{
			Name:           r.region.Name,
			Cost:           r.cost,
			Yield:          r.yield,
			Polls:          r.polls,
			YieldPerCredit: r.yield / float64(r.cost),
		})
	}
	return stats
}
