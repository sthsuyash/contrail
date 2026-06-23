// Package budget implements credit accounting for the OpenSky REST API.
//
// The API meters /states/* access with a daily credit allowance rather than a
// requests-per-second limit, and the cost of a single call depends on how much
// of the earth it asks about. That inverts the usual rate-limiting problem: the
// scarce resource is not throughput but a fixed daily quota that must be spread
// deliberately across 24 hours. Spend it greedily and ingestion stops dead
// before noon; spend it too cautiously and the dataset has holes.
//
// The budgeter therefore paces itself: it divides the credits still unspent by
// the time left before the quota refills, and derives a polling interval from
// that. A run that loses hours to a crash automatically speeds up to use the
// credits it did not burn, and a run that hits an unexpected 429 slows down.
package budget

import (
	"fmt"
	"sync"
	"time"
)

// Quota is the documented daily credit allowance for the /states/* bucket,
// which is metered independently of /flights/* and /tracks/*.
type Quota int

const (
	// QuotaAnonymous applies to unauthenticated callers, bucketed by IP.
	QuotaAnonymous Quota = 400
	// QuotaRegistered applies to any account with API credentials.
	QuotaRegistered Quota = 4000
	// QuotaFeeder applies to accounts whose ADS-B receiver is online at least
	// 30% of the current month.
	QuotaFeeder Quota = 8000
)

// Resolution is the finest interval at which the API produces distinct data.
// Polling faster than this returns state vectors already seen, spending credits
// for nothing, so it forms the floor on the polling interval.
const (
	ResolutionAnonymous  = 10 * time.Second
	ResolutionRegistered = 5 * time.Second
)

// maxInterval caps how far apart polls may drift when the remaining budget is
// very small. Beyond this the feed is too sparse to sessionize into flights,
// and it is better to stop early than to record a trail of disconnected points.
const maxInterval = 15 * time.Minute

// BoundingBox is a geographic query window in WGS-84 decimal degrees.
// The zero value means "global", which is the most expensive query.
type BoundingBox struct {
	LatMin, LonMin float64
	LatMax, LonMax float64
}

// IsGlobal reports whether the box is unset, meaning the whole world.
func (b BoundingBox) IsGlobal() bool {
	return b == BoundingBox{}
}

// AreaSquareDegrees returns the box's extent. This is deliberately a naive
// lat/lon product rather than a true spherical area: the API's own pricing
// tiers are defined in square degrees, so matching its arithmetic is what keeps
// the local estimate in step with the server's accounting.
func (b BoundingBox) AreaSquareDegrees() float64 {
	if b.IsGlobal() {
		return 360 * 180
	}
	return (b.LatMax - b.LatMin) * (b.LonMax - b.LonMin)
}

// Cost returns the credits a single /states/all call over this box consumes.
func (b BoundingBox) Cost() int {
	if b.IsGlobal() {
		return 4
	}
	switch area := b.AreaSquareDegrees(); {
	case area <= 25:
		return 1
	case area <= 100:
		return 2
	case area <= 400:
		return 3
	default:
		return 4
	}
}

// Budget tracks credit consumption against a refilling daily allowance.
// It is safe for concurrent use.
type Budget struct {
	mu         sync.Mutex
	quota      Quota
	resolution time.Duration
	now        func() time.Time

	spent   int
	refill  time.Time
	blocked time.Time // set by a 429; no spending until this instant
}

// Option customises a Budget.
type Option func(*Budget)

// WithClock replaces the time source, for tests.
func WithClock(now func() time.Time) Option {
	return func(b *Budget) { b.now = now }
}

// WithResolution overrides the minimum polling interval.
func WithResolution(d time.Duration) Option {
	return func(b *Budget) { b.resolution = d }
}

// New creates a Budget for the given quota. Registered quotas poll at the
// 5-second data resolution; anonymous callers only see 10-second data.
func New(quota Quota, opts ...Option) *Budget {
	b := &Budget{
		quota:      quota,
		resolution: ResolutionRegistered,
		now:        time.Now,
	}
	if quota == QuotaAnonymous {
		b.resolution = ResolutionAnonymous
	}
	for _, opt := range opts {
		opt(b)
	}
	b.refill = nextRefill(b.now())
	return b
}

// nextRefill returns the next UTC midnight, when the daily allowance resets.
func nextRefill(t time.Time) time.Time {
	utc := t.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
}

// rolloverLocked resets accounting if the refill instant has passed.
// Callers must hold b.mu.
func (b *Budget) rolloverLocked(now time.Time) {
	if !now.Before(b.refill) {
		b.spent = 0
		b.refill = nextRefill(now)
	}
}

// Remaining reports credits still available in the current window.
func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rolloverLocked(b.now())
	return int(b.quota) - b.spent
}

// Spend deducts the cost of one call, reporting whether it was affordable.
// A false return means the caller must wait until the next refill.
func (b *Budget) Spend(cost int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.rolloverLocked(now)

	if now.Before(b.blocked) || b.spent+cost > int(b.quota) {
		return false
	}
	b.spent += cost
	return true
}

// Penalize records a 429 and suspends spending for the interval the server
// asked for. The server's accounting is authoritative; when it disagrees with
// the local estimate (which it will, since other clients may share an
// anonymous IP bucket), the local view is corrected to match by treating the
// remaining allowance as exhausted for the blocked period.
func (b *Budget) Penalize(retryAfter time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if retryAfter <= 0 {
		retryAfter = time.Minute
	}
	b.blocked = b.now().Add(retryAfter)
}

// NextInterval returns how long to wait before the next call of the given cost,
// pacing the unspent allowance evenly across the time left in the window.
//
// The result is clamped to the API's data resolution at the fast end, since
// polling faster cannot surface new observations, and to maxInterval at the
// slow end.
func (b *Budget) NextInterval(cost int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.rolloverLocked(now)

	// A server-imposed penalty outranks any local pacing decision.
	if now.Before(b.blocked) {
		return b.blocked.Sub(now)
	}

	remaining := int(b.quota) - b.spent
	timeLeft := b.refill.Sub(now)
	if remaining < cost || timeLeft <= 0 {
		// Nothing affordable left in this window; sleep until it refills.
		return max(timeLeft, 0)
	}

	callsLeft := remaining / cost
	interval := timeLeft / time.Duration(callsLeft)
	return min(max(interval, b.resolution), maxInterval)
}

// Status is a point-in-time view of the budget, for logging and metrics.
type Status struct {
	Quota     int
	Spent     int
	Remaining int
	RefillsIn time.Duration
}

func (s Status) String() string {
	return fmt.Sprintf("credits %d/%d used, %d left, refill in %s",
		s.Spent, s.Quota, s.Remaining, s.RefillsIn.Round(time.Second))
}

// StatusLine renders the accounting snapshot for logs.
func (b *Budget) StatusLine() string { return b.Status().String() }

// Status returns the current accounting snapshot.
func (b *Budget) Status() Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.rolloverLocked(now)
	return Status{
		Quota:     int(b.quota),
		Spent:     b.spent,
		Remaining: int(b.quota) - b.spent,
		RefillsIn: b.refill.Sub(now),
	}
}
