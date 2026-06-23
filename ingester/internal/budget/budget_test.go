package budget

import (
	"testing"
	"time"
)

// midnight is a fixed UTC day boundary, so pacing arithmetic in these tests is
// exact rather than dependent on when the suite happens to run.
var midnight = time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)

// fakeClock is a manually advanced time source.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestBoundingBoxCost(t *testing.T) {
	tests := []struct {
		name string
		box  BoundingBox
		want int
	}{
		{"global zero value", BoundingBox{}, 4},
		// Switzerland-sized: 2° x 4° = 8 sq°, comfortably in the cheapest tier.
		{"small box", BoundingBox{45, 6, 47, 10}, 1},
		{"exactly 25 sq deg", BoundingBox{0, 0, 5, 5}, 1},
		{"just over 25 sq deg", BoundingBox{0, 0, 5, 5.1}, 2},
		{"exactly 100 sq deg", BoundingBox{0, 0, 10, 10}, 2},
		{"just over 100 sq deg", BoundingBox{0, 0, 10, 10.1}, 3},
		{"exactly 400 sq deg", BoundingBox{0, 0, 20, 20}, 3},
		{"just over 400 sq deg", BoundingBox{0, 0, 20, 20.1}, 4},
		{"continental", BoundingBox{35, -10, 60, 30}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.box.Cost(); got != tt.want {
				t.Errorf("Cost() = %d, want %d (area %.1f sq deg)",
					got, tt.want, tt.box.AreaSquareDegrees())
			}
		})
	}
}

// TestPacingSpreadsQuotaAcrossTheDay is the core guarantee: a full allowance at
// the start of a window is spread evenly rather than burned up front.
func TestPacingSpreadsQuotaAcrossTheDay(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaRegistered, WithClock(clock.now))

	// 4000 credits at 4 per global call is 1000 calls across 24h.
	want := 24 * time.Hour / 1000 // 86.4s
	if got := b.NextInterval(4); got != want {
		t.Fatalf("interval at start of window = %s, want %s", got, want)
	}
}

// TestPacingAcceleratesAfterIdleTime proves the budgeter reclaims credits it
// did not spend, the behaviour that makes a crashed run self-healing.
func TestPacingAcceleratesAfterIdleTime(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaRegistered, WithClock(clock.now))
	before := b.NextInterval(4)

	// Lose half the day without spending anything.
	clock.advance(12 * time.Hour)
	after := b.NextInterval(4)

	if after >= before {
		t.Errorf("interval after idle = %s, want faster than %s", after, before)
	}
	// 4000 credits over the remaining 12h is 1000 calls: half the spacing.
	if want := 12 * time.Hour / 1000; after != want {
		t.Errorf("interval after idle = %s, want %s", after, want)
	}
}

// TestPacingDeceleratesAsCreditsRunLow is the mirror case.
func TestPacingDeceleratesAsCreditsRunLow(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaRegistered, WithClock(clock.now))
	before := b.NextInterval(4)

	// Burn 90% of the allowance in the first hour.
	for range 900 {
		if !b.Spend(4) {
			t.Fatal("Spend rejected a call that should have been affordable")
		}
	}
	clock.advance(time.Hour)

	if after := b.NextInterval(4); after <= before {
		t.Errorf("interval after heavy spend = %s, want slower than %s", after, before)
	}
}

func TestIntervalNeverBeatsDataResolution(t *testing.T) {
	clock := &fakeClock{t: midnight}
	// A large allowance with almost no time left would otherwise imply a
	// sub-second interval, which cannot surface new data.
	b := New(QuotaFeeder, WithClock(clock.now))
	clock.advance(24*time.Hour - time.Second)

	if got := b.NextInterval(1); got < ResolutionRegistered {
		t.Errorf("interval = %s, want at least the %s data resolution",
			got, ResolutionRegistered)
	}
}

func TestIntervalIsCappedWhenCreditsAreScarce(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaRegistered, WithClock(clock.now))

	// Leave a single global call's worth of credit for the whole day.
	for range 999 {
		b.Spend(4)
	}
	if got := b.NextInterval(4); got != maxInterval {
		t.Errorf("interval = %s, want the %s cap", got, maxInterval)
	}
}

func TestSpendRefusesWhenExhausted(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaAnonymous, WithClock(clock.now))

	for i := range 100 {
		if !b.Spend(4) {
			t.Fatalf("Spend refused call %d, which was still within quota", i)
		}
	}
	if b.Spend(4) {
		t.Error("Spend allowed a call past the quota")
	}
	if got := b.Remaining(); got != 0 {
		t.Errorf("Remaining() = %d, want 0", got)
	}
}

func TestExhaustedBudgetWaitsForRefill(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaAnonymous, WithClock(clock.now))
	for range 100 {
		b.Spend(4)
	}
	clock.advance(6 * time.Hour)

	if got, want := b.NextInterval(4), 18*time.Hour; got != want {
		t.Errorf("interval when exhausted = %s, want %s (time to refill)", got, want)
	}
}

func TestQuotaRefillsAtUTCMidnight(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaAnonymous, WithClock(clock.now))
	for range 100 {
		b.Spend(4)
	}
	if b.Remaining() != 0 {
		t.Fatal("precondition failed: budget should be exhausted")
	}

	clock.advance(24 * time.Hour)

	if got, want := b.Remaining(), int(QuotaAnonymous); got != want {
		t.Errorf("Remaining() after refill = %d, want %d", got, want)
	}
	if !b.Spend(4) {
		t.Error("Spend refused a call after the quota refilled")
	}
}

// TestPenalizeHonoursServerBackoff covers the case where the server's
// accounting disagrees with ours, unavoidable on a shared anonymous IP bucket.
func TestPenalizeHonoursServerBackoff(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaRegistered, WithClock(clock.now))

	b.Penalize(90 * time.Second)

	if b.Spend(4) {
		t.Error("Spend proceeded while blocked by a 429 penalty")
	}
	if got, want := b.NextInterval(4), 90*time.Second; got != want {
		t.Errorf("interval while penalized = %s, want %s", got, want)
	}

	clock.advance(91 * time.Second)

	if !b.Spend(4) {
		t.Error("Spend still blocked after the penalty expired")
	}
}

func TestPenalizeWithoutRetryAfterUsesDefault(t *testing.T) {
	clock := &fakeClock{t: midnight}
	b := New(QuotaRegistered, WithClock(clock.now))

	// A 429 that omits X-Rate-Limit-Retry-After-Seconds still has to back off.
	b.Penalize(0)

	if b.Spend(4) {
		t.Error("Spend proceeded after a 429 with no retry-after hint")
	}
}

func TestAnonymousQuotaUsesCoarserResolution(t *testing.T) {
	if got := New(QuotaAnonymous).resolution; got != ResolutionAnonymous {
		t.Errorf("anonymous resolution = %s, want %s", got, ResolutionAnonymous)
	}
	if got := New(QuotaRegistered).resolution; got != ResolutionRegistered {
		t.Errorf("registered resolution = %s, want %s", got, ResolutionRegistered)
	}
}
