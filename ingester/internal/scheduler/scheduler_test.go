package scheduler

import (
	"testing"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
	"github.com/sthsuyash/contrail/ingester/internal/config"
)

var start = time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// cheapDense costs 1 credit; broadSparse costs 3. Their relative yield is what
// the scheduler has to discover.
var (
	cheapDense  = config.Region{Name: "london", Box: budget.BoundingBox{LatMin: 50.5, LonMin: -1.5, LatMax: 52.5, LonMax: 1.5}}
	alsoCheap   = config.Region{Name: "paris", Box: budget.BoundingBox{LatMin: 48, LonMin: 1.5, LatMax: 49.5, LonMax: 3.5}}
	broadSparse = config.Region{Name: "nordics", Box: budget.BoundingBox{LatMin: 55, LonMin: 8, LatMax: 66, LonMax: 26}}
)

func newTestScheduler(t *testing.T, clock *fakeClock, regions ...config.Region) *Scheduler {
	t.Helper()
	s, err := New(regions, WithClock(clock.now))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestNewRejectsEmptyRegionList(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Error("New succeeded with no regions, want an error")
	}
}

func TestCostsComeFromBoxArea(t *testing.T) {
	if got := cheapDense.Cost(); got != 1 {
		t.Errorf("london cost = %d, want 1", got)
	}
	if got := broadSparse.Cost(); got != 3 {
		t.Errorf("nordics cost = %d, want 3", got)
	}
}

// Before any measurements exist, the scheduler must sample every region rather
// than committing to whichever it happened to try first.
func TestFirstPassVisitsEveryRegion(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, alsoCheap, broadSparse)

	seen := map[string]bool{}
	for range 3 {
		clock.advance(time.Second)
		region, _ := s.Next()
		if seen[region.Name] {
			t.Fatalf("region %q polled twice before every region was sampled", region.Name)
		}
		seen[region.Name] = true
		s.Observe(region.Name, 100)
	}
	if len(seen) != 3 {
		t.Errorf("sampled %d regions on the first pass, want 3", len(seen))
	}
}

// The core behaviour: once yields are known, spending concentrates on the
// region returning the most aircraft per credit.
func TestSchedulerFavoursYieldPerCredit(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, broadSparse)

	yields := map[string]int{
		cheapDense.Name:  600, // 600 aircraft for 1 credit
		broadSparse.Name: 90,  // 90 aircraft for 3 credits
	}

	counts := map[string]int{}
	for range 200 {
		clock.advance(10 * time.Second)
		region, _ := s.Next()
		counts[region.Name]++
		s.Observe(region.Name, yields[region.Name])
	}

	if counts[cheapDense.Name] <= counts[broadSparse.Name] {
		t.Errorf("polls: %s=%d %s=%d; want the denser, cheaper region polled more",
			cheapDense.Name, counts[cheapDense.Name],
			broadSparse.Name, counts[broadSparse.Name])
	}

	// 600/1 against 90/3 is a 20:1 ratio in yield per credit. Staleness
	// weighting deliberately tempers that, so the split should be lopsided but
	// nowhere near total.
	ratio := float64(counts[cheapDense.Name]) / float64(counts[broadSparse.Name])
	if ratio < 2 {
		t.Errorf("poll ratio = %.1f, want the high-yield region clearly preferred", ratio)
	}
}

// Cost must actually matter: two regions returning identical aircraft counts
// should be separated by price alone.
func TestEqualYieldIsBrokenByCost(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, broadSparse)

	counts := map[string]int{}
	for range 100 {
		clock.advance(10 * time.Second)
		region, _ := s.Next()
		counts[region.Name]++
		s.Observe(region.Name, 200) // same yield regardless of region
	}

	if counts[cheapDense.Name] <= counts[broadSparse.Name] {
		t.Errorf("polls: 1-credit=%d 3-credit=%d; want the cheaper region preferred at equal yield",
			counts[cheapDense.Name], counts[broadSparse.Name])
	}
}

// The guarantee that matters is not that a quiet region is polled eventually,
// but that the gap between its polls stays short enough for the data to be
// usable. "Eventually" can mean once every six hours, which costs credits and
// yields nothing reconstructable.
func TestCoverageGapStaysBounded(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, broadSparse)

	yields := map[string]int{cheapDense.Name: 800, broadSparse.Name: 5}

	counts := map[string]int{}
	var lastSparsePoll = start
	var worstGap time.Duration

	for range 2000 {
		clock.advance(10 * time.Second)
		region, _ := s.Next()
		counts[region.Name]++
		s.Observe(region.Name, yields[region.Name])

		if region.Name == broadSparse.Name {
			if gap := clock.t.Sub(lastSparsePoll); gap > worstGap {
				worstGap = gap
			}
			lastSparsePoll = clock.t
		}
	}

	if counts[broadSparse.Name] == 0 {
		t.Fatal("the low-yield region was never polled")
	}

	// One poll interval of slack: the ceiling is checked before a poll, so the
	// gap can exceed it by at most the time between consecutive polls.
	limit := DefaultMaxStaleness + 10*time.Second
	if worstGap > limit {
		t.Errorf("worst coverage gap = %s, want at most %s", worstGap, limit)
	}
	t.Logf("polls: %s=%d %s=%d, worst gap for the quiet region = %s",
		cheapDense.Name, counts[cheapDense.Name],
		broadSparse.Name, counts[broadSparse.Name], worstGap)
}

// Disabling the ceiling must restore pure economic ranking, and demonstrates
// the blind spot the ceiling exists to prevent.
func TestWithoutCeilingQuietRegionsAreEffectivelyAbandoned(t *testing.T) {
	clock := &fakeClock{t: start}
	s, err := New([]config.Region{cheapDense, broadSparse},
		WithClock(clock.now), WithMaxStaleness(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	yields := map[string]int{cheapDense.Name: 800, broadSparse.Name: 5}
	counts := map[string]int{}
	for range 500 {
		clock.advance(10 * time.Second)
		region, _ := s.Next()
		counts[region.Name]++
		s.Observe(region.Name, yields[region.Name])
	}

	if counts[broadSparse.Name] > counts[cheapDense.Name]/10 {
		t.Errorf("unbounded ranking polled the quiet region %d times against %d; "+
			"expected it to be crowded out",
			counts[broadSparse.Name], counts[cheapDense.Name])
	}
	t.Logf("without a ceiling: %s=%d %s=%d",
		cheapDense.Name, counts[cheapDense.Name],
		broadSparse.Name, counts[broadSparse.Name])
}

// The ceiling must not fire while every region is being polled comfortably
// often, or it would flatten the ranking into round-robin.
func TestCeilingDoesNotOverrideHealthyRotation(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, alsoCheap)

	counts := map[string]int{}
	for range 100 {
		clock.advance(time.Second) // fast polling: nothing ever goes stale
		region, _ := s.Next()
		counts[region.Name]++
		if region.Name == cheapDense.Name {
			s.Observe(region.Name, 800)
		} else {
			s.Observe(region.Name, 80)
		}
	}

	if counts[cheapDense.Name] <= counts[alsoCheap.Name] {
		t.Errorf("polls: %s=%d %s=%d; the ceiling flattened the ranking",
			cheapDense.Name, counts[cheapDense.Name],
			alsoCheap.Name, counts[alsoCheap.Name])
	}
}

// A region whose traffic dies off should lose priority as the EWMA catches up.
func TestSchedulerAdaptsWhenYieldChanges(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, alsoCheap)

	// Establish london as the busy one.
	for range 50 {
		clock.advance(10 * time.Second)
		region, _ := s.Next()
		if region.Name == cheapDense.Name {
			s.Observe(region.Name, 800)
		} else {
			s.Observe(region.Name, 100)
		}
	}

	// Traffic collapses over london and moves to paris.
	counts := map[string]int{}
	for range 200 {
		clock.advance(10 * time.Second)
		region, _ := s.Next()
		counts[region.Name]++
		if region.Name == cheapDense.Name {
			s.Observe(region.Name, 20)
		} else {
			s.Observe(region.Name, 800)
		}
	}

	if counts[alsoCheap.Name] <= counts[cheapDense.Name] {
		t.Errorf("polls after the shift: %s=%d %s=%d; want the newly busy region preferred",
			alsoCheap.Name, counts[alsoCheap.Name],
			cheapDense.Name, counts[cheapDense.Name])
	}
}

// The optimistic seed must be discarded outright on first measurement, not
// blended, or a genuinely quiet region keeps an inflated score for many polls.
func TestFirstObservationReplacesOptimisticSeed(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense)

	s.Observe(cheapDense.Name, 12)

	stats := s.Stats()
	if len(stats) != 1 {
		t.Fatalf("Stats() = %d entries, want 1", len(stats))
	}
	if stats[0].Yield != 12 {
		t.Errorf("yield after first observation = %.1f, want exactly 12 (seed discarded)", stats[0].Yield)
	}
}

func TestObserveIgnoresUnknownRegion(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense)

	s.Observe("atlantis", 999) // must not panic or corrupt state

	if got := s.Stats()[0].Yield; got != optimisticYield {
		t.Errorf("known region's yield = %.1f, want the untouched seed %.1f", got, optimisticYield)
	}
}

func TestStatsReportYieldPerCredit(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, cheapDense, broadSparse)

	s.Observe(cheapDense.Name, 600)
	s.Observe(broadSparse.Name, 90)

	byName := map[string]Stat{}
	for _, st := range s.Stats() {
		byName[st.Name] = st
	}

	if got, want := byName[cheapDense.Name].YieldPerCredit, 600.0; got != want {
		t.Errorf("%s yield/credit = %.1f, want %.1f", cheapDense.Name, got, want)
	}
	if got, want := byName[broadSparse.Name].YieldPerCredit, 30.0; got != want {
		t.Errorf("%s yield/credit = %.1f, want %.1f", broadSparse.Name, got, want)
	}
}

func TestNextReportsRegionCost(t *testing.T) {
	clock := &fakeClock{t: start}
	s := newTestScheduler(t, clock, broadSparse)

	region, cost := s.Next()
	if region.Name != broadSparse.Name {
		t.Fatalf("Next() region = %q, want %q", region.Name, broadSparse.Name)
	}
	if cost != 3 {
		t.Errorf("Next() cost = %d, want 3", cost)
	}
}
