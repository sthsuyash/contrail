package main

import (
	"fmt"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
)

// pacer decides how fast the poll loop may run and whether a poll is
// affordable at all.
//
// Separating this from the poll loop keeps a distinction the first version of
// this program got wrong: credit accounting describes the *live API*, not
// ingestion in general. Replay reads local files and spends nothing, so pacing
// it with the API's daily allowance made a six-file demo take twenty minutes
// while enforcing a limit that did not apply to it. The budget is one pacing
// policy among others, not a property of the loop.
type pacer interface {
	// Spend reserves the cost of one poll, reporting whether it was affordable.
	Spend(cost int) bool
	// NextInterval is how long to wait before the next poll of this cost.
	NextInterval(cost int) time.Duration
	// Penalize applies a server-instructed backoff.
	Penalize(retryAfter time.Duration)
	// Remaining reports spending capacity left, for logs.
	Remaining() int
	// StatusLine renders the pacer's state for logs.
	StatusLine() string
}

// budget.Budget is the live-API pacer.
var _ pacer = (*budget.Budget)(nil)

// fixedPacer polls at a constant interval with no spending limit. It backs
// replay mode, where the only reason to pause between polls is to make the
// output readable rather than to conserve anything.
type fixedPacer struct {
	interval time.Duration
}

func newFixedPacer(interval time.Duration) *fixedPacer {
	if interval < 0 {
		interval = 0
	}
	return &fixedPacer{interval: interval}
}

func (p *fixedPacer) Spend(int) bool                 { return true }
func (p *fixedPacer) NextInterval(int) time.Duration { return p.interval }
func (p *fixedPacer) Penalize(time.Duration)         {}
func (p *fixedPacer) Remaining() int                 { return -1 }
func (p *fixedPacer) StatusLine() string {
	return fmt.Sprintf("unmetered, %s between polls", p.interval)
}
