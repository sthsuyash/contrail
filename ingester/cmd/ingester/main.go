// Command ingester polls the OpenSky REST API and publishes state vectors.
//
// With no environment set it runs against the bundled fixtures and prints to
// stdout, so a fresh clone produces visible output immediately. Pointing it at
// the live API and a broker is a matter of environment variables; nothing in
// the poll loop changes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
	"github.com/sthsuyash/contrail/ingester/internal/config"
	"github.com/sthsuyash/contrail/ingester/internal/opensky"
	"github.com/sthsuyash/contrail/ingester/internal/scheduler"
	"github.com/sthsuyash/contrail/ingester/internal/sink"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ingester failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Cancel on SIGINT/SIGTERM so an interrupted run flushes buffered records
	// rather than losing the poll in flight.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	source, err := buildSource(cfg)
	if err != nil {
		return err
	}
	destination, err := buildSink(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := destination.Close(); err != nil {
			logger.Error("closing sink", "error", err)
		}
	}()

	sched, err := scheduler.New(cfg.Regions)
	if err != nil {
		return err
	}
	pace := buildPacer(cfg)

	logger.Info("ingester starting",
		"config", cfg.Describe(),
		"source", source.Describe(),
		"sink", destination.Describe(),
		"pacing", pace.StatusLine(),
	)

	return poll(ctx, cfg, source, destination, sched, pace, logger)
}

// buildPacer selects the pacing policy for the configured source. Only the
// live API is metered; replay reads local files and spends nothing.
func buildPacer(cfg *config.Config) pacer {
	if cfg.Source == config.SourceReplay {
		return newFixedPacer(cfg.ReplayInterval)
	}
	return budget.New(cfg.Quota)
}

func buildSource(cfg *config.Config) (opensky.Source, error) {
	switch cfg.Source {
	case config.SourceReplay:
		replay, err := opensky.NewReplay(
			os.DirFS(cfg.FixturesDir), "states-*.json",
			opensky.WithLoop(cfg.ReplayLoop),
			opensky.WithTimeShift(cfg.ReplayShift),
		)
		if err != nil {
			return nil, fmt.Errorf("replay source: %w (set CONTRAIL_FIXTURES_DIR or run `make record-fixtures`)", err)
		}
		return replay, nil
	case config.SourceLive:
		return opensky.NewClient(opensky.Credentials{
			ClientID:     cfg.Credentials.ClientID,
			ClientSecret: cfg.Credentials.ClientSecret,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported source %q", cfg.Source)
	}
}

func buildSink(cfg *config.Config) (sink.Sink, error) {
	switch cfg.Sink {
	case config.SinkStdout:
		return sink.NewStdout(os.Stdout), nil
	case config.SinkKafka:
		return sink.NewKafka(cfg.KafkaBrokers, cfg.KafkaTopic)
	default:
		return nil, fmt.Errorf("unsupported sink %q", cfg.Sink)
	}
}

// poll runs the ingestion loop until the context is cancelled.
func poll(
	ctx context.Context,
	cfg *config.Config,
	source opensky.Source,
	destination sink.Sink,
	sched *scheduler.Scheduler,
	credits pacer,
	logger *slog.Logger,
) error {
	var polls, written int

	for {
		region, cost := sched.Next()

		if !credits.Spend(cost) {
			wait := credits.NextInterval(cost)
			logger.Warn("credit allowance exhausted, waiting for refill",
				"region", region.Name, "cost", cost,
				"wait", wait.Round(time.Second).String(), "status", credits.StatusLine())
			if err := sleep(ctx, wait); err != nil {
				return summarize(logger, polls, written, sched, credits, nil)
			}
			continue
		}

		fetchCtx, cancel := context.WithTimeout(ctx, cfg.PollTimeout)
		resp, err := source.Fetch(fetchCtx, region.Box)
		cancel()

		switch {
		case errors.Is(err, opensky.ErrReplayExhausted):
			logger.Info("replay fixtures exhausted, stopping")
			return summarize(logger, polls, written, sched, credits, nil)

		case ctx.Err() != nil:
			return summarize(logger, polls, written, sched, credits, nil)

		case err != nil:
			// A rate limit is the server correcting our local estimate, so it
			// updates the budget rather than being treated as a transient
			// failure to retry blindly.
			if retryAfter, ok := opensky.IsRateLimit(err); ok {
				credits.Penalize(retryAfter)
				logger.Warn("rate limited by server",
					"region", region.Name, "retry_after", retryAfter.String())
				continue
			}
			// Other failures cost a credit we cannot reclaim, but must not stop
			// ingestion: a single bad poll is not worth losing the run over.
			logger.Error("poll failed", "region", region.Name, "error", err)
			if err := sleep(ctx, credits.NextInterval(cost)); err != nil {
				return summarize(logger, polls, written, sched, credits, nil)
			}
			continue
		}

		polls++
		sched.Observe(region.Name, len(resp.States))

		records := make([]sink.Record, 0, len(resp.States))
		now := time.Now().UTC()
		for i := range resp.States {
			records = append(records, sink.NewRecord(&resp.States[i], region.Name, resp.Time, now))
		}

		if len(records) > 0 {
			if err := destination.Write(ctx, records); err != nil {
				if ctx.Err() != nil {
					return summarize(logger, polls, written, sched, credits, nil)
				}
				// The credit is already spent and the data is already fetched;
				// dropping the batch loses it permanently, so this is logged
				// loudly rather than swallowed.
				logger.Error("writing batch", "region", region.Name,
					"records", len(records), "error", err)
			} else {
				written += len(records)
			}
		}

		wait := credits.NextInterval(cost)
		if cfg.LogEveryPoll {
			attrs := []any{
				"region", region.Name, "cost", cost,
				"vectors", len(resp.States),
				// slog renders time.Duration as its integer nanosecond count in
				// JSON, which is unreadable in a log line; format it here.
				"next_in", wait.Round(time.Millisecond).String(),
			}
			// An unmetered pacer reports -1; surfacing that sentinel as a
			// credit balance would be actively misleading in the logs.
			if remaining := credits.Remaining(); remaining >= 0 {
				attrs = append(attrs, "credits_left", remaining)
			}
			logger.Info("polled", attrs...)
		}

		if err := sleep(ctx, wait); err != nil {
			return summarize(logger, polls, written, sched, credits, nil)
		}
	}
}

// sleep waits for d, returning early if the context is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// summarize logs the run's totals and the scheduler's learned yields, which is
// the evidence that cost-aware ranking actually did something.
func summarize(
	logger *slog.Logger,
	polls, written int,
	sched *scheduler.Scheduler,
	credits pacer,
	err error,
) error {
	for _, stat := range sched.Stats() {
		if stat.Polls == 0 {
			continue
		}
		logger.Info("region summary",
			"region", stat.Name, "polls", stat.Polls, "cost", stat.Cost,
			"avg_vectors", fmt.Sprintf("%.0f", stat.Yield),
			"vectors_per_credit", fmt.Sprintf("%.0f", stat.YieldPerCredit))
	}
	logger.Info("ingester stopped",
		"polls", polls, "records", written, "pacing", credits.StatusLine())
	return err
}
