// Package config assembles ingester settings from the environment.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
)

// Region is a named geographic window to poll.
type Region struct {
	Name string             `json:"name"`
	Box  budget.BoundingBox `json:"box"`
}

// Cost is the credit price of one poll over this region.
func (r Region) Cost() int { return r.Box.Cost() }

// DefaultRegions favours small, dense boxes over broad sparse ones.
//
// The pricing tiers make this the economically obvious choice and it is worth
// stating plainly: a 6 sq° window over London costs 1 credit and typically
// returns several hundred aircraft, while a 400 sq° window over open ocean
// costs 3 and returns almost nothing. Coverage measured in square degrees is
// not the same as coverage measured in aircraft, and the budget only rewards
// the second. The three broader regions are kept so the scheduler has genuinely
// different cost tiers to weigh against each other.
var DefaultRegions = []Region{
	{"benelux", budget.BoundingBox{LatMin: 50.0, LonMin: 3.0, LatMax: 53.5, LonMax: 7.5}},
	{"london", budget.BoundingBox{LatMin: 50.5, LonMin: -1.5, LatMax: 52.5, LonMax: 1.5}},
	{"paris", budget.BoundingBox{LatMin: 48.0, LonMin: 1.5, LatMax: 49.5, LonMax: 3.5}},
	{"rhine-alps", budget.BoundingBox{LatMin: 47.5, LonMin: 7.5, LatMax: 51.0, LonMax: 12.5}},
	{"swiss-alps", budget.BoundingBox{LatMin: 45.5, LonMin: 6.0, LatMax: 47.5, LonMax: 11.0}},
	{"iberia", budget.BoundingBox{LatMin: 36.0, LonMin: -10.0, LatMax: 44.0, LonMax: 3.0}},
	{"nordics", budget.BoundingBox{LatMin: 55.0, LonMin: 8.0, LatMax: 66.0, LonMax: 26.0}},
}

// SourceKind selects where state vectors come from.
type SourceKind string

const (
	// SourceLive calls the OpenSky REST API.
	SourceLive SourceKind = "live"
	// SourceReplay serves recorded fixtures, requiring no credentials.
	SourceReplay SourceKind = "replay"
)

// SinkKind selects where state vectors are written.
type SinkKind string

const (
	// SinkKafka produces to Redpanda or any Kafka-compatible broker.
	SinkKafka SinkKind = "kafka"
	// SinkStdout prints newline-delimited JSON, for running without infra.
	SinkStdout SinkKind = "stdout"
)

// Config is the fully resolved ingester configuration.
type Config struct {
	Source      SourceKind
	Credentials Credentials
	Regions     []Region
	Quota       budget.Quota

	Sink         SinkKind
	KafkaBrokers []string
	KafkaTopic   string

	FixturesDir string
	ReplayLoop  bool
	ReplayShift bool
	// ReplayInterval paces replay mode. Replay reads local files and consumes
	// no credits, so it is deliberately not governed by the credit budget.
	// This exists only to keep streamed output legible.
	ReplayInterval time.Duration
	PollTimeout    time.Duration
	LogEveryPoll   bool
}

// Credentials mirrors the OpenSky client credentials, kept here so the config
// package does not depend on the API client.
type Credentials struct {
	ClientID     string
	ClientSecret string
}

// IsAnonymous reports whether credentials are unusable as supplied.
func (c Credentials) IsAnonymous() bool {
	return c.ClientID == "" || c.ClientSecret == ""
}

// Load reads configuration from the environment, applying defaults.
//
// Every setting has a working default, so `go run ./cmd/ingester` with no
// environment at all starts in replay mode against the bundled fixtures and
// prints to stdout. That is deliberate: the first run should succeed for
// someone who has just cloned the repo, before they have credentials, a
// broker, or Docker.
func Load() (*Config, error) {
	cfg := &Config{
		Source:         SourceKind(envOr("CONTRAIL_SOURCE", string(SourceReplay))),
		Sink:           SinkKind(envOr("CONTRAIL_SINK", string(SinkStdout))),
		KafkaTopic:     envOr("CONTRAIL_KAFKA_TOPIC", "opensky.states.raw"),
		FixturesDir:    envOr("CONTRAIL_FIXTURES_DIR", "fixtures"),
		PollTimeout:    90 * time.Second,
		ReplayInterval: time.Second,
		ReplayLoop:     envBool("CONTRAIL_REPLAY_LOOP", true),
		ReplayShift:    envBool("CONTRAIL_REPLAY_TIME_SHIFT", true),
		LogEveryPoll:   envBool("CONTRAIL_LOG_EVERY_POLL", true),
		Credentials: Credentials{
			ClientID:     os.Getenv("CONTRAIL_OPENSKY_CLIENT_ID"),
			ClientSecret: os.Getenv("CONTRAIL_OPENSKY_CLIENT_SECRET"),
		},
	}

	if brokers := envOr("CONTRAIL_KAFKA_BROKERS", "localhost:19092"); brokers != "" {
		cfg.KafkaBrokers = splitAndTrim(brokers)
	}

	switch cfg.Source {
	case SourceLive, SourceReplay:
	default:
		return nil, fmt.Errorf("CONTRAIL_SOURCE=%q, want %q or %q",
			cfg.Source, SourceLive, SourceReplay)
	}
	switch cfg.Sink {
	case SinkKafka, SinkStdout:
	default:
		return nil, fmt.Errorf("CONTRAIL_SINK=%q, want %q or %q",
			cfg.Sink, SinkKafka, SinkStdout)
	}

	regions, err := loadRegions(os.Getenv("CONTRAIL_REGIONS"))
	if err != nil {
		return nil, err
	}
	cfg.Regions = regions

	quota, err := resolveQuota(os.Getenv("CONTRAIL_QUOTA"), cfg.Credentials)
	if err != nil {
		return nil, err
	}
	cfg.Quota = quota

	if timeout := os.Getenv("CONTRAIL_POLL_TIMEOUT"); timeout != "" {
		d, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("CONTRAIL_POLL_TIMEOUT=%q: %w", timeout, err)
		}
		cfg.PollTimeout = d
	}

	if interval := os.Getenv("CONTRAIL_REPLAY_INTERVAL"); interval != "" {
		d, err := time.ParseDuration(interval)
		if err != nil {
			return nil, fmt.Errorf("CONTRAIL_REPLAY_INTERVAL=%q: %w", interval, err)
		}
		cfg.ReplayInterval = d
	}

	return cfg, nil
}

// loadRegions parses a JSON region list, falling back to the defaults.
func loadRegions(raw string) ([]Region, error) {
	if strings.TrimSpace(raw) == "" {
		return DefaultRegions, nil
	}
	var regions []Region
	if err := json.Unmarshal([]byte(raw), &regions); err != nil {
		return nil, fmt.Errorf("CONTRAIL_REGIONS is not valid JSON: %w", err)
	}
	if len(regions) == 0 {
		return nil, fmt.Errorf("CONTRAIL_REGIONS is empty; omit it to use the defaults")
	}
	for i, r := range regions {
		if r.Name == "" {
			return nil, fmt.Errorf("region %d has no name", i)
		}
		if err := validateBox(r); err != nil {
			return nil, err
		}
	}
	return regions, nil
}

// validateBox rejects inverted or out-of-range windows. A silently inverted box
// returns an empty result forever, which is far harder to diagnose than a
// startup error.
func validateBox(r Region) error {
	b := r.Box
	if b.IsGlobal() {
		return nil
	}
	if b.LatMin >= b.LatMax || b.LonMin >= b.LonMax {
		return fmt.Errorf("region %q has an inverted box: lat %g..%g lon %g..%g",
			r.Name, b.LatMin, b.LatMax, b.LonMin, b.LonMax)
	}
	if b.LatMin < -90 || b.LatMax > 90 {
		return fmt.Errorf("region %q latitude %g..%g out of range", r.Name, b.LatMin, b.LatMax)
	}
	if b.LonMin < -180 || b.LonMax > 180 {
		return fmt.Errorf("region %q longitude %g..%g out of range", r.Name, b.LonMin, b.LonMax)
	}
	return nil
}

// resolveQuota picks the daily allowance, inferring it from whether credentials
// were supplied unless it is set explicitly. Feeder accounts get 8000 but there
// is no way to detect that from the client side, so it must be declared.
func resolveQuota(raw string, creds Credentials) (budget.Quota, error) {
	if strings.TrimSpace(raw) == "" || raw == "auto" {
		if creds.IsAnonymous() {
			return budget.QuotaAnonymous, nil
		}
		return budget.QuotaRegistered, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("CONTRAIL_QUOTA=%q, want a number or \"auto\"", raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("CONTRAIL_QUOTA=%d, want a positive allowance", n)
	}
	return budget.Quota(n), nil
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// Describe renders the resolved configuration for the startup log.
func (c *Config) Describe() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "source=%s sink=%s quota=%d regions=%d",
		c.Source, c.Sink, c.Quota, len(c.Regions))
	if c.Source == SourceLive && c.Credentials.IsAnonymous() {
		sb.WriteString(" (anonymous)")
	}
	return sb.String()
}
