package opensky

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
)

// DefaultBaseURL is the OpenSky REST API root.
const DefaultBaseURL = "https://opensky-network.org/api"

// retryAfterHeader carries the server's own backoff instruction on a 429.
// It is authoritative: the local credit estimate can drift from the server's,
// especially on an anonymous IP bucket shared with other callers.
const retryAfterHeader = "X-Rate-Limit-Retry-After-Seconds"

// maxResponseBytes bounds how much of a response is read. A global query
// returns roughly 10-15k state vectors, a few megabytes of JSON; this cap sits
// far above that while preventing an unbounded read from exhausting memory.
const maxResponseBytes = 128 << 20

// RateLimitError reports an exhausted credit allowance.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("opensky: rate limited, retry after %s", e.RetryAfter)
}

// APIError reports a non-2xx response that is not a rate limit.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("opensky: HTTP %d: %s", e.StatusCode, e.Body)
}

// Source supplies state vectors. The live API and the fixture replay both
// implement it, which is what lets the whole pipeline run without credentials.
type Source interface {
	// Fetch returns the current state vectors within box. A zero box is global.
	Fetch(ctx context.Context, box budget.BoundingBox) (*StatesResponse, error)
	// Describe names the source for logs.
	Describe() string
}

// Client is a live OpenSky REST API client.
type Client struct {
	baseURL string
	http    *http.Client
	anon    bool
}

// ClientOption customises a Client.
type ClientOption func(*Client)

// WithBaseURL overrides the API root, for tests against a stub server.
func WithBaseURL(u string) ClientOption {
	return func(c *Client) { c.baseURL = u }
}

// WithHTTPClient overrides the underlying HTTP client.
func WithHTTPClient(h *http.Client) ClientOption {
	return func(c *Client) { c.http = h }
}

// NewClient builds a client. Anonymous credentials are valid and simply select
// the reduced allowance.
func NewClient(creds Credentials, opts ...ClientOption) *Client {
	c := &Client{
		baseURL: DefaultBaseURL,
		http:    &http.Client{Timeout: 60 * time.Second},
		anon:    creds.IsAnonymous(),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.http = newAuthedClient(c.http, creds)
	return c
}

// Describe implements Source.
func (c *Client) Describe() string {
	if c.anon {
		return "opensky live API (anonymous)"
	}
	return "opensky live API (authenticated)"
}

// Fetch implements Source, calling GET /states/all.
func (c *Client) Fetch(ctx context.Context, box budget.BoundingBox) (*StatesResponse, error) {
	endpoint, err := url.Parse(c.baseURL + "/states/all")
	if err != nil {
		return nil, fmt.Errorf("building request URL: %w", err)
	}

	// Omitting the bounding box entirely is what requests global coverage.
	// Sending a box that happens to span the world would be charged the same
	// 4 credits but is not equivalent to the server, so the distinction is kept.
	if !box.IsGlobal() {
		q := endpoint.Query()
		q.Set("lamin", formatDegrees(box.LatMin))
		q.Set("lomin", formatDegrees(box.LonMin))
		q.Set("lamax", formatDegrees(box.LatMax))
		q.Set("lomax", formatDegrees(box.LonMax))
		endpoint.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling /states/all: %w", err)
	}
	defer resp.Body.Close()

	body := io.LimitReader(resp.Body, maxResponseBytes)

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, &RateLimitError{RetryAfter: parseRetryAfter(resp.Header)}
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		snippet, _ := io.ReadAll(io.LimitReader(body, 512))
		return nil, &APIError{StatusCode: resp.StatusCode, Body: string(snippet)}
	}

	var states StatesResponse
	if err := json.NewDecoder(body).Decode(&states); err != nil {
		return nil, fmt.Errorf("decoding /states/all response: %w", err)
	}
	return &states, nil
}

// formatDegrees renders a coordinate without scientific notation, which the
// API rejects.
func formatDegrees(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// parseRetryAfter reads the server's backoff hint, falling back to the standard
// Retry-After header and finally to a conservative default. A 429 with an
// unreadable hint still has to back off. Treating it as "retry immediately"
// would spin against a server that has already said no.
func parseRetryAfter(h http.Header) time.Duration {
	for _, key := range []string{retryAfterHeader, "Retry-After"} {
		if raw := h.Get(key); raw != "" {
			if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
				return time.Duration(secs) * time.Second
			}
		}
	}
	return time.Minute
}

// IsRateLimit reports whether err is a rate limit, yielding the backoff.
func IsRateLimit(err error) (time.Duration, bool) {
	var rateLimit *RateLimitError
	if errors.As(err, &rateLimit) {
		return rateLimit.RetryAfter, true
	}
	return 0, false
}
