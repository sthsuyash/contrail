package opensky

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthsuyash/contrail/ingester/internal/budget"
)

func stubServer(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewClient(Credentials{}, WithBaseURL(srv.URL))
}

func TestFetchDecodesStates(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/states/all" {
			t.Errorf("path = %q, want /states/all", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"time":1754563201,"states":[` + fullVector + `]}`))
	})

	resp, err := client.Fetch(context.Background(), budget.BoundingBox{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(resp.States) != 1 {
		t.Fatalf("States = %d, want 1", len(resp.States))
	}
	if resp.States[0].ICAO24 != "4b1815" {
		t.Errorf("ICAO24 = %q, want 4b1815", resp.States[0].ICAO24)
	}
}

// A global query must omit the bounding box entirely rather than send one that
// spans the world; the server treats the two differently.
func TestGlobalFetchSendsNoBoundingBox(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			t.Errorf("query = %q, want empty for a global request", r.URL.RawQuery)
		}
		w.Write([]byte(`{"time":1,"states":[]}`))
	})

	if _, err := client.Fetch(context.Background(), budget.BoundingBox{}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestBoundedFetchSendsCoordinates(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for param, want := range map[string]string{
			"lamin": "45.5", "lomin": "5.25", "lamax": "47.5", "lomax": "10.75",
		} {
			if got := q.Get(param); got != want {
				t.Errorf("%s = %q, want %q", param, got, want)
			}
		}
		w.Write([]byte(`{"time":1,"states":[]}`))
	})

	box := budget.BoundingBox{LatMin: 45.5, LonMin: 5.25, LatMax: 47.5, LonMax: 10.75}
	if _, err := client.Fetch(context.Background(), box); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

// Coordinates must never reach the API in scientific notation, which it rejects.
func TestSmallCoordinatesAvoidScientificNotation(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("lamin"); got != "0.0000001" {
			t.Errorf("lamin = %q, want plain decimal notation", got)
		}
		w.Write([]byte(`{"time":1,"states":[]}`))
	})

	box := budget.BoundingBox{LatMin: 0.0000001, LonMin: 1, LatMax: 2, LonMax: 3}
	if _, err := client.Fetch(context.Background(), box); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestRateLimitCarriesServerBackoff(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(retryAfterHeader, "137")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	_, err := client.Fetch(context.Background(), budget.BoundingBox{})

	retryAfter, ok := IsRateLimit(err)
	if !ok {
		t.Fatalf("Fetch error = %v, want a RateLimitError", err)
	}
	if want := 137 * time.Second; retryAfter != want {
		t.Errorf("RetryAfter = %s, want %s", retryAfter, want)
	}
}

// A 429 whose hint is missing or unparseable must still back off. Retrying
// immediately would spin against a server that has already refused.
func TestRateLimitWithoutHintStillBacksOff(t *testing.T) {
	for _, tc := range []struct{ name, header, value string }{
		{"no header", "", ""},
		{"unparseable", retryAfterHeader, "soon"},
		{"non-positive", retryAfterHeader, "0"},
		{"http date form", "Retry-After", "Wed, 07 Aug 2026 12:00:00 GMT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.header != "" {
					w.Header().Set(tc.header, tc.value)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			})

			_, err := client.Fetch(context.Background(), budget.BoundingBox{})

			retryAfter, ok := IsRateLimit(err)
			if !ok {
				t.Fatalf("Fetch error = %v, want a RateLimitError", err)
			}
			if retryAfter <= 0 {
				t.Errorf("RetryAfter = %s, want a positive fallback", retryAfter)
			}
		})
	}
}

// The standard Retry-After header is honoured when OpenSky's custom one is absent.
func TestRateLimitFallsBackToStandardHeader(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "45")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	_, err := client.Fetch(context.Background(), budget.BoundingBox{})
	retryAfter, ok := IsRateLimit(err)
	if !ok {
		t.Fatalf("Fetch error = %v, want a RateLimitError", err)
	}
	if want := 45 * time.Second; retryAfter != want {
		t.Errorf("RetryAfter = %s, want %s", retryAfter, want)
	}
}

func TestAPIErrorSurfacesStatusAndBody(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "requested time is too far in the past", http.StatusBadRequest)
	})

	_, err := client.Fetch(context.Background(), budget.BoundingBox{})

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Fetch error = %v, want an APIError", err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want 400", apiErr.StatusCode)
	}
	if apiErr.Body == "" {
		t.Error("APIError.Body is empty, want the server's explanation")
	}
}

func TestMalformedJSONIsAnError(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"time":1,"states":[[`))
	})

	if _, err := client.Fetch(context.Background(), budget.BoundingBox{}); err == nil {
		t.Error("Fetch succeeded on truncated JSON, want an error")
	}
}

func TestFetchRespectsContextCancellation(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := client.Fetch(ctx, budget.BoundingBox{}); err == nil {
		t.Error("Fetch succeeded despite a cancelled context")
	}
}

// TestAuthRetriesOnceAfterRejectedToken covers the window where a token is
// still valid by the local clock but the server has already invalidated it.
func TestAuthRetriesOnceAfterRejectedToken(t *testing.T) {
	var tokensIssued, stateCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokensIssued.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "token-" + string(rune('0'+tokensIssued.Load())),
				"token_type":   "Bearer",
				"expires_in":   1800,
			})
			return
		}
		// Reject the first token as if it had been invalidated server-side.
		if stateCalls.Add(1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`{"time":1,"states":[]}`))
	}))
	defer srv.Close()

	client := NewClient(
		Credentials{ClientID: "id", ClientSecret: "secret"},
		WithBaseURL(srv.URL),
	)
	// Point the token flow at the stub rather than the real Keycloak endpoint.
	client.http.Transport.(*authTransport).manager.cfg.TokenURL = srv.URL + "/token"

	if _, err := client.Fetch(context.Background(), budget.BoundingBox{}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := stateCalls.Load(); got != 2 {
		t.Errorf("/states/all called %d times, want 2 (initial + retry)", got)
	}
	if got := tokensIssued.Load(); got != 2 {
		t.Errorf("tokens issued = %d, want 2 (the rejected one was refreshed)", got)
	}
}

// A second 401 is a credentials problem, not staleness, so it must surface
// rather than retry forever.
func TestAuthGivesUpAfterSecondRejection(t *testing.T) {
	var tokensIssued atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokensIssued.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "always-rejected",
				"token_type":   "Bearer",
				"expires_in":   1800,
			})
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	client := NewClient(
		Credentials{ClientID: "id", ClientSecret: "secret"},
		WithBaseURL(srv.URL),
	)
	client.http.Transport.(*authTransport).manager.cfg.TokenURL = srv.URL + "/token"

	_, err := client.Fetch(context.Background(), budget.BoundingBox{})

	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Fetch error = %v, want a surfaced 401", err)
	}
	if got := tokensIssued.Load(); got != 2 {
		t.Errorf("tokens issued = %d, want exactly 2 (no retry loop)", got)
	}
}

func TestAnonymousClientSendsNoAuthorization(t *testing.T) {
	client := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("Authorization = %q, want none for an anonymous client", auth)
		}
		w.Write([]byte(`{"time":1,"states":[]}`))
	})

	if _, err := client.Fetch(context.Background(), budget.BoundingBox{}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestCredentialsAnonymousDetection(t *testing.T) {
	tests := []struct {
		name  string
		creds Credentials
		want  bool
	}{
		{"empty", Credentials{}, true},
		{"id only", Credentials{ClientID: "id"}, true},
		{"secret only", Credentials{ClientSecret: "secret"}, true},
		{"both", Credentials{ClientID: "id", ClientSecret: "secret"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.creds.IsAnonymous(); got != tt.want {
				t.Errorf("IsAnonymous() = %v, want %v", got, tt.want)
			}
		})
	}
}
