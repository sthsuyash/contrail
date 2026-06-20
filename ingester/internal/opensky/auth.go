package opensky

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// TokenURL is the OpenSky Keycloak token endpoint.
//
// Basic authentication with a username and password was removed on
// 18 March 2026; the OAuth2 client credentials flow is now the only supported
// mechanism. Credentials are issued per API client from the account page, not
// from the login itself.
const TokenURL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

// Credentials are the client_id and client_secret of an OpenSky API client.
// The zero value selects anonymous access, which still works but drops the
// daily allowance to 400 credits and coarsens data resolution to 10 seconds.
type Credentials struct {
	ClientID     string
	ClientSecret string
}

// IsAnonymous reports whether no usable credentials were supplied.
func (c Credentials) IsAnonymous() bool {
	return c.ClientID == "" || c.ClientSecret == ""
}

// tokenManager caches a bearer token and refreshes it on demand.
//
// Tokens live 30 minutes, but expiry is not the only way one dies: the server
// may invalidate a token early, and the only signal is a 401 on a request that
// the local clock still considers valid. Expiry-based refresh alone therefore
// leaves a window where every request fails until the cached token times out.
// invalidate closes that window by letting the transport discard a token the
// server has rejected and immediately fetch another.
type tokenManager struct {
	cfg clientcredentials.Config

	mu    sync.Mutex
	token *oauth2.Token
}

// get returns a valid access token, fetching one if the cache is empty or stale.
func (m *tokenManager) get(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.token.Valid() {
		return m.token.AccessToken, nil
	}
	token, err := m.cfg.Token(ctx)
	if err != nil {
		return "", fmt.Errorf("fetching OpenSky access token: %w", err)
	}
	m.token = token
	return token.AccessToken, nil
}

// invalidate drops the cached token so the next get fetches a fresh one.
func (m *tokenManager) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.token = nil
}

// authTransport attaches a bearer token to every request and retries once when
// the server rejects it.
type authTransport struct {
	base    http.RoundTripper
	manager *tokenManager
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// RoundTrippers must not mutate the request they are given.
	authed := req.Clone(req.Context())

	token, err := t.manager.get(req.Context())
	if err != nil {
		return nil, err
	}
	authed.Header.Set("Authorization", "Bearer "+token)

	resp, err := t.base.RoundTrip(authed)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}

	// The token was rejected despite looking valid locally. Discard it and try
	// once more; a second 401 is a genuine credentials problem, not staleness,
	// so it is returned to the caller rather than retried again.
	resp.Body.Close()
	t.manager.invalidate()

	retry := req.Clone(req.Context())
	token, err = t.manager.get(req.Context())
	if err != nil {
		return nil, err
	}
	retry.Header.Set("Authorization", "Bearer "+token)
	return t.base.RoundTrip(retry)
}

// newAuthedClient wraps an HTTP client with OpenSky bearer authentication.
// Anonymous credentials yield the client unchanged.
func newAuthedClient(base *http.Client, creds Credentials) *http.Client {
	if creds.IsAnonymous() {
		return base
	}

	transport := base.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	authed := *base
	authed.Transport = &authTransport{
		base: transport,
		manager: &tokenManager{
			cfg: clientcredentials.Config{
				ClientID:     creds.ClientID,
				ClientSecret: creds.ClientSecret,
				TokenURL:     TokenURL,
			},
		},
	}
	return &authed
}
