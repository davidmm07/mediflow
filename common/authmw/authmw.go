// Package authmw implements stateless, distributed authentication for every
// MediFlow service: each service verifies Keycloak-issued JWTs locally
// against the realm's JWKS (no shared session store, no calls back to
// Keycloak on the hot path) and exposes the decoded claims + realm roles to
// handlers via context. This is the "distributed auth" building block used
// by the gateway and by every backend service that wants defense in depth.
package authmw

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

type ctxKey string

const claimsCtxKey ctxKey = "mediflow.claims"

// Claims is the subset of a Keycloak access token payload MediFlow services
// care about.
type Claims struct {
	Subject           string   `json:"sub"`
	PreferredUsername string   `json:"preferred_username"`
	Email             string   `json:"email"`
	RealmRoles        []string `json:"-"`
}

// HasRole reports whether the token carries the given realm role.
func (c Claims) HasRole(role string) bool {
	for _, r := range c.RealmRoles {
		if r == role {
			return true
		}
	}
	return false
}

// Verifier validates access tokens against a Keycloak realm's JWKS endpoint,
// caching the key set and refreshing it in the background.
type Verifier struct {
	issuer  string
	cache   *jwk.Cache
	jwksURL string
}

// NewVerifier builds a Verifier for the given Keycloak issuer URL, e.g.
// "http://keycloak:8080/realms/mediflow". It eagerly registers the JWKS
// endpoint with a background auto-refreshing cache.
func NewVerifier(ctx context.Context, issuer string) (*Verifier, error) {
	jwksURL := strings.TrimRight(issuer, "/") + "/protocol/openid-connect/certs"

	cache := jwk.NewCache(ctx)
	if err := cache.Register(jwksURL, jwk.WithMinRefreshInterval(10*time.Minute)); err != nil {
		return nil, fmt.Errorf("authmw: register jwks: %w", err)
	}
	if _, err := cache.Refresh(ctx, jwksURL); err != nil {
		return nil, fmt.Errorf("authmw: initial jwks fetch: %w", err)
	}

	return &Verifier{issuer: issuer, cache: cache, jwksURL: jwksURL}, nil
}

// Verify parses and validates a raw bearer token, returning the extracted
// claims on success.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Claims, error) {
	keySet, err := v.cache.Get(ctx, v.jwksURL)
	if err != nil {
		return Claims{}, fmt.Errorf("authmw: fetch jwks: %w", err)
	}

	token, err := jwt.Parse([]byte(rawToken),
		jwt.WithKeySet(keySet),
		jwt.WithValidate(true),
		jwt.WithIssuer(v.issuer),
	)
	if err != nil {
		return Claims{}, fmt.Errorf("authmw: invalid token: %w", err)
	}

	claims := Claims{Subject: token.Subject()}

	if v, ok := token.Get("preferred_username"); ok {
		claims.PreferredUsername, _ = v.(string)
	}
	if v, ok := token.Get("email"); ok {
		claims.Email, _ = v.(string)
	}
	if raw, ok := token.Get("realm_access"); ok {
		if m, ok := raw.(map[string]interface{}); ok {
			if roles, ok := m["roles"].([]interface{}); ok {
				for _, r := range roles {
					if s, ok := r.(string); ok {
						claims.RealmRoles = append(claims.RealmRoles, s)
					}
				}
			}
		}
	}

	return claims, nil
}

// Middleware returns an http middleware that requires a valid Bearer token
// on every request and injects the decoded Claims into the request context.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}

		claims, err := v.Verify(r.Context(), strings.TrimPrefix(header, prefix))
		if err != nil {
			http.Error(w, "invalid or expired token", http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), claimsCtxKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireRole wraps a handler chain so that only tokens carrying one of the
// given realm roles may proceed; everyone else gets 403. It must run after
// Middleware, which is responsible for populating the claims in context.
func RequireRole(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := FromContext(r.Context())
			if !ok {
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
				return
			}
			for _, role := range roles {
				if claims.HasRole(role) {
					next.ServeHTTP(w, r)
					return
				}
			}
			http.Error(w, "forbidden: missing required role", http.StatusForbidden)
		})
	}
}

// FromContext extracts the Claims injected by Middleware.
func FromContext(ctx context.Context) (Claims, bool) {
	claims, ok := ctx.Value(claimsCtxKey).(Claims)
	return claims, ok
}

// WithClaims injects claims into a context the way Middleware would. Pact
// provider verification uses it to replay interactions under a known
// identity without standing up Keycloak.
func WithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsCtxKey, claims)
}

// ErrNoClaims is returned by helpers that expect Middleware to have already
// populated the request context.
var ErrNoClaims = errors.New("authmw: no claims in context")
